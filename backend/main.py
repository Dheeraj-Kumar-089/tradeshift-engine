# File: backend/main.py

from fastapi import FastAPI, HTTPException, WebSocket, WebSocketDisconnect
from fastapi.middleware.cors import CORSMiddleware
import uvicorn
from sqlalchemy import create_engine, text
import pandas as pd
from minio import Minio
import io
import os
import json
import asyncio
import datetime
import glob
from redis import Redis
from prometheus_fastapi_instrumentator import Instrumentator
from app.oms import OrderManager
from app import auth
from app.routers import inngest
from app.models import User  # Models must be imported for Base to detect them
from app.database import Base, connect_to_database

# --- DB INITIALIZATION ---
# Connect and Create Tables using the cached connection pattern
try:
    engine = connect_to_database()
    Base.metadata.create_all(bind=engine)
    print("✅ Database Tables Created/Verified")
except Exception as e:
    print(f"❌ Database Initialization Failed: {e}")


# --- 1. ROBUST IMPORT FOR SIMULATION ---
try:
    from app.simulation import TickSynthesizer
    print("✅ Brownian Bridge Engine Loaded")
except ImportError:
    print("⚠️ Warning: simulation.py not found. Using Mock Fallback.")
    class TickSynthesizer:
        def generate_ticks(self, o, h, l, c, num_ticks=60):
            return [o] * num_ticks

app = FastAPI()

# Instrumentator (Monitoring)
Instrumentator().instrument(app).expose(app)

# Global Exception Handler for debugging 500s
from fastapi import Request
from fastapi.responses import JSONResponse
import traceback

@app.exception_handler(Exception)
async def global_exception_handler(request: Request, exc: Exception):
    error_msg = f"🔥 UNHANDLED EXCEPTION: {str(exc)}\n{traceback.format_exc()}"
    print(error_msg)
    return JSONResponse(
        status_code=500,
        content={"message": "Internal Server Error", "detail": str(exc)},
    )

# --- 2. SECURITY (CORS) ---
app.add_middleware(
    CORSMiddleware,
    allow_origins=["http://localhost:5173", "http://127.0.0.1:5173"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

app.include_router(auth.router)
app.include_router(inngest.router)

# --- 3. INFRASTRUCTURE CONNECTIONS ---


# --- 3. INFRASTRUCTURE CONNECTIONS ---
# Database connection is now handled by app.database module (above)

try:
    minio_client = Minio("minio:9000", "minioadmin", "minioadmin", secure=False)
except Exception:
    pass

try:
    redis_client = Redis(host='tradeshift_redis', port=6379, decode_responses=True)
except Exception:
    print("⚠️ Redis not connected")

# --- 4. HELPER FUNCTION: Load Parquet by Symbol ---
def load_parquet_for_symbol(symbol: str, target_date: str = None):
    """
    Load Parquet file for a given symbol from the data/ directory.
    
    Args:
        symbol (str): Symbol name (e.g., 'NIFTY', 'BANKNIFTY', 'NIFTY_50')
        target_date (str): Optional date string (e.g., '2026-03-02') to load specific date file.
        
    Returns:
        tuple: (DataFrame, file_path, symbol_name)
        
    Raises:
        FileNotFoundError: If no matching Parquet file is found
    """
    matching_files = []
    # Try exact match first for date format: NIFTY_2026-03-02.parquet
    if target_date:
        pattern_date = f"data/{symbol}_{target_date}.parquet"
        date_match = glob.glob(pattern_date)
        if date_match:
            matching_files.extend(date_match)

    # Search for files matching the pattern (if no date or date file not found)
    if not matching_files:
        pattern = f"data/{symbol}_*.parquet"
        matching_files = glob.glob(pattern)
        
        # Try exact match (e.g., NIFTY_50_1min.parquet for symbol "NIFTY_50")
        pattern_exact = f"data/{symbol}.parquet"
        exact_match = glob.glob(pattern_exact)
        if exact_match:
            matching_files.extend(exact_match)
    
    if not matching_files:
        raise FileNotFoundError(f"No Parquet file found for symbol '{symbol}' (date: {target_date}) in data/ directory")
    
    # Sort files to get the most recent (if multiple exist)
    # Files are typically named: SYMBOL_YEAR.parquet or SYMBOL_suffix.parquet
    matching_files.sort(reverse=True)
    selected_file = matching_files[0]
    
    print(f"📂 Loading file: {selected_file}")
    
    # Load the Parquet file
    df = pd.read_parquet(selected_file)
    df.columns = df.columns.str.lower()
    
    # --- HANDLING SHOONYA API DATA FORMAT ---
    # Map columns: into->open, inth->high, intl->low, intc->close
    # Note: Handle duplicates - prefer 'intv' over 'v', 'intoi' over 'oi'
    column_map = {
        'into': 'open',
        'inth': 'high',
        'intl': 'low',
        'intc': 'close',
        'intv': 'volume',
        'intoi': 'oi'
    }
    
    # Drop the duplicate columns BEFORE renaming
    if 'v' in df.columns and 'intv' in df.columns:
        df = df.drop(columns=['v'])
    elif 'v' in df.columns:
        column_map['v'] = 'volume'
        
    if 'oi' in df.columns and 'intoi' in df.columns:
        df = df.drop(columns=['oi']) # Shoonya sometimes has both 'oi' and 'intoi'
    elif 'oi' in df.columns:
        column_map['oi'] = 'oi'  # Keep as is
    
    df = df.rename(columns=column_map)
    
    # Clean and parse timestamp
    if 'time' in df.columns:
        # Shoonya sometimes prefixes with "Ok " (e.g., "Ok 2026-02-13 12:12:00")
        df['time'] = df['time'].astype(str).str.replace('Ok ', '', regex=False).str.strip()
        df['datetime'] = pd.to_datetime(df['time'], format='%d-%m-%Y %H:%M:%S', errors='coerce')
        # Fallback for ISO format
        mask = df['datetime'].isnull()
        if mask.any():
            df.loc[mask, 'datetime'] = pd.to_datetime(df.loc[mask, 'time'], errors='coerce')
    
    # FORCE NUMERIC TYPES for OHLC (DO THIS BEFORE CHECKING FOR REQUIRED COLUMNS)
    # This fixes the "ufunc 'add' did not contain a loop..." error
    numeric_cols = ['open', 'high', 'low', 'close']
    for col in numeric_cols:
        if col in df.columns:
            df[col] = pd.to_numeric(df[col], errors='coerce')
            
    # Drop rows with NaNs in critical columns
    df = df.dropna(subset=[c for c in numeric_cols if c in df.columns] + (['datetime'] if 'datetime' in df.columns else []))
    
    # Sort by datetime (if datetime exists)
    if 'datetime' in df.columns:
        df = df.sort_values('datetime')
    
    # Ensure required columns exist
    required_cols = ['open', 'high', 'low', 'close', 'datetime']
    missing = [c for c in required_cols if c not in df.columns]
    if missing:
        print(f"⚠️ Warning: Missing columns in {selected_file}: {missing}")
    
    print(f"✅ Loaded {len(df)} rows. Columns: {list(df.columns)}")

    # Extract the actual symbol name from the filename for display
    # e.g., "data/NIFTY_2026.parquet" -> "NIFTY"
    # or "data/NIFTY_50_1min.parquet" -> "NIFTY_50"
    base_name = os.path.basename(selected_file).replace('.parquet', '')
    
    return df, selected_file, symbol

# --- 5. REST API ENDPOINTS ---

@app.get("/api/search")
async def search_instruments(query: str):
    """
    Search for instruments in the instruments_master table.
    
    Args:
        query (str): Search query string
        
    Returns:
        list: Top 10 matching instruments with symbol and token
    """
    if not query or len(query) < 1:
        return {"results": []}
    
    try:
        # Query the database with LIKE search (case-insensitive)
        search_query = text("""
            SELECT token, symbol, name, instrument_type
            FROM instruments_master
            WHERE LOWER(symbol) LIKE LOWER(:query)
               OR LOWER(name) LIKE LOWER(:query)
            ORDER BY symbol
            LIMIT 10
        """)
        
        with engine.connect() as conn:
            result = conn.execute(search_query, {"query": f"%{query}%"})
            rows = result.fetchall()
            
        # Format results
        instruments = [
            {
                "token": row[0],
                "symbol": row[1],
                "name": row[2] if len(row) > 2 else None,
                "instrument_type": row[3] if len(row) > 3 else None
            }
            for row in rows
        ]
        
        return {"results": instruments}
        
    except Exception as e:
        print(f"❌ Search error: {e}")
        raise HTTPException(status_code=500, detail=f"Search failed: {str(e)}")

@app.get("/api/available-symbols")
async def get_available_symbols():
    """
    Get list of symbols that have Parquet data files available.
    """
    print("🔍 API: /api/available-symbols called")
    try:
        parquet_files = glob.glob("data/*.parquet")
        print(f"📂 Found {len(parquet_files)} parquet files")
        symbols = []
        
        # Open connection ONCE, not in loop
        conn = None
        try:
            conn = engine.connect()
            print("✅ DB Connection established")
        except Exception as e:
            print(f"⚠️ DB Connection failed: {e}")

        for file_path in parquet_files:
            basename = os.path.basename(file_path).replace('.parquet', '')
            symbol = basename.replace('_1min', '').replace('_2026', '').replace('_2025', '')
            print(f"   Processing symbol: {symbol} from {basename}")
            
            name = symbol # Default
            token = "0"   # Default
            
            # Try to get token from database if connection exists
            if conn:
                try:
                    result = conn.execute(
                        text("SELECT token, symbol, name FROM instruments_master WHERE symbol = :sym OR symbol LIKE :pattern LIMIT 1"),
                        {"sym": symbol, "pattern": f"%{symbol}%"}
                    )
                    row = result.fetchone()
                    if row:
                        token = row[0]
                        name = row[2] if len(row) > 2 else symbol
                        print(f"      ✅ Found in DB: {name} ({token})")
                except Exception as db_err:
                    print(f"      ⚠️ DB Query failed for {symbol}: {db_err}")
            
            symbols.append({
                "token": token, 
                "symbol": symbol, 
                "name": name,
                "file": basename
            })
            
        if conn:
            conn.close()
            print("🔒 DB Connection closed")
        
        print(f"✅ Returning {len(symbols)} symbols")
        return {"symbols": symbols}
    except Exception as e:
        print(f"❌ Error getting available symbols: {e}")
        raise HTTPException(status_code=500, detail=f"Failed to get symbols: {str(e)}")

@app.get("/api/historical/{symbol}")
async def get_historical_candles(symbol: str, limit: int = 500, date: str = None):
    """
    Return historical OHLC candles for a given symbol from its Parquet file.
     Optionally filtered by a specific YYYY-MM-DD date.

    Args:
        symbol: Symbol name (e.g. 'NIFTY', 'BANKNIFTY')
        limit:  Max candles to return (default 500, most recent)

    Returns:
        JSON list of {time, open, high, low, close} objects
    """
    try:
        df, _, _ = load_parquet_for_symbol(symbol, date)

        # Resolve the datetime column
        time_col = next((c for c in ['datetime', 'date', 'time'] if c in df.columns), None)
        if time_col is None:
            raise HTTPException(status_code=422, detail="No datetime column found in data")

        # Only parse if it's not already datetime type
        if not pd.api.types.is_datetime64_any_dtype(df[time_col]):
            df[time_col] = pd.to_datetime(
                df[time_col].astype(str).str.replace('Ok ', '', regex=False).str.strip(), 
                dayfirst=False, errors='coerce'
            )
        
        df = df.dropna(subset=[time_col])
        df = df.sort_values(time_col)

        # Filter by date if provided
        if date:
            target_dt = pd.to_datetime(date).date()
            df = df[df[time_col].dt.date == target_dt]

        # Take the most recent `limit` candles
        df = df.tail(limit)

        candles = []
        for _, row in df.iterrows():
            ts = int(row[time_col].timestamp()) + 19800  # shift to IST
            candles.append({
                "time": ts,
                "open":  float(row["open"]),
                "high":  float(row["high"]),
                "low":   float(row["low"]),
                "close": float(row["close"]),
            })

        return {"symbol": symbol, "candles": candles}

    except FileNotFoundError as e:
        raise HTTPException(status_code=404, detail=str(e))
    except HTTPException:
        raise
    except Exception as e:
        print(f"❌ Error fetching historical data for {symbol}: {e}")
        raise HTTPException(status_code=500, detail=f"Failed to fetch history: {str(e)}")

# --- 6. WEBSOCKET ENDPOINT ---
@app.websocket("/ws/ticker")
async def websocket_endpoint(websocket: WebSocket):
    await websocket.accept()
    print("🟢 Client Connected")

    # State
    is_running      = False
    speed           = 1.0
    synthesizer     = TickSynthesizer()
    oms             = OrderManager()
    last_tick_price = 21500.0
    iterator        = None
    current_symbol  = "NIFTY"
    df_current      = None  # keep reference for restart

    try:
        while True:
            # ── A. Check for incoming commands (non-blocking) ──────────────
            try:
                data    = await asyncio.wait_for(websocket.receive_text(), timeout=0.001)
                message = json.loads(data)
                command = message.get("command")

                if command == "START":
                    symbol      = message.get("symbol") or "NIFTY"
                    target_date = message.get("date")
                    speed       = float(message.get("speed", 1.0))
                    current_symbol = symbol

                    # Load Parquet for the requested symbol
                    try:
                        df_current, file_path, _ = load_parquet_for_symbol(symbol, target_date)
                    except FileNotFoundError as e:
                        await websocket.send_json({"type": "ERROR", "message": str(e)})
                        print(f"❌ {e}")
                        continue
                    except Exception as e:
                        await websocket.send_json({"type": "ERROR", "message": f"Error loading data: {str(e)}"})
                        print(f"❌ Data loading error: {e}")
                        continue

                    print(f"✅ Loaded {len(df_current)} records from {file_path}")

                    # Find the date column
                    date_col = next(
                        (c for c in ['datetime', 'date', 'time'] if c in df_current.columns),
                        None
                    )
                    if not date_col:
                        await websocket.send_json({"type": "ERROR", "message": "Dataset has no date/time column"})
                        continue

                    # Convert date column and filter by target_date
                    temp_df = df_current.copy()
                    if not pd.api.types.is_datetime64_any_dtype(temp_df[date_col]):
                        temp_df[date_col] = pd.to_datetime(
                            temp_df[date_col].astype(str).str.replace('Ok ', '', regex=False).str.strip(), 
                            dayfirst=False, errors='coerce'
                        )

                    if not target_date:
                        first_date  = temp_df[date_col].dropna().min().date()
                        target_date = str(first_date)

                    target_dt  = pd.to_datetime(target_date).date()
                    mask       = (
                        (temp_df[date_col].dt.date == target_dt) &
                        (temp_df[date_col].dt.time >= datetime.time(9, 15)) &
                        (temp_df[date_col].dt.time <= datetime.time(15, 30))
                    )
                    filtered_df = temp_df[mask].sort_values(by=date_col)

                    if filtered_df.empty:
                        await websocket.send_json({"type": "ERROR", "message": f"No data found for date: {target_date} in {symbol}"})
                        continue

                    print(f"✅ Streaming {len(filtered_df)} candles for {symbol} on {target_date}")
                    # Use list-of-dicts — consistent with .get() row access below
                    iterator   = iter(filtered_df.to_dict(orient="records"))
                    is_running = True
                    print(f"▶️  Simulation Started | Symbol: {symbol} | Speed: {speed}x")

                elif command == "BUY":
                    oms.buy(last_tick_price, qty=50)

                elif command == "SELL":
                    oms.sell(last_tick_price, qty=50)

                elif command == "STOP":
                    is_running = False
                    print("⏹️ Simulation Stopped by client")

            except asyncio.TimeoutError:
                pass  # No command — continue streaming

            # ── B. Stream data (only when running) ────────────────────────
            if not is_running:
                await asyncio.sleep(0.05)
                continue

            # Get next candle row
            try:
                row = next(iterator)  # type: dict
            except StopIteration:
                print("🏁 End of data for this date — stopping.")
                await websocket.send_json({"type": "END", "message": "Data finished for date"})
                is_running = False
                continue

            # Extract OHLC
            try:
                open_p = float(row.get('open',  0))
                high   = float(row.get('high',  0))
                low    = float(row.get('low',   0))
                close  = float(row.get('close', 0))
                # Resolve timestamp from whichever date column exists
                date_val = (
                    row.get('datetime') or
                    row.get('date')     or
                    row.get('time')     or
                    datetime.datetime.now()
                )
                base_time = pd.to_datetime(date_val) if not isinstance(date_val, datetime.datetime) else date_val
            except Exception as e:
                print(f"❌ Row parse error: {e} | Row keys: {list(row.keys())}")
                continue

            # Generate micro-ticks (Brownian bridge across the minute)
            try:
                ticks = synthesizer.generate_ticks(open_p, high, low, close, num_ticks=60)
            except Exception as e:
                print(f"❌ Synthesizer Error: {e}")
                continue

            # Stream each tick
            for i, price in enumerate(ticks):
                last_tick_price = float(price)
                current_pnl     = oms.calculate_pnl(last_tick_price)
                tick_time       = base_time + datetime.timedelta(seconds=i)

                payload = {
                    "type": "TICK",
                    "data": {
                        "symbol":    current_symbol,
                        "price":     round(last_tick_price, 2),
                        "timestamp": tick_time.isoformat(),
                        "pnl":       round(current_pnl, 2),
                        "volume":    int(row.get('volume', 0)),
                    }
                }
                try:
                    await websocket.send_json(payload)
                except (WebSocketDisconnect, Exception) as e:
                    print(f"🔴 Send Error: {e}")
                    is_running = False
                    break

                await asyncio.sleep(1.0 / max(speed, 0.1))

            if not is_running:
                break

            # Send CANDLE at end of each minute
            try:
                await websocket.send_json({
                    "type": "CANDLE",
                    "data": {
                        "open":      open_p,
                        "high":      high,
                        "low":       low,
                        "close":     close,
                        "timestamp": base_time.isoformat(),
                        "symbol":    current_symbol,
                    }
                })
            except Exception:
                break

    except WebSocketDisconnect:
        print("🔴 Client Disconnected")
    except Exception as e:
        print(f"⚠️ Unhandled WS Error: {e}")
        import traceback
        traceback.print_exc()


if __name__ == "__main__":
    uvicorn.run("main:app", host="0.0.0.0", port=8000, reload=True)
