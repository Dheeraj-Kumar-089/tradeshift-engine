// @refresh reset
import React, { useState, useEffect, useCallback, createContext, useContext } from 'react';
import type { CandleData, Trade } from '../types';
import { marketDataService, fetchHistoricalCandles } from '../services/MarketDataService';
import { toast } from 'sonner';

interface GameState {
  isPlaying: boolean;
  speed: number;
  balance: number;
  currentPrice: number;
  currentCandle: CandleData | null;
  historicalCandles: CandleData[];
  trades: Trade[];
  theme: 'dark' | 'light';
  selectedSymbol: string;
  selectedDate: string;
  isLoadingHistory: boolean;
  togglePlay: () => void;
  toggleTheme: () => void;
  setSpeed: (s: number) => void;
  setSymbol: (symbol: string, token: string) => void;
  setDate: (dateStr: string) => void;
  placeOrder: (type: 'BUY' | 'SELL', qty: number) => void;
  closePosition: (tradeId: string) => void;
  resetSimulation: () => void;
}

export const GameContext = createContext<GameState | null>(null);

export const useGame = (): GameState => {
  const ctx = useContext(GameContext);
  if (!ctx) throw new Error('useGame must be used within a GameProvider');
  return ctx;
};

const DEFAULT_SYMBOL = 'NIFTY';

export const GameProvider: React.FC<{ children: React.ReactNode; }> = ({ children }) => {
  const [isPlaying, setIsPlaying] = useState(false);
  const [speed, setSpeed] = useState(1);
  const [balance, setBalance] = useState(100000);
  const [currentPrice, setCurrentPrice] = useState(21500);
  const [currentCandle, setCurrentCandle] = useState<CandleData | null>(null);
  const [historicalCandles, setHistoricalCandles] = useState<CandleData[]>([]);
  const [trades, setTrades] = useState<Trade[]>([]);
  const [theme, setTheme] = useState<'dark' | 'light'>('dark');
  const [selectedSymbol, setSelectedSymbol] = useState(DEFAULT_SYMBOL);
  const [isLoadingHistory, setIsLoadingHistory] = useState(false);

  // Initialize selected date: Today if past 3:30 PM, else Yesterday
  const [selectedDate, setSelectedDate] = useState(() => {
    const now = new Date();
    const hours = now.getHours();
    const mins = now.getMinutes();

    // If before 15:30
    if (hours < 15 || (hours === 15 && mins < 30)) {
      now.setDate(now.getDate() - 1);
    }

    // Get YYYY-MM-DD in local time
    const offset = now.getTimezoneOffset();
    const localDate = new Date(now.getTime() - (offset * 60 * 1000));
    return localDate.toISOString().split('T')[0];
  });

  // ── Load Historical Candles whenever symbol or date changes ──────────────────────
  const loadHistory = useCallback(async (symbol: string, date: string) => {
    setIsLoadingHistory(true);
    setHistoricalCandles([]);
    try {
      const candles = await fetchHistoricalCandles(symbol, 500, date);
      setHistoricalCandles(candles);
      if (candles.length > 0) {
        setCurrentPrice(candles[candles.length - 1].close);
        console.log(`📊 Loaded ${candles.length} historical candles for ${symbol}`);
      } else {
        toast.error(`No data available for ${symbol} on ${date}`);
      }
    } catch (err) {
      console.error('❌ Failed to load historical candles:', err);
      toast.error(`Data is not available for ${symbol} on ${date}`);
      setHistoricalCandles([]);
    } finally {
      setIsLoadingHistory(false);
    }
  }, []);

  // Load on mount (default symbol and date)
  useEffect(() => {
    loadHistory(DEFAULT_SYMBOL, selectedDate);
  }, [loadHistory, selectedDate]);

  // ── WebSocket streaming ───────────────────────────────────────────────────
  useEffect(() => {
    if (!isPlaying) {
      marketDataService.disconnect();
      return;
    }

    marketDataService.connect(speed, selectedSymbol, selectedDate);

    marketDataService.onMessage((payload: any) => {
      if (payload.type === 'CANDLE') {
        const d = payload.data;
        const rawTime = new Date(d.timestamp).getTime() / 1000;
        const timestamp = rawTime + 19800;

        const newCandle: CandleData = {
          time: timestamp,
          open: d.open,
          high: d.high,
          low: d.low,
          close: d.close,
        };
        setCurrentCandle(newCandle);
        setCurrentPrice(d.close);
      }

      if (payload.type === 'BATCH') {
        const batchData = payload.data;
        if (batchData?.length > 0) {
          const lastItem = batchData[batchData.length - 1];
          setCurrentPrice(lastItem.price);

          setCurrentCandle(prevCandle => {
            let newCandle = prevCandle ? { ...prevCandle } : null;

            batchData.forEach((tick: any) => {
              const rawTime = new Date(tick.timestamp).getTime() / 1000;
              const shiftedTime = rawTime + 19800;
              const candleTime = Math.floor(shiftedTime / 60) * 60;

              if (!newCandle || candleTime !== newCandle.time) {
                newCandle = {
                  time: candleTime,
                  open: tick.price,
                  high: tick.price,
                  low: tick.price,
                  close: tick.price,
                };
              } else {
                newCandle.high = Math.max(newCandle.high, tick.price);
                newCandle.low = Math.min(newCandle.low, tick.price);
                newCandle.close = tick.price;
              }
            });
            return newCandle;
          });
        }
      }

      if (payload.type === 'TICK') {
        const tickPrice = payload.data.price;
        const rawTime = new Date(payload.data.timestamp).getTime() / 1000;
        const shiftedTime = rawTime + 19800; // Shift to IST
        const candleTime = Math.floor(shiftedTime / 60) * 60;

        setCurrentPrice(tickPrice);

        setCurrentCandle(prevCandle => {
          if (!prevCandle || prevCandle.time !== candleTime) {
            // Start new candle
            return {
              time: candleTime,
              open: tickPrice,
              high: tickPrice,
              low: tickPrice,
              close: tickPrice,
            };
          } else {
            // Update current candle
            return {
              ...prevCandle,
              high: Math.max(prevCandle.high, tickPrice),
              low: Math.min(prevCandle.low, tickPrice),
              close: tickPrice,
            };
          }
        });
      }
    });

    return () => {
      marketDataService.disconnect();
    };
  }, [isPlaying, speed, selectedSymbol]);

  const togglePlay = () => setIsPlaying(prev => !prev);
  const toggleTheme = () => setTheme(prev => (prev === 'dark' ? 'light' : 'dark'));

  const setSymbol = (symbol: string, _token: string) => {
    setSelectedSymbol(symbol);
    loadHistory(symbol, selectedDate);
  };

  const setDate = (dateStr: string) => {
    setSelectedDate(dateStr);
    loadHistory(selectedSymbol, dateStr);
  };

  const placeOrder = (type: 'BUY' | 'SELL', quantity: number) => {
    const newTrade: Trade = {
      id: Math.random().toString(36).substr(2, 9),
      symbol: selectedSymbol,
      type,
      entryPrice: currentPrice,
      quantity,
      timestamp: new Date(currentCandle ? currentCandle.time * 1000 : Date.now()),
      status: 'OPEN',
    };
    setTrades(prev => [newTrade, ...prev]);
  };

  const closePosition = (tradeId: string) => {
    setTrades(prevTrades =>
      prevTrades.map(trade => {
        if (trade.id === tradeId && trade.status === 'OPEN') {
          const exitPrice = currentPrice;
          const multiplier = trade.type === 'BUY' ? 1 : -1;
          const pnl = (exitPrice - trade.entryPrice) * trade.quantity * multiplier;
          setBalance(prev => prev + pnl);
          return { ...trade, status: 'CLOSED', exitPrice, pnl };
        }
        return trade;
      }),
    );
  };

  const resetSimulation = () => {
    setIsPlaying(false);
    setBalance(100000);
    setTrades([]);
    setCurrentCandle(null);
  };

  return (
    <GameContext.Provider
      value={{
        isPlaying, speed, balance, currentPrice, currentCandle,
        historicalCandles, trades, theme, selectedSymbol, selectedDate, isLoadingHistory,
        togglePlay, toggleTheme, setSpeed, setSymbol, setDate,
        placeOrder, closePosition, resetSimulation,
      }}
    >
      {children}
    </GameContext.Provider>
  );
};