from sqlalchemy import Column, Integer, String, Float, DateTime
from datetime import datetime
from app.database import Base  # Import Base from database.py

# Removed: engine, SessionLocal, Base definition (now in database.py)

class TradeLog(Base):
    """
    Model representing a record of a completed trade.
    """
    __tablename__ = "trade_logs"

    id = Column(Integer, primary_key=True, index=True)

    symbol = Column(String, index=True)
    direction = Column(String)

    entry_price = Column(Float)
    exit_price = Column(Float)
    quantity = Column(Integer)
    pnl = Column(Float)

    entry_time = Column(DateTime, default=datetime.utcnow)
    exit_time = Column(DateTime, default=datetime.utcnow)

    # 🔥 NEW BEHAVIOR FIELDS (INSIDE CLASS)

    session_id = Column(String, index=True)

    holding_time = Column(Float)
    trade_number = Column(Integer)

    stop_loss = Column(Float, nullable=True)
    take_profit = Column(Float, nullable=True)

    exit_reason = Column(String, nullable=True)

    time_since_last_trade = Column(Float, nullable=True)


class User(Base):
    __tablename__ = "users"

    id = Column(Integer, primary_key=True, index=True)
    email = Column(String, unique=True, index=True)
    hashed_password = Column(String)
    full_name = Column(String, nullable=True)
    country = Column(String, nullable=True)
    investment_goals = Column(String, nullable=True)  # e.g., "Growth", "Value", "Day Trading"
    risk_tolerance = Column(String, nullable=True)    # e.g., "Low", "Medium", "High"
    preferred_industries = Column(String, nullable=True) # e.g., "Technology", "Healthcare"
    created_at = Column(DateTime, default=datetime.utcnow)
