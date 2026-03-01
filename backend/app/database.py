
import os
import time
from sqlalchemy import create_engine
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import sessionmaker
import logging

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# Global Cache for Singleton Pattern
# structure: { "conn": Engine | None, "session_maker": SessionLocal | None }
_db_cache = {
    "engine": None,
    "session_maker": None
}

Base = declarative_base()

def get_database_url():
    # Default to localhost for local dev, 'db' for docker
    default_url = "postgresql://user:password@localhost:5432/tradeshift_db" 
    return os.getenv("DATABASE_URL", default_url)

def connect_to_database():
    """
    Implements the cached connection pattern.
    Returns the SQLAlchemy Engine.
    """
    global _db_cache
    
    # 1. Check cache (Return cached conn)
    if _db_cache["engine"]:
        return _db_cache["engine"]
    
    # 2. Establish Connection (The "Promise" / Initialization phase)
    logger.info("🔄 Establishing new database connection...")
    try:
        db_url = get_database_url()
        logger.info(f"🔌 Connecting to: {db_url}")
        # Create Engine
        engine = create_engine(db_url, pool_pre_ping=True)
        
        # Test Connection (Simulating "await promise")
        with engine.connect() as connection:
             logger.info("✅ Database connected successfully.")
        
        # Cache the connection
        _db_cache["engine"] = engine
        
        # Configure SessionMaker
        _db_cache["session_maker"] = sessionmaker(autocommit=False, autoflush=False, bind=engine)
        
        return engine
        
    except Exception as e:
        logger.error(f"❌ Database connection failed: {e}")
        _db_cache["engine"] = None
        _db_cache["session_maker"] = None
        raise e

def get_db():
    """
    FastAPI Dependency to get DB session.
    """
    # Ensure connection exists
    if not _db_cache["session_maker"]:
        connect_to_database()
    
    SessionLocal = _db_cache["session_maker"]
    db = SessionLocal()
    try:
        yield db
    finally:
        db.close()

def get_session():
    """
    Returns a new SQLAlchemy Session.
    Used for non-FastAPI contexts (like OMS background tasks).
    """
    if not _db_cache["session_maker"]:
        connect_to_database()
    return _db_cache["session_maker"]()
