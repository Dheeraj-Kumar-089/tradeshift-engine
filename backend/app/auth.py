from fastapi import APIRouter, Depends, HTTPException, status, Response, BackgroundTasks
from sqlalchemy.orm import Session
from .models import User
from .database import get_db
from .schemas import UserCreate, UserLogin, Token, User as UserSchema
import bcrypt
from datetime import datetime, timedelta
import jwt
import os
from .config import SECRET_KEY, ALGORITHM, ACCESS_TOKEN_EXPIRE_MINUTES

router = APIRouter(prefix="/auth", tags=["auth"])

# --- AUTH LOGIC ---

def verify_password(plain_password, hashed_password):
    if isinstance(hashed_password, str):
        hashed_password = hashed_password.encode('utf-8')
    return bcrypt.checkpw(plain_password.encode('utf-8'), hashed_password)

def get_password_hash(password):
    pwd_bytes = password.encode('utf-8')
    salt = bcrypt.gensalt()
    return bcrypt.hashpw(pwd_bytes, salt).decode('utf-8')

def create_access_token(data: dict, expires_delta: timedelta | None = None):
    to_encode = data.copy()
    expire = datetime.utcnow() + (expires_delta or timedelta(minutes=15))
    to_encode.update({"exp": expire})
    encoded_jwt = jwt.encode(to_encode, SECRET_KEY, algorithm=ALGORITHM)
    return encoded_jwt

@router.post("/register", response_model=UserSchema)
async def register(
    user: UserCreate, 
    response: Response, 
    background_tasks: BackgroundTasks,
    db: Session = Depends(get_db)
):
    # 1. Check if user exists
    db_user = db.query(User).filter(User.email == user.email).first()
    if db_user:
        raise HTTPException(status_code=400, detail="Email already registered")
    
    # 2. Create User
    hashed_password = get_password_hash(user.password)
    new_user = User(
        email=user.email, 
        hashed_password=hashed_password, 
        full_name=user.full_name,
        country=user.country,
        investment_goals=user.investment_goals,
        risk_tolerance=user.risk_tolerance,
        preferred_industries=user.preferred_industries
    )
    db.add(new_user)
    db.commit()
    db.refresh(new_user)

    # 3. Auto-Login (Set Cookie)
    access_token_expires = timedelta(minutes=ACCESS_TOKEN_EXPIRE_MINUTES)
    access_token = create_access_token(
        data={"sub": new_user.email}, expires_delta=access_token_expires
    )
    
    response.set_cookie(
        key="access_token",
        value=access_token,
        httponly=True,   # Prevent XSS
        secure=False,    # Set to True in Production (HTTPS)
        samesite="lax",  # CSRF Protection
        max_age=ACCESS_TOKEN_EXPIRE_MINUTES * 60
    )

    # 4. Trigger Inngest Workflow
    import inngest
    from app.inngest.client import inngest_client
    
    try:
        await inngest_client.send(
            inngest.Event(
                name="app/user.created",
                data={
                    "email": new_user.email,
                    "firstName": new_user.full_name.split()[0] if new_user.full_name else "Trader",
                    "fullname": new_user.full_name,
                    "country": new_user.country,
                    "investment_goals": new_user.investment_goals,
                    "risk_tolerance": new_user.risk_tolerance,
                    "preferred_industries": new_user.preferred_industries
                }
            )
        )
        print(f"✅ Inngest event sent for {new_user.email}")
    except Exception as e:
        print(f"❌ Failed to send Inngest event: {e}")

    return new_user

@router.post("/login", response_model=UserSchema) # Return User schema, not just Token
def login(
    user: UserLogin, 
    response: Response, 
    db: Session = Depends(get_db)
):
    db_user = db.query(User).filter(User.email == user.email).first()
    if not db_user or not verify_password(user.password, db_user.hashed_password):
        raise HTTPException(status_code=status.HTTP_401_UNAUTHORIZED, detail="Incorrect email or password")
    
    access_token_expires = timedelta(minutes=ACCESS_TOKEN_EXPIRE_MINUTES)
    access_token = create_access_token(data={"sub": db_user.email}, expires_delta=access_token_expires)
    
    # Set HttpOnly Cookie
    response.set_cookie(
        key="access_token",
        value=access_token,
        httponly=True,
        secure=False, 
        samesite="lax",
        max_age=ACCESS_TOKEN_EXPIRE_MINUTES * 60
    )
    
    # Return User Profile for LocalStorage (Hybrid Approach)
    return db_user

@router.post("/logout")
def logout(response: Response):
    response.delete_cookie("access_token")
    return {"message": "Logged out successfully"}

# Import dependencies inside function/module to avoid circular import issues if placed at top
from .dependencies import get_current_user

@router.get("/me", response_model=UserSchema)
def read_users_me(current_user: User = Depends(get_current_user)):
    return current_user
