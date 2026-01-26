from fastapi import APIRouter, Depends, HTTPException
from sqlalchemy.orm import Session
from app.core.security import hash_password, verify_password
from app.core.database import get_db
from app.models.user import User
from app.schemas.user import UserRegister, UserLogin
from app.schemas.response import ApiResponse
from app.schemas.auth import LoginData, RegisterData
from app.core.security import create_access_token
from app.core.deps import get_current_user, get_current_user_refresh

router = APIRouter(prefix="/auth", tags=["Auth"])

@router.post("/register", response_model=ApiResponse[RegisterData])
def register(data: UserRegister, db: Session = Depends(get_db)):
    if db.query(User).filter(User.username == data.username).first():
        raise HTTPException(status_code=400, detail="Username already exists")

    if db.query(User).filter(User.email == data.email).first():
        raise HTTPException(status_code=400, detail="Email already exists")

    user = User(
        username=data.username,
        email=data.email,
        password=hash_password(data.password),
        full_name=data.full_name
    )

    db.add(user)
    db.commit()
    db.refresh(user)

    return {
        "code": 200,
        "message": "Register successfully",
        "data": {
            "user": user,
            "api_key": None
        }
    }

@router.post("/login", response_model=ApiResponse[LoginData])
def login(data: UserLogin, db: Session = Depends(get_db)):
    user = db.query(User).filter(User.email == data.email).first()
    if not user or not verify_password(data.password, user.password):
        raise HTTPException(status_code=400, detail="Invalid credentials")

    token = create_access_token(user.id)

    return {
        "code": 200,
        "message": "Login successfully",
        "data": {
            "user": user,
            "api_token": token
        }
    }

@router.post("/refresh", response_model=ApiResponse[LoginData])
def refresh_token(user_id: int = Depends(get_current_user_refresh), db: Session = Depends(get_db)):
    user = db.query(User).filter(User.id == user_id).first()
    if not user:
        raise HTTPException(status_code=404, detail="User not found")

    token = create_access_token(user.id)

    return {
        "code": 200,
        "message": "Token refreshed successfully",
        "data": {
            "user": user,
            "api_token": token
        }
    }
