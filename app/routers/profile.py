from fastapi import APIRouter, Depends, HTTPException, status
from sqlalchemy.orm import Session
from app.core.database import get_db
from app.core.deps import get_current_user
from app.models.user import User
from app.schemas.user import UserResponse, UserUpdate, ChangePassword
from app.core.security import verify_password, hash_password
from app.schemas.response import ApiResponse

router = APIRouter(
    prefix="/api/profile",
    tags=["Profile"],
    dependencies=[Depends(get_current_user)]
)

@router.get("", response_model=ApiResponse[UserResponse])
def get_profile(
    db: Session = Depends(get_db),
    user_id: int = Depends(get_current_user)
):
    user = db.query(User).filter(User.id == user_id).first()
    if not user:
        raise HTTPException(status_code=404, detail="User not found")
    
    return ApiResponse(
        code=200,
        message="Profile retrieved successfully",
        data=user
    )

@router.put("", response_model=ApiResponse[UserResponse])
def update_profile(
    profile_data: UserUpdate,
    db: Session = Depends(get_db),
    user_id: int = Depends(get_current_user)
):
    user = db.query(User).filter(User.id == user_id).first()
    if not user:
        raise HTTPException(status_code=404, detail="User not found")
    
    # Check if email is taken by another user
    existing_email = db.query(User).filter(User.email == profile_data.email, User.id != user_id).first()
    if existing_email:
        raise HTTPException(status_code=400, detail="Email already registered")
        
    user.full_name = profile_data.full_name
    user.email = profile_data.email
    db.commit()
    db.refresh(user)
    
    return ApiResponse(
        code=200,
        message="Profile updated successfully",
        data=user
    )

@router.put("/password", response_model=ApiResponse)
def change_password(
    password_data: ChangePassword,
    db: Session = Depends(get_db),
    user_id: int = Depends(get_current_user)
):
    user = db.query(User).filter(User.id == user_id).first()
    if not user:
        raise HTTPException(status_code=404, detail="User not found")
    
    if not verify_password(password_data.old_password, user.password):
        raise HTTPException(status_code=400, detail="Incorrect old password")
        
    user.password = hash_password(password_data.new_password)
    db.commit()
    
    return ApiResponse(
        code=200,
        message="Password updated successfully",
        data=None
    )
