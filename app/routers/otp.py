from datetime import datetime
from fastapi import APIRouter, Depends, HTTPException
from sqlalchemy.orm import Session
from app.core.database import get_db
from app.core.mail import fm
from fastapi_mail import MessageSchema
from app.models.otp import EmailOTP
from app.utils.otp import generate_otp, otp_expiry

router = APIRouter(prefix="/auth")

@router.post("/send-otp")
async def send_otp(email: str, db: Session = Depends(get_db)):
    otp = generate_otp()

    db.add(EmailOTP(
        email=email,
        otp_code=otp,
        expires_at=otp_expiry()
    ))
    db.commit()

    message = MessageSchema(
        subject="Your SIWATT OTP Code",
        recipients=[email],
        body=f"Your OTP is {otp}. It expires in 5 minutes.",
        subtype="plain"
    )

    await fm.send_message(message)

    return {"message": "OTP sent"}

@router.post("/verify-otp")
def verify_otp(email: str, otp: str, db: Session = Depends(get_db)):
    record = db.query(EmailOTP).filter(
        EmailOTP.email == email,
        EmailOTP.otp_code == otp,
        EmailOTP.is_used == False
    ).first()

    if not record or record.expires_at < datetime.utcnow():
        raise HTTPException(status_code=400, detail="Invalid or expired OTP")

    record.is_used = True
    db.commit()

    return {"message": "OTP verified"}
