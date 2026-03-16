from datetime import datetime
from fastapi import APIRouter, Depends, HTTPException
from sqlalchemy.orm import Session

from app.core.database import get_db
from app.core.deps import get_current_user
from app.core.mailjet import mailjet, MAILJET_SENDER_EMAIL, MAILJET_SENDER_NAME
from app.models.otp import EmailOTP
from app.models.user import User
from app.schemas.response import ApiResponse
from app.schemas.otp import (
    SendOtpRequest,
    VerifyOtpRequest,
    ResetPasswordRequest,
    SendOtpData,
    VerifyOtpData,
    MailjetMessageDetail,
)
from app.utils.otp import generate_otp, otp_expiry, build_otp_html
from app.core.security import hash_password

router = APIRouter(prefix="/auth", tags=["OTP"])


def _send_otp_email(user: User, db: Session):
    """Helper: buat OTP, simpan ke DB, dan kirim via Mailjet. Return otp_record & mailjet_detail."""
    otp_code = generate_otp()
    expires_at = otp_expiry()

    otp_record = EmailOTP(
        user_id=user.id,
        email=user.email,
        otp_code=otp_code,
        expires_at=expires_at,
    )
    db.add(otp_record)
    db.commit()
    db.refresh(otp_record)

    html_content = build_otp_html(otp_code)
    data = {
        "Messages": [
            {
                "From": {"Email": MAILJET_SENDER_EMAIL, "Name": MAILJET_SENDER_NAME},
                "To": [{"Email": user.email}],
                "Subject": "Kode OTP SIWATT",
                "TextPart": f"Kode OTP kamu adalah {otp_code}",
                "HTMLPart": html_content,
            }
        ]
    }

    result = mailjet.send.create(data=data)

    if result.status_code != 200:
        db.delete(otp_record)
        db.commit()
        raise HTTPException(
            status_code=500,
            detail={
                "message": "Gagal mengirim email via Mailjet",
                "mailjet_error": result.json(),
            },
        )

    mj_messages = result.json().get("Messages", [{}])
    mj_first = mj_messages[0] if mj_messages else {}
    mj_to = mj_first.get("To", [{}])
    mj_to_first = mj_to[0] if mj_to else {}

    mailjet_detail = MailjetMessageDetail(
        status=mj_first.get("Status", "unknown"),
        message_id=mj_to_first.get("MessageID"),
        message_uuid=mj_to_first.get("MessageUUID"),
        message_href=mj_to_first.get("MessageHref"),
    )

    return otp_record, expires_at, mailjet_detail


# ════════════════════════════════════════════════════════
# LUPA PASSWORD (tanpa JWT — email sebagai identitas)
# ════════════════════════════════════════════════════════

# ── POST /auth/forgot-password/send-otp ─────────────
@router.post("/forgot-password/send-otp", response_model=ApiResponse[SendOtpData])
def forgot_send_otp(
    body: SendOtpRequest,
    db: Session = Depends(get_db),
):
    user = db.query(User).filter(User.email == body.email).first()
    if not user:
        raise HTTPException(status_code=404, detail="Email tidak terdaftar")

    active_otp = (
        db.query(EmailOTP)
        .filter(
            EmailOTP.email == body.email,
            EmailOTP.is_used == False,
            EmailOTP.expires_at > datetime.now(),
        )
        .first()
    )
    if active_otp:
        raise HTTPException(
            status_code=429,
            detail="OTP sebelumnya masih berlaku. Silakan tunggu hingga kadaluarsa.",
        )

    otp_record, expires_at, mailjet_detail = _send_otp_email(user, db)

    return {
        "code": 200,
        "message": "OTP berhasil dikirim",
        "data": {
            "otp_id": otp_record.id,
            "email": user.email,
            "expires_at": expires_at,
            "mailjet": mailjet_detail,
        },
    }


# ── POST /auth/forgot-password/verify-otp ───────────
@router.post("/forgot-password/verify-otp", response_model=ApiResponse[VerifyOtpData])
def forgot_verify_otp(
    body: VerifyOtpRequest,
    db: Session = Depends(get_db),
):
    user = db.query(User).filter(User.email == body.email).first()
    if not user:
        raise HTTPException(status_code=404, detail="Email tidak terdaftar")

    record = (
        db.query(EmailOTP)
        .filter(
            EmailOTP.id == body.otp_id,
            EmailOTP.otp_code == body.otp_code,
            EmailOTP.user_id == user.id,
            EmailOTP.is_used == False,
        )
        .first()
    )

    if not record:
        raise HTTPException(status_code=400, detail="OTP tidak ditemukan atau sudah digunakan")

    if record.expires_at < datetime.now():
        raise HTTPException(status_code=400, detail="OTP sudah kadaluarsa")

    # is_used TIDAK diubah — hanya pengecekan
    return {
        "code": 200,
        "message": "OTP valid",
        "data": {
            "otp_id": record.id,
            "is_valid": True,
            "expiration_time": record.expires_at,
        },
    }


# ── POST /auth/forgot-password/reset-password ───────
@router.post("/forgot-password/reset-password", response_model=ApiResponse)
def forgot_reset_password(
    body: ResetPasswordRequest,
    db: Session = Depends(get_db),
):
    user = db.query(User).filter(User.email == body.email).first()
    if not user:
        raise HTTPException(status_code=404, detail="Email tidak terdaftar")

    record = (
        db.query(EmailOTP)
        .filter(
            EmailOTP.id == body.otp_id,
            EmailOTP.otp_code == body.otp_code,
            EmailOTP.user_id == user.id,
            EmailOTP.is_used == False,
        )
        .first()
    )

    if not record:
        raise HTTPException(status_code=400, detail="OTP tidak ditemukan atau sudah digunakan")

    if record.expires_at < datetime.now():
        raise HTTPException(status_code=400, detail="OTP sudah kadaluarsa")

    record.is_used = True
    user.password = hash_password(body.new_password)
    db.commit()

    return {
        "code": 200,
        "message": "Password berhasil direset",
        "data": None,
    }


# ════════════════════════════════════════════════════════
# OTP UMUM (butuh JWT — untuk fitur lain yang sudah login)
# ════════════════════════════════════════════════════════

# ── POST /auth/send-otp ─────────────────────────────
@router.post("/send-otp", response_model=ApiResponse[SendOtpData])
def send_otp(
    db: Session = Depends(get_db),
    user_id: int = Depends(get_current_user),
):
    user = db.query(User).filter(User.id == user_id).first()
    if not user:
        raise HTTPException(status_code=404, detail="User tidak ditemukan")

    active_otp = (
        db.query(EmailOTP)
        .filter(
            EmailOTP.user_id == user.id,
            EmailOTP.is_used == False,
            EmailOTP.expires_at > datetime.now(),
        )
        .first()
    )
    if active_otp:
        raise HTTPException(
            status_code=429,
            detail="OTP sebelumnya masih berlaku. Silakan tunggu hingga kadaluarsa atau gunakan OTP yang sudah dikirim.",
        )

    otp_record, expires_at, mailjet_detail = _send_otp_email(user, db)

    return {
        "code": 200,
        "message": "OTP berhasil dikirim",
        "data": {
            "otp_id": otp_record.id,
            "email": user.email,
            "expires_at": expires_at,
            "mailjet": mailjet_detail,
        },
    }


# ── POST /auth/verify-otp ───────────────────────────
@router.post("/verify-otp", response_model=ApiResponse[VerifyOtpData])
def verify_otp(
    body: VerifyOtpRequest,
    db: Session = Depends(get_db),
    user_id: int = Depends(get_current_user),
):
    record = (
        db.query(EmailOTP)
        .filter(
            EmailOTP.id == body.otp_id,
            EmailOTP.otp_code == body.otp_code,
            EmailOTP.user_id == user_id,
            EmailOTP.is_used == False,
        )
        .first()
    )

    if not record:
        raise HTTPException(status_code=400, detail="OTP tidak ditemukan atau sudah digunakan")

    if record.expires_at < datetime.now():
        raise HTTPException(status_code=400, detail="OTP sudah kadaluarsa")

    return {
        "code": 200,
        "message": "OTP valid",
        "data": {
            "otp_id": record.id,
            "is_valid": True,
            "expiration_time": record.expires_at,
        },
    }

# ── POST /auth/reset-password ───────────────────────
@router.post("/reset-password", response_model=ApiResponse)
def reset_password(
    body: ResetPasswordRequest,
    db: Session = Depends(get_db),
    user_id: int = Depends(get_current_user),
):
    record = (
        db.query(EmailOTP)
        .filter(
            EmailOTP.id == body.otp_id,
            EmailOTP.otp_code == body.otp_code,
            EmailOTP.user_id == user_id,
            EmailOTP.is_used == False,
        )
        .first()
    )

    if not record:
        raise HTTPException(status_code=400, detail="OTP tidak ditemukan atau sudah digunakan")

    if record.expires_at < datetime.now():
        raise HTTPException(status_code=400, detail="OTP sudah kadaluarsa")

    user = db.query(User).filter(User.id == user_id).first()
    if not user:
        raise HTTPException(status_code=404, detail="User tidak ditemukan")

    record.is_used = True
    user.password = hash_password(body.new_password)
    db.commit()

    return {
        "code": 200,
        "message": "Password berhasil direset",
        "data": None,
    }
