from pydantic import BaseModel
from typing import Optional
from datetime import datetime


# ── Request ──────────────────────────────────────

class VerifyOtpRequest(BaseModel):
    otp_id: int
    otp_code: str


class ResetPasswordRequest(BaseModel):
    otp_id: int
    otp_code: str
    new_password: str


# ── Response data ────────────────────────────────

class MailjetMessageDetail(BaseModel):
    status: str
    message_id: Optional[int] = None
    message_uuid: Optional[str] = None
    message_href: Optional[str] = None

    class Config:
        from_attributes = True


class SendOtpData(BaseModel):
    otp_id: int
    email: str
    expires_at: datetime
    mailjet: MailjetMessageDetail

    class Config:
        from_attributes = True


class VerifyOtpData(BaseModel):
    otp_id: int
    is_valid: bool
    expiration_time: datetime

    class Config:
        from_attributes = True
