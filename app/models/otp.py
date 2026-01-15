from sqlalchemy import Column, BigInteger, String, DateTime, Boolean
from app.core.database import Base
from datetime import datetime

class EmailOTP(Base):
    __tablename__ = "email_otps"

    id = Column(BigInteger, primary_key=True, index=True)
    email = Column(String(100), nullable=False, index=True)
    otp_code = Column(String(6), nullable=False)
    expires_at = Column(DateTime, nullable=False)
    is_used = Column(Boolean, default=False)
    created_at = Column(DateTime, default=datetime.utcnow)
