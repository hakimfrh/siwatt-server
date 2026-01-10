from sqlalchemy import Column, BigInteger, Numeric, String, Boolean, DateTime, Integer, ForeignKey
from datetime import datetime
from app.models.user import Base

class Device(Base):
    __tablename__ = "devices"

    id = Column(BigInteger, primary_key=True)
    user_id = Column(BigInteger, ForeignKey("users.id"))
    device_code = Column(String(100), unique=True, nullable=False)
    device_name = Column(String(100))
    location = Column(String(100))
    token_balance = Column(Numeric(12, 2), default=0.0)	
    is_active = Column(Boolean, default=True)
    up_time = Column(Integer, default=0)
    last_online = Column(DateTime)
    created_at = Column(DateTime, default=datetime.utcnow)
