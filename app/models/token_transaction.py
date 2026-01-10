from sqlalchemy import Column, BigInteger, ForeignKey, DateTime, Numeric
from datetime import datetime
from app.models.user import Base

class TokenTransaction(Base):
    __tablename__ = "token_transactions"

    id = Column(BigInteger, primary_key=True)
    user_id = Column(BigInteger, ForeignKey("users.id"))
    device_id = Column(BigInteger, ForeignKey("devices.id"))
    amount_kwh = Column(Numeric(12,4))
    price = Column(Numeric(12,2))
    created_at = Column(DateTime, default=datetime.utcnow)
