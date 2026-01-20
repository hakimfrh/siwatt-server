from sqlalchemy import Column, BigInteger, ForeignKey, DateTime, Numeric, Enum
from datetime import datetime
from app.models.user import Base

class TokenTransaction(Base):
    __tablename__ = "token_transactions"

    id = Column(BigInteger, primary_key=True)
    user_id = Column(BigInteger, ForeignKey("users.id"))
    device_id = Column(BigInteger, ForeignKey("devices.id"))
    type = Column(Enum('topup', 'correction', name='transaction_type_enum'), default='topup')
    amount_kwh = Column(Numeric(12,4))
    price = Column(Numeric(12,2))
    current_balance = Column(Numeric(12,4))
    final_balance = Column(Numeric(12,4))
    created_at = Column(DateTime, default=datetime.utcnow)
