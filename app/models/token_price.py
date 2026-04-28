from datetime import datetime

from sqlalchemy import Column, BigInteger, DateTime, Numeric, String

from app.models.user import Base


class TokenPrice(Base):
    __tablename__ = "token_price"

    id = Column(BigInteger, primary_key=True)
    code = Column(String(20))
    details = Column(String(100))
    price_per_kwh = Column(Numeric(10, 2))
    last_update = Column(DateTime, default=datetime.now)
