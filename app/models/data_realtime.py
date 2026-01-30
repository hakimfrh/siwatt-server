from sqlalchemy import Column, BigInteger, Float, DateTime, ForeignKey
from app.models.user import Base # Assuming Base is here based on other files
# Wait, checking other models imports.
# app/models/device.py: from app.models.user import Base
# app/models/data_hourly.py: from app.models.user import Base

class DataRealtime(Base):
    __tablename__ = "data_realtime"

    id = Column(BigInteger, primary_key=True)
    device_id = Column(BigInteger, ForeignKey("devices.id"), unique=True) # It seems 1-to-1 often for realtime status of device? schema image shows data_realtime has device_id. The repo upsert logic suggests one row per device.
    
    voltage = Column(Float)
    current = Column(Float)
    power = Column(Float)
    energy = Column(Float)
    frequency = Column(Float)
    pf = Column(Float)
    updated_at = Column(DateTime)
