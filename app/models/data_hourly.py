from sqlalchemy import Column, BigInteger, Integer, Float, DateTime, ForeignKey
from sqlalchemy.orm import relationship
from app.models.user import Base


class DataHourly(Base):
    __tablename__ = "data_hourly"

    id = Column(BigInteger, primary_key=True, index=True)
    device_id = Column(BigInteger, ForeignKey("devices.id"), nullable=False, index=True)

    datetime = Column(DateTime, nullable=False)

    voltage = Column(Float)
    current = Column(Float)
    power = Column(Float)
    energy = Column(Float)
    frequency = Column(Float)
    pf = Column(Float)

    energy_hour = Column(Float)

    # optional: relasi ke device
    device = relationship("Device", back_populates="data_hourly")
