from pydantic import BaseModel
from datetime import datetime
from typing import Optional, List
from app.schemas.response import ApiResponse

class DataHourlyBase(BaseModel):
    datetime: datetime
    voltage: Optional[float] = 0.0
    current: Optional[float] = 0.0
    power: Optional[float] = 0.0
    energy: Optional[float] = 0.0
    frequency: Optional[float] = 0.0
    pf: Optional[float] = 0.0
    energy_hour: Optional[float] = 0.0

    class Config:
        from_attributes = True

class DataHourlyResponse(DataHourlyBase):
    id: int
    device_id: int

class DataHourlyListResponse(BaseModel):
    code: int
    message: str
    data_length: Optional[int] = None
    avg_voltage: Optional[float] = None
    avg_current: Optional[float] = None
    avg_power: Optional[float] = None
    avg_energy: Optional[float] = None
    avg_frequency: Optional[float] = None
    avg_pf: Optional[float] = None
    data: Optional[List[DataHourlyResponse]] = None



