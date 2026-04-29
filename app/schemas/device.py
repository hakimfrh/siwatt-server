from datetime import datetime
from pydantic import BaseModel
from typing import Optional, List

from app.schemas.token import TokenPriceResponse

class DeviceCreate(BaseModel):
    device_code: str
    device_name: str
    location: str
    price_id: Optional[int] = None
    effective_tariff: Optional[float] = None

class DeviceUpdate(BaseModel):
    device_name: Optional[str] = None
    location: Optional[str] = None
    price_id: Optional[int] = None
    effective_tariff: Optional[float] = None

class DeviceDeleteRequest(BaseModel):
    password: str

class DeviceResponse(BaseModel):
    id: int
    device_code: str
    device_name: str
    location: str
    price_id: Optional[int] = None
    effective_tariff: Optional[float] = None
    token_balance: float
    is_active: bool
    up_time: int
    last_online: Optional[datetime] = None
    created_at: datetime
    token_price: Optional[TokenPriceResponse] = None
    
    class Config:
        from_attributes = True

class DeviceListResponse(BaseModel):
    code: int
    message: str
    data_length: Optional[int] = None
    total_data: Optional[int] = None
    total_pages: Optional[int] = None
    current_page: Optional[int] = None
    data_per_page: Optional[int] = None
    data: Optional[List[DeviceResponse]] = None
