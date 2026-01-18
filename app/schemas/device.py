from datetime import datetime
from pydantic import BaseModel
from typing import Optional, List

class DeviceCreate(BaseModel):
    device_code: str
    device_name: str
    location: str

class DeviceResponse(BaseModel):
    id: int
    device_code: str
    device_name: str
    location: str
    token_balance: float
    is_active: bool
    up_time: int
    last_online: Optional[datetime] = None
    created_at: datetime
    
class DeviceListResponse(BaseModel):
    code: int
    message: str
    data_length: Optional[int] = None
    total_data: Optional[int] = None
    total_pages: Optional[int] = None
    current_page: Optional[int] = None
    data_per_page: Optional[int] = None
    data: Optional[List[DeviceResponse]] = None
