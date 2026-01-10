from pydantic import BaseModel

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
