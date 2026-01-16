from pydantic import BaseModel
from typing import Optional

class AverageDataResponse(BaseModel):
    code: int
    message: str
    avg_voltage: float
    avg_current: float
    avg_power: float
    avg_energy: float
    avg_frequency: float
    avg_pf: float
