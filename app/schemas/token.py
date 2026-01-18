from pydantic import BaseModel
from decimal import Decimal
from typing import Optional, List

class TokenTopUp(BaseModel):
    device_id: int
    amount_kwh: Decimal
    price: Decimal

class TokenTransactionResponse(BaseModel):
    id: int
    device_id: int
    amount_kwh: Decimal
    price: Decimal
    created_at: str

class TokenTransactionListResponse(BaseModel):
    code: int
    message: str
    data_length: Optional[int] = None
    total_data: Optional[int] = None
    total_pages: Optional[int] = None
    current_page: Optional[int] = None
    data_per_page: Optional[int] = None
    data: Optional[List[TokenTransactionResponse]] = None
