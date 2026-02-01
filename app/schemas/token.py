from pydantic import BaseModel
from decimal import Decimal
from typing import Optional, List
from datetime import datetime

class TokenTopUp(BaseModel):
    device_id: int
    amount_kwh: Decimal
    price: Decimal

class TokenCorrection(BaseModel):
    device_id: int
    current_balance: Optional[Decimal] = None
    final_balance: Decimal

class TokenTransactionResponse(BaseModel):
    id: int
    device_id: int
    type: Optional[str] = 'topup'
    amount_kwh: float
    price: float
    current_balance: Optional[float] = None
    final_balance: Optional[float] = None
    created_at: datetime

class TokenTransactionListResponse(BaseModel):
    code: int
    message: str
    data_length: Optional[int] = None
    total_data: Optional[int] = None
    total_pages: Optional[int] = None
    current_page: Optional[int] = None
    data_per_page: Optional[int] = None
    total_token_bought: Optional[float] = None
    total_price: Optional[float] = None
    data: Optional[List[TokenTransactionResponse]] = None

class TokenBalanceGraphPoint(BaseModel):
    datetime: datetime
    usage: float
    topup: float
    balance: float
    type: str
    final_balance: float

class TokenBalanceGraphResponse(BaseModel):
    code: int
    message: str
    data: List[TokenBalanceGraphPoint]
