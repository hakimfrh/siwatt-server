from pydantic import BaseModel
from decimal import Decimal

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
