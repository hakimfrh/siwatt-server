from fastapi import APIRouter, Depends, HTTPException
from sqlalchemy.orm import Session
from app.core.database import get_db
from app.core.deps import get_current_user
from app.models.device import Device
from app.models.token_transaction import TokenTransaction
from app.schemas.token import TokenTopUp
from app.schemas.response import ApiResponse

router = APIRouter(
    prefix="/api/tokens",
    tags=["Token"],
    dependencies=[Depends(get_current_user)]
)

@router.post("/transactions")
def topup_token(
    data: TokenTopUp,
    db: Session = Depends(get_db),
    user_id: int = Depends(get_current_user)
):
    device = db.query(Device).filter(
        Device.id == data.device_id,
        Device.user_id == user_id
    ).first()

    if not device:
        raise HTTPException(status_code=404, detail="Device not found")

    # 1️⃣ insert transaction
    trx = TokenTransaction(
        user_id=user_id,
        device_id=device.id,
        amount_kwh=data.amount_kwh,
        price=data.price
    )
    db.add(trx)

    # 2️⃣ update saldo
    device.token_balance += data.amount_kwh

    db.commit()
    db.refresh(trx)

    return {
        "code": 200,
        "message": "Token added",
        "data": {
            "transaction_id": trx.id,
            "device_id": device.id,
            "new_balance": device.token_balance
        }
    }

@router.get("/transactions/{device_id}")
def list_token_transactions(
    device_id: int,
    db: Session = Depends(get_db),
    user_id: int = Depends(get_current_user)
):  
    transactions = db.query(TokenTransaction).filter(
        TokenTransaction.user_id == user_id,
        TokenTransaction.device_id == device_id
    ).all()

    return {
        "code": 200,
        "message": "Token transactions retrieved",
        "data": transactions
    }