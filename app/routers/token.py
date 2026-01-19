from fastapi import APIRouter, Depends, HTTPException, Query
from sqlalchemy.orm import Session
from app.core.database import get_db
from app.core.deps import get_current_user
from app.models.device import Device
from app.models.token_transaction import TokenTransaction
from app.schemas.token import TokenTopUp, TokenTransactionListResponse
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

@router.get("/transactions/{device_id}", response_model=TokenTransactionListResponse)
def list_token_transactions(
    device_id: int,
    page: int = Query(1, ge=1),
    limit: int = Query(10, ge=1, le=100),
    db: Session = Depends(get_db),
    user_id: int = Depends(get_current_user)
):  
    query = db.query(TokenTransaction).filter(
        TokenTransaction.user_id == user_id,
        TokenTransaction.device_id == device_id
    ).order_by(TokenTransaction.created_at.desc())

    total = query.count()
    offset = (page - 1) * limit
    transactions = query.offset(offset).limit(limit).all()

    total_pages = (total + limit - 1) // limit if limit > 0 else 0

    return {
        "code": 200,
        "message": "Token transactions retrieved",
        "data_length": len(transactions),
        "total_data": total,
        "total_pages": total_pages,
        "current_page": page,
        "data_per_page": limit,
        "data": transactions
    }