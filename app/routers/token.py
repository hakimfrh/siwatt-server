from fastapi import APIRouter, Depends, HTTPException, Query
from sqlalchemy.orm import Session
from sqlalchemy import func
from datetime import date, datetime, time, timedelta
from typing import Optional, List
from app.core.database import get_db
from app.core.deps import get_current_user
from app.models.device import Device
from app.models.data_hourly import DataHourly
from app.models.token_transaction import TokenTransaction
from app.schemas.token import TokenTopUp, TokenTransactionListResponse, TokenBalanceGraphResponse, TokenCorrection
from app.schemas.response import ApiResponse

router = APIRouter(
    prefix="/api/tokens",
    tags=["Token"],
    dependencies=[Depends(get_current_user)]
)

@router.post("/transactions")
def topup_token(
    data: TokenTopUp,
    device_id: Optional[int] = Query(None),
    db: Session = Depends(get_db),
    user_id: int = Depends(get_current_user)
):
    target_device_id = device_id if device_id else data.device_id
    
    device = db.query(Device).filter(
        Device.id == target_device_id,
        Device.user_id == user_id
    ).first()

    if not device:
        raise HTTPException(status_code=404, detail="Device not found")

    # Calculate balances
    current_balance = device.token_balance
    final_balance = current_balance + data.amount_kwh

    # 1️⃣ insert transaction
    trx = TokenTransaction(
        user_id=user_id,
        device_id=device.id,
        type='topup',
        amount_kwh=data.amount_kwh,
        price=data.price,
        current_balance=current_balance,
        final_balance=final_balance
    )
    db.add(trx)

    # 2️⃣ update saldo
    device.token_balance = final_balance

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

@router.post("/correction")
def create_correction(
    data: TokenCorrection,
    db: Session = Depends(get_db),
    user_id: int = Depends(get_current_user)
):
    device = db.query(Device).filter(
        Device.id == data.device_id,
        Device.user_id == user_id
    ).first()

    if not device:
        raise HTTPException(status_code=404, detail="Device not found")

    current_balance = data.current_balance if data.current_balance is not None else device.token_balance
    amount_kwh = data.final_balance - current_balance

    trx = TokenTransaction(
        user_id=user_id,
        device_id=device.id,
        type='correction',
        amount_kwh=amount_kwh,
        price=0,
        current_balance=current_balance,
        final_balance=data.final_balance
    )
    db.add(trx)

    # 2️⃣ update saldo
    device.token_balance = data.final_balance

    db.commit()
    db.refresh(trx)

    return {
        "code": 200,
        "message": "Token correction applied",
        "data": {
            "transaction_id": trx.id,
            "device_id": device.id,
            "new_balance": device.token_balance
        }
    }

@router.get("/transactions/{device_id}", response_model=TokenTransactionListResponse)
def list_token_transactions(
    device_id: int,
    start_date: Optional[date] = None,
    end_date: Optional[date] = None,
    page: int = Query(1, ge=1),
    limit: int = Query(10, ge=1, le=100),
    db: Session = Depends(get_db),
    user_id: int = Depends(get_current_user)
):  
    query = db.query(TokenTransaction).filter(
        TokenTransaction.user_id == user_id,
        TokenTransaction.device_id == device_id
    )

    if start_date:
        query = query.filter(TokenTransaction.created_at >= datetime.combine(start_date, time.min))
    if end_date:
        query = query.filter(TokenTransaction.created_at <= datetime.combine(end_date, time.max))

    query = query.order_by(TokenTransaction.created_at.desc())

    # Fetch all data to calculate totals in memory
    all_transactions = query.all()
    total = len(all_transactions)
    
    # Calculate totals from fetched data
    total_bought = sum((t.amount_kwh for t in all_transactions if t.amount_kwh is not None and t.type == 'topup'))
    total_price = sum((t.price for t in all_transactions if t.price is not None and t.type == 'topup'))

    # Pagination
    start = (page - 1) * limit
    end = start + limit
    transactions = all_transactions[start:end]

    total_pages = (total + limit - 1) // limit if limit > 0 else 0

    return {
        "code": 200,
        "message": "Token transactions retrieved",
        "data_length": len(transactions),
        "total_data": total,
        "total_pages": total_pages,
        "current_page": page,
        "data_per_page": limit,
        "total_token_bought": total_bought,
        "total_price": total_price,
        "data": transactions
    }

@router.get("/transactions/{device_id}/data", response_model=TokenBalanceGraphResponse)
def get_token_balance_data(
    device_id: int,
    start_date: Optional[date] = None,
    end_date: Optional[date] = None,
    frequency: str = Query("day", regex="^(hour|day)$"),
    db: Session = Depends(get_db),
    user_id: int = Depends(get_current_user)
):
    device = db.query(Device).filter(Device.id == device_id, Device.user_id == user_id).first()
    if not device:
        raise HTTPException(status_code=404, detail="Device not found")

    # Determine date range
    if not start_date:
        start_date = date.today().replace(day=1)
    
    now = datetime.utcnow()
    
    # Adjust end_date to include the full day or limit to last data
    if not end_date:
         # Find last usage data
        last_usage = db.query(func.max(DataHourly.datetime)).filter(DataHourly.device_id == device_id).scalar()
        if last_usage:
            end_date = last_usage.date()
        else:
            end_date = date.today()
    
    start_dt = datetime.combine(start_date, time.min)
    end_dt = datetime.combine(end_date, time.max)
    
    data_points = []
    current_balance = 0.0
    calculation_started = False
    
    if frequency == "day":
        # Group by Date
        usage_data = db.query(
            func.date(DataHourly.datetime).label('date'),
            func.sum(DataHourly.energy_hour).label('usage')
        ).filter(
            DataHourly.device_id == device_id,
            DataHourly.datetime >= start_dt,
            DataHourly.datetime <= end_dt
        ).group_by(func.date(DataHourly.datetime)).all()
        
        # Group by Date and Type to identify transaction types
        topup_data = db.query(
            func.date(TokenTransaction.created_at).label('date'),
            TokenTransaction.type,
            func.sum(TokenTransaction.amount_kwh).label('amount')
        ).filter(
            TokenTransaction.device_id == device_id,
            TokenTransaction.created_at >= start_dt,
            TokenTransaction.created_at <= end_dt
        ).group_by(
            func.date(TokenTransaction.created_at),
            TokenTransaction.type
        ).all()
        
        # Convert to dict for easier access
        usage_map = {str(d[0]): float(d[1] or 0) for d in usage_data}
        
        topup_map = {}
        for d in topup_data:
            d_str = str(d[0])
            if d_str not in topup_map:
                topup_map[d_str] = {'amount': 0.0, 'types': set()}
            
            topup_map[d_str]['amount'] += float(d[2] or 0)
            if d[1]:
                topup_map[d_str]['types'].add(str(d[1]))
        
        # Generate range of dates
        delta = end_date - start_date
        
        for i in range(delta.days + 1):
            day = start_date + timedelta(days=i)
            day_str = str(day)
            
            u = usage_map.get(day_str, 0.0)
            t_info = topup_map.get(day_str, {'amount': 0.0, 'types': set()})
            t = t_info['amount']
            
            # Determine point type
            if t != 0 or len(t_info['types']) > 0:
                if 'correction' in t_info['types']:
                    pt = 'correction'
                else:
                    pt = 'topup'
                calculation_started = True
                current_balance += t
            else:
                pt = 'usage'
            
            if calculation_started:
                current_balance -= u
            else:
                current_balance = 0.0

            data_points.append({
                "datetime": datetime.combine(day, time.min),
                "usage": u,
                "topup": t,
                "balance": current_balance,
                "type": pt,
                "final_balance": current_balance
            })

    else: # hour
        # Fetch all raw data for range
        raw_usage = db.query(DataHourly).filter(
            DataHourly.device_id == device_id,
            DataHourly.datetime >= start_dt,
            DataHourly.datetime <= end_dt
        ).order_by(DataHourly.datetime).all()
        
        raw_topup = db.query(TokenTransaction).filter(
            TokenTransaction.device_id == device_id,
            TokenTransaction.created_at >= start_dt,
            TokenTransaction.created_at <= end_dt
        ).order_by(TokenTransaction.created_at).all()
        
        # Create buckets
        buckets = {}
        curr = start_dt
        while curr <= end_dt:
             # bucket key: start of hour
             key = curr.replace(minute=0, second=0, microsecond=0)
             buckets[key] = {"usage": 0.0, "topup": 0.0, "types": set()}
             curr += timedelta(hours=1)
             
        # Fill buckets
        for r in raw_usage:
            k = r.datetime.replace(minute=0, second=0, microsecond=0)
            if k in buckets:
                buckets[k]["usage"] += float(r.energy_hour or 0)
                
        for r in raw_topup:
            k = r.created_at.replace(minute=0, second=0, microsecond=0)
            if k in buckets:
                buckets[k]["topup"] += float(r.amount_kwh or 0)
                if r.type:
                    buckets[k]["types"].add(str(r.type))
        
        sorted_keys = sorted(buckets.keys())
        for k in sorted_keys:
            u = buckets[k]["usage"]
            t = buckets[k]["topup"]
            types = buckets[k]["types"]
            
            if t != 0 or len(types) > 0:
                if 'correction' in types:
                    pt = 'correction'
                else:
                    pt = 'topup'
                calculation_started = True
                current_balance += t
            else:
                pt = 'usage'
            
            if calculation_started:
                current_balance -= u
            else:
                current_balance = 0.0
            
            data_points.append({
                "datetime": k,
                "usage": u,
                "topup": t,
                "balance": current_balance,
                "type": pt,
                "final_balance": current_balance
            })

    return {
        "code": 200,
        "message": "Token balance graph data retrieved",
        "data": data_points
    }