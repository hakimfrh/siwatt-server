from fastapi import APIRouter, Depends, HTTPException
from typing import Optional
from sqlalchemy.orm import Session
from sqlalchemy import func
from datetime import datetime, timedelta
from app.core.database import get_db
from app.core.deps import get_current_user
from app.models.device import Device
from app.models.data_hourly import DataHourly
from app.schemas.response import ApiResponse
from app.schemas.dashboard import DashboardStats

router = APIRouter(
    prefix="/api/dashboard",
    tags=["Dashboard"]
)

@router.get("/stats", response_model=ApiResponse[DashboardStats])
def get_dashboard_stats(
    device_id: Optional[int] = None,
    db: Session = Depends(get_db),
    user_id: int = Depends(get_current_user)
):
    if device_id:
        device = db.query(Device).filter(Device.user_id == user_id, Device.id == device_id).first()
        if not device:
            raise HTTPException(status_code=404, detail="Device not found")
    else:
        device = db.query(Device).filter(Device.user_id == user_id, Device.is_active == True).first()
    
    avg_usage = 0.0
    token_balance = 0.0
    estimated_days = 0

    if device:
        token_balance = float(device.token_balance or 0)
        
        # Calculate Average Usage Today (Watts)
        today = datetime.utcnow().date()
        today_start = datetime(today.year, today.month, today.day)
        
        avg_power = db.query(func.avg(DataHourly.power))\
            .filter(DataHourly.device_id == device.id)\
            .filter(DataHourly.datetime >= today_start)\
            .scalar()
            
        if avg_power:
            avg_usage = float(avg_power)
            
        # Calculate Estimated Days (Simple projection fallback)
        # Using last 7 days average for better stability
        last_7_days = datetime.utcnow() - timedelta(days=7)
        total_energy_7days = db.query(func.sum(DataHourly.energy_hour))\
            .filter(DataHourly.device_id == device.id)\
            .filter(DataHourly.datetime >= last_7_days)\
            .scalar()
            
        if total_energy_7days and total_energy_7days > 0:
            daily_avg = float(total_energy_7days) / 7
            if daily_avg > 0:
                estimated_days = int(token_balance / daily_avg)

    return {
        "code": 200,
        "message": "Dashboard stats retrieved",
        "data": {
            "avg_usage_today": round(avg_usage, 2),
            "token_balance": round(token_balance, 2),
            "estimated_days": estimated_days
        }
    }
