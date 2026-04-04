import json
import os
from fastapi import APIRouter, Depends, HTTPException
from typing import Any, Optional
from sqlalchemy.orm import Session
from sqlalchemy import func
from datetime import date, datetime, timedelta
from app.core.database import get_db
from app.core.deps import get_current_user
from app.models.device import Device
from app.models.data_hourly import DataHourly
from app.models.prediction import Prediction
from app.schemas.response import ApiResponse
from app.schemas.dashboard import DashboardStats

router = APIRouter(
    prefix="/api/dashboard",
    tags=["Dashboard"]
)


_ESTIMATED_DAYS_MODE_RAW = os.getenv("DASHBOARD_ESTIMATED_DAYS_MODE", "prediction").strip().lower()
ESTIMATED_DAYS_MODE = _ESTIMATED_DAYS_MODE_RAW if _ESTIMATED_DAYS_MODE_RAW in {"prediction", "average_7d"} else "prediction"


def _normalize_prediction_result(raw_value: Any) -> Any:
    if raw_value is None:
        return None

    if isinstance(raw_value, (dict, list)):
        return raw_value

    if isinstance(raw_value, (bytes, bytearray)):
        raw_value = raw_value.decode("utf-8", errors="ignore")

    if isinstance(raw_value, str):
        text = raw_value.strip()
        if text == "":
            return None
        try:
            return json.loads(text)
        except json.JSONDecodeError:
            return raw_value

    return raw_value


def _calculate_estimated_days_from_daily_prediction(
    token_balance: float,
    prediction_result: Any,
    reference_date: date
) -> Optional[int]:
    result_data = _normalize_prediction_result(prediction_result)
    if not isinstance(result_data, dict):
        return None

    prediction_section = result_data.get("prediction")
    if not isinstance(prediction_section, dict):
        return None

    raw_predictions = prediction_section.get("predictions")
    if not isinstance(raw_predictions, list):
        return None

    usable_predictions = []
    for item in raw_predictions:
        if not isinstance(item, dict):
            continue

        raw_date = item.get("date")
        raw_energy_day = item.get("energy_day")
        if raw_date is None or raw_energy_day is None:
            continue

        try:
            prediction_date = datetime.fromisoformat(str(raw_date)).date()
            energy_day = float(raw_energy_day)
        except (TypeError, ValueError):
            continue

        if prediction_date < reference_date or energy_day <= 0:
            continue

        usable_predictions.append((prediction_date, energy_day))

    if not usable_predictions:
        return None

    usable_predictions.sort(key=lambda value: value[0])

    remaining_balance = token_balance
    estimated_days = 0
    for _, energy_day in usable_predictions:
        if remaining_balance < energy_day:
            break
        remaining_balance -= energy_day
        estimated_days += 1

    return estimated_days


def _calculate_estimated_days_from_average_7d(
    db: Session,
    device_id: Any,
    token_balance: float
) -> int:
    last_7_days = datetime.now() - timedelta(days=7)
    total_energy_7days = db.query(func.sum(DataHourly.energy_hour))\
        .filter(DataHourly.device_id == device_id)\
        .filter(DataHourly.datetime >= last_7_days)\
        .scalar()

    if total_energy_7days and total_energy_7days > 0:
        daily_avg = float(total_energy_7days) / 7
        if daily_avg > 0:
            return int(token_balance / daily_avg)

    return 0

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
        raise HTTPException(status_code=404, detail="Device not found")
        # device = db.query(Device).filter(Device.user_id == user_id, Device.is_active == True).first()
    
    avg_usage = 0.0
    token_balance = 0.0
    estimated_days = 0

    if device:
        token_balance = float(device.token_balance or 0) # type: ignore
        today = datetime.now().date()
        
        # Calculate Average Usage Today (Watts)
        today_start = datetime(today.year, today.month, today.day)
        
        avg_power = db.query(func.avg(DataHourly.power))\
            .filter(DataHourly.device_id == device.id)\
            .filter(DataHourly.datetime >= today_start)\
            .scalar()
            
        if avg_power:
            avg_usage = float(avg_power)

        if ESTIMATED_DAYS_MODE == "average_7d":
            estimated_days = _calculate_estimated_days_from_average_7d(
                db=db,
                device_id=device.id,
                token_balance=token_balance
            )
        else:
            # prediction mode
            daily_prediction = db.query(Prediction).filter(
                Prediction.user_id == user_id,
                Prediction.device_id == device.id,
                Prediction.job_type == "daily",
                Prediction.status == "done"
            ).order_by(Prediction.created_at.desc(), Prediction.id.desc()).first()

            if daily_prediction:
                estimated_from_prediction = _calculate_estimated_days_from_daily_prediction(
                    token_balance=token_balance,
                    prediction_result=daily_prediction.result,
                    reference_date=today
                )
                if estimated_from_prediction is not None:
                    estimated_days = estimated_from_prediction

    return {
        "code": 200,
        "message": "Dashboard stats retrieved",
        "data": {
            "avg_usage_today": round(avg_usage, 2),
            "token_balance": round(token_balance, 2),
            "estimated_days": estimated_days
        }
    }
