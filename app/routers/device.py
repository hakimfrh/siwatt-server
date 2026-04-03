import json
from datetime import date, datetime
from typing import Any, Optional

from fastapi import APIRouter, Depends, Query, HTTPException
from sqlalchemy.orm import Session
from sqlalchemy import func
from app.core.database import get_db
from app.core.deps import get_current_user
from app.core.security import verify_password
from app.models.device import Device
from app.models.data_realtime import DataRealtime
from app.models.data_hourly import DataHourly
from app.models.prediction import Prediction
from app.models.user import User
from app.schemas.device import DeviceCreate, DeviceListResponse, DeviceUpdate, DeviceResponse, DeviceDeleteRequest
from app.schemas.response import ApiResponse

router = APIRouter(
    prefix="/api/devices",
    tags=["Devices"],
    dependencies=[Depends(get_current_user)]
)


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

@router.post("", response_model=ApiResponse[DeviceResponse])
def create_device(
    data: DeviceCreate,
    db: Session = Depends(get_db),
    user_id: int = Depends(get_current_user)
):
    # Check if device exists
    existing_device = db.query(Device).filter(Device.device_code == data.device_code).first()
    if existing_device:
        if existing_device.user_id == user_id:
            raise HTTPException(status_code=400, detail="Device already added to your account")
        else:
            raise HTTPException(status_code=400, detail="Device code already registered by another user")

    device = Device(
        device_code=data.device_code,
        user_id=user_id,
        device_name=data.device_name,
        location=data.location
    )
    db.add(device)
    db.commit()
    db.refresh(device)

    return {
        "code": 201,
        "message": "Device created",
        "data": device
    }

@router.put("/{device_id}", response_model=ApiResponse[DeviceResponse])
def update_device(
    device_id: int,
    data: DeviceUpdate,
    db: Session = Depends(get_db),
    user_id: int = Depends(get_current_user)
):
    device = db.query(Device).filter(Device.id == device_id, Device.user_id == user_id).first()
    if not device:
        raise HTTPException(status_code=404, detail="Device not found")
    
    device.device_name = data.device_name
    device.location = data.location
    db.commit()
    db.refresh(device)
    
    return ApiResponse(
        code=200,
        message="Device updated successfully",
        data=device
    )

@router.get("", response_model=DeviceListResponse)
def list_devices(
    page: int = Query(1, ge=1),
    limit: int = Query(10, ge=-1),
    db: Session = Depends(get_db),
    user_id: int = Depends(get_current_user)
):
    query = db.query(Device).filter(Device.user_id == user_id)
    
    total = query.count()
    
    if limit == -1:
        devices = query.all()
        limit = total
        total_pages = 1
    else:
        offset = (page - 1) * limit
        devices = query.offset(offset).limit(limit).all()
        total_pages = (total + limit - 1) // limit if limit > 0 else 0

    return {
        "code": 200,
        "message": "Devices retrieved",
        "data_length": len(devices),
        "total_data": total,
        "total_pages": total_pages,
        "current_page": page,
        "data_per_page": limit,
        "data": devices
    }

@router.get("/{id}", response_model=ApiResponse[DeviceResponse])
def get_device(
    id: int,
    db: Session = Depends(get_db),
    user_id: int = Depends(get_current_user)
):
    device = db.query(Device).filter(
        Device.id == id,
        Device.user_id == user_id
    ).first()
    if not device:
        return {
            "code": 404,
            "message": "Device not found",
            "data": None
        }

    return {
        "code": 200,
        "message": "Device retrieved",
        "data": device
    }

@router.delete("/{id}")
def delete_device(
    id: int,
    data: DeviceDeleteRequest,
    db: Session = Depends(get_db),
    user_id: int = Depends(get_current_user)
):
    device = db.query(Device).filter(
        Device.id == id,
        Device.user_id == user_id
    ).first()
    if not device:
        return {
            "code": 404,
            "message": "Device not found",
            "data": None
        }

    user = db.query(User).filter(User.id == user_id).first()
    if not user:
        raise HTTPException(status_code=404, detail="User not found")

    user_password = getattr(user, "password", None)
    if not user_password or not verify_password(data.password, user_password):
        raise HTTPException(status_code=401, detail="Invalid password")

    db.delete(device)
    db.commit()

    return {
        "code": 200,
        "message": "Device deleted",
        "data": None
    }

@router.get("/{id}/realtime")
def get_device_realtime_data(
    id: int,
    db: Session = Depends(get_db),
    user_id: int = Depends(get_current_user)
):
    device = db.query(Device).filter(
        Device.id == id,
        Device.user_id == user_id
    ).first()
    
    if not device:
        return {
            "code": 404,
            "message": "Device not found",
            "data": None
        }

    realtime = db.query(DataRealtime).filter(DataRealtime.device_id == id).first()
    
    # Calculate total energy today
    today = datetime.now().date()
    today_start = datetime(today.year, today.month, today.day)
    
    total_today = db.query(func.sum(DataHourly.energy_hour))\
        .filter(DataHourly.device_id == id)\
        .filter(DataHourly.datetime >= today_start)\
        .scalar()
        
    return {
        "code": 200,
        "message": "Realtime data retrieved",
        "data": {
            "device_id": device.id,
            "voltage": float(realtime.voltage or 0) if realtime else 0.0,
            "current": float(realtime.current or 0) if realtime else 0.0,
            "power": float(realtime.power or 0) if realtime else 0.0,
            "frequency": float(realtime.frequency or 0) if realtime else 0.0,
            "pf": float(realtime.pf or 0) if realtime else 0.0,
            "updated_at": realtime.updated_at if realtime else None,
            "total_today": float(total_today or 0),
            "is_online": bool(device.is_active),
            "up_time": int(device.up_time or 0)
        }
    }

@router.get("/{id}/prediction")
def get_prediction(
    id: int,
    date_filter: Optional[date] = Query(None, alias="date"),
    prediction_type: str = Query(..., alias="type", regex="^(daily|hourly)$"),
    db: Session = Depends(get_db),
    user_id: int = Depends(get_current_user)
):
    device = db.query(Device).filter(
        Device.id == id,
        Device.user_id == user_id
    ).first()

    if not device:
        return {
            "code": 404,
            "message": "Device not found",
            "data": None
        }

    query = db.query(Prediction).filter(
        Prediction.user_id == user_id,
        Prediction.device_id == id,
        Prediction.job_type == prediction_type,
    )

    if date_filter is not None:
        query = query.filter(func.date(Prediction.created_at) == date_filter)

    prediction_row = query.order_by(Prediction.created_at.desc(), Prediction.id.desc()).first()

    if not prediction_row:
        return {
            "code": 404,
            "message": "prediction notfound",
            "data": "prediction notfound"
        }

    if (prediction_row.status or "").lower() == "error":
        return {
            "code": 200,
            "message": "error",
            "data": "error"
        }

    result_data = _normalize_prediction_result(prediction_row.result)

    if result_data is None:
        return {
            "code": 404,
            "message": "prediction notfound",
            "data": "prediction notfound"
        }

    return {
        "code": 200,
        "message": "Prediction retrieved",
        "data": result_data
    }

