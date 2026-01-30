from fastapi import APIRouter, Depends, Query, HTTPException
from sqlalchemy.orm import Session
from sqlalchemy import func
from datetime import datetime
from app.core.database import get_db
from app.core.deps import get_current_user
from app.models.device import Device
from app.models.data_realtime import DataRealtime
from app.models.data_hourly import DataHourly
from app.schemas.device import DeviceCreate, DeviceListResponse
from app.schemas.response import ApiResponse

router = APIRouter(
    prefix="/api/devices",
    tags=["Devices"],
    dependencies=[Depends(get_current_user)]
)

@router.post("")
def create_device(
    data: DeviceCreate,
    db: Session = Depends(get_db),
    user_id: int = Depends(get_current_user)
):
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

@router.get("/{id}")
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
    today = datetime.utcnow().date()
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
            "voltage": realtime.voltage if realtime else 0,
            "current": realtime.current if realtime else 0,
            "power": realtime.power if realtime else 0,
            "frequency": realtime.frequency if realtime else 0,
            "pf": realtime.pf if realtime else 0,
            "updated_at": realtime.updated_at if realtime else None,
            "total_today": float(total_today or 0),
            "is_online": device.is_active,
            "up_time": device.up_time
        }
    }