from fastapi import APIRouter, Depends, Query
from sqlalchemy.orm import Session
from app.core.database import get_db
from app.core.deps import get_current_user
from app.models.device import Device
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
    limit: int = Query(10, ge=1, le=100),
    db: Session = Depends(get_db),
    user_id: int = Depends(get_current_user)
):
    query = db.query(Device).filter(Device.user_id == user_id)
    
    total = query.count()
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