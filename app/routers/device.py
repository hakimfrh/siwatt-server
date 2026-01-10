from fastapi import APIRouter, Depends
from sqlalchemy.orm import Session
from app.core.database import get_db
from app.core.deps import get_current_user
from app.models.device import Device
from app.schemas.device import DeviceCreate
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

@router.get("")
def list_devices(
    db: Session = Depends(get_db),
    user_id: int = Depends(get_current_user)
):
    devices = db.query(Device).filter(Device.user_id == user_id).all()
    return {
        "code": 200,
        "message": "Devices retrieved",
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