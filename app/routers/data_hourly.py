from fastapi import APIRouter, Depends, HTTPException, Query
from sqlalchemy.orm import Session
from datetime import date, datetime, time
from typing import Optional, List

from app.core.database import get_db
from app.core.deps import get_current_user
from app.models.data_hourly import DataHourly
from app.models.device import Device
from app.schemas.data_hourly import DataHourlyResponse, DataHourlyListResponse
from app.schemas.data_hourly_average import AverageDataResponse
from sqlalchemy import func

router = APIRouter(
    prefix="/api/data-hourly",
    tags=["Data Hourly"]
)

@router.get("/average", response_model=AverageDataResponse)
def get_average_data(
    start_date: Optional[date] = None,
    end_date: Optional[date] = None,
    device_id: Optional[int] = None,
    db: Session = Depends(get_db),
    user_id: int = Depends(get_current_user)
):
    # Determine date range (default to today if not provided)
    if not start_date:
        start_date = datetime.utcnow().date()
    if not end_date:
        end_date = start_date

    # Convert date to datetime range (start of start_date to end of end_date)
    start_dt = datetime.combine(start_date, time.min)
    end_dt = datetime.combine(end_date, time.max)

    # Find device
    device_query = db.query(Device).filter(Device.user_id == user_id)
    if device_id:
        device = device_query.filter(Device.id == device_id).first()
        if not device:
            raise HTTPException(status_code=404, detail="Device not found")
    else:
        # Default to the first active device if not specified
        device = device_query.filter(Device.is_active == True).first()
        if not device:
            # Fallback to any device if no active one, or return empty
             device = device_query.first()
        
    if not device:
         return {
            "code": 200,
            "message": "No device found for user",
             "avg_voltage": 0.0,
            "avg_current": 0.0,
            "avg_power": 0.0,
            "avg_energy_hour": 0.0,
            "avg_frequency": 0.0,
            "avg_pf": 0.0
        }

    # Query DataHourly for average directly from DB
    avg_data = db.query(
        func.avg(DataHourly.voltage).label("voltage"),
        func.avg(DataHourly.current).label("current"),
        func.avg(DataHourly.power).label("power"),
        func.avg(DataHourly.energy_hour).label("energy_hour"),
        func.avg(DataHourly.frequency).label("frequency"),
        func.avg(DataHourly.pf).label("pf")
    ).filter(
        DataHourly.device_id == device.id,
        DataHourly.datetime >= start_dt,
        DataHourly.datetime <= end_dt
    ).first()

    return {
        "code": 200,
        "message": "Average data retrieved successfully",
        "avg_voltage": float(avg_data.voltage or 0),
        "avg_current": float(avg_data.current or 0),
        "avg_power": float(avg_data.power or 0),
        "avg_energy_hour": float(avg_data.energy_hour or 0),
        "avg_frequency": float(avg_data.frequency or 0),
        "avg_pf": float(avg_data.pf or 0)
    }

@router.get("", response_model=DataHourlyListResponse, response_model_exclude_none=True)
def get_hourly_data(
    start_date: Optional[date] = None,
    end_date: Optional[date] = None,
    page: int = Query(1, ge=1),
    limit: int = Query(24, ge=1, le=9999),
    device_id: Optional[int] = None,
    frequency: str = Query("hour", regex="^(hour|day|week|month)$"),
    get_average: bool = False,
    db: Session = Depends(get_db),
    user_id: int = Depends(get_current_user)
):
    # Determine date range (default to today if not provided)
    if not start_date:
        start_date = datetime.utcnow().date()
    if not end_date:
        end_date = start_date

    # Convert date to datetime range (start of start_date to end of end_date)
    start_dt = datetime.combine(start_date, time.min)
    end_dt = datetime.combine(end_date, time.max)

    # Find device
    device_query = db.query(Device).filter(Device.user_id == user_id)
    if device_id:
        device = device_query.filter(Device.id == device_id).first()
        if not device:
            raise HTTPException(status_code=404, detail="Device not found")
    else:
        # Default to the first active device if not specified
        device = device_query.filter(Device.is_active == True).first()
        if not device:
            # Fallback to any device if no active one, or return empty
             device = device_query.first()
        
    if not device:
         return {
            "code": 200,
            "message": "No device found for user",
            "data": []
        }

    # Base Filter
    filters = [
        DataHourly.device_id == device.id,
        DataHourly.datetime >= start_dt,
        DataHourly.datetime <= end_dt
    ]

    if frequency == 'hour':
        # Query DataHourly Normal
        query = db.query(DataHourly).filter(*filters).order_by(DataHourly.datetime.asc())
    else:
        # Aggregation Logic
        if frequency == 'day':
            group_expr = func.date(DataHourly.datetime)
            # MySQL specific for safe sorting/selecting, or generic
        elif frequency == 'week':
            # Group by year and week
            group_expr = func.yearweek(DataHourly.datetime, 1)
        elif frequency == 'month':
            # Group by year and month
            group_expr = func.date_format(DataHourly.datetime, '%Y-%m')
        
        query = db.query(
            func.min(DataHourly.datetime).label("datetime"),
            func.avg(DataHourly.voltage).label("voltage"),
            func.avg(DataHourly.current).label("current"),
            func.avg(DataHourly.power).label("power"),
            func.max(DataHourly.energy).label("energy"),
            func.avg(DataHourly.frequency).label("frequency"),
            func.avg(DataHourly.pf).label("pf"),
            func.sum(DataHourly.energy_hour).label("energy_hour"),
            func.min(DataHourly.device_id).label("device_id") # constant
        ).filter(*filters).group_by(group_expr).order_by(func.min(DataHourly.datetime).asc())

    # Pagination
    total = query.count()
    offset = (page - 1) * limit
    data = query.offset(offset).limit(limit).all()

    total_pages = (total + limit - 1) // limit if limit > 0 else 0

    avg_data = {}
    if get_average:
        count = len(data)
        if count > 0:
            avg_data["avg_voltage"] = sum(d.voltage or 0 for d in data) / count
            avg_data["avg_current"] = sum(d.current or 0 for d in data) / count
            avg_data["avg_power"] = sum(d.power or 0 for d in data) / count
            avg_data["avg_energy_hour"] = sum(d.energy_hour or 0 for d in data) / count
            avg_data["avg_frequency"] = sum(d.frequency or 0 for d in data) / count
            avg_data["avg_pf"] = sum(d.pf or 0 for d in data) / count
        else:
            avg_data["avg_voltage"] = 0.0
            avg_data["avg_current"] = 0.0
            avg_data["avg_power"] = 0.0
            avg_data["avg_energy_hour"] = 0.0
            avg_data["avg_frequency"] = 0.0
            avg_data["avg_pf"] = 0.0

    return {
        "code": 200,
        "message": "Data retrieved successfully",
        "data_length": len(data),
        "total_data": total,
        "total_pages": total_pages,
        "current_page": page,
        "data_per_page": limit,
        **avg_data,
        "data": data
    }


