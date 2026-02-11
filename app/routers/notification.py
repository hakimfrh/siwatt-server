from fastapi import APIRouter, HTTPException, Header
from pydantic import BaseModel
from typing import Optional
import os
from dotenv import load_dotenv
from app.utils.fcm import send_notification_to_topic, send_notification_to_user

load_dotenv()

router = APIRouter(
    prefix="/notification",
    tags=["Notification"]
)

# Secret dari environment variable
API_SECRET = os.getenv("TESTING_API_SECRET")


class NotificationRequest(BaseModel):
    title: str
    body: str
    user_id: Optional[int] = None
    topic: Optional[str] = None
    data: Optional[dict] = None


@router.post("/test")
async def test_notification(
    request: NotificationRequest,
    x_api_secret: str = Header(..., description="API Secret untuk otentikasi")
):
    """
    API untuk testing notifikasi FCM.
    
    Gunakan header X-Api-Secret dengan value dari environment variable TESTING_API_SECRET
    
    Kirim notifikasi dengan 2 cara:
    1. Kirim ke user_id tertentu (otomatis ke topic "user_{user_id}")
    2. Kirim ke topic custom
    """
    # Validasi secret
    if x_api_secret != API_SECRET:
        raise HTTPException(
            status_code=401,
            detail="Invalid API Secret"
        )
    
    # Validasi input
    if not request.user_id and not request.topic:
        raise HTTPException(
            status_code=400,
            detail="Harus mengisi user_id atau topic"
        )
    
    # Kirim notifikasi
    if request.user_id:
        result = send_notification_to_user(
            user_id=request.user_id,
            title=request.title,
            body=request.body,
            data=request.data
        )
    else:
        result = send_notification_to_topic(
            topic=request.topic,
            title=request.title,
            body=request.body,
            data=request.data
        )
    
    if not result["success"]:
        raise HTTPException(
            status_code=500,
            detail=result["message"]
        )
    
    return {
        "code": 200,
        "message": result["message"],
        "data": {
            "message_id": result.get("message_id"),
            "topic": f"user_{request.user_id}" if request.user_id else request.topic
        }
    }

