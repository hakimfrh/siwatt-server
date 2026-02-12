import firebase_admin
from firebase_admin import credentials, messaging
import os

# Initialize Firebase Admin SDK
cred_path = os.path.join(os.path.dirname(__file__), '..', '..', 'firebase-credentials.json')

try:
    cred = credentials.Certificate(cred_path)
    firebase_admin.initialize_app(cred)
    print("Firebase Admin SDK initialized successfully")
except Exception as e:
    print(f"Error initializing Firebase Admin SDK: {e}")


def send_notification_to_topic(
    topic: str,
    title: str,
    body: str,
    data: dict = None
) -> dict:
    """
    Mengirim notifikasi ke topic tertentu via Firebase Cloud Messaging.
    
    Args:
        topic: Topic yang akan menerima notifikasi (contoh: "user_123")
        title: Judul notifikasi
        body: Isi notifikasi
        data: Data tambahan (optional)
    
    Returns:
        dict dengan status success/error dan message
    """
    try:
        # Buat message
        message = messaging.Message(
            notification=messaging.Notification(
                title=title,
                body=body,
            ),
            data=data or {},
            topic=topic,
            android=messaging.AndroidConfig(
                channel_id="high_importance_channel",
                priority="high"
            )
        )
        
        # Kirim message
        response = messaging.send(message)
        
        return {
            "success": True,
            "message": f"Notifikasi berhasil dikirim ke topic '{topic}'",
            "message_id": response
        }
        
    except Exception as e:
        return {
            "success": False,
            "message": f"Gagal mengirim notifikasi: {str(e)}"
        }


def send_notification_to_user(
    user_id: int,
    title: str,
    body: str,
    data: dict = None
) -> dict:
    """
    Mengirim notifikasi ke user tertentu via topic "user_{user_id}".
    
    Args:
        user_id: ID user yang akan menerima notifikasi
        title: Judul notifikasi
        body: Isi notifikasi
        data: Data tambahan (optional)
    
    Returns:
        dict dengan status success/error dan message
    """
    topic = f"user_{user_id}"
    return send_notification_to_topic(topic, title, body, data)
