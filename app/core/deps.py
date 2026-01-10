from fastapi import Depends, HTTPException
from fastapi.security import HTTPBearer
from jose import jwt
import os

security = HTTPBearer()
JWT_SECRET = os.getenv("JWT_SECRET")

def get_current_user(token = Depends(security)):
    try:
        payload = jwt.decode(token.credentials, JWT_SECRET, algorithms=["HS256"])
        return int(payload["sub"])
    except:
        raise HTTPException(status_code=401, detail="Invalid token")
