from pydantic import BaseModel
from app.schemas.user import UserResponse

class LoginData(BaseModel):
    user: UserResponse
    api_token: str | None = None

class RegisterData(BaseModel):
    user: UserResponse
    api_token: str | None = None
