from pydantic import BaseModel, EmailStr

class UserRegister(BaseModel):
    full_name: str
    username: str
    email: EmailStr
    password: str

class UserLogin(BaseModel):
    email: EmailStr
    password: str

class UserResponse(BaseModel):
    id: int
    full_name: str
    username: str
    email: str

    class Config:
        from_attributes = True

class UserUpdate(BaseModel):
    full_name: str
    email: EmailStr

class ChangePassword(BaseModel):
    old_password: str
    new_password: str
