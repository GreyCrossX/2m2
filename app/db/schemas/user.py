from pydantic import BaseModel, EmailStr
from uuid import UUID
from datetime import datetime

class UserCreate (BaseModel):
    first_name:str
    last_name:str
    email:EmailStr
    password: str

class UserOut (BaseModel):
    id: UUID
    email: EmailStr
    is_active: bool
    is_admin: bool
    suscription: str
    created_at: datetime

    class Config:
        from_attributes: True

class UserLogin (BaseModel):
    email:EmailStr
    password: str

class Token (BaseModel):
    access_token: str
    token_type: str = "bearer"
