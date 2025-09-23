from sqlalchemy import Column, String, Boolean, DateTime
from sqlalchemy.dialects.postgresql import UUID
from sqlalchemy.orm import relationship
from sqlalchemy.sql import func
from uuid import uuid4

from app.db.base import Base

class User(Base):
    __tablename__ = "users"

    id = Column(UUID(as_uuid=True), primary_key=True, default=uuid4, index=True)
    email = Column(String, unique=True, nullable=False)
    hashed_pw = Column(String, nullable=False)
    first_name = Column(String, nullable=False)
    last_name = Column(String, nullable=False)

    is_active = Column(Boolean, nullable=False, default=False)
    is_admin = Column(Boolean, nullable=False, default=False)

    suscription = Column(String, nullable=False, default="free tier")
    created_at = Column(DateTime(timezone=True), server_default=func.now(), nullable=False)

    # relations
    bots = relationship("Bot", back_populates="user", cascade="all, delete-orphan")
    creds = relationship("ApiCredential", back_populates="user", cascade="all, delete-orphan")
