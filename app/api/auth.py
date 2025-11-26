# app/api/auth.py
from fastapi import APIRouter, Depends, HTTPException, status
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy import select, func
from sqlalchemy.exc import IntegrityError

# Adjust these imports to your project layout
from app.db.session import get_db
from app.db.models.user import User
from app.db.schemas.user import UserCreate, UserLogin, Token
from app.utils.jwt import create_access_token
from app.utils.auth import hash_password, verify_password

router = APIRouter(prefix="/auth", tags=["Auth"])


@router.post("/signup", response_model=Token, status_code=status.HTTP_201_CREATED)
async def signup(user_data: UserCreate, db: AsyncSession = Depends(get_db)):
    # Normalize email for uniqueness
    email_norm = user_data.email.strip().lower()

    # Check if already exists (case-insensitive)
    stmt = select(User).where(func.lower(User.email) == email_norm)
    existing_user = (await db.execute(stmt)).scalar_one_or_none()
    if existing_user:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail="Email is already registered.",
        )

    # Hash password
    hashed = hash_password(user_data.password)

    # Create and persist
    new_user = User(
        email=email_norm,
        hashed_pw=hashed,
        first_name=user_data.first_name.strip(),
        last_name=user_data.last_name.strip(),
    )

    db.add(new_user)
    try:
        await db.commit()
    except IntegrityError:
        # If a race condition created a duplicate between check and commit
        await db.rollback()
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail="Email is already registered.",
        )
    await db.refresh(new_user)

    # Issue JWT
    access_token = create_access_token(subject=str(new_user.email))
    return {"access_token": access_token, "token_type": "bearer"}


@router.post("/login", response_model=Token, status_code=status.HTTP_200_OK)
async def login(user_data: UserLogin, db: AsyncSession = Depends(get_db)):
    email_norm = user_data.email.strip().lower()

    stmt = select(User).where(func.lower(User.email) == email_norm)
    user = (await db.execute(stmt)).scalar_one_or_none()

    # Same generic message to avoid user enumeration
    invalid_exc = HTTPException(
        status_code=status.HTTP_401_UNAUTHORIZED,
        detail="Invalid credentials",
        headers={"WWW-Authenticate": "Bearer"},
    )

    if not user:
        raise invalid_exc

    if not verify_password(user_data.password, str(user.hashed_pw)):
        raise invalid_exc

    access_token = create_access_token(subject=str(user.email))
    return {"access_token": access_token, "token_type": "bearer"}
