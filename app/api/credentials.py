from __future__ import annotations

from uuid import UUID
from fastapi import APIRouter, Depends, HTTPException, status, Path, Query
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy import select
from sqlalchemy.exc import IntegrityError

from app.db.session import get_db
from app.db.models.credentials import ApiCredential
from app.db.schemas.credentials import (
    ApiCredentialCreate,
    ApiCredentialOut,
)  # your schema module path
from app.utils.jwt import get_current_user  # must return the ORM User
from app.db.models.user import User

router = APIRouter(prefix="/credentials", tags=["Credentials"])

STRICT_LEN = 64  # enforce exactly 64 chars


def _validate_len_64(k: str, field: str) -> None:
    if len(k or "") != STRICT_LEN:
        raise HTTPException(
            status_code=422, detail=f"{field} must be exactly {STRICT_LEN} characters."
        )


@router.post("", response_model=ApiCredentialOut, status_code=status.HTTP_201_CREATED)
async def create_credential(
    payload: ApiCredentialCreate,
    db: AsyncSession = Depends(get_db),
    me: User = Depends(get_current_user),
):
    # Strict 64-char enforcement (defense-in-depth in addition to client-side)
    _validate_len_64(payload.api_key.strip(), "api_key")
    _validate_len_64(payload.api_secret.strip(), "api_secret")

    cred = ApiCredential(
        user_id=me.id,
        env=payload.env,
        label=payload.label.strip(),
    )
    cred.set_secrets(payload.api_key.strip(), payload.api_secret.strip())

    db.add(cred)
    try:
        await db.commit()
    except IntegrityError:
        await db.rollback()
        # unique (user_id, env, label)
        raise HTTPException(
            status_code=400, detail="Credential with this env/label already exists."
        )
    await db.refresh(cred)

    # Return metadata only (no secrets)
    return ApiCredentialOut.model_validate(cred)


@router.get("", response_model=list[ApiCredentialOut])
async def list_credentials(
    db: AsyncSession = Depends(get_db),
    me: User = Depends(get_current_user),
    env: str | None = Query(None, description="Optional filter: testnet | prod"),
):
    stmt = select(ApiCredential).where(ApiCredential.user_id == me.id)
    if env:
        stmt = stmt.where(ApiCredential.env == env)
    rows = (
        (await db.execute(stmt.order_by(ApiCredential.created_at.desc())))
        .scalars()
        .all()
    )
    # Metadata only; secrets never leave server
    return [ApiCredentialOut.model_validate(c) for c in rows]


@router.delete("/{cred_id}", status_code=status.HTTP_204_NO_CONTENT)
async def delete_credential(
    cred_id: UUID = Path(...),
    db: AsyncSession = Depends(get_db),
    me: User = Depends(get_current_user),
):
    stmt = select(ApiCredential).where(
        ApiCredential.id == cred_id, ApiCredential.user_id == me.id
    )
    cred = (await db.execute(stmt)).scalar_one_or_none()
    if not cred:
        return  # 204 on no-op delete
    await db.delete(cred)
    await db.commit()
