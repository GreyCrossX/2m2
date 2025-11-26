from __future__ import annotations

from typing import Optional
from uuid import UUID

from fastapi import APIRouter, Depends, HTTPException, Query, Path, status
from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

from app.db.session import get_db
from app.db.models.bots import Bot
from app.db.models.credentials import ApiCredential
from app.db.schemas.bots import BotCreateRequest, BotOut, BotUpdate
from app.db.models.user import User
from app.utils.jwt import get_current_user

router = APIRouter(prefix="/bots", tags=["Bots"])


async def _get_bot_for_user(
    db: AsyncSession, bot_id: UUID, user_id: UUID
) -> Bot | None:
    stmt = select(Bot).where(Bot.id == bot_id, Bot.user_id == user_id)
    res = await db.execute(stmt)
    return res.scalars().first()


@router.get("", response_model=list[BotOut])
async def list_bots(
    env: Optional[str] = Query(
        default=None, description="Optional filter: testnet | prod"
    ),
    db: AsyncSession = Depends(get_db),
    me: User = Depends(get_current_user),
):
    stmt = select(Bot).where(Bot.user_id == me.id)
    if env:
        stmt = stmt.where(Bot.env == env)
    res = await db.execute(stmt.order_by(Bot.created_at.desc()))
    bots = res.scalars().all()
    return [BotOut.model_validate(b) for b in bots]


@router.post("", response_model=BotOut, status_code=status.HTTP_201_CREATED)
async def create_bot(
    payload: BotCreateRequest,
    db: AsyncSession = Depends(get_db),
    me: User = Depends(get_current_user),
):
    cred_stmt = select(ApiCredential).where(
        ApiCredential.id == payload.cred_id,
        ApiCredential.user_id == me.id,
    )
    cred_res = await db.execute(cred_stmt)
    cred = cred_res.scalars().first()
    if not cred:
        raise HTTPException(
            status_code=400, detail="Credential not found for this user."
        )

    if str(cred.env) != payload.env:
        raise HTTPException(
            status_code=400,
            detail="Credential environment does not match bot environment.",
        )

    bot = Bot(
        user_id=me.id,
        cred_id=payload.cred_id,
        symbol=payload.symbol.upper(),
        timeframe=payload.timeframe,
        enabled=payload.enabled,
        env=payload.env,
        side_whitelist=payload.side_whitelist,
        leverage=payload.leverage,
        use_balance_pct=payload.use_balance_pct,
        balance_pct=payload.balance_pct,
        fixed_notional=payload.fixed_notional,
        max_position_usdt=payload.max_position_usdt,
        nickname=payload.nickname,
    )
    db.add(bot)
    await db.commit()
    await db.refresh(bot)
    return BotOut.model_validate(bot)


@router.patch("/{bot_id}", response_model=BotOut)
async def update_bot(
    bot_id: UUID = Path(...),
    payload: BotUpdate = ...,
    db: AsyncSession = Depends(get_db),
    me: User = Depends(get_current_user),
):
    bot = await _get_bot_for_user(db, bot_id, me.id)
    if not bot:
        raise HTTPException(status_code=404, detail="Bot not found.")

    data = payload.model_dump(exclude_unset=True)
    if "symbol" in data:
        data["symbol"] = str(data["symbol"]).upper()

    for field, value in data.items():
        setattr(bot, field, value)

    await db.commit()
    await db.refresh(bot)
    return BotOut.model_validate(bot)


@router.delete("/{bot_id}", status_code=status.HTTP_204_NO_CONTENT)
async def delete_bot(
    bot_id: UUID = Path(...),
    db: AsyncSession = Depends(get_db),
    me: User = Depends(get_current_user),
):
    bot = await _get_bot_for_user(db, bot_id, me.id)
    if not bot:
        return
    await db.delete(bot)
    await db.commit()
