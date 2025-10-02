from __future__ import annotations

from typing import List
from uuid import UUID

from fastapi import APIRouter, Depends, HTTPException, status
from pydantic import BaseModel
from sqlalchemy import select
from sqlalchemy.exc import IntegrityError
from sqlalchemy.ext.asyncio import AsyncSession

from app.db.session import get_db
from app.db.models.bots import Bot
from app.db.models.user import User
from app.db.models.credentials import ApiCredential
from app.db.schemas.bots import BotCreate, BotOut, BotUpdate
from app.services.tasks.admin import available_symbols, list_signal_stream_pairs, sync_bot_runtime

router = APIRouter(prefix="/bots", tags=["Bots"])


class BotStream(BaseModel):
    symbol: str
    timeframe: str


def _map_side_mode_to_whitelist(mode: str) -> str:
    mapping = {
        "both": "both",
        "long_only": "long",
        "short_only": "short",
    }
    return mapping.get(mode, "both")


async def _get_user(db: AsyncSession, user_id: UUID) -> User:
    user = await db.get(User, user_id)
    if not user:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="User not found")
    return user


async def _get_credential(db: AsyncSession, cred_id: UUID, user_id: UUID) -> ApiCredential:
    cred = await db.get(ApiCredential, cred_id)
    if not cred or cred.user_id != user_id:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="Credential not found")
    return cred


@router.get("/streams", response_model=List[BotStream])
async def list_streams() -> List[BotStream]:
    streams = list_signal_stream_pairs()
    return [BotStream(symbol=sym, timeframe=tf) for sym, tf in streams]


@router.get("/", response_model=List[BotOut])
async def list_bots(db: AsyncSession = Depends(get_db)) -> List[BotOut]:
    result = await db.execute(select(Bot).order_by(Bot.created_at))
    bots = result.scalars().all()
    return list(bots)


@router.get("/{bot_id}", response_model=BotOut)
async def get_bot(bot_id: UUID, db: AsyncSession = Depends(get_db)) -> BotOut:
    bot = await db.get(Bot, bot_id)
    if not bot:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="Bot not found")
    return bot


@router.post("/", response_model=BotOut, status_code=status.HTTP_201_CREATED)
async def create_bot(payload: BotCreate, db: AsyncSession = Depends(get_db)) -> BotOut:
    symbol = payload.symbol.upper()
    available = available_symbols()
    if available and symbol not in available:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail={"code": "invalid_symbol", "message": f"Symbol '{symbol}' not available"},
        )

    await _get_user(db, payload.user_id)
    cred = await _get_credential(db, payload.cred_id, payload.user_id)

    if cred.env != payload.env:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail={"code": "env_mismatch", "message": "Credential environment mismatch"},
        )

    bot = Bot(
        user_id=payload.user_id,
        cred_id=payload.cred_id,
        symbol=symbol,
        timeframe=payload.timeframe,
        env=payload.env,
        side_mode=payload.side_mode,
        side_whitelist=_map_side_mode_to_whitelist(payload.side_mode),
        leverage=payload.leverage,
        risk_per_trade=payload.risk_per_trade,
        tp_ratio=payload.tp_ratio,
        status=payload.status,
        max_qty=payload.max_qty,
        nickname=payload.nickname,
        enabled=(payload.status == "active"),
    )

    db.add(bot)
    try:
        await db.commit()
    except IntegrityError as exc:
        await db.rollback()
        raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail=str(exc))

    await db.refresh(bot)

    sync_bot_runtime(bot)

    return bot


@router.patch("/{bot_id}", response_model=BotOut)
async def update_bot(bot_id: UUID, payload: BotUpdate, db: AsyncSession = Depends(get_db)) -> BotOut:
    bot = await db.get(Bot, bot_id)
    if not bot:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="Bot not found")

    updated = False

    if payload.side_mode is not None:
        bot.side_mode = payload.side_mode
        bot.side_whitelist = _map_side_mode_to_whitelist(payload.side_mode)
        updated = True

    if payload.leverage is not None:
        bot.leverage = payload.leverage
        updated = True

    if payload.risk_per_trade is not None:
        bot.risk_per_trade = payload.risk_per_trade
        updated = True

    if payload.tp_ratio is not None:
        bot.tp_ratio = payload.tp_ratio
        updated = True

    if payload.max_qty is not None:
        bot.max_qty = payload.max_qty
        updated = True

    if payload.nickname is not None:
        bot.nickname = payload.nickname
        updated = True

    if payload.status is not None:
        if bot.status == "ended" and payload.status != "ended":
            raise HTTPException(
                status_code=status.HTTP_400_BAD_REQUEST,
                detail={"code": "immutable", "message": "Ended bots cannot transition"},
            )
        bot.status = payload.status
        bot.enabled = payload.status == "active"
        updated = True

    if not updated:
        return bot

    try:
        await db.commit()
    except IntegrityError as exc:
        await db.rollback()
        raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail=str(exc))

    await db.refresh(bot)

    sync_bot_runtime(bot)

    return bot
