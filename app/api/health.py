# app/api/health.py
from datetime import datetime, timezone
import logging

from fastapi import APIRouter, Depends, HTTPException, status
from sqlalchemy import text
from sqlalchemy.ext.asyncio import AsyncSession

from app.db.session import get_db

logger = logging.getLogger(__name__)

router = APIRouter(prefix="/health", tags=["health"])

@router.get("/", status_code=status.HTTP_200_OK)
async def liveness():
    """Simple liveness probe (no external deps)."""
    return {
        "message": "Trading bot running",
        "status": "running",
        "version": "1.0.0",
        "timestamp": datetime.now(timezone.utc).isoformat(),
    }

@router.get("/db", status_code=status.HTTP_200_OK)
async def readiness_db(db: AsyncSession = Depends(get_db)):
    # Connectivity
    try:
        r = await db.execute(text("select 1"))
        ok = (r.scalar_one() == 1)
    except Exception as e:
        raise HTTPException(status_code=503, detail={"code": "conn_failed", "msg": str(e)})

    # Migrations (soft)
    mig = {"present": None, "version": None}
    try:
        v = await db.execute(text("select version_num from alembic_version limit 1"))
        mig["present"] = True
        mig["version"] = v.scalar_one_or_none()
    except Exception:
        mig["present"] = False

    return {"ok": ok, "migrations": mig}
