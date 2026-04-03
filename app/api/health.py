from fastapi import APIRouter

from app.db import fetch_one

router = APIRouter(prefix="/health", tags=["health"])


@router.get("")
async def health():
    stats = {}
    try:
        row = await fetch_one("SELECT COUNT(*) as c FROM data_series")
        stats["series"] = row["c"] if row else 0
        row = await fetch_one("SELECT COUNT(*) as c FROM data_points")
        stats["data_points"] = row["c"] if row else 0
    except Exception:
        stats["series"] = 0
        stats["data_points"] = 0
    return {
        "status": "ok",
        "engine": "Equilibria v0.1.0",
        "modules": 103,
        "layers": 6,
        "estimators": 12,
        "collectors": 13,
        "ai_tools": 22,
        **stats,
    }
