from __future__ import annotations
import os
from datetime import datetime, timezone
from typing import Optional

import httpx
from prefect import flow, task, get_run_logger
from sqlalchemy.ext.asyncio import create_async_engine
from sqlalchemy import text

DB_HOST = os.getenv("POSTGRES_HOST", "db")
DB_PORT = os.getenv("POSTGRES_PORT", "5432")
DB_NAME = os.getenv("POSTGRES_DB", "heatmap")
DB_USER = os.getenv("POSTGRES_USER", "heatmap")
DB_PASS = os.getenv("POSTGRES_PASSWORD", "heatmap")
DATABASE_URL = f"postgresql+asyncpg://{DB_USER}:{DB_PASS}@{DB_HOST}:{DB_PORT}/{DB_NAME}"

SLACK_WEBHOOK_URL = os.getenv("SLACK_WEBHOOK_URL", "").strip()

engine = create_async_engine(DATABASE_URL, future=True, echo=False)


@task
async def get_latest_score_ts() -> Optional[datetime]:
    async with engine.connect() as conn:
        row = (await conn.execute(text("SELECT MAX(ts) FROM scores"))).first()
        return row[0] if row and row[0] else None


@task
async def get_budget_snapshot() -> dict:
    try:
        from libs.budget import BudgetManager
        mgr = BudgetManager(os.getenv("REDIS_URL", "redis://redis:6379/0"))
        return await mgr.summary()
    except Exception:
        return {}


@task
async def post_slack(message: str) -> None:
    if not SLACK_WEBHOOK_URL:
        return
    async with httpx.AsyncClient(timeout=20) as client:
        await client.post(SLACK_WEBHOOK_URL, json={"text": message})


@flow(name="alerts")
async def alerts(stale_hours: int = 6, budget_warn_pct: int = 80):
    logger = get_run_logger()
    # Staleness alert
    last = await get_latest_score_ts()
    if last:
        age_h = (datetime.now(timezone.utc) - last).total_seconds() / 3600.0
        if age_h > stale_hours:
            await post_slack(f":warning: Scores stale: last={last.isoformat()} age={age_h:.1f}h > {stale_hours}h")
            logger.warning("Scores are stale")

    # Budget alert
    snap = await get_budget_snapshot()
    if snap and "limits" in snap and "usage" in snap:
        msgs = []
        for k, lim in snap.get("limits", {}).items():
            used = float(snap.get("usage", {}).get(k, 0))
            if not lim:
                continue
            pct = (used / float(lim)) * 100.0
            if pct >= float(budget_warn_pct):
                msgs.append(f"{k}: {pct:.0f}% of {lim}")
        if msgs:
            await post_slack(":money_with_wings: Budget nearing limits: " + "; ".join(msgs))
            logger.warning("Budget nearing limits: %s", msgs)
    return "ok"
