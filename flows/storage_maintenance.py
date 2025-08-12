from __future__ import annotations
import os
from prefect import flow, task, get_run_logger
from sqlalchemy.ext.asyncio import create_async_engine
from sqlalchemy import text

DB_HOST = os.getenv("POSTGRES_HOST", "db")
DB_PORT = os.getenv("POSTGRES_PORT", "5432")
DB_NAME = os.getenv("POSTGRES_DB", "heatmap")
DB_USER = os.getenv("POSTGRES_USER", "heatmap")
DB_PASS = os.getenv("POSTGRES_PASSWORD", "heatmap")
DATABASE_URL = f"postgresql+asyncpg://{DB_USER}:{DB_PASS}@{DB_HOST}:{DB_PORT}/{DB_NAME}"

engine = create_async_engine(DATABASE_URL, future=True, echo=False)


@task
async def purge_old_data(signals_keep_days: int = 30, scores_keep_days: int = 60) -> dict:
    """Hard purge old rows beyond retention windows (extra safety on top of Timescale policies).
    Use with caution. Defaults to conservative values matching init.sql policies.
    """
    async with engine.begin() as conn:
        del_signals = await conn.execute(
            text("DELETE FROM signals WHERE ts < NOW() - make_interval(days => :d)"),
            {"d": signals_keep_days},
        )
        del_scores = await conn.execute(
            text("DELETE FROM scores WHERE ts < NOW() - make_interval(days => :d)"),
            {"d": scores_keep_days},
        )
    return {"signals_deleted": del_signals.rowcount or 0, "scores_deleted": del_scores.rowcount or 0}


@task
async def run_db_maintenance(analyze: bool = True, vacuum_full: bool = False) -> dict:
    """Run lightweight maintenance; avoid VACUUM FULL in production unless necessary.
    """
    cmds = []
    if analyze:
        cmds += [
            "ANALYZE entities;",
            "ANALYZE signals;",
            "ANALYZE scores;",
        ]
    if vacuum_full:
        cmds += [
            "VACUUM (FULL, VERBOSE, ANALYZE) signals;",
            "VACUUM (FULL, VERBOSE, ANALYZE) scores;",
        ]
    out: list[str] = []
    async with engine.begin() as conn:
        for c in cmds:
            await conn.execute(text(c))
            out.append(c)
    return {"executed": out}


@flow(name="storage-maintenance")
async def storage_maintenance(signals_keep_days: int = 30, scores_keep_days: int = 60, vacuum_full: bool = False):
    logger = get_run_logger()
    purged = await purge_old_data(signals_keep_days, scores_keep_days)
    maint = await run_db_maintenance(analyze=True, vacuum_full=vacuum_full)
    logger.info(f"Purged={purged} Maint={maint}")
    return {"purged": purged, "maintenance": maint}
