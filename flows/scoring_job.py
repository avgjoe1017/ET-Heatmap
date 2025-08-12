from __future__ import annotations

from datetime import datetime, timezone
from typing import Dict, Any, List

from prefect import flow, task, get_run_logger
from sqlalchemy import text
from sqlalchemy.ext.asyncio import create_async_engine

from libs.scoring_advanced import AdvancedScoringEngine

import os

DB_HOST = os.getenv("POSTGRES_HOST", "db")
DB_PORT = os.getenv("POSTGRES_PORT", "5432")
DB_NAME = os.getenv("POSTGRES_DB", "heatmap")
DB_USER = os.getenv("POSTGRES_USER", "heatmap")
DB_PASS = os.getenv("POSTGRES_PASSWORD", "heatmap")
DATABASE_URL = f"postgresql+asyncpg://{DB_USER}:{DB_PASS}@{DB_HOST}:{DB_PORT}/{DB_NAME}"

engine = create_async_engine(DATABASE_URL, future=True, echo=False)
_adv = AdvancedScoringEngine()


def _accumulate_signal(signals: Dict[str, Any], src: str, metric: str, value: float) -> None:
    ts_map = {("wiki", "views"): "wiki_pageviews", ("trends", "interest"): "trends_interest"}
    key = ts_map.get((src, metric))
    if key:
        arr = signals.setdefault(key, [])
        if isinstance(arr, list) and len(arr) < 90:
            arr.append(float(value))
        return
    if src == "gdelt_gkg":
        if metric == "gkg_mentions":
            signals["gkg_mentions"] = float(value)
        elif metric == "gkg_tone_avg":
            signals["gkg_tone_avg"] = float(value)
        return
    if src in {"tt_search", "apify_tiktok", "tt_cc"}:
        bucket = signals.setdefault("tiktok_data", {})
        if isinstance(bucket, dict):
            bucket[metric] = float(value)


@task
async def fetch_entities() -> List[Dict[str, Any]]:
    async with engine.connect() as conn:
        rows = (await conn.execute(text("SELECT id, name FROM entities WHERE name IS NOT NULL"))).fetchall()
    return [{"id": r[0], "name": r[1]} for r in rows]


@task
async def fetch_signals_for_entity(eid: int, hours: int = 72) -> Dict[str, Any]:
    async with engine.connect() as conn:
        q = text(
            """
            SELECT source, metric, value
            FROM signals
            WHERE entity_id=:eid AND ts >= NOW() - make_interval(hours => :hours)
            ORDER BY ts ASC
            """
        )
        rows = (await conn.execute(q, {"eid": eid, "hours": hours})).fetchall()
    sig: Dict[str, Any] = {}
    for src, metric, value in rows:
        _accumulate_signal(sig, src, metric, float(value))
    return sig


@task
async def persist_score(eid: int, when: datetime, result: Dict[str, Any]) -> None:
    comps = result.get("components", {})
    payload = {
        "velocity_z": float(comps.get("velocity", 0.0)),
        "accel": float(comps.get("acceleration", 0.0)),
        "xplat": float(comps.get("virality", 0.0)),
        "novelty": float(comps.get("novelty", 0.0)),
        "et_fit": float(comps.get("quality", 0.0)),
        "tentpole": 0.0,
        "decay": 0.0,
        "risk": 0.0,
        "heat": float(result.get("heat_score", 0.0)),
    }
    async with engine.begin() as conn:
        await conn.execute(
            text(
                """
                INSERT INTO scores (entity_id, ts, velocity_z, accel, xplat, novelty, et_fit, tentpole, decay, risk, heat)
                VALUES (:eid, :ts, :velocity_z, :accel, :xplat, :novelty, :et_fit, :tentpole, :decay, :risk, :heat)
                ON CONFLICT (entity_id, ts) DO UPDATE
                SET velocity_z=EXCLUDED.velocity_z, accel=EXCLUDED.accel, xplat=EXCLUDED.xplat,
                    novelty=EXCLUDED.novelty, et_fit=EXCLUDED.et_fit, tentpole=EXCLUDED.tentpole,
                    decay=EXCLUDED.decay, risk=EXCLUDED.risk, heat=EXCLUDED.heat
                """
            ),
            {"eid": eid, "ts": when, **payload},
        )


@task
async def score_entity(e: Dict[str, Any]) -> None:
    eid = e["id"]
    sig = await fetch_signals_for_entity(eid)
    result = _adv.calculate_multidimensional_heat_score(e["name"], sig)
    await persist_score(eid, datetime.now(timezone.utc), result)


@flow(name="advanced-scoring-hourly")
async def run_scoring_hourly(limit_entities: int | None = None):
    logger = get_run_logger()
    ents = await fetch_entities()
    if limit_entities:
        ents = ents[: int(limit_entities)]
    for e in ents:
        try:
            await score_entity(e)
        except Exception as ex:
            logger.warning(f"score failed for {e['name']}: {ex}")


@flow(name="scoring-backfill-once")
async def backfill_scoring(hours: int = 72):
    logger = get_run_logger()
    ents = await fetch_entities()
    now = datetime.now(timezone.utc)
    for e in ents:
        try:
            sig = await fetch_signals_for_entity(e["id"], hours=hours)
            result = _adv.calculate_multidimensional_heat_score(e["name"], sig)
            await persist_score(e["id"], now, result)
        except Exception as ex:
            logger.warning(f"backfill failed for {e['name']}: {ex}")
