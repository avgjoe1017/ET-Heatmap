from __future__ import annotations
import os
from datetime import datetime, timezone
from typing import Dict

from prefect import flow, task, get_run_logger
from sqlalchemy import text

from libs.db import conn_ctx, insert_score, sync_engine
from libs.scoring_tiktok import tiktok_component

TIKTOK_WEIGHT = float(os.getenv("TIKTOK_WEIGHT", "0.2"))


@task
async def rescore_entities_with_tiktok(tiktok_weight: float = TIKTOK_WEIGHT) -> int:
    """For each entity with a recent score, compute tiktok_z from last 30d tt_cc + tt_search,
    add weighted TikTok component to HEAT, and insert a new score row (no schema change).
    """
    logger = get_run_logger()
    now = datetime.now(timezone.utc)
    updated = 0

    # Use sync engine to avoid event loop issues when called inside Prefect
    with sync_engine.begin() as sconn:
        # Get each entity's latest score row timestamp within last 7 days
        q_latest = text(
            """
            WITH latest AS (
                SELECT s.entity_id, MAX(s.ts) AS ts
                FROM scores s
                WHERE s.ts >= NOW() - INTERVAL '7 days'
                GROUP BY s.entity_id
            )
            SELECT l.entity_id, l.ts
            FROM latest l
            ORDER BY l.ts DESC
            """
        )
        latest_rows = sconn.execute(q_latest).fetchall()
        if not latest_rows:
            return 0

        for (eid, _ts) in latest_rows:
            # Pull last 30d tt_cc + tt_search signals for this entity
            q_sig = text(
                """
                SELECT metric, value, ts, source
                FROM signals
                WHERE entity_id = :eid
                  AND source IN ('tt_cc','tt_search')
                  AND ts >= NOW() - INTERVAL '30 days'
                ORDER BY ts ASC
                """
            )
            rows = sconn.execute(q_sig, {"eid": eid}).fetchall()
            if not rows:
                continue

            df_cc: Dict[str, list] = {}
            df_search: Dict[str, list] = {}
            for metric, value, ts, source in rows:
                if source == "tt_cc":
                    df_cc.setdefault(metric, []).append(float(value))
                elif source == "tt_search":
                    df_search.setdefault(metric, []).append(float(value))

            if not df_cc and not df_search:
                continue

            tiktok_z, _ = tiktok_component(df_cc, df_search)

            # Fetch latest existing score components
            q_score = text(
                """
                SELECT velocity_z, accel, xplat, novelty, et_fit, tentpole, decay, risk, heat
                FROM scores
                WHERE entity_id = :eid
                ORDER BY ts DESC
                LIMIT 1
                """
            )
            srow = sconn.execute(q_score, {"eid": eid}).fetchone()
            if not srow:
                continue
            velocity_z, accel, xplat, novelty, et_fit, tentpole, decay, risk, heat = srow
            new_heat = float(heat) + float(tiktok_weight) * float(tiktok_z)

            comps = {
                "velocity_z": float(velocity_z),
                "accel": float(accel),
                "xplat": float(xplat),
                "novelty": float(novelty),
                "et_fit": float(et_fit),
                "tentpole": float(tentpole),
                "decay": float(decay),
                "risk": float(risk),
                "heat": float(new_heat),
            }
            # Insert via async helper in a tiny async block to keep API consistent
            # but we can also do it sync to keep same transaction
            sconn.execute(text("""
                INSERT INTO scores (entity_id, ts, velocity_z, accel, xplat, novelty, et_fit, tentpole, decay, risk, heat)
                VALUES (:eid, :ts, :velocity_z, :accel, :xplat, :novelty, :et_fit, :tentpole, :decay, :risk, :heat)
            """), {"eid": int(eid), "ts": now, **comps})
            updated += 1

    logger.info(f"rescore_tiktok: updated heat for {updated} entities (weight={tiktok_weight}).")
    return updated


@flow(name="rescore-tiktok")
def run_rescore_tiktok():
    return rescore_entities_with_tiktok.submit()
