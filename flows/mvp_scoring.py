from __future__ import annotations

from datetime import datetime, timezone
from typing import Dict, Optional

from prefect import flow, task, get_run_logger
from sqlalchemy import text

from libs.db import conn_ctx
from libs.scoring_mvp import compute_heat, platform_spread, map_tone_to_affect, hours_since


@task
async def score_all_mvp() -> int:
    logger = get_run_logger()
    now = datetime.now(timezone.utc)
    async with conn_ctx() as conn:
        # Iterate over entities with a recent score and recompute HEAT per MVP using latest velocity_z
        q = text(
            """
            WITH latest AS (
              SELECT s.entity_id, MAX(s.ts) AS ts
              FROM scores s
              WHERE s.ts >= NOW() - INTERVAL '7 days'
              GROUP BY s.entity_id
            )
            SELECT s.entity_id, s.velocity_z
            FROM scores s
            JOIN latest l ON l.entity_id=s.entity_id AND l.ts=s.ts
            """
        )
        rows = (await conn.execute(q)).fetchall()
        updated = 0
        for eid, velocity_z in rows:
            eid = int(eid)
            v = float(velocity_z or 0.0)
            active = {
                'reddit': (await _exists(conn, eid, 'reddit')),
                'trends': True if v else (await _exists(conn, eid, 'trends')),
                'tiktok': (await _exists(conn, eid, 'tt_search')) or (await _exists(conn, eid, 'tt_cc')) or (await _exists(conn, eid, 'apify_tiktok')),
            }
            spread = platform_spread(active)
            mentions, tone, _ = await _gdelt(conn, eid)
            affect = map_tone_to_affect(tone, float(mentions))
            peak_ts = await _latest_peak_ts(conn, eid)
            hs_peak = hours_since(peak_ts)
            comps = compute_heat(v, spread, affect, hs_peak)
            reasons = f"v={v:.2f}; spread={spread:.2f}; affect={affect:.2f}; hours_since_peak={hs_peak if hs_peak is not None else 'na'}"
            await conn.execute(
                text(
                    """
                    INSERT INTO scores (entity_id, ts, velocity_z, accel, xplat, affect, novelty, et_fit, tentpole, decay, risk, heat, reasons)
                    VALUES (:eid, :ts, :velocity_z, NULL, :spread, :affect, NULL, NULL, NULL, :decay, NULL, :heat, :reasons)
                    """
                ),
                {
                    'eid': eid,
                    'ts': now,
                    'velocity_z': comps.velocity_z,
                    'spread': comps.spread,
                    'affect': affect,
                    'decay': comps.freshness_decay,
                    'heat': comps.heat,
                    'reasons': reasons,
                },
            )
            updated += 1
    logger.info(f"mvp_scoring: updated {updated} rows")
    return updated


async def _exists(conn, eid: int, source: str) -> bool:
    q = text("SELECT 1 FROM signals WHERE entity_id=:eid AND source=:src AND ts >= NOW() - INTERVAL '24 hours' LIMIT 1")
    return (await conn.execute(q, {'eid': eid, 'src': source})).first() is not None


async def _latest(conn, eid: int, source: str, metric: str) -> Optional[float]:
    # Expect we inserted z already for trends/wiki; if not, fallback to last raw value normalized elsewhere
    q = text(
        """
        SELECT value FROM signals
        WHERE entity_id=:eid AND source=:src AND metric=:metric
        ORDER BY ts DESC
        LIMIT 1
        """
    )
    row = (await conn.execute(q, {'eid': eid, 'src': source, 'metric': metric})).first()
    return float(row[0]) if row else None


async def _gdelt(conn, eid: int):
    q = text(
        """
        SELECT metric, value, ts
        FROM signals
        WHERE entity_id=:eid AND source='gdelt_gkg'
        ORDER BY ts DESC
        LIMIT 10
        """
    )
    rows = (await conn.execute(q, {'eid': eid})).fetchall()
    mentions = 0.0
    tone = None
    tone_ts = None
    for m, v, ts in rows:
        if m == 'gkg_mentions' and mentions == 0:
            mentions = float(v)
        if m == 'gkg_tone_avg' and tone is None:
            tone = float(v)
            tone_ts = ts
    return mentions, tone, tone_ts


async def _latest_peak_ts(conn, eid: int):
    q = text(
        """
        SELECT ts
        FROM scores
        WHERE entity_id=:eid AND ts >= NOW() - INTERVAL '48 hours'
        ORDER BY heat DESC NULLS LAST, ts DESC
        LIMIT 1
        """
    )
    row = (await conn.execute(q, {'eid': eid})).first()
    return row[0] if row else None


@flow(name="mvp-scoring")
def run_mvp_scoring():
    return score_all_mvp.submit()
