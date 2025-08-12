from __future__ import annotations

import os
from datetime import datetime, timezone
from typing import List

import httpx
from prefect import flow, task, get_run_logger
from sqlalchemy import text
from sqlalchemy.ext.asyncio import create_async_engine

DB_HOST = os.getenv("POSTGRES_HOST", "db")
DB_PORT = os.getenv("POSTGRES_PORT", "5432")
DB_NAME = os.getenv("POSTGRES_DB", "heatmap")
DB_USER = os.getenv("POSTGRES_USER", "heatmap")
DB_PASS = os.getenv("POSTGRES_PASSWORD", "heatmap")
DATABASE_URL = f"postgresql+asyncpg://{DB_USER}:{DB_PASS}@{DB_HOST}:{DB_PORT}/{DB_NAME}"

SLACK_WEBHOOK_URL = os.getenv("SLACK_WEBHOOK_URL", "").strip()

engine = create_async_engine(DATABASE_URL, future=True, echo=False)

# Gates to alert eligibility per MVP
VEL_GATE = 2.5
SPREAD_GATE = 2.0/3.0
PERSIST_POLLS = 2
DEBOUNCE_HOURS = float(os.getenv("ALERT_DEBOUNCE_HOURS", "6"))
REBOOST_FACTOR = float(os.getenv("ALERT_REBOOST_FACTOR", "1.3"))
TRADE_SUPPRESS_AFTER_HOURS = float(os.getenv("TRADE_SUPPRESS_AFTER_HOURS", "24"))


async def _blocked_alert(conn, eid: int, heat: float) -> bool:
    st = (await conn.execute(text("SELECT last_alert_ts, last_alert_heat, prior_peak_heat FROM trend_state WHERE entity_id=:id"), {"id": eid})).first()
    last_ts = st[0] if st else None
    last_heat = float(st[1]) if st and st[1] is not None else None
    prior_peak = float(st[2]) if st and st[2] is not None else 0.0
    # Debounce
    if last_ts is not None:
        recent = (datetime.now(timezone.utc) - last_ts).total_seconds() < DEBOUNCE_HOURS * 3600
        if recent and (last_heat is None or heat < (last_heat * REBOOST_FACTOR)):
            return True
    # Suppression
    trade_min = (await conn.execute(text("SELECT MIN(first_seen_ts) FROM trade_mentions WHERE entity_id=:id"), {"id": eid})).scalar()
    if trade_min is not None:
        hours_since_trade = (datetime.now(timezone.utc) - trade_min).total_seconds() / 3600.0
        if hours_since_trade > TRADE_SUPPRESS_AFTER_HOURS and heat < prior_peak:
            return True
    return False


async def _latest_scores(conn):
    q_latest = text(
        """
        WITH latest AS (
          SELECT s.entity_id, MAX(s.ts) AS ts
          FROM scores s
          WHERE s.ts >= NOW() - INTERVAL '2 hours'
          GROUP BY s.entity_id
        )
        SELECT s.entity_id, s.ts, s.velocity_z, s.xplat AS spread, s.heat
        FROM scores s
        JOIN latest l ON l.entity_id=s.entity_id AND l.ts=s.ts
        """
    )
    return (await conn.execute(q_latest)).fetchall()


async def _update_trend_state(conn, eid: int, ts, passes_gate: bool):
    if passes_gate:
        upd = text(
            """
            INSERT INTO trend_state(entity_id, last_gate_pass_ts, consecutive_passes, last_alert_ts)
            VALUES (:eid, :ts, 1, NULL)
            ON CONFLICT (entity_id)
            DO UPDATE SET
              last_gate_pass_ts = EXCLUDED.last_gate_pass_ts,
              consecutive_passes = CASE WHEN trend_state.last_gate_pass_ts IS NOT NULL AND EXCLUDED.last_gate_pass_ts > trend_state.last_gate_pass_ts
                                       THEN trend_state.consecutive_passes + 1 ELSE 1 END
            RETURNING consecutive_passes
            """
        )
        res = (await conn.execute(upd, {"eid": int(eid), "ts": ts})).fetchone()
        return int(res[0]) if res else 0
    else:
        await conn.execute(
            text("INSERT INTO trend_state(entity_id, consecutive_passes) VALUES (:eid, 0) ON CONFLICT (entity_id) DO UPDATE SET consecutive_passes=0"),
            {"eid": int(eid)},
        )
        return 0


async def find_eligible_alerts(limit: int = 5):
    # Evaluate last poll, update persistence in trend_state, and return entities that just crossed the 2-pass threshold
    async with engine.begin() as conn:
        rows = await _latest_scores(conn)

        # Update trend_state per entity
        to_alert: list[tuple[int, str, float]] = []
        for eid, ts, vel, spread, heat in rows:
            passes_gate = (vel or 0) >= VEL_GATE and (spread or 0) >= SPREAD_GATE
            consec = await _update_trend_state(conn, int(eid), ts, passes_gate)
            if consec >= PERSIST_POLLS and passes_gate:
                name_row = (await conn.execute(text("SELECT name FROM entities WHERE id=:id"), {"id": int(eid)})).fetchone()
                if name_row:
                    to_alert.append((int(eid), str(name_row[0]), float(heat or 0)))

        filtered = await _apply_debounce_and_suppression(conn, to_alert)
        return filtered[:limit]


async def _apply_debounce_and_suppression(conn, to_alert: list[tuple[int, str, float]]):
    out: list[tuple[int, str, float]] = []
    for eid, name, heat in sorted(to_alert, key=lambda x: x[2], reverse=True):
        if await _blocked_alert(conn, eid, heat):
            continue
        out.append((eid, name, heat))
    return out


def mk_alert_block(name: str, eid: int | None = None) -> list:
    header = {"type": "header", "text": {"type": "plain_text", "text": f"[ALERT] {name} spiking now"}}
    why = {"type": "section", "text": {"type": "mrkdwn", "text": f"Velocity +{VEL_GATE}σ | Spread {int(SPREAD_GATE*3)}/3 | Affect ? | Confidence 0.8"}}
    actions = {
        "type": "actions",
        "elements": [
            {"type": "button", "text": {"type": "plain_text", "text": "Assign Producer"}, "action_id": "assign_producer", "value": f"eid:{eid}|name:{name}" if eid else None},
            {"type": "button", "text": {"type": "plain_text", "text": "Create Rundown Card"}, "action_id": "create_rundown", "value": f"eid:{eid}|name:{name}" if eid else None},
            {"type": "button", "text": {"type": "plain_text", "text": "Promo Tease"}, "action_id": "promo_tease", "value": f"eid:{eid}|name:{name}" if eid else None},
            {"type": "button", "text": {"type": "plain_text", "text": "Open Research Pack"}, "action_id": "open_pack", "value": f"eid:{eid}|name:{name}" if eid else None},
        ],
    }
    return [header, why, actions]


async def post_alerts(rows: List[tuple]):
    if not SLACK_WEBHOOK_URL:
        return 0
    async with httpx.AsyncClient(timeout=20) as client:
        sent = 0
        async with engine.begin() as conn:
            for (eid, name, heat) in rows:
                # Compute lead-time
                trade_min = (await conn.execute(text("SELECT MIN(first_seen_ts) FROM trade_mentions WHERE entity_id=:id"), {"id": int(eid)})).scalar()
                alert_ts = datetime.now(timezone.utc)
                if trade_min is None:
                    pre_trade = True
                    lt_min = None
                else:
                    pre_trade = False
                    lt_min = int((alert_ts - trade_min).total_seconds() // 60)

                # Persist alert
                await conn.execute(
                    text(
                        """
                        INSERT INTO alerts(entity_id, alert_ts, heat, reasons, pre_trade, lead_time_minutes)
                        VALUES (:eid, :ts, :heat, :reasons, :pre_trade, :lead)
                        """
                    ),
                    {"eid": int(eid), "ts": alert_ts, "heat": float(heat), "reasons": f"debounce={DEBOUNCE_HOURS}h, reboost={REBOOST_FACTOR}", "pre_trade": pre_trade, "lead": lt_min},
                )

                # Update trend_state last alert fields and prior_peak
                await conn.execute(
                    text("UPDATE trend_state SET last_alert_ts=:ts, last_alert_heat=:heat, prior_peak_heat=GREATEST(COALESCE(prior_peak_heat,0), :heat) WHERE entity_id=:id"),
                    {"ts": alert_ts, "heat": float(heat), "id": int(eid)}
                )

                # Slack send
                blocks = mk_alert_block(str(name), int(eid))
                await client.post(SLACK_WEBHOOK_URL, json={"blocks": blocks})
                sent += 1
        return sent


@flow(name="actionable-alerts")
async def run_actionable_alerts(limit: int = 5):
    rows = await find_eligible_alerts(limit)
    sent = await post_alerts(rows)
    return int(sent or 0)
