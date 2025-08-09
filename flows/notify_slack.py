from __future__ import annotations
import os, asyncio
from datetime import datetime, timezone, timedelta
from typing import List, Tuple
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

STALE_THRESHOLD_HOURS = 6  # if last score older than this → degraded label

THRESH_Z = 0.8
THRESH_TIKTOK = 3
THRESH_TIKTOK_SEARCH_HITS = 3
THRESH_TIKTOK_CC_Z = 0.8
THRESH_NEWS = 1


async def fetch_latest_rows(limit: int = 10):
    async with engine.connect() as conn:
        q = text(
            """
          WITH latest AS (
            SELECT s.entity_id, MAX(s.ts) AS ts
            FROM scores s
            WHERE s.ts >= NOW() - INTERVAL '7 days'
            GROUP BY s.entity_id
          )
          SELECT e.id, e.name, s.heat, s.velocity_z, s.accel, s.xplat, s.tentpole, s.ts
          FROM scores s
          JOIN latest l ON l.entity_id = s.entity_id AND l.ts = s.ts
          JOIN entities e ON e.id = s.entity_id
          ORDER BY s.heat DESC
          LIMIT :limit
        """)
        rows = (await conn.execute(q, {"limit": limit})).fetchall()
        return rows


async def fetch_signal_map(eid: int):
    async with engine.connect() as conn:
        q = text(
            """
          SELECT source, metric, value
          FROM signals
          WHERE entity_id = :eid AND ts >= NOW() - INTERVAL '48 hours'
        """
        )
        rows = (await conn.execute(q, {"eid": eid})).fetchall()
        sig: dict = {}
        for src, metric, val in rows:
            sig.setdefault(src, {})[metric] = float(val)
    return sig


def compute_confidence(vz: float | None, sig: dict) -> str:
    has_trends = vz is not None and vz >= THRESH_Z
    has_wiki = False
    if "wiki" in sig:
        # conservative MVP assumption
        has_wiki = has_trends

    has_news = sig.get("scrape_news", {}).get("mentions", 0) >= THRESH_NEWS
    # Legacy TikTok metric
    has_tiktok_legacy = sig.get("apify_tiktok", {}).get("hits", 0) >= THRESH_TIKTOK
    # New TikTok metrics
    has_tt_search = sig.get("tt_search", {}).get("hits_24h", 0) >= THRESH_TIKTOK_SEARCH_HITS
    has_tt_cc = sig.get("tt_cc", {}).get("hashtag_score", 0.0) >= THRESH_TIKTOK_CC_Z

    if has_news and (has_trends or has_wiki):
        return "Verified"
    sources_true = sum([has_trends, has_wiki, has_news, has_tiktok_legacy or has_tt_search or has_tt_cc])
    if sources_true >= 2:
        return "Multi-source"
    return "Unverified"


def mk_blocks(items: List[dict], degraded: bool) -> list:
    header = ":fire: *ET Daily Heatmap — Top 10*"
    if degraded:
        header += "  _(degraded: using last good shortlist)_"
    blocks = [{"type": "section", "text": {"type": "mrkdwn", "text": header}}]
    lines = []
    for i, it in enumerate(items, 1):
        rsn = "; ".join(it["reasons"]) if it["reasons"] else "—"
        badge = it.get("confidence", "Unverified")
        lines.append(
            f"*{i}. {it['entity']}*  •  `{badge}`  •  heat `{round(it['heat'],2)}`  •  _{rsn}_"
        )
    if not lines:
        lines = ["_No items available_"]
    blocks.append({"type": "section", "text": {"type": "mrkdwn", "text": "\n".join(lines)}})
    return blocks


def reasons_from(v: float | None, a: float | None, x: float | None, tp: float | None, sig: dict) -> list:
    reasons: list[str] = []
    if v and v > THRESH_Z:
        reasons.append("High velocity vs 30-day baseline")
    if a and a > 0:
        reasons.append("Acceleration positive")
    if x and x >= 1.0:
        reasons.append("Cross-platform confirmation")
    if tp and tp > 0:
        reasons.append("Tentpole boost active")
    if sig.get("scrape_news", {}).get("mentions", 0) >= THRESH_NEWS:
        reasons.append("In headlines today")
    if sig.get("apify_tiktok", {}).get("hits", 0) >= THRESH_TIKTOK:
        reasons.append("Rising on TikTok (legacy)")
    if sig.get("tt_cc", {}).get("hashtag_score", 0) >= THRESH_TIKTOK_CC_Z:
        reasons.append("TikTok trending (global)")
    if sig.get("tt_search", {}).get("hits_24h", 0) >= THRESH_TIKTOK_SEARCH_HITS:
        reasons.append("TikTok rising (watchlist)")
    # Optional TikTok breakdown hints if present
    tt = sig.get("tt_search", {})
    if tt:
        if "unique_authors_24h" in tt:
            reasons.append(f"TT authors {int(tt['unique_authors_24h'])}")
        if "view_vel_median" in tt:
            reasons.append(f"TT vVel {round(float(tt['view_vel_median'])/1000.0,1)}k/h")
    return reasons


async def build_items_with_confidence(limit: int = 10):
    rows = await fetch_latest_rows(limit)
    if not rows:
        return [], None
    latest_ts = max(r[-1] for r in rows)
    items = []
    for (eid, name, heat, v, a, x, tp, ts) in rows:
        sig = await fetch_signal_map(eid)
        conf = compute_confidence(v, sig)
        reasons = reasons_from(v, a, x, tp, sig)
        items.append(
            {
                "entity": name,
                "heat": float(heat),
                "reasons": reasons,
                "confidence": conf,
            }
        )
    return items, latest_ts

@task
async def post_to_slack(items: List[dict], degraded: bool):
    if not SLACK_WEBHOOK_URL:
        raise RuntimeError("SLACK_WEBHOOK_URL is not set.")
    payload = {"blocks": mk_blocks(items, degraded)}
    async with httpx.AsyncClient(timeout=20) as client:
        r = await client.post(SLACK_WEBHOOK_URL, json=payload)
        r.raise_for_status()

@flow(name="notify-slack")
async def notify_slack():
    logger = get_run_logger()
    items, latest_ts = await build_items_with_confidence()
    if latest_ts is None:
        logger.warning("No scores found — posting empty degraded message.")
        await post_to_slack([], True)
        return "posted-degraded-empty"

    age = datetime.now(timezone.utc) - latest_ts
    degraded = age > timedelta(hours=STALE_THRESHOLD_HOURS)
    await post_to_slack(items, degraded)
    return "posted"
