from __future__ import annotations

import os
from datetime import datetime, timezone
from typing import List

import httpx
from prefect import flow, task, get_run_logger
from sqlalchemy import text

from libs.db import conn_ctx
from libs.health import is_circuit_open, record_source_ok, record_source_error
from libs.audit import audit_event
from libs.rate import TokenBucket

TRADE_URLS = [
    os.getenv("VARIETY_RSS", "https://variety.com/feed/"),
    os.getenv("THR_RSS", "https://www.hollywoodreporter.com/feeds/rss"),
]
TRADE_URLS = [u for u in TRADE_URLS if u]

USER_AGENT = os.getenv("RSS_USER_AGENT", "ET-Heatmap/1.0 (RSS)")
TIMEOUT = int(os.getenv("RSS_TIMEOUT", "20"))


async def _fetch_feed(client: httpx.AsyncClient, url: str) -> str:
    r = await client.get(url, headers={"User-Agent": USER_AGENT})
    r.raise_for_status()
    return r.text


def _extract_mentions(feed_xml: str) -> List[tuple[str, datetime]]:
    # Minimal parse: rely on existing entity names matching in text
    # For robustness, a real feed parser can be used later.
    from xml.etree import ElementTree as ET
    root = ET.fromstring(feed_xml)
    items = []
    for item in root.iterfind(".//item"):
        title = (item.findtext("title") or "")
        pub = item.findtext("pubDate")
        try:
            ts = datetime.fromtimestamp(datetime.strptime(pub, "%a, %d %b %Y %H:%M:%S %z").timestamp(), tz=timezone.utc) if pub else datetime.now(timezone.utc)
        except Exception:
            ts = datetime.now(timezone.utc)
        items.append((title, ts))
    return items


async def _entity_map():
    async with conn_ctx() as conn:
        rows = (await conn.execute(text("SELECT id, name FROM entities"))).fetchall()
        return {str(r[1]).lower(): int(r[0]) for r in rows}


async def _fetch_all_feeds(urls: List[str]) -> List[str]:
    texts: List[str] = []
    bucket = TokenBucket(key="trades:fetch", rate=12, interval=60, burst=6, redis_url=os.getenv("REDIS_URL"))
    async with httpx.AsyncClient(timeout=TIMEOUT) as client:
        for url in urls:
            try:
                if not await bucket.acquire(1):
                    await audit_event("trades", "rate_limited_skip", extra={"url": url})
                    continue
                xml = await _fetch_feed(client, url)
                texts.append(xml)
                await audit_event("trades", "fetched_feed", status=200, extra={"url": url})
            except Exception as e:
                await audit_event("trades", "fetch_failed", level="warning", extra={"url": url, "error": str(e)})
    return texts


def _detect_source_hint(xml: str) -> str:
    head = xml[:512].lower()
    if "variety" in head:
        return "variety"
    if "hollywoodreporter" in head or "hollywood reporter" in head:
        return "thr"
    return "trade"


def _match_mentions(xml_list: List[str], ent_map: dict[str, int]) -> List[tuple[int, datetime, str]]:
    out: List[tuple[int, datetime, str]] = []
    for xml in xml_list:
        src_hint = _detect_source_hint(xml)
        for title, ts in _extract_mentions(xml):
            low = title.lower()
            for name_low, eid in ent_map.items():
                if name_low in low:
                    out.append((eid, ts, src_hint))
    return out


@task
async def ingest_trade_rss() -> int:
    logger = get_run_logger()
    if await is_circuit_open("trades"):
        await audit_event("trades", "circuit_open_skip")
        return 0

    texts = await _fetch_all_feeds(TRADE_URLS)
    if not texts:
        await record_source_error("trades")
        return 0

    ent_map = await _entity_map()
    mentions = _match_mentions(texts, ent_map)

    inserted = 0
    async with conn_ctx() as conn:
        for eid, ts, src in mentions:
            await conn.execute(
                text(
                    """
                    INSERT INTO trade_mentions(entity_id, source, first_seen_ts)
                    VALUES (:eid, :src, :ts)
                    ON CONFLICT (entity_id, source)
                    DO NOTHING
                    """
                ),
                {"eid": eid, "src": src, "ts": ts},
            )
            inserted += 1

    await record_source_ok("trades")
    await audit_event("trades", "inserted_mentions", extra={"count": inserted})
    logger.info(f"trades: inserted {inserted} mentions")
    return inserted


@flow(name="trade-rss-ingest")
def run_trade_rss_ingest():
    return ingest_trade_rss.submit()
