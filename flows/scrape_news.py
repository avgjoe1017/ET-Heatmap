from __future__ import annotations
import os, asyncio, pandas as pd, httpx, re
from datetime import datetime, timezone
from sqlalchemy import text
from prefect import flow, task, get_run_logger

from libs.db import conn_ctx, insert_signal
from libs.config import is_enabled

SCRAPERAPI_KEY = os.getenv("SCRAPERAPI_KEY", "").strip()
SCRAPER_URL = "https://api.scraperapi.com"

HEADERS = {"User-Agent": "ET-Heatmap/1.0 (+internal)"}


def norm_name_for_regex(name: str) -> str:
    return r"\b" + re.escape(name) + r"\b"


async def fetch_via_scraperapi(url: str) -> str:
    if not SCRAPERAPI_KEY:
        return ""
    params = {"api_key": SCRAPERAPI_KEY, "url": url, "render": "true"}
    async with httpx.AsyncClient(timeout=30, headers=HEADERS) as client:
        r = await client.get(SCRAPER_URL, params=params)
        if r.status_code != 200:
            return ""
        return r.text or ""


async def count_mentions_in_sources(name: str, sources_csv: str = "configs/news_sources.csv") -> int:
    df = pd.read_csv(sources_csv)
    pattern = re.compile(norm_name_for_regex(name), flags=re.IGNORECASE)
    total = 0
    for url in df["url"].tolist():
        html = await fetch_via_scraperapi(url)
        if not html:
            continue
        hits = len(pattern.findall(html))
        total += hits
    return total


@task
async def scrape_news_topn(top_n: int = 8) -> int:
    """Pull current top entities from scores and count news-page mentions."""
    if not is_enabled("scrape_news") or not SCRAPERAPI_KEY:
        return 0
    logger = get_run_logger()
    now = datetime.now(timezone.utc)
    async with conn_ctx() as conn:
        q = text(
            """
          WITH latest AS (
            SELECT s.entity_id, MAX(s.ts) AS ts
            FROM scores s
            WHERE s.ts >= NOW() - INTERVAL '7 days'
            GROUP BY s.entity_id
          )
          SELECT e.id, e.name FROM scores s
          JOIN latest l ON l.entity_id=s.entity_id AND l.ts=s.ts
          JOIN entities e ON e.id=s.entity_id
          ORDER BY s.heat DESC
          LIMIT :n
        """
        )
        rows = (await conn.execute(q, {"n": top_n})).fetchall()
        inserted = 0
        for eid, name in rows:
            mentions = await count_mentions_in_sources(name)
            await insert_signal(conn, eid, "scrape_news", now, "mentions", float(mentions))
            inserted += 1
        logger.info(f"scrape_news inserted signals for {inserted} entities.")
        return inserted


@flow(name="scrape-news")
def run_scrape_news():
    return scrape_news_topn.submit()
