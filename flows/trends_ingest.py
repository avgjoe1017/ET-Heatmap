from __future__ import annotations

import os
from datetime import datetime, timezone, timedelta
from typing import List, Dict

from prefect import flow, task, get_run_logger
from sqlalchemy import text
from pytrends.request import TrendReq
import pandas as pd

from libs.db import conn_ctx, insert_signal
from libs.rate import TokenBucket
from libs.audit import audit_event

REGION = os.getenv("TRENDS_REGION", "US")
CATEGORY = int(os.getenv("TRENDS_CATEGORY", "3"))  # 3 ~ Entertainment
CACHE_TTL_MIN = int(os.getenv("TRENDS_CACHE_TTL_MIN", "15"))


async def _entities() -> List[tuple[int, str]]:
    async with conn_ctx() as conn:
        rows = (await conn.execute(text("SELECT id, name FROM entities"))).fetchall()
        return [(int(r[0]), str(r[1])) for r in rows]


def _best_kw(name: str) -> str:
    return name


def _fetch_trends(py: TrendReq, kw: str) -> List[tuple[datetime, float]]:
    py.build_payload([kw], cat=CATEGORY, geo=REGION, timeframe="now 7-d")
    df = py.interest_over_time()
    if df.empty:
        return []
    s = df[kw]
    s = s.tail(24)
    idx = pd.to_datetime(s.index, utc=True)
    out: List[tuple[datetime, float]] = []
    for ts, val in zip(idx, s.values):
        out.append((ts.to_pydatetime(), float(val)))
    return out


@task
async def ingest_trends() -> int:
    py = TrendReq(hl="en-US", tz=360)
    bucket = TokenBucket(key="trends:pytrends", rate=20, interval=60, burst=10, redis_url=os.getenv("REDIS_URL"))

    e = await _entities()
    inserted = 0
    async with conn_ctx() as conn:
        for eid, name in e:
            if not await bucket.acquire(1):
                continue
            kw = _best_kw(name)
            try:
                points = _fetch_trends(py, kw)
                for ts, val in points:
                    await insert_signal(conn, eid, "trends", ts, "interest", float(val))
                if points:
                    inserted += 1
                await audit_event("trends", "inserted_interest", status=200, extra={"entity_id": eid, "points": len(points)})
            except Exception as ex:
                await audit_event("trends", "fetch_failed", level="warning", extra={"entity_id": eid, "error": str(ex)})
    return inserted


@flow(name="trends-ingest")
def run_trends_ingest():
    return ingest_trends.submit()
