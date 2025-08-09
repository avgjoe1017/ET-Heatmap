from __future__ import annotations
import os, json, asyncio
from datetime import datetime, timedelta, timezone
import pandas as pd
import httpx
from dateutil import tz
from rapidfuzz import fuzz, process
from pytrends.request import TrendReq
from prefect import flow, task, get_run_logger
from sqlalchemy import text

from libs.db import conn_ctx, upsert_entity, insert_signal, insert_score, engine as async_engine
from libs.scoring import zscore, acceleration, novelty, heat_lite, tentpole_boost

CONFIG_ENTITIES = "configs/entities.csv"
CONFIG_TENTPOLES = "configs/tentpoles.csv"

WIKI_ENDPOINT = "https://wikimedia.org/api/rest_v1/metrics/pageviews/per-article/{project}/{access}/{agent}/{article}/{granularity}/{start}/{end}"

def load_entities_df() -> pd.DataFrame:
    df = pd.read_csv(CONFIG_ENTITIES)
    def parse_aliases(x):
        try:
            return json.loads(x) if isinstance(x, str) else []
        except Exception:
            return []
    df["aliases"] = df["aliases"].apply(parse_aliases)
    return df

def load_tentpoles_df() -> pd.DataFrame:
    df = pd.read_csv(CONFIG_TENTPOLES)
    df["start_date"] = pd.to_datetime(df["start_date"]).dt.date
    df["end_date"] = pd.to_datetime(df["end_date"]).dt.date
    return df

async def fetch_wiki_series(article: str, lang="en", days=30) -> pd.Series:
    end = datetime.utcnow().date()
    start = end - timedelta(days=days)
    url = WIKI_ENDPOINT.format(
        project=f"{lang}.wikipedia",
        access="all-access",
        agent="user",
        article=article.replace(" ", "_"),
        granularity="daily",
        start=start.strftime("%Y%m%d"),
        end=end.strftime("%Y%m%d")
    )
    async with httpx.AsyncClient(timeout=30) as client:
        r = await client.get(url)
        if r.status_code != 200:
            return pd.Series(dtype=float)
        items = r.json().get("items", [])
        idx = [datetime.strptime(i["timestamp"][:8], "%Y%m%d").replace(tzinfo=timezone.utc) for i in items]
        vals = [i["views"] for i in items]
        return pd.Series(vals, index=idx, dtype=float)

def fetch_trends_series(pytrends: TrendReq, kw: str, days=30) -> pd.Series:
    timeframe = f"now {days}-d"
    pytrends.build_payload([kw], cat=0, timeframe=timeframe, geo="US", gprop="")
    df = pytrends.interest_over_time()
    if df.empty:
        return pd.Series(dtype=float)
    s = df[kw]
    s.index = s.index.tz_localize(timezone.utc) if s.index.tz is None else s.index
    return s.resample("1D").mean()

def best_keyword(name: str, aliases: list[str]) -> str:
    candidates = [name] + (aliases or [])
    candidates = sorted(candidates, key=lambda x: (-len(x), x))
    return candidates[0]

@task
async def ingest_once() -> int:
    logger = get_run_logger()
    entities = load_entities_df()
    tentpoles = load_tentpoles_df()
    inserted = 0

    pytrends = TrendReq(hl="en-US", tz=360)
    now = datetime.utcnow().replace(tzinfo=timezone.utc)

    async with conn_ctx() as conn:
        for _, row in entities.iterrows():
            eid = await upsert_entity(conn, row["name"], row["type"], row["aliases"], row.get("wiki_id"))
            wiki_series = pd.Series(dtype=float)
            if pd.notna(row.get("wiki_id")):
                wiki_series = await fetch_wiki_series(article=row["name"])
                if not wiki_series.empty:
                    for ts, val in wiki_series.tail(3).items():
                        await insert_signal(conn, eid, "wiki", ts, "views", float(val))
            kw = best_keyword(row["name"], row["aliases"])
            trends_series = fetch_trends_series(pytrends, kw)
            if not trends_series.empty:
                for ts, val in trends_series.tail(3).items():
                    await insert_signal(conn, eid, "trends", ts, "interest", float(val))
            df = pd.read_sql_query(text("""
                SELECT source, ts, value FROM signals
                WHERE entity_id = :eid AND ts >= NOW() - INTERVAL '35 days'
                ORDER BY ts
            """), async_engine.sync_engine, params={"eid": eid})
            if df.empty:
                continue
            w = df[df["source"]=="wiki"].set_index("ts")["value"] if (df["source"]=="wiki").any() else pd.Series(dtype=float)
            t = df[df["source"]=="trends"].set_index("ts")["value"] if (df["source"]=="trends").any() else pd.Series(dtype=float)

            zt = zscore(t) if not t.empty else 0.0
            zw = zscore(w) if not w.empty else 0.0
            acc = 0.5*(acceleration(t) if not t.empty else 0.0) + 0.5*(acceleration(w) if not w.empty else 0.0)
            nov = 0.5*(novelty(t) if not t.empty else 0.0) + 0.5*(novelty(w) if not w.empty else 0.0)
            tent = tentpole_boost(now, tentpoles, row["name"])

            heat, comps = heat_lite(zt, zw, acc, nov, tent, et_fit=0.6, decay=0.0, risk=0.0)
            await insert_score(conn, eid, now, comps)
            inserted += 1

    return inserted

@flow(name="wiki-trends-ingest")
def run_ingest():
    return ingest_once.submit()
