from __future__ import annotations
import os, asyncio, httpx, json
import pandas as pd
from datetime import datetime, timezone
from sqlalchemy import text
from prefect import flow, task, get_run_logger

from libs.db import conn_ctx, insert_signal, upsert_entity
from libs.config import is_enabled

APIFY_TOKEN = os.getenv("APIFY_TOKEN", "").strip()
ACTOR_ID = os.getenv("APIFY_TIKTOK_ACTOR", "clockworks/tiktok-scraper")
TASK_ID = os.getenv("APIFY_TIKTOK_TASK_ID", "").strip()
APIFY_API_BASE = "https://api.apify.com/v2"
PAYLOAD_OVERRIDE = os.getenv("APIFY_TIKTOK_PAYLOAD", "").strip()
MAX_ITEMS = int(os.getenv("APIFY_TIKTOK_MAX", "10"))
SDK_MODE = os.getenv("APIFY_USE_SDK", "false").strip().lower() in ("1","true","yes","on")
SDK_ACTOR_ID = os.getenv("APIFY_TIKTOK_SDK_ACTOR", "GdWCkxBtKWOsKjdch").strip()


async def apify_run_actor(actor_id: str, token: str, input_payload: dict, logger=None) -> dict:
    async with httpx.AsyncClient(timeout=120) as client:
        # Start actor run
        start = await client.post(
            f"{APIFY_API_BASE}/actors/{actor_id}/runs?token={token}",
            json=input_payload,
        )
        if start.status_code >= 300:
            if logger:
                logger.warning(f"Apify actor start failed {start.status_code}: {start.text[:200]}")
            return {}
        run = start.json()
        run_id = run.get("data", {}).get("id")
        if not run_id:
            return {}
        # Poll for completion
        while True:
            r = await client.get(f"{APIFY_API_BASE}/runs/{run_id}?token={token}")
            data = r.json().get("data", {})
            status = data.get("status")
            if status in ("SUCCEEDED", "FAILED", "ABORTED", "TIMED-OUT"):
                break
            await asyncio.sleep(3)
        if status != "SUCCEEDED":
            if logger:
                logger.warning(f"Apify run status={status}")
            return {}
        dataset_id = data.get("defaultDatasetId")
        if not dataset_id:
            return {}
        items = await client.get(
            f"{APIFY_API_BASE}/datasets/{dataset_id}/items?token={token}"
        )
        return {"items": items.json()}


async def apify_run_task(task_id: str, token: str, input_payload: dict, logger=None) -> dict:
    async with httpx.AsyncClient(timeout=120) as client:
        start = await client.post(
            f"{APIFY_API_BASE}/actor-tasks/{task_id}/runs?token={token}",
            json=input_payload,
        )
        if start.status_code >= 300:
            if logger:
                logger.warning(f"Apify task start failed {start.status_code}: {start.text[:200]}")
            return {}
        run = start.json()
        run_id = run.get("data", {}).get("id")
        if not run_id:
            return {}
        while True:
            r = await client.get(f"{APIFY_API_BASE}/runs/{run_id}?token={token}")
            data = r.json().get("data", {})
            status = data.get("status")
            if status in ("SUCCEEDED", "FAILED", "ABORTED", "TIMED-OUT"):
                break
            await asyncio.sleep(3)
        if status != "SUCCEEDED":
            if logger:
                logger.warning(f"Apify task run status={status}")
            return {}
        dataset_id = data.get("defaultDatasetId")
        if not dataset_id:
            return {}
        items = await client.get(
            f"{APIFY_API_BASE}/datasets/{dataset_id}/items?token={token}"
        )
        return {"items": items.json()}


@task
async def apify_diag() -> dict:
    if not APIFY_TOKEN:
        return {"ok": False, "reason": "missing-token"}
    async with httpx.AsyncClient(timeout=30) as client:
        r = await client.get(f"{APIFY_API_BASE}/me?token={APIFY_TOKEN}")
        return {"ok": r.status_code == 200, "status": r.status_code, "body": r.json() if r.headers.get('content-type','').startswith('application/json') else r.text[:200]}


@task
async def apify_tiktok_topn(top_n: int = 5) -> int:
    if not is_enabled("apify_tiktok") or not APIFY_TOKEN:
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
        # Fallback: if no recent scores, seed from configs/entities.csv
        if not rows:
            try:
                df = pd.read_csv("configs/entities.csv")
                # parse aliases column if JSON-like strings
                if "aliases" in df.columns:
                    def parse_aliases(x):
                        try:
                            return json.loads(x) if isinstance(x, str) else []
                        except Exception:
                            return []
                    df["aliases"] = df["aliases"].apply(parse_aliases)
                seed = df.head(top_n).to_dict(orient="records")
                seeded = []
                for r in seed:
                    name = r.get("name")
                    etype = r.get("type", "person")
                    aliases = r.get("aliases", [])
                    wiki_id = r.get("wiki_id")
                    if not name:
                        continue
                    eid = await upsert_entity(conn, name, etype, aliases, wiki_id)
                    seeded.append((eid, name))
                rows = seeded
                logger.info(f"apify_tiktok fallback seeded {len(rows)} entities from CSV")
            except Exception as e:
                logger.warning(f"apify_tiktok fallback failed: {e}")
        inserted = 0
        for eid, name in rows:
            items = []
            if SDK_MODE:
                # Use official Apify SDK and the provided actor with hashtag-based input
                try:
                    from apify_client import ApifyClient
                    client = ApifyClient(APIFY_TOKEN)
                    # Map our entity name to hashtag search (basic heuristic)
                    hashtags = [name.replace(" ", "")]  # strip spaces for hashtag format
                    run_input = {
                        "hashtags": hashtags,
                        "resultsPerPage": min(100, max(1, MAX_ITEMS)),
                        "profileScrapeSections": ["videos"],
                        "profileSorting": "latest",
                        "excludePinnedPosts": False,
                        "searchSection": "",
                        "maxProfilesPerQuery": 3,
                        "scrapeRelatedVideos": False,
                        "shouldDownloadVideos": False,
                        "shouldDownloadCovers": False,
                        "shouldDownloadSubtitles": False,
                        "shouldDownloadSlideshowImages": False,
                        "shouldDownloadAvatars": False,
                        "shouldDownloadMusicCovers": False,
                        "proxyCountryCode": "None",
                    }
                    if PAYLOAD_OVERRIDE:
                        try:
                            override = json.loads(PAYLOAD_OVERRIDE)
                            if isinstance(override, dict):
                                run_input.update(override)
                        except Exception:
                            if logger:
                                logger.warning("Invalid APIFY_TIKTOK_PAYLOAD JSON for SDK; ignoring")
                    run = client.actor(SDK_ACTOR_ID).call(run_input=run_input)
                    dataset_id = run.get("defaultDatasetId") if run else None
                    if dataset_id:
                        items = list(client.dataset(dataset_id).iterate_items())
                except Exception as e:
                    if logger:
                        logger.warning(f"Apify SDK run failed for '{name}': {e}")
            else:
                # HTTP mode using REST API
                payload = {
                    "searchTerms": [name],
                    "queries": [name],
                    "search": name,
                    "query": name,
                    "maxItems": MAX_ITEMS,
                    "maxPosts": MAX_ITEMS,
                }
                if PAYLOAD_OVERRIDE:
                    try:
                        override = json.loads(PAYLOAD_OVERRIDE)
                        if isinstance(override, dict):
                            payload.update(override)
                    except Exception:
                        if logger:
                            logger.warning("Invalid APIFY_TIKTOK_PAYLOAD JSON; ignoring")
                if TASK_ID:
                    res = await apify_run_task(TASK_ID, APIFY_TOKEN, payload, logger)
                else:
                    res = await apify_run_actor(ACTOR_ID, APIFY_TOKEN, payload, logger)
                items = res.get("items", []) if res else []
            if logger:
                logger.info(f"apify_tiktok '{name}' -> {len(items)} items")
            await insert_signal(conn, eid, "apify_tiktok", now, "hits", float(len(items)))
            inserted += 1
        logger.info(f"apify_tiktok inserted signals for {inserted} entities.")
        return inserted


@flow(name="apify-tiktok")
def run_apify_tiktok():
    return apify_tiktok_topn.submit()


@flow(name="apify-diag")
def run_apify_diag():
    return apify_diag.submit()
