from __future__ import annotations
import os
from datetime import datetime, timezone
from prefect import flow, task, get_run_logger
from sqlalchemy import text

from libs.db import conn_ctx, insert_signal
from libs.config import is_enabled

APIFY_TOKEN = os.getenv("APIFY_TOKEN", "").strip()
# Actor: https://apify.com/pocesar/tiktok-creative-center-scraper
ACTOR_ID = os.getenv("APIFY_CC_ACTOR", "pocesar/tiktok-creative-center-scraper").strip()


@task
async def scrape_cc_hashtags(limit: int = 100) -> int:
    """Scrape top trending hashtags from TikTok Creative Center (US, 1d + 7d),
    map to entities by simple hashtag guess, and write signals under source='tt_cc'.
    """
    if not is_enabled("tt_cc") or not APIFY_TOKEN:
        return 0
    logger = get_run_logger()

    # Import here to avoid importing when feature is disabled
    try:
        from apify_client import ApifyClient  # type: ignore
    except Exception as e:
        logger.warning(f"apify-client not available: {e}")
        return 0

    client = ApifyClient(APIFY_TOKEN)
    now = datetime.now(timezone.utc)

    # Pull 1d and 7d for momentum calculation
    data_by_tf: dict[str, dict[str, int]] = {}
    for tf in ("1d", "7d"):
        run_input = {
            "type": "hashtags",
            "region": "US",
            "timeframe": tf,
            "limit": int(limit),
            "language": "en",
            "proxyConfiguration": {"useApifyProxy": True},
        }
        try:
            run = client.actor(ACTOR_ID).call(run_input=run_input)
            if not isinstance(run, dict):
                logger.warning("Creative Center run returned no metadata")
                return 0
            ds_id = run.get("defaultDatasetId")
            if not ds_id:
                logger.warning("Creative Center run returned no datasetId")
                return 0
            items = list(client.dataset(ds_id).iterate_items())
        except Exception as e:
            logger.warning(f"Creative Center scrape failed ({tf}): {e}")
            return 0

        # Rank map: hashtagName -> rank (1-based)
        data_by_tf[tf] = {}
        for idx, item in enumerate(items):
            tag = (item.get("hashtagName") or "").strip().lower()
            if not tag:
                continue
            data_by_tf[tf][tag] = idx + 1

    # Write signals for any matching entities
    async with conn_ctx() as conn:
        q = text("SELECT id, name FROM entities")
        rows = (await conn.execute(q)).fetchall()
        inserted = 0
        max_rank = float(max(1, int(limit)))
        for eid, name in rows:
            tag_guess = name.lower().replace(" ", "")
            r1 = data_by_tf.get("1d", {}).get(tag_guess)
            r7 = data_by_tf.get("7d", {}).get(tag_guess)
            if r1:
                score_1d = 1.0 - (float(r1) - 1.0) / (max_rank - 1.0) if max_rank > 1 else 1.0
                await insert_signal(conn, eid, "tt_cc", now, "hashtag_score", float(score_1d))
                inserted += 1
            if r1 and r7:
                score_7d = 1.0 - (float(r7) - 1.0) / (max_rank - 1.0) if max_rank > 1 else 1.0
                momentum = float(score_1d) - float(score_7d)
                await insert_signal(conn, eid, "tt_cc", now, "momentum", float(momentum))

        logger.info(f"tt_cc: inserted signals for {inserted} entities.")
        return inserted


@flow(name="cc-tiktok")
def run_cc_tiktok():
    return scrape_cc_hashtags.submit()
