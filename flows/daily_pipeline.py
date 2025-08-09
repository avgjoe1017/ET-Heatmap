from prefect import flow, get_run_logger
import os
import asyncio

from flows.wiki_trends_ingest import run_ingest
from flows.scrape_news import run_scrape_news
from flows.apify_tiktok import run_apify_tiktok
from flows.cc_tiktok import run_cc_tiktok
from flows.apify_tiktok_search import run_apify_tiktok_search
from flows.rescore_tiktok import run_rescore_tiktok
from flows.notify_slack import notify_slack
from flows.gdelt_gkg import run_gdelt_gkg  # bulk news intelligence


@flow(name="daily-pipeline")
def daily_pipeline():
    logger = get_run_logger()
    degraded = False
    inserted = None
    try:
        inserted = run_ingest()  # subflow call; returns the inserted count
        wait_s = int(os.getenv("WAIT_AFTER_INGEST_SECONDS", "45"))
        logger.info(f"Ingest done (inserted={inserted}). Waiting {wait_s}s to ensure DB writes settle...")
        asyncio.run(asyncio.sleep(wait_s))
    except Exception as e:
        degraded = True
        logger.warning(f"Ingest failed (degraded mode). Proceeding with last good shortlist. Error: {e}")

    # Scrapers (do nothing unless enabled and keys set)
    run_scrape_news()
    # TikTok: legacy aggregated hits (apify_tiktok), Creative Center, then targeted search
    run_apify_tiktok()
    run_cc_tiktok()
    run_apify_tiktok_search()

    # Bulk news intelligence via GDELT GKG (entity mentions + tone)
    run_gdelt_gkg()

    logger.info("Waiting 20s for scraper signals to land...")
    asyncio.run(asyncio.sleep(20))

    # Re-score entities by folding TikTok component into HEAT
    run_rescore_tiktok()

    # Notify
    asyncio.run(notify_slack())
    return "ok"
