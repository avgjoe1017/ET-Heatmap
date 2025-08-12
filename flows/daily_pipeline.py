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
from flows.reddit_ingest import run_reddit_ingest
from flows.mvp_scoring import run_mvp_scoring
from flows.actionable_alerts import run_actionable_alerts
from flows.gdelt_gkg import run_gdelt_gkg  # bulk news intelligence
from flows.entity_discovery_advanced import run_discovery_flow

# New Tier 0 comprehensive flows
from flows.tier0_orchestration import run_tier0_once
from flows.reddit_ingest_enhanced import run_reddit_ingest as run_reddit_enhanced
from flows.youtube_ingest import run_youtube_ingest
from flows.trade_news_ingest import run_trade_news_ingest
from flows.imdb_box_office_ingest import run_imdb_box_office_ingest

from libs.config import is_enabled


@flow(name="daily-pipeline")
def daily_pipeline():
    logger = get_run_logger()
    inserted = None
    try:
        inserted = run_ingest()  # subflow call; returns the inserted count
        wait_s = int(os.getenv("WAIT_AFTER_INGEST_SECONDS", "45"))
        logger.info(f"Ingest done (inserted={inserted}). Waiting {wait_s}s to ensure DB writes settle...")
        asyncio.run(asyncio.sleep(wait_s))
    except Exception as e:
        logger.warning(f"Ingest failed (degraded mode). Proceeding with last good shortlist. Error: {e}")

    # NEW: Tier 0 Comprehensive Scraping (replaces individual scraper calls)
    logger.info("Running Tier 0 comprehensive scraping orchestration...")
    try:
        asyncio.run(run_tier0_once())
        logger.info("Tier 0 orchestration completed successfully")
    except Exception as e:
        logger.error(f"Tier 0 orchestration failed: {e}")
        # Fallback to legacy scrapers if Tier 0 fails
        logger.info("Falling back to legacy scraper flows...")
        run_reddit_ingest()
        run_scrape_news()

    # Legacy TikTok flows (still valuable for specific TikTok analytics)
    run_apify_tiktok()
    run_cc_tiktok()
    run_apify_tiktok_search()

    # Bulk news intelligence via GDELT GKG (entity mentions + tone)
    run_gdelt_gkg()

    # Optional: run discovery to add new entities to track
    if is_enabled("entity_discovery"):
        run_discovery_flow()

    logger.info("Waiting 30s for all scraper signals to land...")
    asyncio.run(asyncio.sleep(30))  # Increased wait time for more sources

    # MVP Scoring pass (fast) and then fold TikTok
    run_mvp_scoring()
    # Re-score entities by folding TikTok component into HEAT
    run_rescore_tiktok()

    # Notify
    asyncio.run(notify_slack())
    asyncio.run(run_actionable_alerts())
    return "ok"
