"""
Master orchestration flow for Tier 0 comprehensive scraping plan.
Coordinates all always-on, high-ROI data sources with optimal scheduling.
"""

from prefect import flow, task, get_run_logger
import asyncio
from datetime import datetime, timezone, timedelta
from typing import List, Dict, Optional
import os

from libs.config import is_enabled
from libs.rate import rate_limiter
from libs.health import record_source_ok, record_source_error
from libs.db import conn_ctx
from sqlalchemy import text


async def record_flow_health(flow_name: str, status: str, duration_seconds: float, details: Optional[dict] = None):
    """Simple health recording function."""
    try:
        if status == "success":
            await record_source_ok(flow_name)
        else:
            await record_source_error(flow_name)
    except Exception:
        pass  # Health recording is non-critical

# Import all the new scraper flows 
from flows.reddit_ingest_enhanced import run_reddit_ingest
from flows.youtube_ingest import run_youtube_ingest  
from flows.trade_news_ingest import run_trade_news_ingest
from flows.imdb_box_office_ingest import run_imdb_box_office_ingest

# Import existing flows
from flows.wiki_trends_ingest import run_ingest as run_wiki_trends_ingest
from flows.trends_ingest import run_trends_ingest
from flows.mvp_scoring import run_mvp_scoring as run_scoring_job
from flows.scrape_news import run_scrape_news


@task
async def check_api_keys() -> Dict[str, bool]:
    """Check availability of required API keys for Tier 0 sources."""
    logger = get_run_logger()
    
    api_status = {
        "reddit": bool(os.getenv("REDDIT_CLIENT_ID") and os.getenv("REDDIT_CLIENT_SECRET")),
        "youtube": bool(os.getenv("YOUTUBE_API_KEY")),
        "google_trends": True,  # No API key required
        "wikipedia": True,  # No API key required
        "trade_rss": True,  # No API key required
        "google_news": True,  # No API key required
        "imdb_scraping": True,  # No API key required
        "box_office": True  # No API key required
    }
    
    missing_keys = [source for source, available in api_status.items() if not available]
    if missing_keys:
        logger.warning(f"Missing API keys for: {', '.join(missing_keys)}")
    else:
        logger.info("All Tier 0 API keys are available")
    
    return api_status


@task
async def get_source_cadences() -> Dict[str, Dict[str, int]]:
    """Define optimal cadences for each Tier 0 source based on ROI and rate limits."""
    return {
        # Always-on sources (every 5-15 minutes)
        "reddit": {"interval_minutes": 10, "priority": 1},
        "google_trends": {"interval_minutes": 15, "priority": 1},
        "trade_rss": {"interval_minutes": 5, "priority": 1},
        "google_news": {"interval_minutes": 8, "priority": 1},
        
        # High-frequency sources (every 30-60 minutes)  
        "youtube": {"interval_minutes": 45, "priority": 2},
        "wikipedia": {"interval_minutes": 60, "priority": 2},
        
        # Moderate frequency sources (every 2-6 hours)
        "imdb_box_office": {"interval_minutes": 240, "priority": 3},  # 4 hours
        "box_office": {"interval_minutes": 360, "priority": 3},  # 6 hours
        
        # Scoring and aggregation (every 10-30 minutes)
        "scoring": {"interval_minutes": 20, "priority": 1}
    }


@task
async def should_run_source(source: str, cadence_info: Dict[str, int]) -> bool:
    """Determine if a source should run based on its cadence and last run time."""
    logger = get_run_logger()
    
    if not is_enabled(source):
        logger.info(f"Source {source} is disabled in config")
        return False
    
    interval_minutes = cadence_info["interval_minutes"]
    now = datetime.now(timezone.utc)
    
    # Check last run time from database
    async with conn_ctx() as conn:
        result = await conn.execute(
            text("""
                SELECT MAX(ts) as last_run 
                FROM signals 
                WHERE source = :source
            """),
            {"source": source}
        )
        last_run_row = result.first()
        
        if not last_run_row or not last_run_row[0]:
            logger.info(f"Source {source} has never run, starting now")
            return True
        
        last_run = last_run_row[0]
        minutes_since_last = (now - last_run).total_seconds() / 60
        
        should_run = minutes_since_last >= interval_minutes
        
        logger.info(f"Source {source}: {minutes_since_last:.1f}min since last run, interval {interval_minutes}min, should_run={should_run}")
        return should_run


@flow(name="tier0-orchestrator")
async def run_tier0_orchestration():
    """
    Master orchestration flow for Tier 0 comprehensive scraping.
    Runs continuously with intelligent scheduling based on source cadences.
    """
    logger = get_run_logger()
    
    # Check API availability
    api_status = await check_api_keys()
    cadences = await get_source_cadences()
    
    logger.info("Starting Tier 0 comprehensive scraping orchestration")
    
    # Track flow execution for health monitoring
    flow_start = datetime.now(timezone.utc)
    sources_attempted = []
    sources_succeeded = []
    sources_failed = []
    
    try:
        # Priority 1 sources (always-on, high frequency)
        priority_1_sources = [
            ("scrape_news", run_scrape_news),  # No external API needed
            ("trade_rss", run_trade_news_ingest),  # RSS feeds, no API keys needed
            ("reddit", run_reddit_ingest),  # Now has API keys
        ]
        
        # Priority 2 sources (high frequency with API keys)
        priority_2_sources = [
            ("youtube", run_youtube_ingest),  # Now has API key
            ("wiki", run_wiki_trends_ingest),  # May hit rate limits
        ]
        
        # Priority 3 sources (moderate frequency)
        priority_3_sources = [
            ("imdb_box_office", run_imdb_box_office_ingest),  # Web scraping
        ]
        
        all_source_groups = [
            ("Priority 1", priority_1_sources),
            ("Priority 2", priority_2_sources), 
            ("Priority 3", priority_3_sources)
        ]
        
        for group_name, source_group in all_source_groups:
            logger.info(f"Running {group_name} sources: {len(source_group)} sources")
            
            for source_name, flow_func in source_group:
                # Check if API key is available for this source
                if source_name in api_status and api_status[source_name]:
                    cadence = cadences.get(source_name, {"interval_minutes": 60})
                    should_run = await should_run_source(source_name, cadence)
                    
                    logger.info(f"Checking {source_name}: enabled={is_enabled(source_name)}, api_available={api_status[source_name]}, should_run={should_run}")
                    
                    # Be more aggressive for testing new scrapers
                    if should_run or source_name in ["scrape_news", "trade_rss", "reddit", "youtube"]:
                        try:
                            sources_attempted.append(source_name)
                            logger.info(f"Running {group_name} source: {source_name}")
                            
                            # Handle different flow types
                            if source_name in ["scrape_news"]:
                                # Prefect flows, call synchronously
                                flow_func()
                            elif source_name in ["wiki"]:
                                # Handle Google Trends rate limiting gracefully
                                try:
                                    await flow_func()
                                    sources_succeeded.append(source_name)
                                    logger.info(f"Successfully completed {source_name}")
                                    continue
                                except Exception as e:
                                    if "429" in str(e) or "TooManyRequestsError" in str(e):
                                        logger.warning(f"Google Trends rate limited for {source_name}, skipping")
                                        sources_failed.append(source_name)
                                        continue
                                    else:
                                        raise e
                            else:
                                # New async flows
                                await flow_func()
                            
                            sources_succeeded.append(source_name)
                            logger.info(f"Successfully completed {source_name}")
                            
                            # Pause between sources to be respectful
                            if group_name == "Priority 1":
                                await asyncio.sleep(3)
                            elif group_name == "Priority 2": 
                                await asyncio.sleep(5)
                            else:
                                await asyncio.sleep(10)
                                
                        except Exception as e:
                            logger.error(f"Error running {source_name}: {e}")
                            sources_failed.append(source_name)
                    else:
                        logger.info(f"Skipping {source_name} - not due to run yet")
                else:
                    logger.info(f"Skipping {source_name} - API key not available or source disabled")
        
        # Run scoring if we got any data
        if sources_succeeded:
            try:
                sources_attempted.append("scoring")
                logger.info("Running final scoring update after data collection")
                run_scoring_job()
                sources_succeeded.append("scoring")
                logger.info("Successfully completed scoring")
            except Exception as e:
                logger.error(f"Error in final scoring run: {e}")
                sources_failed.append("scoring")
        
        logger.info("Tier 0 orchestration completed with available sources")
        
        # Record health metrics
        flow_duration = (datetime.now(timezone.utc) - flow_start).total_seconds()
        
        await record_flow_health(
            flow_name="tier0_orchestration",
            status="success" if not sources_failed else "partial_success",
            duration_seconds=flow_duration,
            details={
                "sources_attempted": len(sources_attempted),
                "sources_succeeded": len(sources_succeeded),
                "sources_failed": len(sources_failed),
                "failed_sources": sources_failed,
                "api_status": api_status
            }
        )
        
        logger.info(f"Tier 0 orchestration complete: {len(sources_succeeded)}/{len(sources_attempted)} sources successful")
        
        if sources_failed:
            logger.warning(f"Failed sources: {', '.join(sources_failed)}")
        
    except Exception as e:
        logger.error(f"Critical error in Tier 0 orchestration: {e}")
        
        await record_flow_health(
            flow_name="tier0_orchestration",
            status="failure",
            duration_seconds=(datetime.now(timezone.utc) - flow_start).total_seconds(),
            details={
                "error": str(e),
                "sources_attempted": len(sources_attempted),
                "sources_succeeded": len(sources_succeeded)
            }
        )
        
        raise


@flow(name="tier0-continuous")
async def run_tier0_continuous():
    """
    Continuous orchestration wrapper that runs Tier 0 flows in a loop.
    Designed for always-on deployment with intelligent spacing.
    """
    logger = get_run_logger()
    
    logger.info("Starting continuous Tier 0 orchestration loop")
    
    iteration = 0
    while True:
        try:
            iteration += 1
            logger.info(f"Starting Tier 0 orchestration iteration {iteration}")
            
            await run_tier0_orchestration()
            
            # Dynamic sleep based on system load and time of day
            now = datetime.now(timezone.utc)
            hour = now.hour
            
            # Sleep shorter during peak hours (9 AM - 11 PM UTC)
            if 9 <= hour <= 23:
                sleep_seconds = 300  # 5 minutes during peak hours
            else:
                sleep_seconds = 600  # 10 minutes during off-peak hours
            
            logger.info(f"Tier 0 iteration {iteration} complete, sleeping {sleep_seconds}s until next run")
            await asyncio.sleep(sleep_seconds)
            
        except KeyboardInterrupt:
            logger.info("Continuous orchestration interrupted by user")
            break
        except Exception as e:
            logger.error(f"Error in continuous orchestration iteration {iteration}: {e}")
            # Sleep longer after errors to avoid rapid failure loops
            await asyncio.sleep(900)  # 15 minutes


@flow(name="tier0-once")
async def run_tier0_once():
    """
    Single-shot Tier 0 orchestration for manual runs and testing.
    Forces all sources to run regardless of cadence.
    """
    logger = get_run_logger()
    
    logger.info("Running forced Tier 0 orchestration (all enabled sources)")
    
    # Check API keys first
    api_status = await check_api_keys()
    
    sources_attempted = []
    sources_succeeded = []
    sources_failed = []
    
    # Define all sources to test
    all_sources = [
        ("scrape_news", run_scrape_news, "sync"),
        ("trade_rss", run_trade_news_ingest, "async"),
        ("imdb_box_office", run_imdb_box_office_ingest, "async"),
    ]
    
    # Add API sources if available
    if api_status.get("reddit", False):
        all_sources.append(("reddit", run_reddit_ingest, "async"))
    if api_status.get("youtube", False):
        all_sources.append(("youtube", run_youtube_ingest, "async"))
    
    # Add rate-limited sources
    all_sources.append(("wiki", run_wiki_trends_ingest, "rate_limited"))
    
    # Execute each source
    for source_name, flow_func, execution_type in all_sources:
        if not is_enabled(source_name):
            logger.info(f"Source {source_name} is disabled")
            continue
            
        sources_attempted.append(source_name)
        
        try:
            logger.info(f"Running source: {source_name}")
            
            if execution_type == "sync":
                flow_func()
            elif execution_type == "async":
                await flow_func()
            elif execution_type == "rate_limited":
                await _handle_rate_limited_source(source_name, flow_func, logger)
                
            sources_succeeded.append(source_name)
            logger.info(f"Successfully completed {source_name}")
            
            # Brief pause between sources
            await asyncio.sleep(2)
            
        except Exception as e:
            logger.error(f"Error running {source_name}: {e}")
            sources_failed.append(source_name)
    
    # Run scoring if we have new data
    await _run_final_scoring(sources_succeeded, sources_attempted, sources_failed, logger)
    
    logger.info(f"Forced Tier 0 complete: {len(sources_succeeded)}/{len(sources_attempted)} sources successful")
    if sources_failed:
        logger.warning(f"Failed sources: {', '.join(sources_failed)}")
    
    return f"Completed: {len(sources_succeeded)} succeeded, {len(sources_failed)} failed"


async def _handle_rate_limited_source(source_name, flow_func, logger):
    """Handle rate-limited sources like Wiki with graceful degradation."""
    try:
        await flow_func()
    except Exception as e:
        if "429" in str(e) or "TooManyRequestsError" in str(e):
            logger.warning(f"Google Trends rate limited for {source_name}, skipping")
            raise e
        else:
            raise e


async def _run_final_scoring(sources_succeeded, sources_attempted, sources_failed, logger):
    """Run MVP scoring if we have new data."""
    if sources_succeeded:
        try:
            sources_attempted.append("mvp_scoring")
            logger.info("Running MVP scoring with new data")
            run_scoring_job()
            sources_succeeded.append("mvp_scoring")
            logger.info("Successfully completed MVP scoring")
        except Exception as e:
            logger.error(f"Error running MVP scoring: {e}")
            sources_failed.append("mvp_scoring")


if __name__ == "__main__":
    # Run single orchestration for testing
    asyncio.run(run_tier0_once())
