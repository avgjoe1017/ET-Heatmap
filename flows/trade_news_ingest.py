"""
Trade RSS and Google News ingestion flow.
Implements Tier 0 and Tier 1 scraping plan for entertainment trade publications and news aggregation.
"""

from prefect import flow, task, get_run_logger
import os
import asyncio
from datetime import datetime, timezone, timedelta
from typing import List, Dict, Optional, Any, Tuple
import requests
import feedparser
from dataclasses import dataclass
import re
from urllib.parse import urljoin

from libs.db import conn_ctx
from libs.config import is_enabled
from libs.rate import rate_limiter
from libs.health import record_source_ok, record_source_error
from sqlalchemy import text


@dataclass
class NewsSignal:
    entity_name: str
    source: str
    title: str
    url: str
    published: datetime
    is_breaking: bool = False
    tier: int = 1


# Trade publication RSS feeds (Tier 1 authority)
TRADE_RSS_FEEDS = {
    "variety": "https://variety.com/feed/",
    "hollywood_reporter": "https://www.hollywoodreporter.com/feed/", 
    "deadline": "https://deadline.com/feed/",
    "entertainment_weekly": "https://ew.com/feed/",
    "vulture": "https://www.vulture.com/feeds/all.xml",
    "indiewire": "https://www.indiewire.com/feed/",
    "the_wrap": "https://www.thewrap.com/feed/"
}

# Google News RSS by category
GOOGLE_NEWS_FEEDS = {
    "entertainment": "https://news.google.com/rss/topics/CAAqJggKIiBDQkFTRWdvSUwyMHZNREpxYW5RU0FtVnVHZ0pWVXlnQVAB?hl=en-US&gl=US&ceid=US:en",
    "business": "https://news.google.com/rss/topics/CAAqJggKIiBDQkFTRWdvSUwyMHZNRGx6TVdZU0FtVnVHZ0pWVXlnQVAB?hl=en-US&gl=US&ceid=US:en"
}

# Breaking news indicators
BREAKING_KEYWORDS = [
    "breaking", "urgent", "just in", "developing", "exclusive", 
    "confirmed", "announced", "reveals", "dies", "dead"
]


@task
async def get_tracked_entities() -> List[str]:
    """Get list of entities to track from database.""" 
    async with conn_ctx() as conn:
        result = await conn.execute(text("SELECT name FROM entities ORDER BY name"))
        entities = [row[0] for row in result.fetchall()]
    return entities


@task
async def fetch_rss_feed(feed_url: str, source_name: str) -> List[Dict]:
    """Fetch and parse RSS feed."""
    logger = get_run_logger()
    
    try:
        response = requests.get(feed_url, timeout=30, headers={
            "User-Agent": "ET-Heatmap/1.0 (Entertainment Trend Monitor)"
        })
        response.raise_for_status()
        
        feed = feedparser.parse(response.content)
        
        items = []
        for entry in feed.entries:
            # Only process recent articles (last 6 hours)
            published = datetime.now(timezone.utc)  # Default fallback
            
            if hasattr(entry, "published_parsed") and entry.published_parsed:
                try:
                    # Validate that published_parsed is a tuple with at least 6 integer elements
                    if (isinstance(entry.published_parsed, tuple) and 
                        len(entry.published_parsed) >= 6 and 
                        all(isinstance(i, int) for i in entry.published_parsed[:6])):
                        published = datetime(*entry.published_parsed[:6], tzinfo=timezone.utc)
                except (TypeError, ValueError):
                    # If datetime construction fails, use current time
                    published = datetime.now(timezone.utc)
            
            if (datetime.now(timezone.utc) - published).total_seconds() > 21600:  # 6 hours
                continue
            
            items.append({
                "title": entry.get("title", ""),
                "link": entry.get("link", ""),
                "published": published,
                "summary": entry.get("summary", ""),
                "source": source_name
            })
        
        logger.info(f"Fetched {len(items)} recent items from {source_name}")
        return items
        
    except Exception as e:
        logger.error(f"Error fetching RSS feed {source_name}: {e}")
        return []


@task 
async def analyze_news_for_entities(news_items: List[Dict], entities: List[str]) -> List[NewsSignal]:
    """Analyze news items for entity mentions."""
    logger = get_run_logger()
    signals = []
    
    for item in news_items:
        title = item["title"].lower()
        summary = item.get("summary", "").lower()
        search_text = f"{title} {summary}"
        
        # Check for breaking news indicators
        is_breaking = any(keyword in title for keyword in BREAKING_KEYWORDS)
        
        # Determine source tier
        source = item["source"]
        if source in ["variety", "hollywood_reporter", "deadline"]:
            tier = 1  # Top tier trades
        elif source in ["entertainment_weekly", "vulture", "indiewire"]:
            tier = 2  # Secondary trades
        else:
            tier = 3  # General news
        
        for entity in entities:
            if entity.lower() in search_text:
                signal = NewsSignal(
                    entity_name=entity,
                    source=source,
                    title=item["title"],
                    url=item["link"],
                    published=item["published"],
                    is_breaking=is_breaking,
                    tier=tier
                )
                signals.append(signal)
                
                logger.info(f"Found {entity} in {source}: {item['title'][:50]}...")
    
    return signals


@task
async def fetch_google_news_for_entity(entity: str) -> List[Dict]:
    """Fetch Google News results for a specific entity."""
    logger = get_run_logger()
    
    try:
        # URL encode the entity name for Google News search
        query = entity.replace(" ", "+")
        search_url = f"https://news.google.com/rss/search?q={query}&hl=en-US&gl=US&ceid=US:en"
        
        response = requests.get(search_url, timeout=30, headers={
            "User-Agent": "ET-Heatmap/1.0 (Entertainment Trend Monitor)"
        })
        response.raise_for_status()
        
        feed = feedparser.parse(response.content)
        
        items = []
        for entry in feed.entries:
            # Only process recent articles (last 12 hours for entity-specific searches)
            published = datetime.now(timezone.utc)  # Default fallback
            
            if hasattr(entry, "published_parsed") and entry.published_parsed:
                try:
                    # Validate that published_parsed is a tuple with at least 6 integer elements
                    if (isinstance(entry.published_parsed, tuple) and 
                        len(entry.published_parsed) >= 6 and 
                        all(isinstance(i, int) for i in entry.published_parsed[:6])):
                        published = datetime(*entry.published_parsed[:6], tzinfo=timezone.utc)
                except (TypeError, ValueError):
                    # If datetime construction fails, use current time
                    published = datetime.now(timezone.utc)
            
            if (datetime.now(timezone.utc) - published).total_seconds() > 43200:  # 12 hours
                continue
            
            items.append({
                "title": entry.get("title", ""),
                "link": entry.get("link", ""),
                "published": published,
                "summary": entry.get("summary", ""),
                "source": "google_news"
            })
        
        logger.info(f"Found {len(items)} Google News items for {entity}")
        return items
        
    except Exception as e:
        logger.error(f"Error fetching Google News for {entity}: {e}")
        return []


@task
async def calculate_news_metrics(signals: List[NewsSignal]) -> Dict[str, Dict[str, float]]:
    """Calculate aggregated news metrics per entity."""
    entity_metrics = {}
    
    for signal in signals:
        entity = signal.entity_name
        if entity not in entity_metrics:
            entity_metrics[entity] = {
                "total_mentions": 0,
                "tier1_mentions": 0,
                "tier2_mentions": 0, 
                "tier3_mentions": 0,
                "breaking_count": 0,
                "source_diversity": set(),
                "authority_score": 0
            }
        
        metrics = entity_metrics[entity]
        metrics["total_mentions"] += 1
        metrics["source_diversity"].add(signal.source)
        
        if signal.tier == 1:
            metrics["tier1_mentions"] += 1
            metrics["authority_score"] += 1.0
        elif signal.tier == 2:
            metrics["tier2_mentions"] += 1
            metrics["authority_score"] += 0.5
        else:
            metrics["tier3_mentions"] += 1
            metrics["authority_score"] += 0.2
        
        if signal.is_breaking:
            metrics["breaking_count"] += 1
            metrics["authority_score"] += 0.5  # Breaking news bonus
    
    # Calculate final metrics
    final_metrics = {}
    for entity, metrics in entity_metrics.items():
        if metrics["total_mentions"] > 0:
            final_metrics[entity] = {
                "headline_count": metrics["total_mentions"],
                "tier1_hits": metrics["tier1_mentions"],
                "tier2_hits": metrics["tier2_mentions"],
                "tier3_hits": metrics["tier3_mentions"],
                "breaking_count": metrics["breaking_count"],
                "source_count": len(metrics["source_diversity"]),
                "authority_score": metrics["authority_score"]
            }
    
    return final_metrics


@task
async def store_news_signals(entity_metrics: Dict[str, Dict[str, float]]):
    """Store news metrics in signals table."""
    logger = get_run_logger()
    
    if not entity_metrics:
        logger.info("No news signals to store")
        return
    
    async with conn_ctx() as conn:
        now = datetime.now(timezone.utc)
        
        for entity_name, metrics in entity_metrics.items():
            # Get entity ID
            result = await conn.execute(
                text("SELECT id FROM entities WHERE name = :name"),
                {"name": entity_name}
            )
            entity_row = result.first()
            if not entity_row:
                continue
                
            entity_id = entity_row[0]
            
            # Store each metric as separate signal
            signals_to_insert = [
                (entity_id, now, "news", "headline_count", metrics["headline_count"]),
                (entity_id, now, "news", "tier1_hits", metrics["tier1_hits"]),
                (entity_id, now, "news", "tier2_hits", metrics["tier2_hits"]),
                (entity_id, now, "news", "tier3_hits", metrics["tier3_hits"]),
                (entity_id, now, "news", "breaking_count", metrics["breaking_count"]),
                (entity_id, now, "news", "source_count", metrics["source_count"]),
                (entity_id, now, "news", "authority_score", metrics["authority_score"])
            ]
            
            for signal_data in signals_to_insert:
                await conn.execute(
                    text("""
                        INSERT INTO signals (entity_id, ts, source, metric, value)
                        VALUES (:entity_id, :ts, :source, :metric, :value)
                    """),
                    {
                        "entity_id": signal_data[0],
                        "ts": signal_data[1],
                        "source": signal_data[2],
                        "metric": signal_data[3],
                        "value": signal_data[4]
                    }
                )
        
        await conn.commit()
        logger.info(f"Stored news signals for {len(entity_metrics)} entities")


@flow(name="trade-news-ingest")
async def run_trade_news_ingest():
    """Main trade and news ingestion flow - runs every 5-10 minutes."""
    logger = get_run_logger()
    start_time = datetime.now(timezone.utc)
    
    try:
        if not is_enabled("trade_rss") and not is_enabled("google_news"):
            logger.info("Trade RSS and Google News ingestion disabled")
            return
        
        # Rate limiting
        await rate_limiter("news", max_calls=200, window_seconds=3600)
        
        entities = await get_tracked_entities()
        if not entities:
            logger.warning("No entities to track")
            return
        
        all_news_items = []
        all_signals = []
        
        # Fetch trade RSS feeds
        if is_enabled("trade_rss"):
            logger.info(f"Fetching {len(TRADE_RSS_FEEDS)} trade RSS feeds")
            for source_name, feed_url in TRADE_RSS_FEEDS.items():
                items = await fetch_rss_feed(feed_url, source_name)
                all_news_items.extend(items)
                await asyncio.sleep(0.5)
        
        # Fetch Google News feeds (general categories)
        if is_enabled("google_news"):
            logger.info(f"Fetching {len(GOOGLE_NEWS_FEEDS)} Google News category feeds")
            for category, feed_url in GOOGLE_NEWS_FEEDS.items():
                items = await fetch_rss_feed(feed_url, f"google_news_{category}")
                all_news_items.extend(items)
                await asyncio.sleep(0.5)
            
            # Entity-specific Google News searches (limited to prevent quota issues)
            high_priority_entities = entities[:5]
            logger.info(f"Searching Google News for {len(high_priority_entities)} high-priority entities")
            
            for entity in high_priority_entities:
                items = await fetch_google_news_for_entity(entity)
                for item in items:
                    signal = NewsSignal(
                        entity_name=entity,
                        source="google_news_search",
                        title=item["title"],
                        url=item["link"],
                        published=item["published"],
                        is_breaking=any(keyword in item["title"].lower() for keyword in BREAKING_KEYWORDS),
                        tier=3
                    )
                    all_signals.append(signal)
                
                await asyncio.sleep(1)
        
        # Analyze news items for entity mentions
        if all_news_items:
            feed_signals = await analyze_news_for_entities(all_news_items, entities)
            all_signals.extend(feed_signals)
        
        # Calculate and store metrics
        entity_metrics = await calculate_news_metrics(all_signals)
        await store_news_signals(entity_metrics)
        
        # Record successful execution
        await record_source_ok("trade_rss")
        
        duration = (datetime.now(timezone.utc) - start_time).total_seconds()
        logger.info(f"Trade/news ingestion complete: {len(all_signals)} signals processed from {len(all_news_items)} articles in {duration:.1f}s")
        
    except Exception as e:
        # Record error
        await record_source_error("trade_rss")
        logger.error(f"Trade news ingestion failed: {e}")
        raise


if __name__ == "__main__":
    asyncio.run(run_trade_news_ingest())
