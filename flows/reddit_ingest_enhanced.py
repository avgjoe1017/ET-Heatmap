"""
Reddit ingestion flow for entertainment and pop culture monitoring.
Implements Tier 0 scraping plan with subreddit monitoring and sentiment analysis.
"""

from prefect import flow, task, get_run_logger
import os
import asyncio
from datetime import datetime, timezone, timedelta
from typing import List, Dict, Optional, Any
import praw
import requests
from dataclasses import dataclass

from libs.db import conn_ctx
from libs.config import is_enabled
from libs.rate import rate_limiter
from sqlalchemy import text


@dataclass
class RedditSignal:
    entity_name: str
    subreddit: str
    post_id: str
    score: int
    comment_count: int
    created_utc: float
    title: str
    upvote_ratio: float


# Target subreddits for entertainment monitoring
ENTERTAINMENT_SUBREDDITS = [
    "movies", "television", "popculturechat", "entertainment", 
    "boxoffice", "Deuxmoi", "Music", "popheads", "marvelstudios",
    "DCcomics", "StarWars", "harrypotter", "gameofthrones"
]


@task
async def get_reddit_client() -> Optional[praw.Reddit]:
    """Initialize Reddit client with credentials."""
    client_id = os.getenv("REDDIT_CLIENT_ID")
    client_secret = os.getenv("REDDIT_CLIENT_SECRET")
    user_agent = os.getenv("REDDIT_USER_AGENT", "ET-Heatmap/1.0")
    
    if not client_id or not client_secret:
        logger = get_run_logger()
        logger.warning("Reddit credentials not found. Add REDDIT_CLIENT_ID and REDDIT_CLIENT_SECRET to .env")
        return None
    
    try:
        reddit = praw.Reddit(
            client_id=client_id,
            client_secret=client_secret,
            user_agent=user_agent
        )
        return reddit
    except Exception as e:
        logger = get_run_logger()
        logger.error(f"Failed to initialize Reddit client: {e}")
        return None


@task
async def get_tracked_entities() -> List[str]:
    """Get list of entities to track from database."""
    async with conn_ctx() as conn:
        result = await conn.execute(text("SELECT name FROM entities ORDER BY name"))
        entities = [row[0] for row in result.fetchall()]
    return entities


@task
async def scan_subreddit_for_entities(
    reddit: praw.Reddit, 
    subreddit_name: str, 
    entities: List[str],
    time_filter: str = "hour"
) -> List[RedditSignal]:
    """Scan a subreddit for mentions of tracked entities."""
    logger = get_run_logger()
    signals = []
    
    try:
        subreddit = reddit.subreddit(subreddit_name)
        
        # Get recent posts (hot + new for comprehensive coverage)
        posts = list(subreddit.hot(limit=50)) + list(subreddit.new(limit=50))
        
        for post in posts:
            # Check if post is recent (last 2 hours)
            post_age = datetime.now(timezone.utc).timestamp() - post.created_utc
            if post_age > 7200:  # 2 hours
                continue
                
            # Check title and selftext for entity mentions
            text_to_search = f"{post.title} {getattr(post, 'selftext', '')}".lower()
            
            for entity in entities:
                if entity.lower() in text_to_search:
                    signal = RedditSignal(
                        entity_name=entity,
                        subreddit=subreddit_name,
                        post_id=post.id,
                        score=post.score,
                        comment_count=post.num_comments,
                        created_utc=post.created_utc,
                        title=post.title[:200],  # Truncate for storage
                        upvote_ratio=getattr(post, 'upvote_ratio', 0.5)
                    )
                    signals.append(signal)
                    logger.info(f"Found {entity} mention in r/{subreddit_name}: {post.title[:50]}...")
                    
    except Exception as e:
        logger.error(f"Error scanning r/{subreddit_name}: {e}")
    
    return signals


@task 
async def calculate_reddit_metrics(signals: List[RedditSignal]) -> Dict[str, Dict[str, float]]:
    """Calculate aggregated metrics per entity from Reddit signals."""
    entity_metrics = {}
    
    for signal in signals:
        entity = signal.entity_name
        if entity not in entity_metrics:
            entity_metrics[entity] = {
                "total_mentions": 0,
                "total_score": 0,
                "total_comments": 0,
                "avg_upvote_ratio": 0,
                "subreddit_spread": set(),
                "post_ids": set()
            }
        
        metrics = entity_metrics[entity]
        
        # Avoid double-counting same post
        if signal.post_id not in metrics["post_ids"]:
            metrics["total_mentions"] += 1
            metrics["total_score"] += signal.score
            metrics["total_comments"] += signal.comment_count
            metrics["avg_upvote_ratio"] += signal.upvote_ratio
            metrics["subreddit_spread"].add(signal.subreddit)
            metrics["post_ids"].add(signal.post_id)
    
    # Calculate final metrics
    final_metrics = {}
    for entity, metrics in entity_metrics.items():
        if metrics["total_mentions"] > 0:
            final_metrics[entity] = {
                "mention_count": metrics["total_mentions"],
                "score_sum": metrics["total_score"],
                "comment_sum": metrics["total_comments"],
                "avg_upvote_ratio": metrics["avg_upvote_ratio"] / metrics["total_mentions"],
                "subreddit_count": len(metrics["subreddit_spread"]),
                "engagement_rate": (metrics["total_score"] + metrics["total_comments"]) / metrics["total_mentions"]
            }
    
    return final_metrics


@task
async def store_reddit_signals(entity_metrics: Dict[str, Dict[str, float]]):
    """Store Reddit metrics in signals table."""
    logger = get_run_logger()
    
    if not entity_metrics:
        logger.info("No Reddit signals to store")
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
                (entity_id, now, "reddit", "mention_count", metrics["mention_count"]),
                (entity_id, now, "reddit", "score_sum", metrics["score_sum"]),
                (entity_id, now, "reddit", "comment_sum", metrics["comment_sum"]),
                (entity_id, now, "reddit", "upvote_ratio", metrics["avg_upvote_ratio"]),
                (entity_id, now, "reddit", "subreddit_count", metrics["subreddit_count"]),
                (entity_id, now, "reddit", "engagement_rate", metrics["engagement_rate"])
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
        logger.info(f"Stored Reddit signals for {len(entity_metrics)} entities")


@flow(name="reddit-ingest")
async def run_reddit_ingest():
    """Main Reddit ingestion flow - runs every 5 minutes."""
    logger = get_run_logger()
    
    if not is_enabled("reddit_ingest"):
        logger.info("Reddit ingestion disabled")
        return
    
    # Rate limiting
    await rate_limiter("reddit", max_calls=100, window_seconds=3600)
    
    reddit = await get_reddit_client()
    if not reddit:
        logger.warning("Skipping Reddit ingestion - no client available")
        return
    
    entities = await get_tracked_entities()
    if not entities:
        logger.warning("No entities to track")
        return
    
    logger.info(f"Scanning {len(ENTERTAINMENT_SUBREDDITS)} subreddits for {len(entities)} entities")
    
    all_signals = []
    for subreddit_name in ENTERTAINMENT_SUBREDDITS:
        try:
            signals = await scan_subreddit_for_entities(reddit, subreddit_name, entities)
            all_signals.extend(signals)
            
            # Small delay between subreddit scans
            await asyncio.sleep(0.5)
            
        except Exception as e:
            logger.warning(f"Failed to scan r/{subreddit_name}: {e}")
    
    # Calculate and store metrics
    entity_metrics = await calculate_reddit_metrics(all_signals)
    await store_reddit_signals(entity_metrics)
    
    logger.info(f"Reddit ingestion complete: {len(all_signals)} signals processed")


if __name__ == "__main__":
    asyncio.run(run_reddit_ingest())
