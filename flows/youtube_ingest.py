"""
YouTube ingestion flow for trending videos and entity search.
Implements Tier 0 scraping plan with trending analysis and entity-specific searches.
"""

from prefect import flow, task, get_run_logger
import os
import asyncio
from datetime import datetime, timezone, timedelta
from typing import List, Dict, Optional, Any
import requests
from dataclasses import dataclass

from libs.db import conn_ctx
from libs.config import is_enabled
from libs.rate import rate_limiter
from sqlalchemy import text


@dataclass
class YouTubeSignal:
    entity_name: str
    video_id: str
    title: str
    view_count: int
    like_count: int
    comment_count: int
    published_at: datetime
    channel_title: str
    category_id: str


class YouTubeAPI:
    """YouTube Data API v3 client."""
    
    def __init__(self):
        self.api_key = os.getenv("YOUTUBE_API_KEY")
        self.base_url = "https://www.googleapis.com/youtube/v3"
    
    async def get_trending_videos(self, region_code: str = "US", max_results: int = 50) -> List[Dict]:
        """Get trending videos from YouTube."""
        if not self.api_key:
            return []
        
        url = f"{self.base_url}/videos"
        params = {
            "part": "snippet,statistics",
            "chart": "mostPopular",
            "regionCode": region_code,
            "maxResults": max_results,
            "key": self.api_key
        }
        
        try:
            response = requests.get(url, params=params, timeout=30)
            response.raise_for_status()
            data = response.json()
            
            return data.get("items", [])
        except Exception as e:
            logger = get_run_logger()
            logger.error(f"YouTube trending API error: {e}")
            return []
    
    async def search_videos(self, query: str, max_results: int = 10, order: str = "relevance") -> List[Dict]:
        """Search YouTube videos for entity mentions."""
        if not self.api_key:
            return []
        
        url = f"{self.base_url}/search"
        params = {
            "part": "snippet",
            "q": query,
            "type": "video",
            "order": order,
            "maxResults": max_results,
            "publishedAfter": (datetime.now(timezone.utc) - timedelta(hours=24)).isoformat(),
            "key": self.api_key
        }
        
        try:
            response = requests.get(url, params=params, timeout=30)
            response.raise_for_status()
            data = response.json()
            
            # Get detailed stats for found videos
            video_ids = [item["id"]["videoId"] for item in data.get("items", [])]
            if video_ids:
                return await self.get_video_details(video_ids)
            
            return []
        except Exception as e:
            logger = get_run_logger()
            logger.error(f"YouTube search API error for '{query}': {e}")
            return []
    
    async def get_video_details(self, video_ids: List[str]) -> List[Dict]:
        """Get detailed statistics for specific video IDs."""
        if not self.api_key or not video_ids:
            return []
        
        url = f"{self.base_url}/videos"
        params = {
            "part": "snippet,statistics",
            "id": ",".join(video_ids),
            "key": self.api_key
        }
        
        try:
            response = requests.get(url, params=params, timeout=30)
            response.raise_for_status()
            data = response.json()
            
            return data.get("items", [])
        except Exception as e:
            logger = get_run_logger()
            logger.error(f"YouTube video details API error: {e}")
            return []


@task
async def get_tracked_entities() -> List[str]:
    """Get list of entities to track from database."""
    async with conn_ctx() as conn:
        result = await conn.execute(text("SELECT name FROM entities ORDER BY name"))
        entities = [row[0] for row in result.fetchall()]
    return entities


@task
async def scan_youtube_trending(youtube: YouTubeAPI, entities: List[str]) -> List[YouTubeSignal]:
    """Scan YouTube trending videos for entity mentions."""
    logger = get_run_logger()
    signals = []
    
    trending_videos = await youtube.get_trending_videos()
    logger.info(f"Retrieved {len(trending_videos)} trending videos")
    
    for video in trending_videos:
        snippet = video.get("snippet", {})
        stats = video.get("statistics", {})
        
        title = snippet.get("title", "").lower()
        description = snippet.get("description", "").lower()
        search_text = f"{title} {description}"
        
        for entity in entities:
            if entity.lower() in search_text:
                try:
                    signal = YouTubeSignal(
                        entity_name=entity,
                        video_id=video["id"],
                        title=snippet.get("title", "")[:200],
                        view_count=int(stats.get("viewCount", 0)),
                        like_count=int(stats.get("likeCount", 0)),
                        comment_count=int(stats.get("commentCount", 0)),
                        published_at=datetime.fromisoformat(snippet.get("publishedAt", "").replace("Z", "+00:00")),
                        channel_title=snippet.get("channelTitle", ""),
                        category_id=snippet.get("categoryId", "")
                    )
                    signals.append(signal)
                    logger.info(f"Found {entity} in trending video: {snippet.get('title', '')[:50]}...")
                except (ValueError, KeyError) as e:
                    logger.warning(f"Error parsing video data: {e}")
    
    return signals


@task
async def search_youtube_by_entity(youtube: YouTubeAPI, entities: List[str]) -> List[YouTubeSignal]:
    """Search YouTube directly for each entity."""
    logger = get_run_logger()
    signals = []
    
    for entity in entities:
        try:
            # Search for recent videos about this entity
            videos = await youtube.search_videos(entity, max_results=5, order="date")
            
            for video in videos:
                snippet = video.get("snippet", {})
                stats = video.get("statistics", {})
                
                try:
                    signal = YouTubeSignal(
                        entity_name=entity,
                        video_id=video["id"],
                        title=snippet.get("title", "")[:200],
                        view_count=int(stats.get("viewCount", 0)),
                        like_count=int(stats.get("likeCount", 0)),
                        comment_count=int(stats.get("commentCount", 0)),
                        published_at=datetime.fromisoformat(snippet.get("publishedAt", "").replace("Z", "+00:00")),
                        channel_title=snippet.get("channelTitle", ""),
                        category_id=snippet.get("categoryId", "")
                    )
                    signals.append(signal)
                except (ValueError, KeyError) as e:
                    logger.warning(f"Error parsing video data for {entity}: {e}")
            
            # Small delay between entity searches to be respectful
            await asyncio.sleep(1)
            
        except Exception as e:
            logger.warning(f"Failed to search YouTube for {entity}: {e}")
    
    logger.info(f"Found {len(signals)} signals from entity searches")
    return signals


@task
async def calculate_youtube_metrics(signals: List[YouTubeSignal]) -> Dict[str, Dict[str, float]]:
    """Calculate aggregated YouTube metrics per entity."""
    entity_metrics = {}
    
    for signal in signals:
        entity = signal.entity_name
        if entity not in entity_metrics:
            entity_metrics[entity] = {
                "total_views": 0,
                "total_likes": 0,
                "total_comments": 0,
                "video_count": 0,
                "unique_channels": set(),
                "avg_engagement_rate": 0
            }
        
        metrics = entity_metrics[entity]
        metrics["total_views"] += signal.view_count
        metrics["total_likes"] += signal.like_count
        metrics["total_comments"] += signal.comment_count
        metrics["video_count"] += 1
        metrics["unique_channels"].add(signal.channel_title)
    
    # Calculate final metrics
    final_metrics = {}
    for entity, metrics in entity_metrics.items():
        if metrics["video_count"] > 0:
            total_engagement = metrics["total_likes"] + metrics["total_comments"]
            engagement_rate = total_engagement / max(metrics["total_views"], 1) * 100
            
            final_metrics[entity] = {
                "view_count": metrics["total_views"],
                "like_count": metrics["total_likes"], 
                "comment_count": metrics["total_comments"],
                "video_count": metrics["video_count"],
                "unique_channels": len(metrics["unique_channels"]),
                "engagement_rate": engagement_rate
            }
    
    return final_metrics


@task
async def store_youtube_signals(entity_metrics: Dict[str, Dict[str, float]]):
    """Store YouTube metrics in signals table."""
    logger = get_run_logger()
    
    if not entity_metrics:
        logger.info("No YouTube signals to store")
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
                (entity_id, now, "youtube", "view_count", metrics["view_count"]),
                (entity_id, now, "youtube", "like_count", metrics["like_count"]),
                (entity_id, now, "youtube", "comment_count", metrics["comment_count"]),
                (entity_id, now, "youtube", "video_count", metrics["video_count"]),
                (entity_id, now, "youtube", "unique_channels", metrics["unique_channels"]),
                (entity_id, now, "youtube", "engagement_rate", metrics["engagement_rate"])
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
        logger.info(f"Stored YouTube signals for {len(entity_metrics)} entities")


@flow(name="youtube-ingest")
async def run_youtube_ingest():
    """Main YouTube ingestion flow - runs every hour."""
    logger = get_run_logger()
    
    if not is_enabled("youtube_ingest"):
        logger.info("YouTube ingestion disabled")
        return
    
    # Rate limiting (YouTube API has quota limits)
    await rate_limiter("youtube", max_calls=100, window_seconds=3600)
    
    youtube = YouTubeAPI()
    if not youtube.api_key:
        logger.warning("YouTube API key not found. Add YOUTUBE_API_KEY to .env")
        return
    
    entities = await get_tracked_entities()
    if not entities:
        logger.warning("No entities to track")
        return
    
    logger.info(f"Starting YouTube ingestion for {len(entities)} entities")
    
    # Get signals from trending videos
    trending_signals = await scan_youtube_trending(youtube, entities)
    
    # Get signals from entity-specific searches  
    search_signals = await search_youtube_by_entity(youtube, entities)
    
    # Combine and process all signals
    all_signals = trending_signals + search_signals
    entity_metrics = await calculate_youtube_metrics(all_signals)
    await store_youtube_signals(entity_metrics)
    
    logger.info(f"YouTube ingestion complete: {len(all_signals)} signals processed")


if __name__ == "__main__":
    asyncio.run(run_youtube_ingest())
