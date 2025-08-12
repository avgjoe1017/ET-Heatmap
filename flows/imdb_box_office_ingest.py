"""
IMDb calendar and box office data ingestion flow.
Implements Tier 0 scraping for upcoming releases, box office performance, and entertainment calendar.
"""

from prefect import flow, task, get_run_logger
import os
import asyncio
from datetime import datetime, timezone, timedelta
from typing import List, Dict, Optional, Any, Tuple
import aiohttp
from dataclasses import dataclass
import re
from bs4 import BeautifulSoup
import json

from libs.db import conn_ctx
from libs.config import is_enabled
from libs.rate import rate_limiter
from sqlalchemy import text


@dataclass
class BoxOfficeSignal:
    entity_name: str
    title: str
    weekend_gross: Optional[float]
    total_gross: Optional[float]
    theaters: Optional[int]
    weeks_in_release: int
    rank: int
    change_pct: Optional[float] = None


@dataclass
class ReleaseSignal:
    entity_name: str
    title: str
    release_date: datetime
    genre: Optional[str]
    mpaa_rating: Optional[str]
    anticipation_score: float = 0.0
    theater_count: Optional[int] = None


# IMDb endpoints and selectors
IMDB_BOX_OFFICE_URL = "https://www.imdb.com/chart/boxoffice/"
IMDB_COMING_SOON_URL = "https://www.imdb.com/calendar/"
IMDB_MOST_POPULAR_URL = "https://www.imdb.com/chart/moviemeter/"

# Box Office Mojo (owned by IMDb) for more detailed data
BOM_WEEKEND_URL = "https://www.boxofficemojo.com/weekend/"
BOM_YEARLY_URL = "https://www.boxofficemojo.com/year/world/"


@task
async def get_tracked_entities() -> List[str]:
    """Get list of entities to track from database."""
    async with conn_ctx() as conn:
        result = await conn.execute(text("SELECT name FROM entities ORDER BY name"))
        entities = [row[0] for row in result.fetchall()]
    return entities


@task
async def scrape_imdb_box_office() -> List[Dict]:
    """Scrape IMDb box office chart."""
    logger = get_run_logger()
    
    try:
        async with aiohttp.ClientSession() as session:
            headers = {
                "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36"
            }
            
            async with session.get(IMDB_BOX_OFFICE_URL, headers=headers) as response:
                if response.status != 200:
                    logger.error(f"Failed to fetch IMDb box office: {response.status}")
                    return []
                
                html = await response.text()
                soup = BeautifulSoup(html, 'html.parser')
                
                movies = []
                
                # Find box office table
                table = soup.find('table', class_='chart')
                if not table:
                    logger.warning("Could not find box office table")
                    return []
                
                from bs4.element import Tag
                rows = table.find_all('tr')[1:] if isinstance(table, Tag) else []  # Skip header
                
                for i, row in enumerate(rows[:20]):  # Top 20
                    cells = row.find_all('td') if isinstance(row, Tag) else []
                    if len(cells) >= 4:
                        title_cell = cells[1]
                        title_link = title_cell.find('a') if isinstance(title_cell, Tag) else None
                        title = title_link.text.strip() if title_link else ""
                        
                        weekend_gross_text = cells[2].text.strip()
                        weekend_gross = parse_box_office_amount(weekend_gross_text)
                        
                        total_gross_text = cells[3].text.strip() 
                        total_gross = parse_box_office_amount(total_gross_text)
                        
                        theaters_text = cells[4].text.strip() if len(cells) > 4 else ""
                        theaters = parse_theater_count(theaters_text)
                        
                        weeks_text = cells[5].text.strip() if len(cells) > 5 else "1"
                        match = re.search(r'\d+', weeks_text)
                        weeks = int(match.group()) if match else 1
                        
                        movies.append({
                            "title": title,
                            "rank": i + 1,
                            "weekend_gross": weekend_gross,
                            "total_gross": total_gross,
                            "theaters": theaters,
                            "weeks_in_release": weeks
                        })
                
                logger.info(f"Scraped {len(movies)} movies from IMDb box office")
                return movies
                
    except Exception as e:
        logger.error(f"Error scraping IMDb box office: {e}")
        return []


@task
async def scrape_imdb_coming_soon() -> List[Dict]:
    """Scrape IMDb coming soon releases."""
    logger = get_run_logger()
    
    try:
        async with aiohttp.ClientSession() as session:
            headers = {
                "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36"
            }
            
            # Get current month's releases
            now = datetime.now()
            calendar_url = f"{IMDB_COMING_SOON_URL}?year={now.year}&month={now.month}"
            
            async with session.get(calendar_url, headers=headers) as response:
                if response.status != 200:
                    logger.error(f"Failed to fetch IMDb calendar: {response.status}")
                    return []
                
                html = await response.text()
                soup = BeautifulSoup(html, 'html.parser')
                
                releases = []
                
                # Find release sections
                release_items = soup.find_all('div', class_='list_item')
                
                from bs4.element import Tag
                for item in release_items:
                    try:
                        # Ensure item is a Tag object
                        if not isinstance(item, Tag):
                            continue
                            
                        # Extract title
                        title_elem = item.find('h4')
                        if not title_elem:
                            title_elem = item.find('a')
                        title = title_elem.text.strip() if title_elem else ""
                        
                        # Extract release date
                        date_elem = item.find('span', class_='release_date')
                        release_date = parse_release_date(date_elem.text if date_elem else "")
                        
                        # Extract genre
                        genre_elem = item.find('span', class_='genre')
                        genre = genre_elem.text.strip() if genre_elem else None
                        
                        # Extract MPAA rating
                        rating_elem = item.find('span', class_='certificate')
                        mpaa_rating = rating_elem.text.strip() if rating_elem else None
                        
                        # Calculate anticipation score based on various factors
                        anticipation_score = calculate_anticipation_score(item)
                        
                        if title and release_date:
                            releases.append({
                                "title": title,
                                "release_date": release_date,
                                "genre": genre,
                                "mpaa_rating": mpaa_rating,
                                "anticipation_score": anticipation_score
                            })
                    
                    except Exception as e:
                        logger.warning(f"Error parsing release item: {e}")
                        continue
                
                logger.info(f"Scraped {len(releases)} upcoming releases from IMDb")
                return releases
                
    except Exception as e:
        logger.error(f"Error scraping IMDb coming soon: {e}")
        return []


@task
async def scrape_box_office_mojo() -> List[Dict]:
    """Scrape Box Office Mojo for detailed weekend data."""
    logger = get_run_logger()
    
    try:
        async with aiohttp.ClientSession() as session:
            headers = {
                "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36"
            }
            
            async with session.get(BOM_WEEKEND_URL, headers=headers) as response:
                if response.status != 200:
                    logger.error(f"Failed to fetch Box Office Mojo: {response.status}")
                    return []
                
                html = await response.text()
                soup = BeautifulSoup(html, 'html.parser')
                
                movies = []
                
                # Find weekend chart table
                table = soup.find('table')
                if not table:
                    logger.warning("Could not find Box Office Mojo table")
                    return []
                
                from bs4.element import Tag
                rows = table.find_all('tr')[1:] if isinstance(table, Tag) else []  # Skip header
                
                for i, row in enumerate(rows[:15]):  # Top 15
                    cells = row.find_all('td') if isinstance(row, Tag) else []
                    if len(cells) >= 6:
                        title_cell = cells[1]
                        title_link = title_cell.find('a') if isinstance(title_cell, Tag) else None
                        title = title_link.text.strip() if title_link else ""
                        
                        weekend_gross = parse_box_office_amount(cells[2].text.strip())
                        change_text = cells[3].text.strip()
                        change_pct = parse_percentage_change(change_text)
                        
                        theaters = parse_theater_count(cells[4].text.strip())
                        total_gross = parse_box_office_amount(cells[5].text.strip())
                        
                        weeks_text = cells[6].text.strip() if len(cells) > 6 else "1"
                        match = re.search(r'\d+', weeks_text)
                        weeks = int(match.group()) if match else 1
                        
                        movies.append({
                            "title": title,
                            "rank": i + 1,
                            "weekend_gross": weekend_gross,
                            "total_gross": total_gross,
                            "theaters": theaters,
                            "weeks_in_release": weeks,
                            "change_pct": change_pct
                        })
                
                logger.info(f"Scraped {len(movies)} movies from Box Office Mojo")
                return movies
                
    except Exception as e:
        logger.error(f"Error scraping Box Office Mojo: {e}")
        return []


def parse_box_office_amount(text: str) -> Optional[float]:
    """Parse box office amount from text like '$12.5M' or '$1,234,567'."""
    if not text or text == "-":
        return None
    
    # Remove $ and commas
    clean_text = re.sub(r'[$,]', '', text)
    
    # Handle millions and thousands
    if 'M' in clean_text:
        match = re.search(r'[\d.]+', clean_text)
        return float(match.group()) * 1_000_000 if match else None
    elif 'K' in clean_text:
        match = re.search(r'[\d.]+', clean_text)
        return float(match.group()) * 1_000 if match else None
    else:
        match = re.search(r'[\d.]+', clean_text)
        return float(match.group()) if match else None


def parse_theater_count(text: str) -> Optional[int]:
    """Parse theater count from text like '3,547'."""
    if not text or text == "-":
        return None
    
    clean_text = re.sub(r'[,]', '', text)
    match = re.search(r'\d+', clean_text)
    return int(match.group()) if match else None


def parse_percentage_change(text: str) -> Optional[float]:
    """Parse percentage change from text like '-12.5%' or '+5.2%'."""
    if not text or text == "-":
        return None
    
    match = re.search(r'[+-]?[\d.]+', text)
    return float(match.group()) if match else None


def parse_release_date(text: str) -> Optional[datetime]:
    """Parse release date from various text formats."""
    if not text:
        return None
    
    try:
        # Try common date formats
        for fmt in ["%B %d, %Y", "%b %d, %Y", "%m/%d/%Y", "%Y-%m-%d"]:
            try:
                return datetime.strptime(text.strip(), fmt).replace(tzinfo=timezone.utc)
            except ValueError:
                continue
        return None
    except Exception:
        return None


def calculate_anticipation_score(item_soup) -> float:
    """Calculate anticipation score based on IMDb page elements."""
    score = 0.0
    
    # Check for star rating
    rating_elem = item_soup.find('span', class_='rating-rating')
    if rating_elem:
        rating_text = rating_elem.text.strip()
        try:
            match = re.search(r'[\d.]+', rating_text)
            if match:
                rating = float(match.group())
            score += min(rating / 10.0, 1.0) * 0.4  # Max 0.4 points for rating
        except:
            pass
    
    # Check for number of votes/interest
    votes_elem = item_soup.find('span', class_='rating-votes')
    if votes_elem:
        votes_text = votes_elem.text.strip()
        try:
            votes = int(re.sub(r'[,()]', '', votes_text))
            if votes > 10000:
                score += 0.3
            elif votes > 1000:
                score += 0.2
            elif votes > 100:
                score += 0.1
        except:
            pass
    
    # Check for notable cast/director mentions
    cast_elem = item_soup.find('div', class_='cast')
    if cast_elem and len(cast_elem.text) > 50:
        score += 0.2
    
    # Check for award mentions or festival presence
    if any(keyword in item_soup.text.lower() for keyword in ['oscar', 'golden globe', 'cannes', 'sundance', 'venice']):
        score += 0.1
    
    return min(score, 1.0)


@task
async def match_entities_to_box_office(box_office_data: List[Dict], entities: List[str]) -> List[BoxOfficeSignal]:
    """Match box office data to tracked entities."""
    signals = []
    
    for movie in box_office_data:
        title = movie["title"].lower()
        
        for entity in entities:
            if entity.lower() in title:
                signal = BoxOfficeSignal(
                    entity_name=entity,
                    title=movie["title"],
                    weekend_gross=movie["weekend_gross"],
                    total_gross=movie["total_gross"],
                    theaters=movie["theaters"],
                    weeks_in_release=movie["weeks_in_release"],
                    rank=movie["rank"],
                    change_pct=movie.get("change_pct")
                )
                signals.append(signal)
                break
    
    return signals


@task
async def match_entities_to_releases(release_data: List[Dict], entities: List[str]) -> List[ReleaseSignal]:
    """Match upcoming releases to tracked entities."""
    signals = []
    
    for release in release_data:
        title = release["title"].lower()
        
        for entity in entities:
            if entity.lower() in title:
                signal = ReleaseSignal(
                    entity_name=entity,
                    title=release["title"],
                    release_date=release["release_date"],
                    genre=release["genre"],
                    mpaa_rating=release["mpaa_rating"],
                    anticipation_score=release["anticipation_score"]
                )
                signals.append(signal)
                break
    
    return signals


@task
async def store_box_office_signals(signals: List[BoxOfficeSignal]):
    """Store box office signals in database."""
    logger = get_run_logger()
    
    if not signals:
        logger.info("No box office signals to store")
        return
    
    async with conn_ctx() as conn:
        now = datetime.now(timezone.utc)
        
        for signal in signals:
            # Get entity ID
            result = await conn.execute(
                text("SELECT id FROM entities WHERE name = :name"),
                {"name": signal.entity_name}
            )
            entity_row = result.first()
            if not entity_row:
                continue
                
            entity_id = entity_row[0]
            
            # Store box office metrics
            metrics_to_store = [
                ("box_office_rank", signal.rank),
                ("box_office_weeks", signal.weeks_in_release)
            ]
            
            if signal.weekend_gross:
                metrics_to_store.append(("weekend_gross", int(signal.weekend_gross)))
            if signal.total_gross:
                metrics_to_store.append(("total_gross", int(signal.total_gross)))
            if signal.theaters:
                metrics_to_store.append(("theater_count", signal.theaters))
            if signal.change_pct is not None:
                metrics_to_store.append(("box_office_change", int(signal.change_pct)))
            
            for metric_name, value in metrics_to_store:
                await conn.execute(
                    text("""
                        INSERT INTO signals (entity_id, ts, source, metric, value)
                        VALUES (:entity_id, :ts, :source, :metric, :value)
                    """),
                    {
                        "entity_id": entity_id,
                        "ts": now,
                        "source": "box_office",
                        "metric": metric_name,
                        "value": float(value)
                    }
                )
        
        await conn.commit()
        logger.info(f"Stored box office signals for {len(signals)} movies")


@task
async def store_release_signals(signals: List[ReleaseSignal]):
    """Store upcoming release signals in database."""
    logger = get_run_logger()
    
    if not signals:
        logger.info("No release signals to store")
        return
    
    async with conn_ctx() as conn:
        now = datetime.now(timezone.utc)
        
        for signal in signals:
            # Get entity ID
            result = await conn.execute(
                text("SELECT id FROM entities WHERE name = :name"),
                {"name": signal.entity_name}
            )
            entity_row = result.first()
            if not entity_row:
                continue
                
            entity_id = entity_row[0]
            
            # Calculate days until release
            days_until_release = (signal.release_date - now).days
            
            # Store release metrics
            await conn.execute(
                text("""
                    INSERT INTO signals (entity_id, ts, source, metric, value)
                    VALUES (:entity_id, :ts, :source, :metric, :value)
                """),
                {
                    "entity_id": entity_id,
                    "ts": now,
                    "source": "releases",
                    "metric": "days_until_release",
                    "value": float(days_until_release)
                }
            )
            
            await conn.execute(
                text("""
                    INSERT INTO signals (entity_id, ts, source, metric, value)
                    VALUES (:entity_id, :ts, :source, :metric, :value)
                """),
                {
                    "entity_id": entity_id,
                    "ts": now,
                    "source": "releases",
                    "metric": "anticipation_score",
                    "value": signal.anticipation_score
                }
            )
        
        await conn.commit()
        logger.info(f"Stored release signals for {len(signals)} upcoming movies")


@flow(name="imdb-box-office-ingest")
async def run_imdb_box_office_ingest():
    """Main IMDb and box office ingestion flow - runs twice daily."""
    logger = get_run_logger()
    
    if not is_enabled("imdb_box_office"):
        logger.info("IMDb box office ingestion disabled")
        return
    
    # Rate limiting
    await rate_limiter("imdb", max_calls=50, window_seconds=3600)
    
    entities = await get_tracked_entities()
    if not entities:
        logger.warning("No entities to track")
        return
    
    # Scrape box office data
    logger.info("Scraping box office data...")
    imdb_box_office = await scrape_imdb_box_office()
    bom_box_office = await scrape_box_office_mojo()
    
    # Combine box office data (prefer BOM for more detailed info)
    all_box_office = bom_box_office if bom_box_office else imdb_box_office
    
    # Scrape upcoming releases
    logger.info("Scraping upcoming releases...")
    releases = await scrape_imdb_coming_soon()
    
    # Match to entities and store
    if all_box_office:
        box_office_signals = await match_entities_to_box_office(all_box_office, entities)
        await store_box_office_signals(box_office_signals)
    
    if releases:
        release_signals = await match_entities_to_releases(releases, entities)
        await store_release_signals(release_signals)
    
    logger.info(f"IMDb/box office ingestion complete: {len(all_box_office)} box office entries, {len(releases)} upcoming releases")


if __name__ == "__main__":
    asyncio.run(run_imdb_box_office_ingest())
