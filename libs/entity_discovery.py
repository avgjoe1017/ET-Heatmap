from __future__ import annotations
from dataclasses import dataclass
from typing import List, Dict, Optional
from datetime import datetime, timezone, timedelta
import asyncio
import re
import math

import pandas as pd
import httpx
from sqlalchemy import text

from libs.db import conn_ctx, upsert_entity

RSS_SOURCES_CSV = "configs/news_sources.csv"


@dataclass
class DiscoveredEntity:
    name: str
    confidence: float
    sources: List[str]
    category: str
    first_seen: datetime
    velocity: float
    related_entities: List[str]
    discovery_context: Dict


class AdvancedEntityDiscovery:
    """Lightweight multi-source discovery using existing deps (pytrends + Wikipedia).
    Avoids heavy ML libs; safe to run inside current stack.
    """

    WIKI_SUMMARY = "https://en.wikipedia.org/api/rest_v1/page/summary/{title}"

    def __init__(self):
        # Lazy import pytrends to keep startup fast
        from pytrends.request import TrendReq
        self.pytrends = TrendReq(hl="en-US", tz=360)
    # No-op; using module-level RSS_SOURCES_CSV

    async def discover_entities_once(self, country: str = "united_states", top_n: int = 20) -> List[DiscoveredEntity]:
        """Discover candidate entities via Google trending searches + Wikipedia validation.
        Splits work into helpers to keep complexity low.
        """
        trending = await asyncio.get_event_loop().run_in_executor(
            None, self._fetch_trending_searches, country
        )
        candidates = trending[: top_n]
        # Augment with simple heuristics from RSS and GDELT-derived names
        extra = await self._discover_from_rss(limit=top_n)
        candidates.extend([c for c in extra if c not in candidates])
        gdelt_names = await self._recent_gdelt_top_names(limit=top_n)
        candidates.extend([g for g in gdelt_names if g not in candidates])
        validated: List[DiscoveredEntity] = []

        async with httpx.AsyncClient(timeout=20) as client:
            for query in candidates:
                entity = await self._build_entity_from_query(client, query)
                if entity:
                    validated.append(entity)

        validated = await self._filter_new_entities(validated)
        validated.sort(key=lambda e: (e.velocity, e.confidence), reverse=True)
        return validated

    async def _build_entity_from_query(self, client: httpx.AsyncClient, query: str) -> Optional[DiscoveredEntity]:
        wiki = await self._wiki_summary(client, query)
        if not wiki:
            return None
        wtype = wiki.get("type")
        if wtype == "disambiguation":
            return None
        if wtype not in {None, "standard"} and not wiki.get("description"):
            return None

        canonical = wiki.get("title") or query
        category = self._infer_category_from_summary(wiki)
        vel = await asyncio.get_event_loop().run_in_executor(None, self._interest_velocity, canonical)
        if math.isnan(vel):
            vel = 0.0
        confidence = self._compute_confidence(vel, bool(wiki.get("description")))
        return DiscoveredEntity(
            name=canonical,
            confidence=confidence,
            sources=["google_trending"],
            category=category,
            first_seen=datetime.now(timezone.utc),
            velocity=float(vel),
            related_entities=[],
            discovery_context={
                "query": query,
                "wiki": {"title": wiki.get("title"), "description": wiki.get("description")},
            },
        )

    def _compute_confidence(self, velocity: float, has_desc: bool) -> float:
        c = 0.5
        if velocity > 0.1:
            c += 0.2
        if has_desc:
            c += 0.1
        return min(0.95, c)

    def _fetch_trending_searches(self, country: str) -> List[str]:
        # pytrends trending_searches
        try:
            df = self.pytrends.trending_searches(pn=country)
            if isinstance(df, pd.DataFrame) and not df.empty:
                col = df.columns[0]
                out = [str(v).strip() for v in df[col].tolist() if str(v).strip()]
                return out
        except Exception:
            pass
        return []

    async def _wiki_summary(self, client: httpx.AsyncClient, title: str) -> Optional[Dict]:
        url = self.WIKI_SUMMARY.format(title=title.replace(" ", "_"))
        try:
            r = await client.get(url, headers={"User-Agent": "ET-Heatmap/1.0"})
            if r.status_code != 200:
                return None
            data = r.json()
            # Some summaries include a 'type' field. We treat 'standard' as valid.
            return data
        except Exception:
            return None

    def _interest_velocity(self, name: str) -> float:
        # Build payload and compute last-7-day gradient mean as velocity proxy
        try:
            self.pytrends.build_payload([name], cat=0, timeframe="now 7-d", geo="US", gprop="")
            df = self.pytrends.interest_over_time()
            if df is None or df.empty or name not in df.columns:
                return 0.0
            s = df[name].astype(float)
            s = s.reset_index(drop=True)
            if len(s) < 4:
                return 0.0
            diffs = s.diff().fillna(0)
            recent = diffs.tail(7)
            return float(recent.mean() / (s.max() + 1e-6))  # scaled
        except Exception:
            return 0.0

    def _infer_category_from_summary(self, wiki: Dict) -> str:
        desc = (wiki.get("description") or "").lower()
        # very rough heuristic
        if any(k in desc for k in ["singer", "actor", "rapper", "artist", "movie", "film", "tv"]):
            return "entertainment"
        if any(k in desc for k in ["company", "startup", "software", "ai", "tech", "technology"]):
            return "technology"
        if any(k in desc for k in ["team", "league", "tournament", "player", "coach"]):
            return "sports"
        if any(k in desc for k in ["politician", "election", "party", "policy", "government"]):
            return "politics"
        return "general"

    async def _discover_from_rss(self, limit: int = 20) -> List[str]:
        """Very light RSS heuristic: fetch page titles from configured news sources via httpx and extract capitalized tokens as entity candidates."""
        names: List[str] = []
        try:
            import pandas as pd
            df = pd.read_csv(RSS_SOURCES_CSV)
            urls = df["url"].dropna().tolist()[:limit]
        except Exception:
            return names
        pattern = re.compile(r"\b([A-Z][a-z]+(?:\s+[A-Z][a-z]+){0,3})\b")
        async with httpx.AsyncClient(timeout=10) as client:
            for u in urls:
                try:
                    r = await client.get(u, headers={"User-Agent": "ET-Heatmap/1.0"})
                    if r.status_code != 200:
                        continue
                    # Extract <title>...</title>
                    m = re.search(r"<title[^>]*>(.*?)</title>", r.text or "", flags=re.IGNORECASE|re.DOTALL)
                    title = m.group(1) if m else ""
                    for match in pattern.findall(title):
                        nm = match.strip()
                        if len(nm.split()) <= 4 and nm not in names:
                            names.append(nm)
                except Exception:
                    continue
        return names[:limit]

    async def _recent_gdelt_top_names(self, limit: int = 20) -> List[str]:
        """Mine recent GDELT mentions already ingested: pick names absent from entities with higher recent signals."""
        sql = text(
            """
            WITH recent AS (
                SELECT s.entity_id, SUM(CASE WHEN s.metric='gkg_mentions' THEN s.value ELSE 0 END) AS mentions
                FROM signals s
                WHERE s.source='gdelt_gkg' AND s.ts >= NOW() - INTERVAL '24 hours'
                GROUP BY s.entity_id
            ), ranked AS (
                SELECT e.name, r.mentions
                FROM recent r
                JOIN entities e ON e.id=r.entity_id
                ORDER BY r.mentions DESC NULLS LAST
            )
            SELECT name FROM ranked LIMIT :lim
            """
        )
        try:
            async with conn_ctx() as conn:
                rows = (await conn.execute(sql, {"lim": limit})).scalars().all()
                return [str(r) for r in rows]
        except Exception:
            return []

    async def _filter_new_entities(self, entities: List[DiscoveredEntity]) -> List[DiscoveredEntity]:
        names = [e.name for e in entities]
        if not names:
            return []
        async with conn_ctx() as conn:
            q = text("SELECT name FROM entities WHERE name = ANY(:names)")
            rows = (await conn.execute(q, {"names": names})).scalars().all()
            known = set(rows)
        return [e for e in entities if e.name not in known]

    async def persist_discoveries(self, entities: List[DiscoveredEntity]) -> int:
        """Insert discovered entities into DB (entities table)."""
        inserted = 0
        async with conn_ctx() as conn:
            for e in entities:
                try:
                    # Store category separately from type; use category as type fallback to avoid NULLs
                    await upsert_entity(conn, e.name, e.category or "general", aliases=[], wiki_id=None, category=e.category or "general")
                    inserted += 1
                except Exception:
                    continue
        return inserted
