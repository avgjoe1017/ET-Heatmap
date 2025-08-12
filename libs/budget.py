from __future__ import annotations

import os
import json
from datetime import datetime, timezone
from typing import Any, Dict, Optional

try:  # optional dependency
    import redis.asyncio as _redis_async  # type: ignore
except Exception:  # pragma: no cover
    _redis_async = None


class BudgetManager:
    """Redis-backed budget manager with safe fallbacks.

    Tracks:
    - ScraperAPI monthly credits
    - OpenAI monthly USD spend
    - NewsAPI daily requests
    """

    def __init__(self, redis_url: Optional[str] = None) -> None:
        self.redis_url = redis_url or os.getenv("REDIS_URL", "redis://redis:6379/0")
        self._client = None
        # Limits
        self.scraperapi_monthly_limit = int(os.getenv("SCRAPERAPI_MONTHLY_LIMIT", "100000"))
        self.openai_monthly_budget = float(os.getenv("OPENAI_MONTHLY_BUDGET", "30"))
        self.newsapi_daily_limit = int(os.getenv("NEWS_API_DAILY_LIMIT", "500"))
        # In-memory fallback counters
        self._mem = {"scraperapi": 0, "openai_usd": 0.0, "newsapi": 0}

    async def _get_client(self):
        if _redis_async is None:
            return None
        if self._client is None:
            try:
                self._client = _redis_async.from_url(self.redis_url, encoding="utf-8", decode_responses=True)
                await self._client.ping()
            except Exception:
                self._client = None
        return self._client

    @staticmethod
    def _month_key(prefix: str) -> str:
        now = datetime.now(timezone.utc)
        return f"budget:{prefix}:{now.year}{now.month:02d}"

    @staticmethod
    def _day_key(prefix: str) -> str:
        now = datetime.now(timezone.utc)
        return f"budget:{prefix}:{now.year}{now.month:02d}{now.day:02d}"

    # ---- ScraperAPI ----
    async def can_use_scraperapi(self, needed: int = 1) -> bool:
        client = await self._get_client()
        key = self._month_key("scraperapi")
        if client:
            try:
                used = int(await client.get(key) or 0)
                return used + needed <= self.scraperapi_monthly_limit
            except Exception:
                pass
        return (self._mem["scraperapi"] + needed) <= self.scraperapi_monthly_limit

    async def inc_scraperapi(self, count: int = 1) -> None:
        client = await self._get_client()
        key = self._month_key("scraperapi")
        if client:
            try:
                await client.incrby(key, count)
                # set TTL roughly to end of month if not set
                await client.expire(key, 60 * 60 * 24 * 40)
                return
            except Exception:
                pass
        self._mem["scraperapi"] += count

    # ---- OpenAI ----
    async def can_spend_openai(self, usd: float) -> bool:
        client = await self._get_client()
        key = self._month_key("openai_usd")
        if client:
            try:
                spent = float(await client.get(key) or 0.0)
                return spent + usd <= self.openai_monthly_budget
            except Exception:
                pass
        return (self._mem["openai_usd"] + usd) <= self.openai_monthly_budget

    async def add_openai_spend(self, usd: float) -> None:
        client = await self._get_client()
        key = self._month_key("openai_usd")
        if client:
            try:
                pipe = client.pipeline()
                pipe.incrbyfloat(key, usd)
                pipe.expire(key, 60 * 60 * 24 * 40)
                await pipe.execute()
                return
            except Exception:
                pass
        self._mem["openai_usd"] += usd

    # ---- NewsAPI ----
    async def can_use_newsapi(self, needed: int = 1) -> bool:
        client = await self._get_client()
        key = self._day_key("newsapi")
        if client:
            try:
                used = int(await client.get(key) or 0)
                return used + needed <= self.newsapi_daily_limit
            except Exception:
                pass
        return (self._mem["newsapi"] + needed) <= self.newsapi_daily_limit

    async def inc_newsapi(self, count: int = 1) -> None:
        client = await self._get_client()
        key = self._day_key("newsapi")
        if client:
            try:
                pipe = client.pipeline()
                pipe.incrby(key, count)
                # 2 days TTL ensures rollover
                pipe.expire(key, 60 * 60 * 24 * 2)
                await pipe.execute()
                return
            except Exception:
                pass
        self._mem["newsapi"] += count

    async def summary(self) -> Dict[str, Any]:
        client = await self._get_client()
        keys = {
            "scraperapi": self._month_key("scraperapi"),
            "openai_usd": self._month_key("openai_usd"),
            "newsapi": self._day_key("newsapi"),
        }
        values: Dict[str, Any] = {}
        if client:
            try:
                res = await client.mget(*keys.values())
                for (kname, key), val in zip(keys.items(), res):
                    values[kname] = float(val) if kname == "openai_usd" else int(val or 0)
            except Exception:
                values = {}
        if not values:
            values = {
                "scraperapi": int(self._mem["scraperapi"]),
                "openai_usd": float(self._mem["openai_usd"]),
                "newsapi": int(self._mem["newsapi"]),
            }
        return {
            "limits": {
                "scraperapi_monthly": self.scraperapi_monthly_limit,
                "openai_monthly_usd": self.openai_monthly_budget,
                "newsapi_daily": self.newsapi_daily_limit,
            },
            "usage": values,
            "remaining": {
                "scraperapi": max(0, self.scraperapi_monthly_limit - int(values["scraperapi"])),
                "openai_usd": max(0.0, self.openai_monthly_budget - float(values["openai_usd"])),
                "newsapi": max(0, self.newsapi_daily_limit - int(values["newsapi"]))
            },
        }
