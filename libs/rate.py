from __future__ import annotations

import asyncio
from datetime import datetime, timezone
from typing import Optional

try:
    import redis.asyncio as _redis  # type: ignore
except Exception:
    _redis = None


class TokenBucket:
    """Simple Redis-backed token bucket with in-memory fallback.

    rate: tokens per interval seconds added; burst: max tokens.
    """

    def __init__(self, key: str, rate: int, interval: int = 60, burst: Optional[int] = None, redis_url: Optional[str] = None):
        self.key = key
        self.rate = max(1, int(rate))
        self.interval = max(1, int(interval))
        self.burst = int(burst) if burst is not None else self.rate
        self.redis_url = redis_url
        self._mem_tokens = float(self.burst)
        self._mem_ts = datetime.now(timezone.utc).timestamp()
        self._client = None

    async def _get_client(self):
        if not self.redis_url or _redis is None:
            return None
        if self._client is None:
            try:
                self._client = _redis.from_url(self.redis_url, encoding="utf-8", decode_responses=True)
                await self._client.ping()
            except Exception:
                self._client = None
        return self._client

    async def acquire(self, tokens: int = 1) -> bool:
        # Fast path: try redis script, else fallback to memory
        client = await self._get_client()
        if client:
            try:
                lua = (
                    "local key=KEYS[1]; local now=tonumber(ARGV[1]); local rate=tonumber(ARGV[2]); local interval=tonumber(ARGV[3]); local burst=tonumber(ARGV[4]); local need=tonumber(ARGV[5]); "
                    "local data=redis.call('HMGET', key, 'tokens','ts'); local tokens=tonumber(data[1]) or burst; local ts=tonumber(data[2]) or now; "
                    "local elapsed=math.max(0, now-ts); tokens=math.min(burst, tokens + (elapsed/interval)*rate); if tokens >= need then tokens=tokens-need; redis.call('HMSET', key, 'tokens', tokens, 'ts', now); redis.call('EXPIRE', key, interval*2); return 1 else redis.call('HMSET', key, 'tokens', tokens, 'ts', now); redis.call('EXPIRE', key, interval*2); return 0 end"
                )
                now = datetime.now(timezone.utc).timestamp()
                ok = await client.eval(lua, 1, self.key, now, self.rate, self.interval, self.burst, tokens)
                return bool(ok)
            except Exception:
                pass
        # In-memory
        now = datetime.now(timezone.utc).timestamp()
        elapsed = max(0.0, now - self._mem_ts)
        self._mem_tokens = min(self.burst, self._mem_tokens + (elapsed / self.interval) * self.rate)
        self._mem_ts = now
        if self._mem_tokens >= tokens:
            self._mem_tokens -= tokens
            return True
        return False


async def rate_limiter(source: str, max_calls: int = 100, window_seconds: int = 3600):
    """Simple rate limiter using TokenBucket."""
    bucket = TokenBucket(
        key=f"rate_limit:{source}",
        rate=max_calls,
        interval=window_seconds,
        burst=max_calls
    )
    
    if not await bucket.acquire(1):
        raise ValueError(f"Rate limit exceeded for {source}: {max_calls} calls per {window_seconds}s")
    return True
