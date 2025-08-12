import os
from typing import List, Optional, Dict, Any, Tuple, AsyncGenerator
from datetime import datetime, timezone
from fastapi import FastAPI, Request, HTTPException, WebSocket
from fastapi import Body
from fastapi.responses import StreamingResponse, HTMLResponse, PlainTextResponse
from fastapi.responses import JSONResponse
from fastapi.middleware.cors import CORSMiddleware
import json
import os
import asyncio
from pydantic import BaseModel
from sqlalchemy import text
from sqlalchemy.ext.asyncio import create_async_engine, AsyncEngine
from urllib.parse import parse_qs
import hmac
import hashlib

DB_HOST = os.getenv("POSTGRES_HOST", "db")
DB_PORT = os.getenv("POSTGRES_PORT", "5432")
DB_NAME = os.getenv("POSTGRES_DB", "heatmap")
DB_USER = os.getenv("POSTGRES_USER", "heatmap")
DB_PASS = os.getenv("POSTGRES_PASSWORD", "heatmap")
DATABASE_URL = f"postgresql+asyncpg://{DB_USER}:{DB_PASS}@{DB_HOST}:{DB_PORT}/{DB_NAME}"

app = FastAPI(title="ET Heatmap API")
_CORS = os.getenv("API_CORS_ORIGINS", "*")
_CORS_LIST = [o.strip() for o in _CORS.split(",") if o.strip()]
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"] if _CORS_LIST == ["*"] else _CORS_LIST,
    allow_methods=["*"],
    allow_headers=["*"]
)
engine: AsyncEngine = create_async_engine(DATABASE_URL, future=True, echo=False)

# simple metrics
_metrics = {
    "requests_total": 0,
    "latencies_ms": [],
    "latency_sum_ms": 0.0,
    "latency_count": 0,
    "by_path": {},  # path -> count
    "by_method": {},  # method -> count
}

@app.middleware("http")
async def metrics_middleware(request: Request, call_next):
    import time
    start = time.perf_counter()
    response = await call_next(request)
    elapsed = (time.perf_counter() - start) * 1000.0
    try:
        _metrics["requests_total"] += 1
        # by method
        m = request.method.upper()
        _metrics["by_method"][m] = _metrics["by_method"].get(m, 0) + 1
        # by path (route pattern best-effort)
        p = request.url.path
        _metrics["by_path"][p] = _metrics["by_path"].get(p, 0) + 1
        if len(_metrics["latencies_ms"]) < 1000:
            _metrics["latencies_ms"].append(elapsed)
        _metrics["latency_sum_ms"] += elapsed
        _metrics["latency_count"] += 1
    except Exception:
        pass
    return response


# ---- Auth / Rate limiting helpers ----
_API_REQUIRE_KEY = os.getenv("API_REQUIRE_KEY", "0") == "1"
_API_KEY = os.getenv("API_KEY", "").strip()
_rate_counters: Dict[str, Tuple[float, int]] = {}

def _client_key(request: Request) -> str:
    return request.headers.get("X-API-Key") or request.client.host if request.client else "anon"

def _require_api_key(request: Request) -> None:
    if not _API_REQUIRE_KEY:
        return
    key = request.headers.get("X-API-Key") or request.query_params.get("api_key")
    if not key or (_API_KEY and key != _API_KEY):
        raise HTTPException(status_code=401, detail="Unauthorized")

def _rate_limit(bucket: str, request: Request, rpm: int = 60) -> None:
    # naive in-memory limiter with 60s windows; can extend to Redis
    now = datetime.now(timezone.utc).timestamp()
    ident = f"{bucket}:{_client_key(request)}"
    window, count = _rate_counters.get(ident, (now, 0))
    if now - window >= 60:
        window, count = now, 0
    count += 1
    _rate_counters[ident] = (window, count)
    if count > rpm:
        raise HTTPException(status_code=429, detail="Rate limit exceeded")

def _with_cache_headers(data: Any, max_age: int = 15) -> JSONResponse:
    body = json.dumps(data, default=str, separators=(",", ":")).encode()
    import hashlib
    etag = hashlib.sha1(body).hexdigest()
    resp = JSONResponse(content=data)
    resp.headers["ETag"] = etag
    resp.headers["Cache-Control"] = f"public, max-age={max_age}"
    return resp


# -------- Simple in-memory TTL cache (avoid extra deps) ---------
class _SimpleTTLCache:
    def __init__(self, default_ttl_seconds: int = 30) -> None:
        self._store: Dict[str, Tuple[float, Any]] = {}
        self._ttl = default_ttl_seconds
        self._hits = 0
        self._misses = 0

    def get(self, key: str) -> Optional[Any]:
        item = self._store.get(key)
        if not item:
            self._misses += 1
            return None
        expires_at, value = item
        now = datetime.now(timezone.utc).timestamp()
        if now >= expires_at:
            self._store.pop(key, None)
            self._misses += 1
            return None
        self._hits += 1
        return value

    def set(self, key: str, value: Any, ttl: Optional[int] = None) -> None:
        exp = (ttl if ttl is not None else self._ttl)
        expires_at = datetime.now(timezone.utc).timestamp() + max(1, int(exp))
        self._store[key] = (expires_at, value)

    def clear(self) -> None:
        self._store.clear()
        self._hits = 0
        self._misses = 0

    def stats(self) -> Dict[str, Any]:
        total = self._hits + self._misses
        hit_rate = (self._hits / total) if total else 0.0
        return {"entries": len(self._store), "hits": self._hits, "misses": self._misses, "hit_rate": hit_rate}


_cache = _SimpleTTLCache(default_ttl_seconds=30)

# Optional Redis cache (feature-flagged)
try:  # redis is optional
    import redis.asyncio as _redis_async  # type: ignore
except Exception:  # pragma: no cover
    _redis_async = None

_REDIS_ENABLED = os.getenv("API_REDIS_CACHE", "0") == "1" and _redis_async is not None
_REDIS_URL = os.getenv("REDIS_URL", "redis://redis:6379/0")
_redis_client = None


async def _get_redis_client():
    global _redis_client
    if not _REDIS_ENABLED:
        return None
    if _redis_client is None:
        try:
            assert _redis_async is not None  # for type checkers
            _redis_client = _redis_async.from_url(_REDIS_URL, encoding="utf-8", decode_responses=True)
            await _redis_client.ping()
        except Exception:
            return None
    return _redis_client


async def _redis_get_json(key: str):
    client = await _get_redis_client()
    if not client:
        return None
    try:
        val = await client.get(key)
        return json.loads(val) if val else None
    except Exception:
        return None


async def _redis_set_json(key: str, value: Any, ttl: int = 30) -> None:
    client = await _get_redis_client()
    if not client:
        return
    try:
        await client.setex(key, ttl, json.dumps(value, default=str))
    except Exception:
        return


@app.get("/budget")
async def get_budget():
    try:
        from libs.budget import BudgetManager  # lazy import
        mgr = BudgetManager(os.getenv("REDIS_URL", "redis://redis:6379/0"))
        return await mgr.summary()
    except Exception:
        return {"limits": {}, "usage": {}, "remaining": {}, "note": "budget module unavailable"}

class HeatItem(BaseModel):
    rank: int
    entity: str
    heat: float
    reasons: List[str] = []

@app.get("/health")
async def health():
    try:
        async with engine.connect() as conn:
            await conn.execute(text("SELECT 1"))
        return {"status": "ok"}
    except Exception as e:
        return {"status": "error", "detail": str(e)}

@app.get("/source-health")
async def source_health():
    try:
        async with engine.connect() as conn:
            rows = (await conn.execute(text("SELECT source, last_ok, last_error, consecutive_errors, circuit_open_until FROM source_health"))).fetchall()
        items = [
            {
                "source": r[0],
                "last_ok": r[1].isoformat() if r[1] else None,
                "last_error": r[2].isoformat() if r[2] else None,
                "consecutive_errors": int(r[3]) if r[3] is not None else 0,
                "circuit_open_until": r[4].isoformat() if r[4] else None,
            } for r in rows
        ]
        return _with_cache_headers({"count": len(items), "items": items}, max_age=10)
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@app.get("/health/sources")
async def health_sources():
    # Extended health summary per source, including rolling error rate, qps and cache hits (placeholders where not tracked)
    try:
        async with engine.connect() as conn:
            health_rows = (await conn.execute(text("SELECT source, last_ok, last_error, consecutive_errors, circuit_open_until FROM source_health"))).fetchall()
            # Aggregate recent audit logs (15 minutes)
            audit = (await conn.execute(text(
                """
                SELECT source,
                       SUM(CASE WHEN LOWER(level) = 'error' THEN 1 ELSE 0 END) AS errors,
                       COUNT(*) AS total
                FROM audit_logs
                WHERE ts >= NOW() - INTERVAL '15 minutes'
                GROUP BY source
                """
            ))).fetchall()
        # Map audit stats
        a_map: Dict[str, Dict[str, float]] = {}
        window_s = 15 * 60.0
        for src, errs, tot in audit:
            tot_i = int(tot or 0)
            errs_i = int(errs or 0)
            a_map[str(src)] = {
                "rolling_error_rate": (errs_i / tot_i) if tot_i > 0 else 0.0,
                "qps": (tot_i / window_s),
            }
        items: Dict[str, Any] = {}
        for r in health_rows:
            src = str(r[0])
            stats = a_map.get(src, {"rolling_error_rate": 0.0, "qps": 0.0})
            items[src] = {
                "status": "degraded" if r[3] and int(r[3]) > 0 else "ok",
                "last_success_ts": r[1].isoformat() if r[1] else None,
                "last_error": r[2].isoformat() if r[2] else None,
                "rolling_error_rate": float(stats["rolling_error_rate"]),
                "qps": float(stats["qps"]),
                "cache_hit_rate": _cache.stats().get("hit_rate", 0.0),
                "circuit_breaker": "open" if (r[4] and r[4] > datetime.now(timezone.utc)) else "closed",
            }
        return _with_cache_headers(items, max_age=5)
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

@app.get("/top", response_model=List[HeatItem])
async def top(limit: int = 10):
    q = text("""
        WITH latest AS (
          SELECT s.entity_id, MAX(s.ts) AS ts
          FROM scores s
          WHERE s.ts >= NOW() - INTERVAL '3 days'
          GROUP BY s.entity_id
        )
        SELECT e.name, s.heat, s.velocity_z, s.accel, s.xplat, s.tentpole
        FROM scores s
        JOIN latest l ON l.entity_id = s.entity_id AND l.ts = s.ts
        JOIN entities e ON e.id = s.entity_id
        ORDER BY s.heat DESC
        LIMIT :limit
    """)
    async with engine.connect() as conn:
        res = await conn.execute(q, {"limit": limit})
        rows = res.fetchall()
    items = []
    for i, (name, heat, v, a, x, tp) in enumerate(rows, start=1):
        reasons = []
        if v and v > 0.8: reasons.append("High velocity vs 30-day baseline")
        if a and a > 0: reasons.append("Acceleration positive")
        if x and x >= 1.0: reasons.append("Cross-platform confirmation")
        if tp and tp > 0: reasons.append("Tentpole boost active")
        items.append(HeatItem(rank=i, entity=name, heat=float(heat), reasons=reasons))
    return items


# -------- Intelligent API (lightweight implementation) ---------

def _calculate_change(hist_data, metric: str) -> Tuple[float, str]:
    """Calculate percentage change and direction for a metric."""
    if len(hist_data) < 2:
        return 0.0, "─"
    
    metric_idx = {'heat': 0, 'velocity': 1, 'acceleration': 2}.get(metric, 0)
    current = float(hist_data[0][metric_idx] or 0)
    previous = float(hist_data[1][metric_idx] or 0) if len(hist_data) > 1 else current
    
    if previous == 0:
        return 0.0, "─"
    
    change_pct = ((current - previous) / abs(previous)) * 100
    
    if change_pct > 5:
        return change_pct, "▲+"
    elif change_pct < -5:
        return abs(change_pct), "▼-"
    else:
        return abs(change_pct), "─"

def _generate_sparkline(hist_data) -> str:
    """Generate ASCII sparkline from historical heat data."""
    if len(hist_data) < 2:
        return "▄▄▄"
    
    heat_values = [float(row[0] or 0) for row in hist_data[:7]]  # Last 7 points
    heat_values.reverse()  # Chronological order
    
    if not heat_values or all(v == 0 for v in heat_values):
        return "▄▄▄"
    
    min_val, max_val = min(heat_values), max(heat_values)
    if max_val == min_val:
        return "▄" * len(heat_values)
    
    # Normalize to 0-7 range for spark chars
    chars = "▁▂▃▄▅▆▇█"
    normalized = [(v - min_val) / (max_val - min_val) * 7 for v in heat_values]
    return "".join(chars[min(int(n), 7)] for n in normalized)

def _get_active_platforms(entity_name: str, cross_platform: Optional[float]) -> str:
    """Determine which platforms are driving the signal."""
    platforms = []
    
    # This is a simplified version - in a full implementation, 
    # you'd query the signals table to see which sources have recent data
    if cross_platform and cross_platform > 0:
        platforms.append("Multi")
    
    # Placeholder logic - extend based on actual signal sources
    platforms.extend(["Trends", "Wiki"])  # Base platforms always active
    
    if len(platforms) > 3:
        return f"{len(platforms)} platforms"
    return " + ".join(platforms)

def _generate_narrative_insight(entity_name: str, heat: float, velocity: Optional[float], 
                               acceleration: Optional[float], heat_change: float, platforms: str) -> str:
    """Generate human-readable narrative insight."""
    insights = []
    
    # Heat-based insights
    if heat > 50:
        insights.append(f"🔥 Exceptionally hot")
    elif heat > 10:
        insights.append(f"📈 Trending strongly")
    elif heat > 0:
        insights.append(f"📊 Steady activity")
    else:
        insights.append(f"📉 Cooling down")
    
    # Acceleration insights
    if acceleration and acceleration > 100:
        insights.append(f"⚡ Rapid acceleration (+{acceleration:.0f})")
    elif acceleration and acceleration > 10:
        insights.append(f"🚀 Building momentum")
    elif acceleration and acceleration < -50:
        insights.append(f"🛑 Sharp decline")
    
    # Change insights
    if abs(heat_change) > 20:
        direction = "surge" if heat_change > 0 else "drop"
        insights.append(f"📱 {direction} in mentions")
    
    # Platform insights  
    if "Multi" in platforms:
        insights.append("🌐 Cross-platform buzz")
    
    # Entity-specific insights (placeholder for ML-driven insights)
    if "Taylor" in entity_name:
        insights.append("🎵 Music industry impact")
    elif "Emmy" in entity_name:
        insights.append("🏆 Awards season activity")
    elif "Dune" in entity_name:
        insights.append("🎬 Film/franchise discussion")
    
    result = " • ".join(insights[:3])  # Limit to 3 insights
    return result if result else "📋 Standard tracking"

def _calculate_priority(heat: float, acceleration: Optional[float], heat_change: float) -> Dict[str, str]:
    """Calculate priority level and action recommendation."""
    accel = acceleration or 0
    
    # Critical: High heat + high acceleration
    if heat > 20 and accel > 50:
        return {"level": "critical", "label": "🚨 URGENT"}
    
    # High: High heat OR high acceleration OR major change
    if heat > 10 or accel > 20 or abs(heat_change) > 30:
        return {"level": "high", "label": "⚡ HIGH"}
    
    # Medium: Moderate activity
    if heat > 0 or accel > 0 or abs(heat_change) > 10:
        return {"level": "medium", "label": "📊 MEDIUM"}
    
    # Low: Declining or stable
    return {"level": "low", "label": "📈 LOW"}


@app.get("/trends/enhanced")
async def get_enhanced_trends(
    request: Request,
    category: Optional[str] = None,
    limit: int = 20,
    include_history: bool = True,
    include_signals: bool = False
):
    """Enhanced trends API with historical context, change indicators, and narrative insights."""
    _rate_limit("trends", request, rpm=120)
    
    async with engine.connect() as conn:
        # Get current scores
        rows = await _query_latest_scores(conn, category, limit)
        
        enhanced_trends = []
        for i, row in enumerate(rows, start=1):
            entity_name, cat, heat, v, a, x, tp = row
            
            trend_item = {
                "rank": i,
                "entity": entity_name,
                "category": cat,
                "heat": {
                    "current": float(heat),
                    "change_24h": 0.0,
                    "direction": "stable",
                    "percentile": 0  # Placeholder
                },
                "velocity": float(v) if v is not None else None,
                "acceleration": float(a) if a is not None else None,
                "cross_platform": float(x) if x is not None else None,
                "tentpole": float(tp) if tp is not None else None,
                "platforms": {"active": [], "count": 0},
                "priority": {"level": "medium", "score": 0.5, "action": "monitor"},
                "insights": {
                    "narrative": f"{entity_name} showing steady activity",
                    "key_drivers": [],
                    "predicted_trajectory": "stable"
                },
                "timestamp": datetime.now(timezone.utc).isoformat()
            }
            
            if include_history:
                # Get historical data for trends
                hist_q = text("""
                    SELECT s.heat, s.velocity_z, s.accel, s.ts
                    FROM scores s
                    JOIN entities e ON e.id = s.entity_id
                    WHERE e.name = :name 
                    AND s.ts >= NOW() - INTERVAL '7 days'
                    ORDER BY s.ts DESC
                    LIMIT 20
                """)
                hist_rows = (await conn.execute(hist_q, {"name": entity_name})).fetchall()
                
                if len(hist_rows) >= 2:
                    current_heat = float(hist_rows[0][0] or 0)
                    prev_heat = float(hist_rows[1][0] or 0)
                    
                    if prev_heat != 0:
                        change_pct = ((current_heat - prev_heat) / abs(prev_heat)) * 100
                        trend_item["heat"]["change_24h"] = round(change_pct, 2)
                        
                        if change_pct > 5:
                            trend_item["heat"]["direction"] = "rising"
                        elif change_pct < -5:
                            trend_item["heat"]["direction"] = "falling"
                
                # Add sparkline data
                trend_item["sparkline"] = {
                    "data": [float(row[0] or 0) for row in hist_rows[:7]],
                    "ascii": _generate_sparkline(hist_rows)
                }
            
            if include_signals:
                # Get signal breakdown
                signals_q = text("""
                    SELECT s.source, COUNT(*) as signal_count, MAX(s.ts) as latest
                    FROM signals s
                    JOIN entities e ON e.id = s.entity_id
                    WHERE e.name = :name 
                    AND s.ts >= NOW() - INTERVAL '24 hours'
                    GROUP BY s.source
                """)
                signal_rows = (await conn.execute(signals_q, {"name": entity_name})).fetchall()
                
                active_platforms = []
                for source, count, latest in signal_rows:
                    active_platforms.append({
                        "source": source,
                        "signal_count": int(count),
                        "latest_signal": latest.isoformat() if latest else None
                    })
                
                trend_item["platforms"] = {
                    "active": active_platforms,
                    "count": len(active_platforms)
                }
            
            # Enhanced priority and insights
            priority = _calculate_priority(float(heat), a, trend_item["heat"]["change_24h"])
            trend_item["priority"] = {
                "level": priority["level"],
                "score": {"critical": 1.0, "high": 0.8, "medium": 0.5, "low": 0.2}[priority["level"]],
                "action": {
                    "critical": "immediate_action",
                    "high": "investigate_now", 
                    "medium": "monitor_closely",
                    "low": "routine_tracking"
                }[priority["level"]]
            }
            
            # Enhanced narrative
            narrative = _generate_narrative_insight(
                entity_name, float(heat), v, a, 
                trend_item["heat"]["change_24h"], 
                f"{len(active_platforms) if include_signals else 2} platforms"
            )
            trend_item["insights"]["narrative"] = narrative
            
            # Predicted trajectory (simplified)
            if a and a > 10:
                trend_item["insights"]["predicted_trajectory"] = "accelerating"
            elif a and a < -10:
                trend_item["insights"]["predicted_trajectory"] = "declining"
            
            enhanced_trends.append(trend_item)
    
    return _with_cache_headers({
        "count": len(enhanced_trends),
        "format": "enhanced",
        "category": category,
        "timestamp": datetime.now(timezone.utc).isoformat(),
        "trends": enhanced_trends
    }, max_age=10)


def _build_reasons(heat: float, v: Optional[float], a: Optional[float], x: Optional[float], tp: Optional[float]) -> List[str]:
    reasons: List[str] = []
    if v and v > 0.8:
        reasons.append("High velocity vs 30-day baseline")
    if a and a > 0:
        reasons.append("Acceleration positive")
    if x and x >= 1.0:
        reasons.append("Cross-platform confirmation")
    if tp and tp > 0:
        reasons.append("Tentpole boost active")
    if heat and heat > 0.85:
        reasons.append("Exceptional composite heat")
    return reasons

async def _query_latest_scores(conn, category: Optional[str], limit: int = 20, offset: int = 0, hours: int = 72):
    base_sql = """
        WITH latest AS (
          SELECT s.entity_id, MAX(s.ts) AS ts
          FROM scores s
        WHERE s.ts >= NOW() - make_interval(hours => :hours)
          GROUP BY s.entity_id
        )
        SELECT e.name, COALESCE(e.category, 'unknown') as category,
               s.heat, s.velocity_z, s.accel, s.xplat, s.tentpole
        FROM scores s
        JOIN latest l ON l.entity_id = s.entity_id AND l.ts = s.ts
        JOIN entities e ON e.id = s.entity_id
    """
    where_clause = " WHERE 1=1"
    params: Dict[str, Any] = {"limit": limit, "offset": offset, "hours": hours}
    if category:
        where_clause += " AND LOWER(COALESCE(e.category, '')) = LOWER(:category)"
        params["category"] = category
    order_limit = " ORDER BY s.heat DESC LIMIT :limit OFFSET :offset"
    q = text(base_sql + where_clause + order_limit)
    res = await conn.execute(q, params)
    return res.fetchall()

def _format_trend_item(rank: int, row) -> Dict[str, Any]:
    name, category, heat, v, a, x, tp = row
    return {
        "rank": rank,
        "entity": name,
        "category": category,
        "heat": float(heat),
        "components": {
            "velocity": float(v) if v is not None else None,
            "acceleration": float(a) if a is not None else None,
            "cross_platform": float(x) if x is not None else None,
            "tentpole": float(tp) if tp is not None else None,
        },
        "reasons": _build_reasons(float(heat), v, a, x, tp),
    "timestamp": datetime.now(timezone.utc).isoformat(),
    }

def _identify_user(request: Request) -> str:
    # Stateless lightweight identification (placeholder)
    return request.headers.get("X-User-ID", "anonymous")

def _get_user_profile(user_id: str) -> Dict[str, Any]:
    # Placeholder user profile; can be wired to DB later
    return {"id": user_id, "interests": [], "request_frequency": 0, "preferred_categories": []}

def _determine_strategy(user_profile: Dict[str, Any], depth: str) -> str:
    if user_profile.get("request_frequency", 0) > 100:
        return "cached"
    if depth == "deep":
        return "deep"
    return "standard"

def _format_response(trends: List[Dict[str, Any]], out_format: str, meta: Optional[Dict[str, Any]] = None) -> Any:
    # For now we only support JSON; extend later to CSV/PDF
    return {"count": len(trends), "format": out_format, **({} if meta is None else meta), "trends": trends}

@app.get("/trends/intelligent")
async def get_intelligent_trends(
    request: Request,
    category: Optional[str] = None,
    timeframe: str = "now",
    depth: str = "standard",
    format: str = "json",
    personalized: bool = True,
    limit: int = 20,
    offset: int = 0,
    hours: int = 72,
):
    """Intelligent trend endpoint (lightweight). Reuses latest scores, adds structure and reasons.
    - category: optional exact category filter (case-insensitive)
    - timeframe: currently informational; extend later
    - depth: standard|deep (deep reserved)
    - format: json (csv/pdf reserved)
    - personalized: reserved for future use
    """

    user_id = _identify_user(request)
    user_profile = _get_user_profile(user_id) if personalized else {"request_frequency": 0}
    strategy = _determine_strategy(user_profile, depth)

    # Try cache (only for standard depth + json output)
    cache_key = f"trends:intelligent:{(category or '').lower()}:{limit}:{offset}:{hours}:{depth}:{format}"
    if depth == "standard" and format == "json":
        # Prefer Redis if enabled
        cached = await _redis_get_json(cache_key) if _REDIS_ENABLED else None
        if cached is None:
            cached = _cache.get(cache_key)
        if cached is not None:
            return cached

    async with engine.connect() as conn:
        rows = await _query_latest_scores(conn, category, limit, offset, hours)

    trends = [_format_trend_item(i, row) for i, row in enumerate(rows, start=1)]

    # Placeholder personalization hook (no-op for now)
    if personalized and user_profile.get("preferred_categories"):
        # Could re-rank here; leaving as-is for safety
        pass

    meta = {"strategy": strategy, "category": category, "timeframe": timeframe}

    if format == "csv":
        return StreamingResponse(
            _csv_trends_iter(trends),
            media_type="text/csv",
            headers={"Content-Disposition": "attachment; filename=trends.csv"},
        )

    response = _format_response(trends, format, meta)
    _maybe_cache_response(depth, format, cache_key, response)
    # Add cache headers + ETag only for JSON standard depth
    if format == "json" and depth == "standard":
        return _with_cache_headers(response, max_age=15)
    return response


# -------- Packages and votes --------

@app.get("/packages/{entity_id}")
async def get_package(entity_id: int):
    try:
        async with engine.connect() as conn:
            row = (await conn.execute(text("SELECT name FROM entities WHERE id=:id"), {"id": int(entity_id)})).first()
            if not row:
                raise HTTPException(status_code=404, detail="entity not found")
            name = str(row[0])
        from libs.packages import generate_package
        pack = generate_package(name, receipts=[])
        return {
            "entity": pack.entity,
            "generated_ts": pack.generated_ts,
            "booking_brief": pack.booking_brief,
            "promo_lines": pack.promo_lines,
            "graphics": pack.graphics,
            "receipts": pack.receipts,
        }
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


class VoteBody(BaseModel):
    useful: bool


@app.post("/alerts/{alert_uuid}/vote")
async def vote_alert(alert_uuid: str, body: VoteBody, request: Request):
    _rate_limit("vote", request, rpm=120)
    voter = _client_key(request)
    try:
        async with engine.begin() as conn:
            # Ensure alert exists
            row = (await conn.execute(text("SELECT 1 FROM alerts WHERE alert_uuid::text=:u"), {"u": alert_uuid})).first()
            if not row:
                raise HTTPException(status_code=404, detail="alert not found")
            await conn.execute(
                text("INSERT INTO alert_votes(alert_uuid, useful, voter, ts) VALUES (:u, :useful, :voter, :ts)"),
                {"u": alert_uuid, "useful": bool(body.useful), "voter": voter, "ts": datetime.now(timezone.utc)},
            )
        return {"ok": True}
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


def _csv_trends_iter(trends: List[Dict[str, Any]]):
    cols = [
        "rank",
        "entity",
        "category",
        "heat",
        "velocity",
        "acceleration",
        "cross_platform",
        "tentpole",
        "reasons",
    ]
    yield ",".join(cols) + "\n"
    for it in trends:
        row = [
            str(it.get("rank", "")),
            it.get("entity", ""),
            it.get("category", ""),
            f"{it.get('heat', '')}",
            f"{(it.get('components',{}).get('velocity') or '')}",
            f"{(it.get('components',{}).get('acceleration') or '')}",
            f"{(it.get('components',{}).get('cross_platform') or '')}",
            f"{(it.get('components',{}).get('tentpole') or '')}",
            " ".join(it.get("reasons", [])),
        ]
        yield ",".join(row) + "\n"


def _maybe_cache_response(depth: str, format: str, cache_key: str, response: Any) -> None:
    if depth == "standard" and format == "json":
        _cache.set(cache_key, response, ttl=30)


@app.get("/discoveries/recent")
async def recent_discoveries(limit: int = 20, days: int = 14):
    """List recently discovered entities with confidence and velocity (if available)."""
    q = text(
        """
        SELECT d.entity,
               COALESCE(e.category, 'unknown') as category,
               MAX(d.ts) as last_seen,
               MAX(d.confidence) as confidence,
               MAX(d.velocity) as velocity
        FROM discovery_outcomes d
        LEFT JOIN entities e ON e.name = d.entity
        WHERE d.ts >= NOW() - make_interval(days => :days)
        GROUP BY d.entity, e.category
        ORDER BY last_seen DESC
        LIMIT :limit
        """
    )
    async with engine.connect() as conn:
        rows = (await conn.execute(q, {"days": days, "limit": limit})).fetchall()
    items = [
        {
            "entity": r[0],
            "category": r[1],
            "last_seen": r[2].isoformat() if hasattr(r[2], "isoformat") else str(r[2]),
            "confidence": float(r[3]) if r[3] is not None else None,
            "velocity": float(r[4]) if r[4] is not None else None,
        }
        for r in rows
    ]
    return _with_cache_headers({"count": len(items), "items": items}, max_age=30)


@app.get("/metrics-lite")
async def metrics_lite():
    lat = _metrics.get("latencies_ms", [])
    p50 = sorted(lat)[int(0.5 * len(lat))] if lat else 0.0
    p90 = sorted(lat)[int(0.9 * len(lat))] if len(lat) > 1 else p50
    return {"requests_total": _metrics.get("requests_total", 0), "p50_ms": round(p50, 2), "p90_ms": round(p90, 2)}


@app.get("/metrics")
async def metrics_prometheus():
    total = int(_metrics.get("requests_total", 0))
    lat_sum = float(_metrics.get("latency_sum_ms", 0.0))
    lat_count = int(_metrics.get("latency_count", 0))
    lat = _metrics.get("latencies_ms", [])
    p50 = sorted(lat)[int(0.5 * len(lat))] if lat else 0.0
    p90 = sorted(lat)[int(0.9 * len(lat))] if len(lat) > 1 else p50
    lines = []
    lines.append("# HELP api_requests_total Total HTTP requests")
    lines.append("# TYPE api_requests_total counter")
    lines.append(f"api_requests_total {total}")
    # per method
    lines.append("# HELP api_requests_by_method Total HTTP requests by method")
    lines.append("# TYPE api_requests_by_method counter")
    for m, c in _metrics.get("by_method", {}).items():
        lines.append(f'api_requests_by_method{{method="{m}"}} {int(c)}')
    # per path (raw path; can be high-cardinality, keep light)
    lines.append("# HELP api_requests_by_path Total HTTP requests by raw path (high-cardinality)")
    lines.append("# TYPE api_requests_by_path counter")
    for p, c in _metrics.get("by_path", {}).items():
        # escape quotes in path
        sp = p.replace('\\', '\\\\').replace('"', '\\"')
        lines.append(f'api_requests_by_path{{path="{sp}"}} {int(c)}')
    lines.append("# HELP api_request_latency_ms Request latency in milliseconds (summary)")
    lines.append("# TYPE api_request_latency_ms summary")
    lines.append(f'api_request_latency_ms{{quantile="0.5"}} {p50:.6f}')
    lines.append(f'api_request_latency_ms{{quantile="0.9"}} {p90:.6f}')
    lines.append(f"api_request_latency_ms_sum {lat_sum:.6f}")
    lines.append(f"api_request_latency_ms_count {lat_count}")
    body = "\n".join(lines) + "\n"
    return PlainTextResponse(content=body, media_type="text/plain; version=0.0.4")


# ---- Advanced scoring (optional) ----
try:
    from libs.scoring_advanced import AdvancedScoringEngine  # type: ignore
except Exception:  # pragma: no cover
    AdvancedScoringEngine = None  # type: ignore

_adv_engine = AdvancedScoringEngine() if AdvancedScoringEngine else None


def _accumulate_signal(signals: Dict[str, Any], src: str, metric: str, value: float) -> None:
    ts_map = {
        ("wiki", "views"): "wiki_pageviews",
        ("trends", "interest"): "trends_interest",
    }
    key = ts_map.get((src, metric))
    if key:
        arr = signals.setdefault(key, [])
        if isinstance(arr, list) and len(arr) < 60:
            arr.append(float(value))
        return

    if src == "gdelt_gkg":
        gkg_map = {"gkg_mentions": "gkg_mentions", "gkg_tone_avg": "gkg_tone_avg"}
        gkg_key = gkg_map.get(metric)
        if gkg_key:
            signals[gkg_key] = float(value)
        return

    if src == "tt_search":
        ttd = signals.setdefault("tiktok_data", {})
        if isinstance(ttd, dict):
            ttd[metric] = float(value)


@app.get("/score/advanced/{entity}")
async def score_advanced(entity: str):
    from libs.config import is_enabled


    if not is_enabled("advanced_scoring"):
        raise HTTPException(status_code=404, detail="Advanced scoring disabled")
    if not _adv_engine:
        raise HTTPException(status_code=503, detail="Advanced scoring unavailable")
    # Gather minimal signals from DB
    async with engine.connect() as conn:
        sig_q = text(
            """
            SELECT s.source, s.metric, s.value
            FROM signals s
            JOIN entities e ON e.id=s.entity_id
            WHERE e.name=:name AND s.ts >= NOW() - make_interval(days => 30)
            """
        )
        rows = (await conn.execute(sig_q, {"name": entity})).fetchall()
    signals: Dict[str, Any] = {}
    for src, metric, value in rows:
        _accumulate_signal(signals, src, metric, value)
    result = _adv_engine.calculate_multidimensional_heat_score(entity, signals)
    return result


@app.websocket("/ws")
async def ws_trends(ws: WebSocket):
    # simple header/query API key guard if enabled
    if _API_REQUIRE_KEY and _API_KEY:
        key = ws.headers.get("x-api-key") or ws.query_params.get("api_key")
        if key != _API_KEY:
            await ws.close(code=1008)
            return
    await ws.accept()
    category = ws.query_params.get("category") if hasattr(ws, "query_params") else None
    last_hash = None
    try:
        while True:
            async with engine.connect() as conn:
                rows = await _query_latest_scores(conn, category, limit=10, offset=0, hours=72)
            items = [_format_trend_item(i, r) for i, r in enumerate(rows, start=1)]
            payload = {"type": "trends", "category": category, "count": len(items), "items": items}
            import hashlib
            h = hashlib.sha1(json.dumps(payload, default=str, separators=(",", ":")).encode()).hexdigest()
            if h != last_hash:
                await ws.send_json(payload)
                last_hash = h
            # ping every loop to keepalive
            try:
                await ws.send_text("ping")
            except Exception:
                pass
            await asyncio.sleep(5)
    except Exception:
        return


@app.get("/entity/{entity_name}/analysis")
async def get_entity_analysis(
    entity_name: str,
    include_predictions: bool = False,
    include_related: bool = False,
    include_timeline: bool = True,
    days: int = 7,
):
    """Return lightweight analysis for an entity from recent stored scores.
    No heavy ML; this is DB-powered and safe.
    """

    async with engine.connect() as conn:
        entity_id, canon_name, category = await _resolve_entity(conn, entity_name)
        latest = await _fetch_latest_score(conn, entity_id)
        timeline = await _fetch_timeline(conn, entity_id, days) if include_timeline else []

    result = _assemble_entity_analysis_response(
        canon_name,
        category,
        latest,
        timeline if include_timeline else None,
        include_related,
        include_predictions,
    )
    return result


async def _resolve_entity(conn, name: str):
    q = text(
        """
        SELECT id, name, COALESCE(category, 'unknown')
        FROM entities
        WHERE LOWER(name) = LOWER(:name)
        LIMIT 1
        """
    )
    res = await conn.execute(q, {"name": name})
    row = res.first()
    if not row:
        raise HTTPException(status_code=404, detail="Entity not found")
    return row


async def _fetch_latest_score(conn, entity_id: int):
    q = text(
        """
        SELECT s.ts, s.heat, s.velocity_z, s.accel, s.xplat, s.tentpole
        FROM scores s
        WHERE s.entity_id = :eid
        ORDER BY s.ts DESC
        LIMIT 1
        """
    )
    res = await conn.execute(q, {"eid": entity_id})
    return res.first()


async def _fetch_timeline(conn, entity_id: int, days: int):
    q = text(
        """
        SELECT s.ts, s.heat, s.velocity_z, s.accel
        FROM scores s
        WHERE s.entity_id = :eid
          AND s.ts >= NOW() - make_interval(days => :days)
        ORDER BY s.ts ASC
        """
    )
    res = await conn.execute(q, {"eid": entity_id, "days": days})
    return [
        {
            "ts": ts.isoformat() if hasattr(ts, "isoformat") else str(ts),
            "heat": float(h) if h is not None else None,
            "velocity": float(v) if v is not None else None,
            "acceleration": float(a) if a is not None else None,
        }
        for (ts, h, v, a) in res.fetchall()
    ]


def _assemble_entity_analysis_response(
    canon_name: str,
    category: str,
    latest_row,
    timeline: Optional[List[Dict[str, Any]]],
    include_related: bool,
    include_predictions: bool,
) -> Dict[str, Any]:
    current_score = _format_current_score(latest_row)

    result: Dict[str, Any] = {
        "entity": canon_name,
        "category": category,
        "current_score": current_score,
        "status": _classify_status(latest_row[1]) if latest_row else "unknown",
    }

    _maybe_add_timeline(result, timeline)
    _maybe_add_related(result, include_related)
    _maybe_add_predictions(result, include_predictions)
    return result


def _classify_status(heat: Any) -> str:
    try:
        h = float(heat)
    except (TypeError, ValueError):
        return "unknown"
    if h >= 0.8:
        return "hot"
    if h >= 0.5:
        return "warm"
    return "cool"


def _format_current_score(latest_row):
    if not latest_row:
        return None
    ts, heat, v, a, x, tp = latest_row
    return {
        "ts": ts.isoformat() if hasattr(ts, "isoformat") else str(ts),
        "heat": float(heat) if heat is not None else None,
        "components": {
            "velocity": float(v) if v is not None else None,
            "acceleration": float(a) if a is not None else None,
            "cross_platform": float(x) if x is not None else None,
            "tentpole": float(tp) if tp is not None else None,
        },
        "reasons": _build_reasons(float(heat) if heat is not None else 0.0, v, a, x, tp),
    }


def _maybe_add_timeline(result: Dict[str, Any], timeline: Optional[List[Dict[str, Any]]]) -> None:
    if timeline is not None:
        result["timeline"] = timeline


def _maybe_add_related(result: Dict[str, Any], include_related: bool) -> None:
    if include_related:
        result["related"] = []


def _maybe_add_predictions(result: Dict[str, Any], include_predictions: bool) -> None:
    if include_predictions:
        result["predictions"] = {
            "will_trend": None,
            "peak_score": None,
            "days_to_peak": None,
            "confidence": 0.0,
        }


@app.get("/export/{fmt}")
async def export_trends(fmt: str, timerange: str = "24h", category: Optional[str] = None, limit: int = 100):
    """Export trends as CSV or JSON using recent scores.
    timerange currently accepted for compatibility; using 3-day window internally.
    """
    async with engine.connect() as conn:
        rows = await _query_latest_scores(conn, category, limit)
    records = [
        {
            "rank": i,
            "entity": r[0],
            "category": r[1],
            "heat": float(r[2]) if r[2] is not None else None,
            "velocity": float(r[3]) if r[3] is not None else None,
            "acceleration": float(r[4]) if r[4] is not None else None,
            "cross_platform": float(r[5]) if r[5] is not None else None,
            "tentpole": float(r[6]) if r[6] is not None else None,
        }
        for i, r in enumerate(rows, start=1)
    ]

    if fmt.lower() == "json":
        return {"count": len(records), "items": records}
    if fmt.lower() == "csv":
        def csv_iter():
            cols = [
                "rank",
                "entity",
                "category",
                "heat",
                "velocity",
                "acceleration",
                "cross_platform",
                "tentpole",
            ]
            yield ",".join(cols) + "\n"
            for rec in records:
                row = [str(rec.get(c, "")) for c in cols]
                yield ",".join(row) + "\n"

        return StreamingResponse(csv_iter(), media_type="text/csv", headers={"Content-Disposition": "attachment; filename=trends.csv"})

    raise HTTPException(status_code=400, detail="Unsupported format. Use 'json' or 'csv'.")


def _time_ago(dt):
    """Format a datetime as a human-readable time ago string."""
    now = datetime.now(timezone.utc)
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=timezone.utc)
    
    diff = now - dt
    seconds = int(diff.total_seconds())
    
    if seconds < 60:
        return f"{seconds}s"
    elif seconds < 3600:
        return f"{seconds // 60}m"
    elif seconds < 86400:
        return f"{seconds // 3600}h"
    else:
        return f"{seconds // 86400}d"


@app.get("/trends/dashboard")
async def trends_dashboard(category: Optional[str] = None, limit: int = 20, timeframe: str = "24h"):
    async with engine.connect() as conn:
        # Get current scores
        rows = await _query_latest_scores(conn, category, limit)
        
        # Get source health data for dashboard
        health_query = text("""
            SELECT 
                source,
                last_ok,
                last_error,
                consecutive_errors,
                CASE 
                    WHEN last_ok IS NULL AND last_error IS NULL THEN 'never_run'
                    WHEN last_ok > COALESCE(last_error, '1900-01-01') THEN 'healthy'
                    WHEN consecutive_errors >= 3 THEN 'failing'
                    ELSE 'warning'
                END as status,
                CASE 
                    WHEN last_ok > COALESCE(last_error, '1900-01-01') THEN last_ok
                    ELSE last_error
                END as last_run
            FROM source_health 
            ORDER BY last_run DESC NULLS LAST
        """)
        health_rows = (await conn.execute(health_query)).fetchall()
        
        # Get historical data for trend calculations
        hist_data = {}
        for row in rows:
            entity_name = row[0]
            hist_q = text("""
                SELECT s.heat, s.velocity_z, s.accel, s.ts
                FROM scores s
                JOIN entities e ON e.id = s.entity_id
                WHERE e.name = :name 
                AND s.ts >= NOW() - INTERVAL '7 days'
                ORDER BY s.ts DESC
                LIMIT 10
            """)
            hist_rows = (await conn.execute(hist_q, {"name": entity_name})).fetchall()
            hist_data[entity_name] = hist_rows
    
    # Calculate enhanced metrics
    items = []
    for i, row in enumerate(rows, start=1):
        entity_name, category, heat, v, a, x, tp = row
        hist = hist_data.get(entity_name, [])
        
        # Calculate trends and changes
        heat_change, heat_direction = _calculate_change(hist, 'heat')
        velocity_change, velocity_direction = _calculate_change(hist, 'velocity')
        accel_change, accel_direction = _calculate_change(hist, 'acceleration')
        
        # Generate sparkline data
        sparkline = _generate_sparkline(hist)
        
        # Enhanced platform info
        platforms = _get_active_platforms(entity_name, x)
        
        # Enhanced narrative insights
        insight = _generate_narrative_insight(entity_name, heat, v, a, heat_change, platforms)
        
        # Priority scoring
        priority = _calculate_priority(heat, a, heat_change)
        
        items.append({
            'rank': i,
            'entity': entity_name,
            'category': category,
            'heat': float(heat),
            'heat_change': heat_change,
            'heat_direction': heat_direction,
            'velocity': float(v) if v is not None else None,
            'velocity_change': velocity_change,
            'velocity_direction': velocity_direction,
            'acceleration': float(a) if a is not None else None,
            'accel_change': accel_change,
            'accel_direction': accel_direction,
            'cross_platform': float(x) if x is not None else None,
            'tentpole': float(tp) if tp is not None else None,
            'platforms': platforms,
            'sparkline': sparkline,
            'insight': insight,
            'priority': priority,
            'reasons': _build_reasons(float(heat), v, a, x, tp)
        })
    
    # Process source status for dashboard
    source_definitions = {
        'scrape_news': {'name': 'News Scraper', 'type': 'core'},
        'trade_rss': {'name': 'Trade RSS', 'type': 'rss'},
        'reddit': {'name': 'Reddit Monitor', 'type': 'api'},
        'youtube': {'name': 'YouTube Trends', 'type': 'api'},
        'imdb_box_office': {'name': 'IMDb Box Office', 'type': 'scraping'},
        'wiki': {'name': 'Wikipedia Trends', 'type': 'rate_limited'},
        'mvp_scoring': {'name': 'Heat Scoring', 'type': 'processing'}
    }
    
    # Type icon mapping
    type_icons = {
        'core': '🔧',
        'rss': '📰',
        'api': '🔗',
        'scraping': '🕷️',
        'rate_limited': '⏱️',
        'processing': '⚙️',
        'unknown': '❓'
    }
    
    source_status_html = ""
    status_counts = {'healthy': 0, 'warning': 0, 'failing': 0, 'never_run': 0}
    
    for row in health_rows:
        source, last_ok, last_error, consecutive_errors, status, last_run = row
        definition = source_definitions.get(source, {'name': source.title(), 'type': 'unknown'})
        
        status_counts[status] += 1
        
        # Format last run time
        if last_run:
            time_ago = _time_ago(last_run)
            last_run_display = f"{time_ago} ago"
        else:
            last_run_display = "Never"
        
        # Status styling
        status_class = f"status-{status}"
        status_text = status.replace('_', ' ').title()
        
        # Type icon
        type_icon = type_icons.get(definition['type'], '❓')
        
        source_status_html += f"""
        <div class="source-item {status_class}">
            <div class="source-header">
                <span class="source-icon">{type_icon}</span>
                <strong>{definition['name']}</strong>
                <span class="source-status {status_class}">{status_text}</span>
            </div>
            <div class="source-details">
                Last run: {last_run_display}
                {f" • {consecutive_errors} consecutive errors" if consecutive_errors > 0 else ""}
            </div>
        </div>
        """
    
    # Add sources that haven't run yet
    for source, definition in source_definitions.items():
        if not any(row[0] == source for row in health_rows):
            status_counts['never_run'] += 1
            type_icon = type_icons.get(definition['type'], '❓')
            source_status_html += f"""
            <div class="source-item status-never_run">
                <div class="source-header">
                    <span class="source-icon">{type_icon}</span>
                    <strong>{definition['name']}</strong>
                    <span class="source-status status-never_run">Never Run</span>
                </div>
                <div class="source-details">Not yet executed</div>
            </div>
            """
    
    # Generate enhanced HTML
    html_items = ""
    for it in items:
        # Format heat with trend indicator
        heat_display = f"{it['heat']:.2f} {it['heat_direction']}{abs(it['heat_change']):.1f}%" if it['heat_change'] != 0 else f"{it['heat']:.2f}"
        
        # Format velocity with trend
        vel_display = f"{it['velocity']:.3f} {it['velocity_direction']}" if it['velocity'] is not None else "N/A"
        
        # Format acceleration with trend  
        accel_display = f"{it['acceleration']:.1f} {it['accel_direction']}" if it['acceleration'] is not None else "N/A"
        
        # Priority badge
        priority_badge = f"<span class='priority-{it['priority']['level']}'>{it['priority']['label']}</span>"
        
        # Row styling based on priority
        row_class = f"priority-{it['priority']['level']}"
        
        html_items += f"""
        <tr class="{row_class}">
            <td>{it['rank']}</td>
            <td><strong>{it['entity']}</strong><br><small>{it['category']}</small></td>
            <td class="heat-cell">{heat_display}</td>
            <td>{vel_display}</td>
            <td>{accel_display}</td>
            <td>{it['platforms']}</td>
            <td class="sparkline">{it['sparkline']}</td>
            <td class="insight">{it['insight']}</td>
            <td>{priority_badge}</td>
        </tr>
        """
    
    html = f"""
    <html>
        <head>
            <title>ET Heatmap Dashboard</title>
            <style>
                body {{ 
                    font-family: system-ui, -apple-system, sans-serif; 
                    margin: 20px; 
                    background: #f8f9fa;
                }}
                .header {{
                    background: white;
                    padding: 20px;
                    border-radius: 8px;
                    margin-bottom: 20px;
                    box-shadow: 0 2px 4px rgba(0,0,0,0.1);
                }}
                .filters {{
                    margin: 10px 0;
                }}
                .filters select, .filters button {{
                    padding: 8px 12px;
                    margin-right: 10px;
                    border: 1px solid #ddd;
                    border-radius: 4px;
                }}
                table {{ 
                    border-collapse: collapse; 
                    width: 100%; 
                    background: white;
                    border-radius: 8px;
                    overflow: hidden;
                    box-shadow: 0 2px 4px rgba(0,0,0,0.1);
                }}
                th, td {{ 
                    border: 1px solid #e9ecef; 
                    padding: 12px 8px; 
                    text-align: left;
                }}
                th {{ 
                    background: #495057; 
                    color: white;
                    font-weight: 600;
                }}
                .heat-cell {{
                    font-weight: bold;
                }}
                .sparkline {{
                    font-family: monospace;
                    font-size: 14px;
                    color: #666;
                }}
                .insight {{
                    font-size: 14px;
                    max-width: 250px;
                    line-height: 1.4;
                }}
                .priority-critical {{
                    background: #dc3545;
                    color: white;
                    padding: 2px 6px;
                    border-radius: 3px;
                    font-size: 12px;
                }}
                .priority-high {{
                    background: #fd7e14;
                    color: white;
                    padding: 2px 6px;
                    border-radius: 3px;
                    font-size: 12px;
                }}
                .priority-medium {{
                    background: #ffc107;
                    color: #000;
                    padding: 2px 6px;
                    border-radius: 3px;
                    font-size: 12px;
                }}
                .priority-low {{
                    background: #6c757d;
                    color: white;
                    padding: 2px 6px;
                    border-radius: 3px;
                    font-size: 12px;
                }}
                tr.priority-critical {{
                    border-left: 4px solid #dc3545;
                }}
                tr.priority-high {{
                    border-left: 4px solid #fd7e14;
                }}
                .trend-up {{ color: #28a745; }}
                .trend-down {{ color: #dc3545; }}
                .trend-stable {{ color: #6c757d; }}
                .auto-refresh {{
                    position: fixed;
                    top: 20px;
                    right: 20px;
                    background: #007bff;
                    color: white;
                    padding: 8px 12px;
                    border-radius: 4px;
                    border: none;
                    cursor: pointer;
                }}
                .sources-section {{
                    background: white;
                    padding: 20px;
                    border-radius: 8px;
                    margin-bottom: 20px;
                    box-shadow: 0 2px 4px rgba(0,0,0,0.1);
                }}
                .sources-grid {{
                    display: grid;
                    grid-template-columns: repeat(auto-fit, minmax(300px, 1fr));
                    gap: 15px;
                    margin-top: 15px;
                }}
                .source-item {{
                    padding: 12px;
                    border-radius: 6px;
                    border-left: 4px solid #6c757d;
                }}
                .source-item.status-healthy {{
                    border-left-color: #28a745;
                    background: #f8fff9;
                }}
                .source-item.status-warning {{
                    border-left-color: #ffc107;
                    background: #fffef8;
                }}
                .source-item.status-failing {{
                    border-left-color: #dc3545;
                    background: #fff8f8;
                }}
                .source-item.status-never_run {{
                    border-left-color: #6c757d;
                    background: #f8f9fa;
                }}
                .source-header {{
                    display: flex;
                    align-items: center;
                    gap: 8px;
                    margin-bottom: 4px;
                }}
                .source-icon {{
                    font-size: 16px;
                }}
                .source-status {{
                    padding: 2px 6px;
                    border-radius: 3px;
                    font-size: 11px;
                    font-weight: 600;
                    text-transform: uppercase;
                    margin-left: auto;
                }}
                .source-status.status-healthy {{
                    background: #28a745;
                    color: white;
                }}
                .source-status.status-warning {{
                    background: #ffc107;
                    color: #000;
                }}
                .source-status.status-failing {{
                    background: #dc3545;
                    color: white;
                }}
                .source-status.status-never_run {{
                    background: #6c757d;
                    color: white;
                }}
                .source-details {{
                    font-size: 12px;
                    color: #666;
                }}
                .status-summary {{
                    display: flex;
                    gap: 20px;
                    margin-bottom: 15px;
                    font-size: 14px;
                }}
                .status-count {{
                    display: flex;
                    align-items: center;
                    gap: 5px;
                }}
                .status-dot {{
                    width: 8px;
                    height: 8px;
                    border-radius: 50%;
                }}
                .status-dot.healthy {{ background: #28a745; }}
                .status-dot.warning {{ background: #ffc107; }}
                .status-dot.failing {{ background: #dc3545; }}
                .status-dot.never_run {{ background: #6c757d; }}
            </style>
            <script>
                let autoRefresh = false;
                let refreshInterval;
                
                function toggleAutoRefresh() {{
                    autoRefresh = !autoRefresh;
                    const btn = document.getElementById('autoRefreshBtn');
                    
                    if (autoRefresh) {{
                        btn.textContent = 'Auto-Refresh: ON';
                        btn.style.background = '#28a745';
                        refreshInterval = setInterval(() => {{
                            location.reload();
                        }}, 30000); // 30 seconds
                    }} else {{
                        btn.textContent = 'Auto-Refresh: OFF';
                        btn.style.background = '#007bff';
                        clearInterval(refreshInterval);
                    }}
                }}
                
                function filterByCategory(cat) {{
                    const url = new URL(window.location);
                    if (cat === 'all') {{
                        url.searchParams.delete('category');
                    }} else {{
                        url.searchParams.set('category', cat);
                    }}
                    window.location = url;
                }}
                
                function changeTimeframe(tf) {{
                    const url = new URL(window.location);
                    url.searchParams.set('timeframe', tf);
                    window.location = url;
                }}
            </script>
        </head>
        <body>
            <button id="autoRefreshBtn" class="auto-refresh" onclick="toggleAutoRefresh()">Auto-Refresh: OFF</button>
            
            <div class="header">
                <h1>🔥 ET Heatmap — Intelligent Trends Dashboard</h1>
                <p><strong>Live Analysis</strong> • Updated: {datetime.now(timezone.utc).strftime('%H:%M UTC')} • {len(items)} entities tracked</p>
                
                <div class="filters">
                    <select onchange="filterByCategory(this.value)">
                        <option value="all">All Categories</option>
                        <option value="entertainment">Entertainment</option>
                        <option value="sports">Sports</option>
                        <option value="politics">Politics</option>
                        <option value="technology">Technology</option>
                    </select>
                    
                    <select onchange="changeTimeframe(this.value)">
                        <option value="24h" {"selected" if timeframe == "24h" else ""}>24 Hours</option>
                        <option value="7d" {"selected" if timeframe == "7d" else ""}>7 Days</option>
                        <option value="30d" {"selected" if timeframe == "30d" else ""}>30 Days</option>
                    </select>
                    
                    <button onclick="location.reload()">🔄 Refresh</button>
                </div>
            </div>
            
            <div class="sources-section">
                <h2>📊 Data Sources Status</h2>
                <div class="status-summary">
                    <div class="status-count">
                        <div class="status-dot healthy"></div>
                        <span>{status_counts['healthy']} Healthy</span>
                    </div>
                    <div class="status-count">
                        <div class="status-dot warning"></div>
                        <span>{status_counts['warning']} Warning</span>
                    </div>
                    <div class="status-count">
                        <div class="status-dot failing"></div>
                        <span>{status_counts['failing']} Failing</span>
                    </div>
                    <div class="status-count">
                        <div class="status-dot never_run"></div>
                        <span>{status_counts['never_run']} Not Started</span>
                    </div>
                </div>
                <div class="sources-grid">
                    {source_status_html}
                </div>
            </div>
            
            <table>
                <thead>
                    <tr>
                        <th>#</th>
                        <th>Entity</th>
                        <th>Heat (Δ%)</th>
                        <th>Velocity</th>
                        <th>Acceleration</th>
                        <th>Platforms</th>
                        <th>Trend (7d)</th>
                        <th>Why It Matters</th>
                        <th>Priority</th>
                    </tr>
                </thead>
                <tbody>
                    {html_items}
                </tbody>
            </table>
            
            <div style="margin-top: 20px; padding: 15px; background: white; border-radius: 8px; font-size: 14px; color: #666;">
                <strong>Legend:</strong> 
                ▲ Rising • ▼ Falling • ─ Stable • 
                <span class="priority-critical">Critical</span>: High heat + high acceleration • 
                <span class="priority-high">High</span>: High heat or high acceleration • 
                <span class="priority-medium">Medium</span>: Moderate activity • 
                <span class="priority-low">Low</span>: Steady/declining
            </div>
        </body>
    </html>
    """
    return HTMLResponse(content=html)


@app.get("/health/extended")
async def health_extended():
        db_ok = True
        db_err: Optional[str] = None
        try:
                async with engine.connect() as conn:
                        await conn.execute(text("SELECT 1"))
        except Exception as e:  # pragma: no cover
                db_ok = False
                db_err = str(e)

        return {
                "status": "ok" if db_ok else "degraded",
                "db": {"ok": db_ok, "error": db_err},
                "cache": _cache.stats(),
                "time": datetime.now(timezone.utc).isoformat(),
        }


@app.get("/trends/sse")
async def trends_sse(category: Optional[str] = None, limit: int = 20, interval: int = 5):
    """Server-Sent Events stream of top trends. No websockets, no extra deps."""

    async def event_stream() -> AsyncGenerator[bytes, None]:
        while True:
            try:
                async with engine.connect() as conn:
                    rows = await _query_latest_scores(conn, category, limit)
                items = [_format_trend_item(i, row) for i, row in enumerate(rows, start=1)]
                payload = {"ts": datetime.now(timezone.utc).isoformat(), "items": items}
                data = f"data: {json.dumps(payload)}\n\n".encode()
                yield data
                # Sleep between updates using asyncio.sleep to avoid blocking
                import asyncio
                await asyncio.sleep(max(1, int(interval)))
            except Exception as e:  # pragma: no cover
                err = {"error": str(e)}
                yield f"data: {json.dumps(err)}\n\n".encode()
                import asyncio
                await asyncio.sleep(2)

    headers = {
        "Cache-Control": "no-cache",
        "Connection": "keep-alive",
    }
    return StreamingResponse(event_stream(), media_type="text/event-stream", headers=headers)


@app.get("/sources/status")
async def get_source_status():
    """Get status of all data sources including last run time and success status."""
    async with engine.connect() as conn:
        # Get source health data
        health_query = text("""
            SELECT 
                source,
                last_ok,
                last_error,
                consecutive_errors,
                circuit_open_until,
                CASE 
                    WHEN last_ok IS NULL AND last_error IS NULL THEN 'never_run'
                    WHEN last_ok > COALESCE(last_error, '1900-01-01') THEN 'healthy'
                    WHEN consecutive_errors >= 3 THEN 'failing'
                    ELSE 'warning'
                END as status,
                CASE 
                    WHEN last_ok > COALESCE(last_error, '1900-01-01') THEN last_ok
                    ELSE last_error
                END as last_run
            FROM source_health 
            ORDER BY last_run DESC NULLS LAST, source
        """)
        health_rows = (await conn.execute(health_query)).fetchall()

    # Define known sources with their descriptions
    source_definitions = {
        'scrape_news': {
            'name': 'News Scraper',
            'description': 'Entertainment news from major publications',
            'type': 'core',
            'expected_interval': '15 minutes'
        },
        'trade_rss': {
            'name': 'Trade RSS',
            'description': 'Entertainment trade publications (Variety, Deadline, etc.)',
            'type': 'rss',
            'expected_interval': '30 minutes'
        },
        'reddit': {
            'name': 'Reddit Monitor',
            'description': '13 entertainment subreddits',
            'type': 'api',
            'expected_interval': '30 minutes'
        },
        'youtube': {
            'name': 'YouTube Trends',
            'description': 'YouTube trending and entity searches',
            'type': 'api',
            'expected_interval': '60 minutes'
        },
        'imdb_box_office': {
            'name': 'IMDb Box Office',
            'description': 'Box office data and movie releases',
            'type': 'scraping',
            'expected_interval': '2 hours'
        },
        'wiki': {
            'name': 'Wikipedia Trends',
            'description': 'Wikipedia search trends (Google Trends)',
            'type': 'rate_limited',
            'expected_interval': '4 hours'
        },
        'mvp_scoring': {
            'name': 'Heat Scoring',
            'description': 'Calculate entity heat scores',
            'type': 'processing',
            'expected_interval': '30 minutes'
        }
    }

    # Build status response
    sources = []
    health_dict = {row[0]: row for row in health_rows}
    
    # Add sources from health table
    for source, data in health_dict.items():
        source_name, last_ok, last_error, consecutive_errors, circuit_open, status, last_run = data
        
        definition = source_definitions.get(source, {
            'name': source.title(),
            'description': 'Unknown source',
            'type': 'unknown',
            'expected_interval': 'unknown'
        })
        
        sources.append({
            'source': source,
            'name': definition['name'],
            'description': definition['description'],
            'type': definition['type'],
            'status': status,
            'last_ok': last_ok.isoformat() if last_ok else None,
            'last_error': last_error.isoformat() if last_error else None,
            'last_run': last_run.isoformat() if last_run else None,
            'consecutive_errors': consecutive_errors or 0,
            'circuit_open': circuit_open.isoformat() if circuit_open else None,
            'expected_interval': definition['expected_interval']
        })
    
    # Add sources that haven't run yet
    for source, definition in source_definitions.items():
        if source not in health_dict:
            sources.append({
                'source': source,
                'name': definition['name'],
                'description': definition['description'],
                'type': definition['type'],
                'status': 'never_run',
                'last_ok': None,
                'last_error': None,
                'last_run': None,
                'consecutive_errors': 0,
                'circuit_open': None,
                'expected_interval': definition['expected_interval']
            })
    
    # Sort by status priority and last run
    status_priority = {'failing': 0, 'warning': 1, 'never_run': 2, 'healthy': 3}
    sources.sort(key=lambda x: (status_priority.get(x['status'], 4), x['last_run'] or ''))
    
    return {
        'total_sources': len(sources),
        'healthy': len([s for s in sources if s['status'] == 'healthy']),
        'warning': len([s for s in sources if s['status'] == 'warning']),
        'failing': len([s for s in sources if s['status'] == 'failing']),
        'never_run': len([s for s in sources if s['status'] == 'never_run']),
        'sources': sources,
        'last_updated': datetime.now(timezone.utc).isoformat()
    }


@app.get("/recommendations")
async def get_recommendations(based_on: Optional[str] = None, limit: int = 10):
    """Basic recommendations:
    - If based_on is provided, find that entity's category and suggest other top entities in same category.
    - Otherwise, return top entities overall.
    """
    async with engine.connect() as conn:
        category = await _get_entity_category_by_name(conn, based_on) if based_on else None
        rows = await _query_latest_scores(conn, category, limit + 1 if based_on else limit)

    recs = _build_recommendations(rows, based_on, category, limit)
    return {"count": len(recs), "items": recs}


async def _get_entity_category_by_name(conn, name: str) -> Optional[str]:
    q = text(
        """
        SELECT COALESCE(category, 'unknown')
        FROM entities
        WHERE LOWER(name) = LOWER(:name)
        LIMIT 1
        """
    )
    res = await conn.execute(q, {"name": name})
    row = res.first()
    return row[0] if row else None


def _build_recommendations(rows, based_on: Optional[str], category: Optional[str], limit: int):
    recs: List[Dict[str, Any]] = []
    base_name = (based_on or "").lower()
    for i, row in enumerate(rows, start=1):
        item = _format_trend_item(i, row)
        if _is_same_entity(item, base_name):
            continue
        item["explanation"] = _recommendation_reason(item, base_name, category)
        recs.append(item)
        if len(recs) >= limit:
            break
    return recs


def _is_same_entity(item: Dict[str, Any], base_name: str) -> bool:
    return bool(base_name) and item.get("entity", "").lower() == base_name


def _recommendation_reason(item: Dict[str, Any], base_name: str, category: Optional[str]) -> str:
    reasons: List[str] = []
    if base_name and category:
        reasons.append(f"Similar category: {category}")
    cross = item.get("components", {}).get("cross_platform")
    if isinstance(cross, (int, float)) and cross >= 1.0:
        reasons.append("Cross-platform traction")
    heat = item.get("heat")
    if isinstance(heat, (int, float)) and heat >= 0.8:
        reasons.append("Currently hot")
    return " | ".join(reasons) if reasons else "Trending pick"


# ---- Slack interactions (optional) ----
SLACK_SIGNING_SECRET = os.getenv("SLACK_SIGNING_SECRET", "").strip()
SLACK_SIG_TOLERANCE_SECONDS = int(os.getenv("SLACK_SIG_TOLERANCE_SECONDS", "300"))  # 5 minutes default

def _verify_slack_signature(secret: str, ts: str, sig: str, body: bytes) -> bool:
    try:
        # Basic timestamp replay protection
        ts_i = int(ts)
        now = int(datetime.now(timezone.utc).timestamp())
        if abs(now - ts_i) > max(0, SLACK_SIG_TOLERANCE_SECONDS):
            return False
        base = f"v0:{ts}:{body.decode()}".encode()
        mac = hmac.new(secret.encode(), base, hashlib.sha256).hexdigest()
        expected = f"v0={mac}"
        # Constant time compare
        return hmac.compare_digest(expected, sig)
    except Exception:
        return False


from libs.rate import TokenBucket  # rate limiting
_TB_REDIS_URL = os.getenv("REDIS_URL", "redis://redis:6379/0")

@app.post("/slack/interact")
@app.post("/slack/interactions")
async def slack_interact(request: Request):
    # Redis-backed rate limit with in-memory fallback
    ident = _client_key(request)
    tb = TokenBucket(key=f"slack:interact:{ident}", rate=10, interval=60, burst=20, redis_url=_TB_REDIS_URL)
    if not await tb.acquire():
        raise HTTPException(status_code=429, detail="rate limited")
    # Optional signature verification
    if SLACK_SIGNING_SECRET:
        ts = request.headers.get("X-Slack-Request-Timestamp", "")
        sig = request.headers.get("X-Slack-Signature", "")
        raw = await request.body()
        if not _verify_slack_signature(SLACK_SIGNING_SECRET, ts, sig, raw):
            raise HTTPException(status_code=401, detail="invalid slack signature")
        # Parse body form
        data = parse_qs(raw.decode())
    else:
        # Fallback parse without signature
        raw = await request.body()
        data = parse_qs(raw.decode())

    payload_str = (data.get("payload", ["{}"])[0])
    try:
        payload = json.loads(payload_str)
    except Exception:
        payload = {}

    # Extract action
    actions = payload.get("actions") or []
    action_id = actions[0].get("action_id") if actions else None
    value = actions[0].get("value") if actions else ""
    name = ""
    eid = None
    # value format: eid:123|name:Some Name
    try:
        parts = {k: v for k, v in (x.split(":", 1) for x in (value or "").split("|") if ":" in x)}
        eid = int(str(parts.get("eid"))) if parts.get("eid") is not None else None
        name = parts.get("name") or ""
    except Exception:
        pass

    if action_id == "open_pack" and eid:
        # Generate a lightweight package and return a message with a link
        try:
            from libs.packages import generate_package
            pack = generate_package(name or "", receipts=[])
            text = f"Research Pack for {pack.entity}\n- Booking: {pack.booking_brief[:140]}…\n- Promo: {', '.join(pack.promo_lines[:2])}"
            link = f"/packages/{eid}"
            return {"response_type": "in_channel", "replace_original": False, "text": text, "attachments": [{"text": f"Open full pack: {link}"}]}
        except Exception as e:
            return {"response_type": "ephemeral", "text": f"Could not generate pack: {e}"}

    # Default noop response to acknowledge
    return {"response_type": "ephemeral", "text": "Action received."}


@app.post("/slack/open_pack")
async def slack_open_pack(request: Request):
    # Redis-backed rate limit with in-memory fallback
    ident = _client_key(request)
    tb = TokenBucket(key=f"slack:open_pack:{ident}", rate=10, interval=60, burst=20, redis_url=_TB_REDIS_URL)
    if not await tb.acquire():
        raise HTTPException(status_code=429, detail="rate limited")
    raw = await request.body()
    if SLACK_SIGNING_SECRET:
        ts = request.headers.get("X-Slack-Request-Timestamp", "")
        sig = request.headers.get("X-Slack-Signature", "")
        if not _verify_slack_signature(SLACK_SIGNING_SECRET, ts, sig, raw):
            raise HTTPException(status_code=401, detail="invalid slack signature")
    # Try JSON first, else form-encoded
    payload = None
    try:
        payload = json.loads(raw.decode() or "{}")
    except Exception:
        try:
            data = parse_qs(raw.decode())
            payload_str = (data.get("payload", ["{}"])[0])
            payload = json.loads(payload_str)
        except Exception:
            payload = {}
    name = payload.get("entity") or payload.get("name") or ""
    eid = payload.get("eid") or payload.get("id")
    try:
        from libs.packages import generate_package
        pack = generate_package(str(name), receipts=[])
        text = f"Research Pack for {pack.entity}\n- Booking: {pack.booking_brief[:140]}…\n- Promo: {', '.join(pack.promo_lines[:2])}"
        link = f"/packages/{eid or 0}"
        return {"response_type": "in_channel", "replace_original": False, "text": text, "attachments": [{"text": f"Open full pack: {link}"}]}
    except Exception as e:
        return {"response_type": "ephemeral", "text": f"Could not generate pack: {e}"}
