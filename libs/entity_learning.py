from __future__ import annotations
from dataclasses import dataclass
from typing import List, Dict
from datetime import datetime, timezone

from libs.db import conn_ctx
from sqlalchemy import text

@dataclass
class DiscoveryOutcome:
    entity: str
    outcome: str  # 'trending' | 'failed' | 'steady'
    timestamp: datetime
    peak_score: float
    duration_days: float


class EntityLearningSystem:
    """Minimal learning tracker for discovery outcomes.
    Stores discovery records to a simple table (if exists) or keeps in-memory fallback.
    """

    def __init__(self):
        self._memory: List[DiscoveryOutcome] = []

    async def record_discovery_outcome(self, entity: str, outcome: str, confidence: float | None = None, velocity: float | None = None):
        peak, duration = await self._fetch_peak_and_duration(entity)
        rec = DiscoveryOutcome(
            entity=entity,
            outcome=outcome,
            timestamp=datetime.now(timezone.utc),
            peak_score=peak,
            duration_days=duration,
        )
        # Try persist, else keep in memory
        try:
            await self._persist(rec, confidence=confidence, velocity=velocity)
        except Exception:
            self._memory.append(rec)

    async def _fetch_peak_and_duration(self, entity: str) -> tuple[float, float]:
        peak = 0.0
        duration = 0.0
        async with conn_ctx() as conn:
            q = text(
                """
                SELECT MAX(s.heat) AS peak,
                       COALESCE(
                         EXTRACT(DAY FROM (MAX(s.ts) - MIN(s.ts))), 0
                       ) AS duration
                FROM scores s
                JOIN entities e ON e.id=s.entity_id
                WHERE e.name=:name AND s.ts >= NOW() - INTERVAL '60 days'
                """
            )
            row = (await conn.execute(q, {"name": entity})).first()
            if row is not None:
                peak = float(row[0] or 0.0)
                duration = float(row[1] or 0.0)
        return peak, duration

    async def _persist(self, rec: DiscoveryOutcome, confidence: float | None, velocity: float | None):
        async with conn_ctx() as conn:
            # Optional table; create if missing would require DDL permissions; we avoid that.
            # Insert only if table exists.
            exists = await conn.execute(text("""
                SELECT to_regclass('public.discovery_outcomes') IS NOT NULL
            """))
            ok = bool(exists.scalar())
            if not ok:
                # Skip DB persist if table not present
                return
            await conn.execute(
                text(
                    """
                    INSERT INTO discovery_outcomes (entity, outcome, ts, peak_score, duration_days, confidence, velocity)
                    VALUES (:entity, :outcome, :ts, :peak, :dur, :conf, :vel)
                    """
                ),
                {
                    "entity": rec.entity,
                    "outcome": rec.outcome,
                    "ts": rec.timestamp,
                    "peak": rec.peak_score,
                    "dur": rec.duration_days,
                    "conf": float(confidence) if confidence is not None else None,
                    "vel": float(velocity) if velocity is not None else None,
                },
            )

    async def mark_trending_if_threshold(self, entity: str, threshold: float = 0.6, window_hours: int = 72) -> bool:
        """If entity's heat crosses threshold in the recent window, record a 'trending' outcome."""
        async with conn_ctx() as conn:
            row = (await conn.execute(
                text(
                    """
                    SELECT MAX(s.heat) FROM scores s
                    JOIN entities e ON e.id=s.entity_id
                    WHERE e.name=:name AND s.ts >= NOW() - make_interval(hours => :hrs)
                    """
                ),
                {"name": entity, "hrs": window_hours},
            )).first()
        max_heat = float(row[0] or 0.0) if row else 0.0
        if max_heat >= threshold:
            await self.record_discovery_outcome(entity, "trending")
            return True
        return False
