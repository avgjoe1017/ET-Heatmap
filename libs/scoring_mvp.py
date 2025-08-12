from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from typing import Dict, Optional

import math
import numpy as np


@dataclass
class MVPComponents:
    velocity_z: float
    spread: float
    affect: float
    freshness_decay: float
    heat: float


def cap(x: float, lo: float, hi: float) -> float:
    return float(max(lo, min(hi, x)))


def compute_heat(velocity_z: float, spread: float, affect: float, hours_since_peak: float) -> MVPComponents:
    # Cap velocity_z at 4.0 per spec
    v = cap(velocity_z, -4.0, 4.0)
    s = cap(spread, 0.0, 1.0)
    a = cap(abs(affect), 0.0, 1.0)
    raw = 0.5 * v + 0.3 * s + 0.2 * a
    decay = math.exp(-(max(0.0, hours_since_peak)) / 24.0)
    heat = raw * decay
    return MVPComponents(velocity_z=v, spread=s, affect=a, freshness_decay=decay, heat=heat)


def platform_spread(active: Dict[str, bool]) -> float:
    # active keys: reddit | trends | tiktok
    types = ["reddit", "trends", "tiktok"]
    c = sum(1 for k in types if active.get(k))
    return float(c) / 3.0


def map_tone_to_affect(avg_tone: Optional[float], volume: float, volume_floor: float = 3.0) -> float:
    """Map GDELT tone in [-inf, +inf] to [-1, 1] -> [0,1] by |tone|, with controversy bonus only if volume > floor."""
    if avg_tone is None:
        return 0.0
    x = float(avg_tone)
    a = min(1.0, abs(x) / 5.0)  # normalize |tone| to ~[0,1]
    if volume >= volume_floor:
        return a
    # below floor, suppress affect
    return 0.0


def hours_since(ts: Optional[datetime]) -> float:
    if not ts:
        return 999.0
    return (datetime.now(timezone.utc) - ts).total_seconds() / 3600.0
