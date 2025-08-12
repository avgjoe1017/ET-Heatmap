from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime
from typing import List

@dataclass
class ActionPackage:
    entity: str
    generated_ts: datetime
    booking_brief: List[str]
    promo_lines: List[str]
    graphics: List[str]
    receipts: List[str]


def generate_package(entity: str, receipts: List[str] | None = None) -> ActionPackage:
    from datetime import datetime, timezone
    receipts = receipts or []
    # Simple templates; can be plugged into LLM later if desired
    booking = [
        f"Angle 1: Why {entity} is breaking now",
        "Angle 2: What we can add beyond the trades",
        "Angle 3: The audience hook (why our viewers care)",
    ]
    promo = [
        f"On-air: {entity} heats up — what it means tonight",
        f"On-air: Inside {entity}'s surge and what's next",
        f"On-air: {entity} set to dominate — our take",
        f"Push: {entity} trending — details inside",
        f"Social: {entity} is blowing up; our breakdown",
    ]
    gfx = [
        "Lower-third: Trending Now",
        "OTS: Trend Heatmap",
        "B-roll: Social clips and press images",
    ]
    return ActionPackage(
        entity=entity,
        generated_ts=datetime.now(timezone.utc),
        booking_brief=booking,
        promo_lines=promo,
        graphics=gfx,
        receipts=receipts[:3],
    )
