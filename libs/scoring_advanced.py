"""
Lightweight AdvancedScoringEngine scaffold.
Implements a safe calculate_multidimensional_heat_score using only numpy/pandas
to avoid heavy dependency changes. Can be extended incrementally.
"""
from __future__ import annotations

from typing import Dict, List, Any
import numpy as np


class AdvancedScoringEngine:
    """Compute a composite heat score from multi-source signals.

    Contract:
    - input: entity (str), signals (dict)
      signals may include keys: wiki_pageviews (list[float]), trends_interest (list[float]),
      sentiment_scores (list[float]), related_entities (list[str]), influencer_mentions (int),
      influencer_total_reach (int), news_mentions (int).
    - output: dict with heat_score [0..1], components dict, trajectory stub, confidence [0..1], reasons list.
    """

    def __init__(self) -> None:
        # Placeholder: keep constructor minimal to avoid adding runtime dependencies.
        # Extend with historical pattern loaders or dynamic weights when ready.
        pass

    def calculate_multidimensional_heat_score(self, entity: str, signals: Dict[str, Any]) -> Dict[str, Any]:
        velocity = self._velocity(signals)
        acceleration = self._acceleration(signals)
        virality = self._virality(signals)
        sentiment = self._sentiment(signals)
        network = self._network(signals)
        novelty = self._novelty(entity, signals)
        quality = self._quality(signals)

        components = {
            "velocity": velocity,
            "acceleration": acceleration,
            "virality": virality,
            "sentiment": sentiment,
            "network": network,
            "novelty": novelty,
            "quality": quality,
        }

        weights = {
            "velocity": 0.25,
            "acceleration": 0.15,
            "virality": 0.20,
            "sentiment": 0.10,
            "network": 0.15,
            "novelty": 0.10,
            "quality": 0.05,
        }

        total = float(sum(components[k] * weights[k] for k in weights))
        # Non-linear spread
        heat = min(1.0, total ** 1.25)

        return {
            "heat_score": heat,
            "components": components,
            "trajectory": {"trend": "stable", "confidence": 0.3},
            "confidence": self._confidence(signals),
            "reasons": self._reasons(components, signals),
            "peak_probability": min(1.0, (velocity + virality) / 2.0),
        }

    # ---- component calculators (minimal) ----
    def _series(self, signals: Dict[str, Any]) -> np.ndarray:
        s1 = signals.get("wiki_pageviews") or []
        s2 = signals.get("trends_interest") or []
        # unify length
        if not s1 and not s2:
            return np.array([], dtype=float)
        # pad shorter
        max_len = max(len(s1), len(s2))
        a = np.array((s1 + [0] * (max_len - len(s1))) if s1 else [0] * max_len, dtype=float)
        b = np.array((s2 + [0] * (max_len - len(s2))) if s2 else [0] * max_len, dtype=float)
        denom = max(1.0, float(np.nanmax([np.max(a) if a.size else 0, np.max(b) if b.size else 0])))
        return (a + b) / denom

    def _velocity(self, signals: Dict[str, Any]) -> float:
        series = self._series(signals)
        if series.size < 3:
            return 0.0
        grad = np.gradient(series)
        recent = grad[-min(7, grad.size):]
        val = float(np.mean(recent))
        # squash to 0..1
        return float(1.0 / (1.0 + np.exp(-5 * val)))

    def _acceleration(self, signals: Dict[str, Any]) -> float:
        series = self._series(signals)
        if series.size < 5:
            return 0.0
        acc = np.gradient(np.gradient(series))
        recent = float(np.mean(acc[-3:]))
        return float(np.clip(0.5 + recent, 0.0, 1.0))

    def _virality(self, signals: Dict[str, Any]) -> float:
        platforms = [
            "tiktok_data",
            "twitter_data",
            "reddit_data",
            "youtube_data",
            "news_data",
        ]
        active = sum(1 for p in platforms if signals.get(p))
        diversity = active / max(1, len(platforms))
        return float(np.clip(0.3 * diversity + 0.2 * self._velocity(signals), 0.0, 1.0))

    def _sentiment(self, signals: Dict[str, Any]) -> float:
        s = signals.get("sentiment_scores") or []
        if not s:
            return 0.5
        avg = float(np.mean(s[-min(7, len(s)):]))
        return float(np.clip((avg + 1.0) / 2.0, 0.0, 1.0))

    def _network(self, signals: Dict[str, Any]) -> float:
        rel = signals.get("related_entities") or []
        infl_mentions = signals.get("influencer_mentions", 0) or 0
        infl_reach = signals.get("influencer_total_reach", 0) or 0
        base = min(0.25, 0.05 * len(rel))
        amp = min(0.25, np.log10(max(1, infl_reach)) / 8.0) if infl_mentions else 0.0
        return float(np.clip(base + amp, 0.0, 1.0))

    def _novelty(self, _entity: str, _signals: Dict[str, Any]) -> float:
        # Without history, assume moderately novel
        return 0.6

    def _quality(self, signals: Dict[str, Any]) -> float:
        news = signals.get("news_mentions", 0) or 0
        return float(np.clip(news / 100.0, 0.0, 1.0))

    def _confidence(self, signals: Dict[str, Any]) -> float:
        # crude proxy: number of active sources
        keys = [k for k, v in signals.items() if v]
        return float(np.clip(len(keys) / 12.0, 0.2, 0.95))

    def _reasons(self, components: Dict[str, float], _signals: Dict[str, Any]) -> List[str]:
        reasons: List[str] = []
        if components["velocity"] > 0.7:
            reasons.append("Strong recent velocity")
        if components["virality"] > 0.5:
            reasons.append("Cross-platform traction")
        if components["network"] > 0.4:
            reasons.append("Network amplification present")
        if components["sentiment"] > 0.65:
            reasons.append("Positive public sentiment")
        if components["novelty"] > 0.7:
            reasons.append("Unprecedented spike vs baseline")
        return reasons
