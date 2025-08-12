from __future__ import annotations
from typing import Dict, Any

class PlatformSpecificScoring:
    """Minimal platform-specific scorers using existing signal metrics."""

    def score_tiktok(self, signals: Dict[str, Any]) -> Dict[str, Any]:
        tt = signals.get("tiktok_data", {})
        hits = float(tt.get("hits_24h", 0) or 0)
        view_vel = float(tt.get("view_vel_median", 0) or 0)
        eng = float(tt.get("eng_ratio_median", 0) or 0)
        score = 0.0
        if hits > 0:
            score += 0.2
        score += min(0.4, view_vel / 1e6)
        score += min(0.4, eng)
        return {"score": min(1.0, score), "breakdown": {"hits": hits, "view_vel": view_vel, "eng_ratio": eng}}

    def score_reddit(self, signals: Dict[str, Any]) -> Dict[str, Any]:
        rd = signals.get("reddit_data", {})
        mentions = float(rd.get("mentions_24h", 0) or 0)
        score = min(1.0, mentions / 100.0)
        return {"score": score, "breakdown": {"mentions_24h": mentions}}

    def score_news(self, signals: Dict[str, Any]) -> Dict[str, Any]:
        news = int(signals.get("news_mentions", 0) or 0)
        gkg = int(signals.get("gkg_mentions", 0) or 0)
        tone = float(signals.get("gkg_tone_avg", 0.0) or 0.0)
        score = min(0.6, news / 100.0) + min(0.4, gkg / 200.0) + max(0.0, tone / 20.0)
        return {"score": min(1.0, score), "breakdown": {"news_mentions": news, "gkg_mentions": gkg, "gkg_tone_avg": tone}}
