from __future__ import annotations
from typing import Dict, Tuple, Optional
import numpy as np
import pandas as pd


def _z(series) -> float:
    s = pd.Series(series).dropna()
    if len(s) < 3:
        return 0.0
    return float((s.iloc[-1] - s.mean()) / (s.std(ddof=0) + 1e-9))


def tiktok_component(
    df_tt_cc: Optional[dict],
    df_tt_search: Optional[dict],
) -> Tuple[float, Dict[str, float]]:
    """Blend Creative Center and TikTok search metrics into a single z-like component.

    df_tt_cc: mapping metric->list[float] for metrics like 'hashtag_score', 'momentum'
    df_tt_search: mapping metric->list[float] for metrics like 'hits_24h', 'unique_authors_24h', 'view_vel_median', 'eng_ratio_median'
    """
    z_cc_tag = _z((df_tt_cc or {}).get("hashtag_score", []))
    z_cc_mom = _z((df_tt_cc or {}).get("momentum", []))
    z_hits = _z((df_tt_search or {}).get("hits_24h", []))
    z_auth = _z((df_tt_search or {}).get("unique_authors_24h", []))
    z_vvel = _z((df_tt_search or {}).get("view_vel_median", []))
    z_eng = _z((df_tt_search or {}).get("eng_ratio_median", []))

    latest_hits = float(((df_tt_search or {}).get("hits_24h", [0]) or [0])[-1]) if (df_tt_search or {}).get("hits_24h") else 0.0
    latest_auth = float(((df_tt_search or {}).get("unique_authors_24h", [0]) or [0])[-1]) if (df_tt_search or {}).get("unique_authors_24h") else 0.0
    spam_pen = max(0.0, latest_hits - 2.0 * latest_auth)
    spam_pen = min(spam_pen, 5.0)

    w = {"cc_tag": 0.30, "cc_mom": 0.15, "hits": 0.15, "auth": 0.20, "vvel": 0.15, "eng": 0.10, "spam": 0.25}
    raw = (
        w["cc_tag"] * z_cc_tag
        + w["cc_mom"] * z_cc_mom
        + w["hits"] * z_hits
        + w["auth"] * z_auth
        + w["vvel"] * z_vvel
        + w["eng"] * z_eng
        - w["spam"] * (spam_pen / 5.0)
    )
    tiktok_z = float(np.clip(raw, -3, 3))
    comps = {
        "tt_cc_tag_z": float(z_cc_tag),
        "tt_cc_mom_z": float(z_cc_mom),
        "tt_hits_z": float(z_hits),
        "tt_auth_z": float(z_auth),
        "tt_vvel_z": float(z_vvel),
        "tt_eng_z": float(z_eng),
        "tt_spam_pen": float(spam_pen),
        "tiktok_raw": float(raw),
        "tiktok_z": float(tiktok_z),
    }
    return tiktok_z, comps
