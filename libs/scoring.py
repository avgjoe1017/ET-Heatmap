from __future__ import annotations
import numpy as np
import pandas as pd
from datetime import datetime, timezone

def zscore(series: pd.Series) -> float:
    s = series.dropna().astype(float)
    if len(s) < 5:
        return 0.0
    return float((s.iloc[-1] - s.mean()) / (s.std(ddof=0) + 1e-9))

def acceleration(series: pd.Series) -> float:
    s = series.dropna().astype(float)
    if len(s) < 3:
        return 0.0
    return float((s.iloc[-1] - s.iloc[-2]) - (s.iloc[-2] - s.iloc[-3]))

def novelty(series: pd.Series) -> float:
    s = series.dropna().astype(float)
    if len(s) < 8:
        return 0.0
    med = s.rolling(7, min_periods=3).median()
    return float((s.iloc[-1] - med.iloc[-1]) / (np.abs(med.iloc[-1]) + 1e-9))

def cross_platform_confirm(z_trends: float, z_wiki: float, thresh: float = 0.8) -> float:
    hits = int(z_trends >= thresh) + int(z_wiki >= thresh)
    return 1.0 if hits >= 2 else 0.0

def tentpole_boost(today: datetime, tentpoles_df: pd.DataFrame, entity_name: str) -> float:
    if tentpoles_df is None or tentpoles_df.empty:
        return 0.0
    mask = (tentpoles_df["start_date"] <= today.date()) & (today.date() <= tentpoles_df["end_date"])
    active = tentpoles_df.loc[mask]
    if active.empty:
        return 0.0
    for _, row in active.iterrows():
        if row["title"].lower() in entity_name.lower():
            return float(row["boost"])
    return float(active["boost"].max())

def heat_lite(z_trends, z_wiki, accel_avg, nov, tentpole, et_fit=0.6, decay=0.0, risk=0.0) -> tuple[float, dict]:
    w1, w2, w3, w4, w5, w6, w7, w8 = 0.35, 0.20, 0.20, 0.10, 0.10, 0.10, 0.10, 0.10
    xplat = cross_platform_confirm(z_trends, z_wiki)
    heat = (
        w1 * (0.5*z_trends + 0.5*z_wiki) +
        w2 * accel_avg +
        w3 * xplat +
        w4 * nov +
        w5 * et_fit +
        w6 * tentpole -
        w7 * decay -
        w8 * risk
    )
    comps = {
        "velocity_z": float(0.5*z_trends + 0.5*z_wiki),
        "accel": float(accel_avg),
        "xplat": float(xplat),
        "novelty": float(nov),
        "et_fit": float(et_fit),
        "tentpole": float(tentpole),
        "decay": float(decay),
        "risk": float(risk),
        "heat": float(heat),
    }
    return float(heat), comps
