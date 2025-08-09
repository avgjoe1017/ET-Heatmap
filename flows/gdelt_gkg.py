from __future__ import annotations
import io
import os
import unicodedata
from datetime import datetime, timedelta, timezone
from typing import Dict, List, Tuple, Optional
import zipfile
import httpx
import pandas as pd
from prefect import flow, task, get_run_logger
from sqlalchemy import text
from rapidfuzz import process, fuzz

from libs.db import conn_ctx, insert_signal
from libs.config import is_enabled


GDELT_BASE = "http://data.gdeltproject.org/gdeltv2"


def _round_down_15(dt: datetime) -> datetime:
    m = (dt.minute // 15) * 15
    return dt.replace(minute=m, second=0, microsecond=0)


def _gdelt_url_for(ts: datetime) -> str:
    # GKG file name format: YYYYMMDDHHMMSS.gkg.csv.zip
    stamp = ts.strftime("%Y%m%d%H%M%S")
    return f"{GDELT_BASE}/{stamp}.gkg.csv.zip"


def _split_semi_list(val: str) -> List[str]:
    if not isinstance(val, str) or not val:
        return []
    out: List[str] = []
    for part in val.split(";"):
        part = part.strip()
        if not part:
            continue
        # Items may be like "Name,1"; take the name before first comma
        name = part.split(",")[0].strip()
        if name:
            out.append(name)
    return out

def _to_ascii(s: str) -> str:
    return unicodedata.normalize("NFKD", s).encode("ascii", "ignore").decode("ascii")


def _swap_comma_name(s: str) -> str:
    # Convert "Last, First" -> "First Last" when applicable
    if "," in s:
        parts = [p.strip() for p in s.split(",")]
        if len(parts) == 2 and parts[0] and parts[1]:
            return f"{parts[1]} {parts[0]}"
    return s


def _norm(s: str) -> str:
    s = str(s)
    s = _to_ascii(s)
    s = s.replace("_", " ")
    s = _swap_comma_name(s)
    s = " ".join(s.strip().lower().split())
    return s


def _detect_columns(df: pd.DataFrame) -> Tuple[List[int], Optional[int]]:
    """Heuristically detect name list columns and the tone column. Returns (name_cols, tone_col)."""
    n = min(len(df), 200)
    sample = df.iloc[:n]
    name_cols: List[int] = []
    tone_col: Optional[int] = None
    for i in range(df.shape[1]):
        col = sample.iloc[:, i].astype(str)
        # Name list columns tend to have semicolons and decent length
        try:
            semi_ratio = float((col.str.contains(";", regex=False)).mean())
        except Exception:
            semi_ratio = 0.0
        if semi_ratio > 0.1 and col.str.len().mean() > 5:
            name_cols.append(i)
        if tone_col is None:
            # Tone column begins like: "-1.23,0.5,0.7,..."
            try:
                tone_ratio = float(col.str.match(r"^\s*-?\d+(?:\.\d+)?\s*,").mean())
            except Exception:
                tone_ratio = 0.0
            if tone_ratio > 0.3:
                tone_col = i
    # Fallback to common indices if heuristics failed
    if not name_cols:
        for idx in (23, 24):
            if df.shape[1] > idx:
                name_cols.append(idx)
    if tone_col is None and df.shape[1] > 34:
        tone_col = 34
    name_cols = sorted(set(name_cols))[:4]
    return name_cols, tone_col


async def _download_url(url: str, client: httpx.AsyncClient) -> bytes:
    r = await client.get(url, timeout=60)
    if r.status_code != 200:
        raise RuntimeError(f"HTTP {r.status_code}")
    return r.content


@task
async def ingest_gkg_last_hours(hours: int = 12, step_minutes: int = 15) -> int:
    """Download recent GDELT GKG files and derive signals per known entity."""
    if not is_enabled("gdelt_gkg"):
        return 0
    logger = get_run_logger()
    now = datetime.now(timezone.utc)

    alias_to_eid = await _load_entities()
    urls = _generate_urls(now, hours, step_minutes)
    counts, tone_sum, fetched = await _process_urls(urls, alias_to_eid)

    if not counts:
        logger.info(f"gdelt_gkg: no matches in recent window (files={fetched})")
        return 0

    inserted = await _insert_signals(counts, tone_sum, now)
    logger.info(f"gdelt_gkg: inserted signals for {inserted} entities from {fetched} files")
    return inserted


async def _load_entities() -> Dict[str, int]:
    """Load entities and build a robust alias map with common variants."""
    async with conn_ctx() as conn:
        rows = (await conn.execute(text("SELECT id, name, aliases FROM entities"))).mappings().all()
    alias_to_eid: Dict[str, int] = {}
    for r in rows:
        eid = int(r["id"])
        name = r["name"]
        variants = set()
        base = _norm(name)
        variants.add(base)
        variants.add(base.replace(" ", "_"))
        variants.add(_norm(_swap_comma_name(name)))
        for a in r.get("aliases") or []:
            na = _norm(a)
            variants.add(na)
            variants.add(na.replace(" ", "_"))
            variants.add(_norm(_swap_comma_name(a)))
        for v in variants:
            if v:
                alias_to_eid[v] = eid
    return alias_to_eid


def _generate_urls(now: datetime, hours: int, step_minutes: int) -> List[str]:
    """Generate GDELT URLs for the given time range."""
    start = now - timedelta(hours=hours)
    cur = _round_down_15(start)
    end = _round_down_15(now)
    urls = []
    while cur <= end:
        urls.append(_gdelt_url_for(cur))
        cur += timedelta(minutes=step_minutes)
    return urls


async def _process_urls(urls: List[str], alias_to_eid: Dict[str, int]) -> tuple[Dict[int, int], Dict[int, float], int]:
    """Process GDELT URLs and extract counts and tone sums."""
    counts: Dict[int, int] = {}
    tone_sum: Dict[int, float] = {}
    fetched = 0

    async with httpx.AsyncClient(headers={"User-Agent": "ET-Heatmap/1.0"}) as client:
        for url in urls:
            try:
                blob = await _download_url(url, client)
                fetched += 1
                _process_blob(blob, alias_to_eid, counts, tone_sum)
            except Exception:
                continue  # missing interval files are common
    return counts, tone_sum, fetched


def _process_blob(blob: bytes, alias_to_eid: Dict[str, int], counts: Dict[int, int], tone_sum: Dict[int, float]):
    """Process a single GDELT blob."""
    try:
        with zipfile.ZipFile(io.BytesIO(blob)) as zf:
            names = zf.namelist()
            if not names:
                return
            with zf.open(names[0]) as f:
                df = pd.read_csv(f, sep='\t', header=None, dtype=str, quotechar='"', na_filter=False)
                _process_dataframe(df, alias_to_eid, counts, tone_sum)
    except Exception:
        pass


def _process_dataframe(df: pd.DataFrame, alias_to_eid: Dict[str, int], counts: Dict[int, int], tone_sum: Dict[int, float]):
    """Process a GDELT dataframe using dynamic column detection and fuzzy matching fallback."""
    name_cols, tone_col = _detect_columns(df)
    if not name_cols or tone_col is None:
        return

    alias_keys = list(alias_to_eid.keys())
    tone_series = df.iloc[:, tone_col]

    for row_idx in range(len(df)):
        # Collect names from all detected name list columns
        row_names: List[str] = []
        for c in name_cols:
            try:
                row_names.extend(_split_semi_list(df.iat[row_idx, c]))
            except Exception:
                continue
        raw_names = set(row_names)
        if not raw_names:
            continue
        norms = {_norm(n) for n in raw_names if n}
        # Exact hits first
        hit_eids = {eid for n in norms if (eid := alias_to_eid.get(n)) is not None}
        # Fuzzy fallback
        if not hit_eids:
            for n in norms:
                if not n or len(n) < 4:
                    continue
                match = process.extractOne(n, alias_keys, scorer=fuzz.token_set_ratio)
                if match and match[2] >= 90:
                    eid = alias_to_eid.get(match[0])
                    if eid is not None:
                        hit_eids.add(eid)
        if not hit_eids:
            continue

        tval = tone_series.iat[row_idx]
        tone = _parse_tone(tval)
        for eid in hit_eids:
            counts[eid] = counts.get(eid, 0) + 1
            tone_sum[eid] = tone_sum.get(eid, 0.0) + tone


def _parse_tone(tval: str) -> float:
    """Parse the tone value."""
    try:
        return float(str(tval).split(',')[0]) if tval else 0.0
    except Exception:
        return 0.0


async def _insert_signals(counts: Dict[int, int], tone_sum: Dict[int, float], now: datetime) -> int:
    """Insert signals into the database."""
    inserted = 0
    async with conn_ctx() as conn:
        for eid, c in counts.items():
            await insert_signal(conn, eid, "gdelt_gkg", now, "gkg_mentions", float(c))
            avg_tone = tone_sum.get(eid, 0.0) / max(1.0, c)
            await insert_signal(conn, eid, "gdelt_gkg", now, "gkg_tone_avg", float(avg_tone))
            inserted += 1
    return inserted


@flow(name="gdelt-gkg-ingest")
def run_gdelt_gkg(hours: int = 12, step_minutes: int = 15):
    return ingest_gkg_last_hours.submit(hours=hours, step_minutes=step_minutes)
