from __future__ import annotations

import os
import re
from datetime import datetime, timedelta, timezone
from typing import List

from prefect import flow, task, get_run_logger

from libs.db import conn_ctx, insert_signal
from sqlalchemy import text
from libs.health import is_circuit_open, record_source_ok, record_source_error
from libs.audit import audit_event
from libs.rate import TokenBucket

# Reddit OAuth creds
REDDIT_CLIENT_ID = os.getenv("REDDIT_CLIENT_ID", "").strip()
REDDIT_CLIENT_SECRET = os.getenv("REDDIT_CLIENT_SECRET", "").strip()
REDDIT_USER_AGENT = os.getenv("REDDIT_USER_AGENT", "ET-Heatmap/1.0 by internal").strip()

TARGET_SUBS = os.getenv("REDDIT_TARGET_SUBS", "entertainment,movies,television,PopCulture,Music,hiphopheads").split(",")
TARGET_SUBS = [s.strip() for s in TARGET_SUBS if s.strip()]

FETCH_LIMIT = int(os.getenv("REDDIT_FETCH_LIMIT", "200"))
LOOKBACK_MINUTES = int(os.getenv("REDDIT_LOOKBACK_MINUTES", "120"))


def _compile_patterns(names: List[str]) -> List[tuple[int, str, re.Pattern]]:
    pats = []
    for idx, name in enumerate(names):
        if not name:
            continue
        pat = re.compile(r"\\b" + re.escape(name) + r"\\b", flags=re.IGNORECASE)
        pats.append((idx, name, pat))
    return pats


async def _load_entities() -> List[tuple[int, str]]:
    async with conn_ctx() as conn:
        rows = (await conn.execute(text("SELECT id, name FROM entities"))).fetchall()
        return [(int(r[0]), str(r[1])) for r in rows]


def _make_reddit_client(logger):
    try:
        import praw  # type: ignore
    except Exception as e:  # pragma: no cover
        logger.warning(f"praw not available: {e}")
        return None, e
    try:
        reddit = praw.Reddit(
            client_id=REDDIT_CLIENT_ID,
            client_secret=REDDIT_CLIENT_SECRET,
            user_agent=REDDIT_USER_AGENT,
            ratelimit_seconds=60,
        )
        return reddit, None
    except Exception as e:  # pragma: no cover
        return None, e


async def _process_subreddit(reddit, sub: str, patterns: List[tuple[int, str, re.Pattern]], ids: List[int], counts: dict, since_ts: datetime, now: datetime):
    # Fetch latest posts and count mentions
    try:
        for post in reddit.subreddit(sub).new(limit=FETCH_LIMIT):
            try:
                created = datetime.fromtimestamp(float(post.created_utc), tz=timezone.utc)
            except Exception:
                created = now
            if created < since_ts:
                continue
            text = f"{post.title or ''} {post.selftext or ''}"
            for (idx, name, pat) in patterns:
                if pat.search(text):
                    counts[ids[idx]] += 1
        await audit_event("reddit", "fetched_sub", status=200, extra={"sub": sub})
    except Exception as e:
        # PRAW handles rate limits internally, but capture failures
        await audit_event("reddit", "fetch_failed", level="warning", extra={"sub": sub, "error": str(e)})


@task
async def ingest_reddit_mentions() -> int:
    logger = get_run_logger()
    if not (REDDIT_CLIENT_ID and REDDIT_CLIENT_SECRET and REDDIT_USER_AGENT):
        await audit_event("reddit", "disabled_or_missing_creds")
        return 0
    if await is_circuit_open("reddit"):
        await audit_event("reddit", "circuit_open_skip")
        return 0

    reddit, err = _make_reddit_client(logger)
    if reddit is None:
        await record_source_error("reddit")
        await audit_event("reddit", "auth_or_import_failed", level="error", extra={"error": str(err)})
        return 0

    ent_rows = await _load_entities()
    ids = [eid for eid, _ in ent_rows]
    names = [nm for _, nm in ent_rows]
    patterns = _compile_patterns(names)

    since_ts = datetime.now(timezone.utc) - timedelta(minutes=LOOKBACK_MINUTES)
    now = datetime.now(timezone.utc)
    counts = dict.fromkeys(ids, 0)

    # r/all and targeted subs
    subreddits = ["all"] + TARGET_SUBS
    bucket = TokenBucket(key="reddit:fetch", rate=60, interval=60, burst=60, redis_url=os.getenv("REDIS_URL"))
    for sub in subreddits:
        if not await bucket.acquire(1):
            await audit_event("reddit", "rate_limited_skip", extra={"sub": sub})
            continue
        await _process_subreddit(reddit, sub, patterns, ids, counts, since_ts, now)

    # Write signals
    inserted = 0
    async with conn_ctx() as conn:
        for eid, c in counts.items():
            if c <= 0:
                continue
            await insert_signal(conn, eid, "reddit", now, "mentions", float(c))
            inserted += 1

    await record_source_ok("reddit")
    await audit_event("reddit", "inserted_signals", extra={"entities": inserted})
    logger.info(f"reddit: inserted signals for {inserted} entities")
    return inserted


@flow(name="reddit-ingest")
def run_reddit_ingest():
    return ingest_reddit_mentions.submit()
