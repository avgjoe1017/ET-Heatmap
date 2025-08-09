from __future__ import annotations
import os
from datetime import datetime, timezone
import pandas as pd
import json
import re
from prefect import flow, task, get_run_logger
from sqlalchemy import text

from libs.db import conn_ctx, insert_signal, upsert_entity
from libs.config import is_enabled

APIFY_TOKEN = os.getenv("APIFY_TOKEN", "").strip()
# Actor: https://apify.com/clockworks/tiktok-scraper
ACTOR_ID = os.getenv("APIFY_TIKTOK_ACTOR", "clockworks/tiktok-scraper").strip()
HASHTAG_ACTOR_ID = os.getenv("APIFY_TIKTOK_HASHTAG_ACTOR", "clockworks/tiktok-hashtag-scraper").strip()


def _to_search_term(name: str) -> str:
    return name.strip()


@task
async def scrape_tt_search(top_n: int = 20, max_items: int = 24) -> int:
    """Search TikTok for Top N entities and store watchlist velocity metrics under source='tt_search'."""
    if not is_enabled("tt_search") or not APIFY_TOKEN:
        return 0
    logger = get_run_logger()

    try:
        from apify_client import ApifyClient  # type: ignore
    except Exception as e:
        logger.warning(f"apify-client not available: {e}")
        return 0

    client = ApifyClient(APIFY_TOKEN)
    now = datetime.now(timezone.utc)

    async with conn_ctx() as conn:
        # Top N by latest heat in last 7 days
        q = text(
            """
          WITH latest AS (
            SELECT s.entity_id, MAX(s.ts) AS ts
            FROM scores s
            WHERE s.ts >= NOW() - INTERVAL '7 days'
            GROUP BY s.entity_id
          )
          SELECT e.id, e.name, e.aliases
          FROM scores s
          JOIN latest l ON l.entity_id = s.entity_id AND l.ts = s.ts
          JOIN entities e ON e.id = s.entity_id
          ORDER BY s.heat DESC
          LIMIT :n
        """
        )
        rows = (await conn.execute(q, {"n": top_n})).mappings().all()

        # Fallback: if no recent scores, seed from configs/entities.csv
        if not rows:
            try:
                df = pd.read_csv("configs/entities.csv")
                if "aliases" in df.columns:
                    def parse_aliases(x):
                        try:
                            return json.loads(x) if isinstance(x, str) else []
                        except Exception:
                            return []
                    df["aliases"] = df["aliases"].apply(parse_aliases)
                seed = df.head(top_n).to_dict(orient="records")
                seeded = []
                for r in seed:
                    name = r.get("name")
                    etype = r.get("type", "person")
                    aliases = r.get("aliases", [])
                    wiki_id = r.get("wiki_id")
                    if not name:
                        continue
                    eid = await upsert_entity(conn, name, etype, aliases, wiki_id)
                    seeded.append({"id": eid, "name": name, "aliases": aliases})
                rows = seeded
                logger.info(f"tt_search fallback seeded {len(rows)} entities from CSV")
            except Exception as e:
                logger.warning(f"tt_search fallback failed: {e}")

        inserted = 0
        for row in rows:
            eid = row["id"]
            name = row["name"]
            aliases = row.get("aliases") or []

            # Build search terms from name + aliases
            base_terms = []
            for t in [name, *aliases]:
                if not t:
                    continue
                t = str(t).strip()
                if t and t not in base_terms:
                    base_terms.append(t)
            # Limit to avoid overly long inputs
            base_terms = base_terms[:5]
            # Derive hashtag guesses from terms (alphanumeric only)
            def to_hashtag(s: str) -> str:
                s = s.lower()
                s = re.sub(r"[^a-z0-9]+", "", s)
                return s
            hashtags = []
            for t in base_terms:
                h = to_hashtag(t)
                if h and h not in hashtags:
                    hashtags.append(h)

            run_input = {
                "queries": base_terms or [_to_search_term(name)],
                "hashtags": hashtags,
                "maxItems": max_items,
                "excludePinnedPosts": True,
                "shouldDownloadVideos": False,
                "shouldDownloadCovers": False,
                "shouldDownloadSubtitles": False,
                "proxyCountryCode": "US",
            }
            items = []
            try:
                run = client.actor(ACTOR_ID).call(run_input=run_input)
                ds_id = (run or {}).get("defaultDatasetId")
                if ds_id:
                    items = list(client.dataset(ds_id).iterate_items())
            except Exception as e:
                logger.warning(f"TikTok search failed for '{name}' via {ACTOR_ID}: {e}")

            if not items and HASHTAG_ACTOR_ID:
                # Fallback: try hashtag scraper actor with our derived hashtags
                try:
                    h_input = {
                        "hashtags": hashtags,
                        "maxItems": max_items,
                        "excludePinnedPosts": True,
                        "shouldDownloadVideos": False,
                        "shouldDownloadCovers": False,
                        "shouldDownloadSubtitles": False,
                        "proxyCountryCode": "US",
                    }
                    run2 = client.actor(HASHTAG_ACTOR_ID).call(run_input=h_input)
                    ds2 = (run2 or {}).get("defaultDatasetId")
                    if ds2:
                        items = list(client.dataset(ds2).iterate_items())
                except Exception as e:
                    logger.warning(f"TikTok hashtag fallback failed for '{name}' via {HASHTAG_ACTOR_ID}: {e}")

            if not items:
                logger.info(f"tt_search: 0 items for '{name}' (queries={len(run_input.get('queries', []))}, hashtags={len(hashtags)})")
                continue

            # Filter last 168h (7 days) to bootstrap signals in low-activity periods
            recent_items = []
            ts_field_counts = {"createTimeISO": 0, "createTime": 0}
            for it in items:
                create_ts = it.get("createTimeISO") or it.get("createTime")
                if not create_ts:
                    if "createTimeISO" in it:
                        ts_field_counts["createTimeISO"] += 1
                    if "createTime" in it:
                        ts_field_counts["createTime"] += 1
                    continue
                try:
                    if isinstance(create_ts, (int, float)) or (isinstance(create_ts, str) and create_ts.isdigit()):
                        posted = datetime.fromtimestamp(int(create_ts), tz=timezone.utc)
                    else:
                        posted = datetime.fromisoformat(str(create_ts).replace("Z", "+00:00"))
                except Exception:
                    continue
                hours_old = (now - posted).total_seconds() / 3600.0
                if hours_old <= 168:
                    recent_items.append((it, hours_old))
            if not recent_items:
                logger.info(f"tt_search: {len(items)} items for '{name}', 0 recent; tsFields={ts_field_counts} -> falling back to treat as recent")
                # Fallback: treat returned items as recent to bootstrap metrics
                recent_items = [(it, 9999.0) for it in items[:max_items]]

            hits = float(len(recent_items))
            authors = set()
            view_vel = []
            eng_ratio = []
            for it, hrs in recent_items:
                author = (it.get("author") or {}).get("uniqueId")
                if author:
                    authors.add(author)
                stats = it.get("stats", {})
                views = float(stats.get("playCount") or 0)
                likes = float(stats.get("diggCount") or 0)
                comments = float(stats.get("commentCount") or 0)
                shares = float(stats.get("shareCount") or 0)
                if hrs > 0 and views > 0:
                    view_vel.append(views / hrs)
                    eng_ratio.append((likes + comments + shares) / max(1.0, views))

            await insert_signal(conn, eid, "tt_search", now, "hits_24h", hits)
            await insert_signal(conn, eid, "tt_search", now, "unique_authors_24h", float(len(authors)))
            if view_vel:
                view_vel.sort()
                await insert_signal(conn, eid, "tt_search", now, "view_vel_median", float(view_vel[len(view_vel)//2]))
            if eng_ratio:
                eng_ratio.sort()
                await insert_signal(conn, eid, "tt_search", now, "eng_ratio_median", float(eng_ratio[len(eng_ratio)//2]))
            if hits > 0:
                inserted += 1

    logger.info(f"tt_search: inserted signals for {inserted} entities.")
    return inserted


@flow(name="apify-tiktok-search")
def run_apify_tiktok_search():
    return scrape_tt_search.submit()


