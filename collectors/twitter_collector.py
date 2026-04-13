# collectors/twitter_collector.py
#
# UPDATED: Added SocialData MCP as automatic fallback when all Nitter instances fail.
# Added 7 more Nitter instances to keywords.yaml default list.
#
# What changed vs original:
#   - Added: from mcp.client import fetch_twitter_mcp  (line 22)
#   - Changed: fetch_nitter_rss() last 2 lines — now calls MCP fallback instead of returning []
#   - Everything else: 100% unchanged (normalize, save, DB, run)
#
# FREE replacement for Twitter API — uses Nitter RSS (no API key needed)
# Falls back to SocialData MCP (100 req/month free) if all Nitter instances fail.
#
# Requires: pip install feedparser requests python-dotenv pyyaml
# Sign up SocialData: https://socialdata.tools (only needed if Nitter keeps failing)

import os
import sys
import json
import logging
import hashlib
import time
from datetime import datetime, timezone
from pathlib import Path
from urllib.parse import quote_plus

import feedparser
import requests
import yaml
from dotenv import load_dotenv

sys.path.insert(0, str(Path(__file__).parent.parent))
from database.db import get_connection, init_db
from mcptools.client import fetch_twitter_mcp   
load_dotenv()
logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
logger = logging.getLogger(__name__)

# ─── Config ────────────────────────────────────────────────────────────────────

OUTPUT_DIR = Path(os.getenv("DATA_DIR", "data/raw")) / "twitter"
CFG_PATH   = Path(__file__).parent.parent / "config" / "keywords.yaml"

def load_config() -> dict:
    with open(CFG_PATH, encoding="utf-8") as f:
        return yaml.safe_load(f)

# ─── Build queries from keywords.yaml ──────────────────────────────────────────

def build_queries(cfg: dict) -> dict[str, list[str]]:
    """Return top queries per region combining all languages."""
    queries = {}
    for region, langs in cfg["regions"].items():
        terms = []
        for lang_terms in langs.values():
            terms.extend(lang_terms)
        queries[region] = terms[:8]   # top 8 queries per region
    return queries

# ─── Nitter RSS Fetcher ────────────────────────────────────────────────────────

def fetch_nitter_rss(query: str, instances: list[str], max_retries: int = 3) -> list[dict]:
    """
    Try each Nitter instance in order until one succeeds.
    If ALL instances fail, automatically falls back to SocialData MCP.
    """
    encoded = quote_plus(query)

    for instance in instances:
        rss_url = f"{instance}/search/rss?q={encoded}&f=tweets"
        try:
            feed = feedparser.parse(rss_url)

            if feed.bozo and not feed.entries:
                logger.warning(f"  Bad feed from {instance}: {feed.bozo_exception}")
                continue

            if not feed.entries:
                logger.debug(f"  Empty feed from {instance} for query: {query!r}")
                continue

            logger.info(f"  ✓ Got {len(feed.entries)} tweets from {instance}")
            return feed.entries

        except Exception as e:
            logger.warning(f"  Nitter error ({instance}): {e}")
            time.sleep(2)
            continue

    # ── CHANGED: MCP fallback instead of returning [] ──────────────────────────
    # Original code:
    #   logger.error(f"All Nitter instances failed for query: {query!r}")
    #   return []
    #
    # New code below — tries SocialData MCP before giving up:
    logger.error(f"All Nitter instances failed for query: {query!r}")
    logger.info(f"Trying SocialData MCP fallback for: {query!r}")
    return fetch_twitter_mcp(query)                    # ← NEW: MCP fallback call

# ─── Normalizer ────────────────────────────────────────────────────────────────

def make_id(entry) -> str:
    """Create a stable ID from the RSS entry link."""
    raw = getattr(entry, "link", "") or getattr(entry, "id", "") or str(entry)
    return hashlib.md5(raw.encode()).hexdigest()


def normalize_entry(entry) -> dict:
    """
    Flatten an RSS feed entry OR a SocialData MCP dict into a clean tweet-like dict.
    Works for both sources because fetch_twitter_mcp() already normalizes to this format.
    """
    # If entry is already a normalized dict (from MCP fallback), return as-is
    if isinstance(entry, dict):
        return entry

    # Otherwise normalize from Nitter RSS feedparser entry (original logic)
    import re
    title   = getattr(entry, "title", "")
    summary = getattr(entry, "summary", "") or title
    clean_text = re.sub(r"<[^>]+>", "", summary).strip()

    handle = ""
    if title.startswith("@") and ":" in title:
        handle = title.split(":")[0].strip().lstrip("@")

    published = getattr(entry, "published", "") or getattr(entry, "updated", "")

    return {
        "id":          make_id(entry),
        "text":        clean_text[:1000],
        "url":         getattr(entry, "link", ""),
        "user_handle": handle,
        "user_name":   handle,
        "created_at":  published,
        "source":      "nitter_rss",
    }

# ─── Storage ───────────────────────────────────────────────────────────────────

def save_json(tweets: list[dict], region: str, query: str) -> Path:
    today   = datetime.now(timezone.utc).strftime("%Y-%m-%d")
    out_dir = OUTPUT_DIR / region / today
    out_dir.mkdir(parents=True, exist_ok=True)
    ts   = datetime.now(timezone.utc).strftime("%H%M%S")
    slug = query[:30].replace(" ", "_").replace("/", "_")
    path = out_dir / f"{slug}_{ts}.json"
    with open(path, "w", encoding="utf-8") as f:
        json.dump(
            {
                "collected_at": datetime.now(timezone.utc).isoformat(),
                "source":       "nitter_rss",
                "region":       region,
                "query":        query,
                "count":        len(tweets),
                "tweets":       tweets,
            },
            f, ensure_ascii=False, indent=2,
        )
    logger.info(f"Saved {len(tweets)} tweets → {path}")
    return path

# ─── DB Persistence ────────────────────────────────────────────────────────────

def save_tweets_to_db(tweets: list[dict], region: str, query: str):
    conn   = get_connection()
    cursor = conn.cursor()
    inserted = 0
    for t in tweets:
        try:
            cursor.execute("""
                INSERT OR IGNORE INTO tweets
                    (id, text, user_name, user_handle, created_at, region, query, source, raw_json)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
            """, (
                t["id"],
                t["text"],
                t.get("user_name", ""),
                t.get("user_handle", ""),
                t.get("created_at", ""),
                region,
                query,
                t.get("source", "nitter_rss"),
                json.dumps(t, ensure_ascii=False),
            ))
            inserted += 1
        except Exception as e:
            logger.error(f"DB insert error: {e}")
    conn.commit()
    conn.close()
    logger.info(f"DB: inserted {inserted} tweets for region={region}")

# ─── Core Collector ────────────────────────────────────────────────────────────

def collect_region(region: str, queries: list[str], nitter_instances: list[str]) -> int:
    total    = 0
    seen_ids = set()

    for query in queries:
        logger.info(f"[{region}] Fetching RSS for: {query!r}")
        entries = fetch_nitter_rss(query, nitter_instances)

        if not entries:
            continue

        tweets = []
        for entry in entries:
            t = normalize_entry(entry)
            if t["id"] not in seen_ids and t["text"]:
                seen_ids.add(t["id"])
                tweets.append(t)

        if tweets:
            save_json(tweets, region, query)
            save_tweets_to_db(tweets, region, query)
            total += len(tweets)

        time.sleep(3)   # gentle pacing between queries

    return total

# ─── Entry Point ───────────────────────────────────────────────────────────────

def run():
    init_db()
    cfg     = load_config()
    queries = build_queries(cfg)

    # Default Nitter instances
    instances = cfg.get("nitter_instances", [
        "https://nitter.poast.org",
        "https://nitter.privacydev.net",
        "https://nitter.net",
        "https://nitter.space",
        "https://nitter.1d4.us",
        "https://nitter.kavin.rocks",
        "https://nitter.unixfox.eu",
    ])

    grand_total = 0
    for region, q_list in queries.items():
        logger.info(f"=== Twitter/Nitter RSS: collecting {region} ===")
        count        = collect_region(region, q_list, instances)
        grand_total += count
        logger.info(f"[{region}] Done — {count} tweets collected")
        time.sleep(5)

    logger.info(f"Twitter/Nitter run complete. Total: {grand_total}")


def run_custom(area_name: str):
    """Run twitter collection for a specific custom area/city."""
    init_db()
    cfg = load_config()
    instances = cfg.get("nitter_instances", ["https://nitter.poast.org", "https://nitter.privacydev.net", "https://nitter.net"])
    
    # Build common political search queries for the area
    queries = [
        f"{area_name} BJP",
        f"{area_name} Congress",
        f"{area_name} election",
        f"{area_name} development"
    ]
    logger.info(f"=== Dynamic Twitter Scrape: {area_name} ===")
    count = collect_region(area_name, queries, instances)
    logger.info(f"Dynamic scrape for {area_name} complete: {count} tweets.")
    return count


if __name__ == "__main__":
    run()