# collectors/facebook_scraper.py
#
# UPDATED: Replaced Playwright + m.facebook.com scraping with Bright Data MCP.
# Playwright always hit Facebook login walls — MCP fixes this completely.
#
# What changed vs original:
#   - Removed: playwright import, sync_playwright, browser/context/page logic
#   - Removed: facebook_login(), is_login_wall(), scroll_and_collect(), scrape_page(playwright, ...)
#   - Added:   from mcp.client import fetch_facebook_posts_mcp
#   - Added:   new scrape_page(slug, region) — 5 lines, no browser needed
#   - Kept:    PAGES config, save_json(), save_posts_to_db(), run() — all unchanged
#
# Requires: pip install requests python-dotenv
# Setup:    Sign up at https://brightdata.com → get BRIGHTDATA_API_KEY → add to .env

import os
import sys
import json
import logging
import hashlib
import time
import re
from datetime import datetime, timezone
from pathlib import Path

from dotenv import load_dotenv

sys.path.insert(0, str(Path(__file__).parent.parent))
from database.db import get_connection, init_db
from mcptools.client import fetch_facebook_posts_mcp          # ← NEW: MCP client import

load_dotenv()
logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
logger = logging.getLogger(__name__)

# ─── Config ────────────────────────────────────────────────────────────────────

OUTPUT_DIR = Path(os.getenv("DATA_DIR", "data/raw")) / "facebook"

# Mobile Facebook page slugs
# Add/remove slugs here as needed — no code change required
PAGES = {
    "ahmedabad": [
        "BJPAhmedabad",
        "INCGujarat",
        "AAPGujarat",
        "DivyaBhaskarNews",
        "sandesh.news",
        "ahmedabadmirror",
    ],
    "sanand": [
        "SanandTimes",
    ],
}

MAX_POSTS_PER_PAGE = 15

# ─── Utilities ─────────────────────────────────────────────────────────────────

def post_id(text: str, url: str) -> str:
    return hashlib.md5(f"{url}::{text[:80]}".encode()).hexdigest()

def clean(text: str) -> str:
    return re.sub(r"\s+", " ", text).strip()

# ─── Storage ───────────────────────────────────────────────────────────────────

def save_json(posts: list[dict], region: str, slug: str) -> Path:
    today   = datetime.now(timezone.utc).strftime("%Y-%m-%d")
    out_dir = OUTPUT_DIR / region / today
    out_dir.mkdir(parents=True, exist_ok=True)
    ts   = datetime.now(timezone.utc).strftime("%H%M%S")
    path = out_dir / f"{slug}_{ts}.json"
    with open(path, "w", encoding="utf-8") as f:
        json.dump(
            {
                "page":         slug,
                "region":       region,
                "collected_at": datetime.now(timezone.utc).isoformat(),
                "count":        len(posts),
                "posts":        posts,
            },
            f, ensure_ascii=False, indent=2,
        )
    logger.info(f"Saved {len(posts)} posts → {path}")
    return path


def save_posts_to_db(posts: list[dict], region: str):
    conn   = get_connection()
    cursor = conn.cursor()
    inserted = 0
    for p in posts:
        try:
            cursor.execute("""
                INSERT OR IGNORE INTO facebook_posts
                    (id, text, author, url, created_at, region, reactions)
                VALUES (?, ?, ?, ?, ?, ?, ?)
            """, (
                p["id"],
                p["text"],
                p.get("author", ""),
                p.get("url", ""),
                p.get("created_at", ""),
                region,
                p.get("reactions", ""),
            ))
            inserted += 1
        except Exception as e:
            logger.error(f"DB insert error: {e}")
    conn.commit()
    conn.close()
    logger.info(f"DB: inserted {inserted} Facebook posts for region={region}")

# ─── Core Collector ────────────────────────────────────────────────────────────
# REPLACED: old scrape_page() used Playwright → login walls → 0 posts
# NEW:      calls Bright Data MCP → structured JSON → no login wall

def scrape_page(slug: str, region: str) -> list[dict]:
    """
    Fetch Facebook page posts via Bright Data MCP.
    No browser, no login wall, no fragile CSS selectors.
    Returns list of normalized post dicts matching your DB schema.
    """
    logger.info(f"[{region}] Fetching via Bright Data MCP: facebook.com/{slug}")
    posts = fetch_facebook_posts_mcp(slug)

    # Cap to MAX_POSTS_PER_PAGE (same limit as before)
    posts = posts[:MAX_POSTS_PER_PAGE]

    # Ensure every post has a stable id (fallback if MCP didn't return post_id)
    for p in posts:
        if not p.get("id"):
            p["id"] = post_id(p.get("text", ""), p.get("url", ""))

    logger.info(f"  Got {len(posts)} posts from {slug}")
    return posts

# ─── Entry Point ───────────────────────────────────────────────────────────────

def run():
    init_db()
    grand_total = 0

    for region, slugs in PAGES.items():
        logger.info(f"=== Facebook: collecting {region} ===")
        region_total = 0

        for slug in slugs:
            posts = scrape_page(slug, region)       # ← no playwright arg needed
            if posts:
                save_json(posts, region, slug)
                save_posts_to_db(posts, region)
            region_total  += len(posts)
            time.sleep(5)                           # polite delay between pages

        logger.info(f"[{region}] Done — {region_total} posts")
        grand_total += region_total

    logger.info(f"Facebook scraper complete. Total: {grand_total}")


if __name__ == "__main__":
    run()