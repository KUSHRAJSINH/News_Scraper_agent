# collectors/facebook_scraper.py
# Strategy: m.facebook.com (mobile) + optional login for better reach
# Requires: pip install playwright python-dotenv
# Setup: playwright install chromium

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

load_dotenv()
logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
logger = logging.getLogger(__name__)

# ─── Config ────────────────────────────────────────────────────────────────────

OUTPUT_DIR = Path(os.getenv("DATA_DIR", "data/raw")) / "facebook"
FB_EMAIL   = os.getenv("FB_EMAIL", "")
FB_PASSWORD= os.getenv("FB_PASSWORD", "")
USE_LOGIN  = bool(FB_EMAIL and FB_PASSWORD)

# Mobile Facebook page slugs — we use m.facebook.com which has simpler HTML
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
SCROLL_PAUSE_MS    = 2500
MAX_SCROLLS        = 6

MOBILE_UA = (
    "Mozilla/5.0 (Linux; Android 13; Pixel 7) "
    "AppleWebKit/537.36 (KHTML, like Gecko) "
    "Chrome/124.0.6367.82 Mobile Safari/537.36"
)

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
            {"page": slug, "region": region,
             "collected_at": datetime.now(timezone.utc).isoformat(),
             "count": len(posts), "posts": posts},
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
            """, (p["id"], p["text"], p.get("author",""), p.get("url",""),
                  p.get("created_at",""), region, p.get("reactions","")))
            inserted += 1
        except Exception as e:
            logger.error(f"DB insert error: {e}")
    conn.commit()
    conn.close()
    logger.info(f"DB: inserted {inserted} Facebook posts for region={region}")

# ─── Login Helper ──────────────────────────────────────────────────────────────

def facebook_login(page):
    """Log in to mobile Facebook if credentials are provided."""
    logger.info("  Attempting Facebook login...")
    page.goto("https://m.facebook.com/login", wait_until="domcontentloaded", timeout=20000)
    page.wait_for_timeout(2000)
    try:
        page.fill("input[name='email']",  FB_EMAIL)
        page.fill("input[name='pass']",   FB_PASSWORD)
        page.click("button[name='login']")
        page.wait_for_timeout(4000)
        if "login" in page.url:
            logger.warning("  Facebook login may have failed (still on login page)")
        else:
            logger.info("  ✓ Facebook login successful")
    except Exception as e:
        logger.error(f"  Login error: {e}")

# ─── Post Collector ────────────────────────────────────────────────────────────

def is_login_wall(page) -> bool:
    """Detect if Facebook is blocking us with a login wall."""
    try:
        content = page.content()
        wall_signals = [
            "log in to continue",
            "login_form",
            "you must log in",
            "create new account",
        ]
        content_lower = content.lower()
        return any(sig in content_lower for sig in wall_signals) and "timeline" not in content_lower
    except Exception:
        return False

def scroll_and_collect(page, max_posts: int = MAX_POSTS_PER_PAGE) -> list[dict]:
    """
    Collect post text from m.facebook.com (mobile).
    Mobile Facebook uses simpler HTML than desktop — easier to parse.
    """
    posts = []
    seen  = set()

    MOBILE_POST_SELECTORS = [
        # m.facebook.com post containers
        "div[data-ft]",
        "div.story_body_container",
        "article",
        # Fallback: any div with meaningful text
    ]

    for scroll_i in range(MAX_SCROLLS):
        page.evaluate("window.scrollBy(0, 1500)")
        page.wait_for_timeout(SCROLL_PAUSE_MS)

        html    = page.content()
        from bs4 import BeautifulSoup
        soup    = BeautifulSoup(html, "lxml")

        # Remove nav/footer/header noise
        for tag in soup.select("header, footer, nav, [role='navigation']"):
            tag.decompose()

        # Try post selectors in order
        cards = []
        for sel in MOBILE_POST_SELECTORS:
            cards = soup.select(sel)
            if len(cards) > 2:
                break

        logger.debug(f"  Scroll {scroll_i+1}: found {len(cards)} cards")

        for card in cards:
            if len(posts) >= max_posts:
                break

            try:
                # Get all text from the card, excluding buttons/links
                for tag in card.select("button, [role='button'], a.see_more_link"):
                    tag.decompose()

                text = clean(card.get_text(separator=" "))
                if not text or len(text) < 20 or text in seen:
                    continue
                seen.add(text)

                # Find post link
                link_el = card.find("a", href=re.compile(r"/story\.php|/permalink/|/posts/"))
                post_url = ("https://m.facebook.com" + link_el["href"]) if link_el else ""

                # Find timestamp
                time_el = card.find("abbr") or card.find("time")
                ts      = time_el.get_text(strip=True) if time_el else ""

                posts.append({
                    "id":         post_id(text, post_url),
                    "text":       text[:2000],
                    "url":        post_url,
                    "created_at": ts,
                    "reactions":  "",
                    "author":     "",
                })

            except Exception as e:
                logger.debug(f"  Card parse error: {e}")
                continue

        if len(posts) >= max_posts:
            break

    return posts

def scrape_page(playwright_instance, slug: str, region: str) -> list[dict]:
    logger.info(f"[{region}] Scraping m.facebook.com/{slug}")

    browser = playwright_instance.chromium.launch(
        headless=True,
        args=["--no-sandbox", "--disable-blink-features=AutomationControlled"]
    )
    context = browser.new_context(
        user_agent=MOBILE_UA,
        viewport={"width": 390, "height": 844},   # iPhone 14 size
        locale="en-IN",
        timezone_id="Asia/Kolkata",
    )
    page = context.new_page()

    posts = []
    try:
        # Optional login
        if USE_LOGIN:
            facebook_login(page)

        # Navigate to mobile page
        url = f"https://m.facebook.com/{slug}"
        page.goto(url, wait_until="domcontentloaded", timeout=25000)
        page.wait_for_timeout(3000)

        # Check for login wall
        if is_login_wall(page):
            logger.warning(f"  Login wall detected for {slug} — skipping")
            browser.close()
            return []

        posts = scroll_and_collect(page, MAX_POSTS_PER_PAGE)
        logger.info(f"  Collected {len(posts)} posts from {slug}")

    except Exception as e:
        logger.error(f"Error scraping {slug}: {e}")
    finally:
        browser.close()

    return posts

# ─── Entry Point ───────────────────────────────────────────────────────────────

def run():
    from playwright.sync_api import sync_playwright
    init_db()

    if USE_LOGIN:
        logger.info("FB_EMAIL and FB_PASSWORD found — will attempt login")
    else:
        logger.info("No FB credentials in .env — running without login (public pages only)")

    grand_total = 0

    with sync_playwright() as p:
        for region, slugs in PAGES.items():
            logger.info(f"=== Facebook: collecting {region} ===")
            region_total = 0

            for slug in slugs:
                posts = scrape_page(p, slug, region)
                if posts:
                    save_json(posts, region, slug)
                    save_posts_to_db(posts, region)
                region_total  += len(posts)
                time.sleep(10)   # generous delay to avoid rate limits

            logger.info(f"[{region}] Done — {region_total} posts")
            grand_total += region_total

    logger.info(f"Facebook scraper complete. Total: {grand_total}")

if __name__ == "__main__":
    run()
