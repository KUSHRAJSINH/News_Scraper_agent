# collectors/news_scraper.py — RSS-first approach
# Requires: pip install feedparser requests beautifulsoup4 lxml playwright python-dotenv pyyaml

import os
import sys
import json
import logging
import hashlib
import time
import re
from datetime import datetime, timezone
from pathlib import Path

import feedparser
import requests
import yaml
from bs4 import BeautifulSoup
from dotenv import load_dotenv

sys.path.insert(0, str(Path(__file__).parent.parent))
from database.db import get_connection, init_db

load_dotenv()
logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
logger = logging.getLogger(__name__)

# ─── Config ────────────────────────────────────────────────────────────────────

OUTPUT_DIR = Path(os.getenv("DATA_DIR", "data/raw")) / "news"
CFG_PATH   = Path(__file__).parent.parent / "config" / "keywords.yaml"

HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/124.0.0.0 Safari/537.36"
    ),
    "Accept-Language": "en-US,en;q=0.9,hi;q=0.8,gu;q=0.7",
    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
}

# ─── Source Definitions ────────────────────────────────────────────────────────
# rss_urls  → list of RSS feed URLs (primary data source)
# base_url  → used for resolving relative article links
# body_sel  → CSS selectors tried IN ORDER for article body text
# region    → which region this source covers

NEWS_SOURCES = {
    "divya_bhaskar": {
        "rss_urls": [
            "https://www.divyabhaskar.co.in/rss-v1/dainik-bhaskar-gujarati-news-rss.xml",
            "https://www.divyabhaskar.co.in/rss-feed/",
        ],
        "base_url": "https://www.divyabhaskar.co.in",
        "body_sel": [
            "div.story_details",
            "div[class*='article']",
            "div.content",
            "article p",
        ],
        "use_playwright": False,
        "region": "ahmedabad",
    },
    "times_of_india": {
        "rss_urls": [
            "https://timesofindia.indiatimes.com/rssfeeds/-2128838597.cms",   # Ahmedabad
            "https://timesofindia.indiatimes.com/rssfeeds/1081479906.cms",    # Gujarat
        ],
        "base_url": "https://timesofindia.indiatimes.com",
        "body_sel": [
            "div.Normal",
            "article p",
            "div[class*='article']",
            "div.ANS",
        ],
        "use_playwright": False,
        "region": "ahmedabad",
    },
    "the_hindu": {
        "rss_urls": [
            "https://www.thehindu.com/news/cities/Ahmedabad/?service=rss",
        ],
        "base_url": "https://www.thehindu.com",
        "body_sel": [
            "div.articlebodycontent p",
            "div[class*='article-body'] p",
            "div.article-text p",
            "article p",
        ],
        "use_playwright": False,
        "region": "ahmedabad",
    },
    "sandesh": {
        "rss_urls": [
            "https://sandesh.com/feed",
        ],
        "base_url": "https://sandesh.com",
        "body_sel": [
            "div.entry-content p",
            "div.post-content p",
            "div.content-area p",
            "article p",
        ],
        "use_playwright": False,
        "region": "ahmedabad",
    },
    "gujarat_samachar": {
        "rss_urls": [
            "https://www.gujaratsamachar.com/rss/top-stories",
        ],
        "base_url": "https://www.gujaratsamachar.com",
        "body_sel": [
            "div.news-detail p",
            "div.article-content p",
            "div.content p",
            "article p",
        ],
        "use_playwright": False,
        "region": "ahmedabad",
    },
}

# ─── RSS Fetcher ───────────────────────────────────────────────────────────────

def fetch_rss(url: str) -> list:
    """Fetch entries from an RSS feed URL. Returns a list of feed entries."""
    try:
        # feedparser handles redirects, encoding, malformed XML gracefully
        feed = feedparser.parse(url, request_headers=HEADERS)
        if feed.bozo and not feed.entries:
            logger.warning(f"  RSS parse issue for {url}: {feed.bozo_exception}")
            return []
        logger.info(f"  RSS: got {len(feed.entries)} entries from {url}")
        return feed.entries
    except Exception as e:
        logger.error(f"  RSS fetch error for {url}: {e}")
        return []

# ─── Article Body Scraper ──────────────────────────────────────────────────────

def clean_text(html_or_text: str) -> str:
    """Strip HTML tags and collapse whitespace."""
    text = re.sub(r"<[^>]+>", " ", html_or_text)
    text = re.sub(r"\s+", " ", text).strip()
    return text

def scrape_article_body(url: str, selectors: list[str], use_playwright: bool = False) -> str:
    """
    Scrape full article body from a URL.
    Tries each CSS selector in order and returns the first non-empty result.
    Falls back to extracting all <p> tags if nothing matches.
    """
    html = ""
    try:
        if use_playwright:
            from playwright.sync_api import sync_playwright
            with sync_playwright() as p:
                browser = p.chromium.launch(headless=True)
                page    = browser.new_page(user_agent=HEADERS["User-Agent"])
                page.goto(url, wait_until="networkidle", timeout=25000)
                html = page.content()
                browser.close()
        else:
            resp = requests.get(url, headers=HEADERS, timeout=15)
            resp.raise_for_status()
            html = resp.text
    except Exception as e:
        logger.debug(f"  Body fetch failed for {url}: {e}")
        return ""

    soup = BeautifulSoup(html, "lxml")

    # Remove noise elements
    for tag in soup.select("nav, header, footer, .ad, .advertisement, script, style, aside"):
        tag.decompose()

    # Try each source-specific selector
    for sel in selectors:
        found = soup.select(sel)
        if found:
            text = " ".join(el.get_text(strip=True) for el in found if el.get_text(strip=True))
            if len(text) > 200:   # must be meaningful
                return text[:6000]

    # Generic fallback — all paragraph tags
    paragraphs = soup.find_all("p")
    text = " ".join(p.get_text(strip=True) for p in paragraphs if len(p.get_text(strip=True)) > 40)
    return text[:6000]

# ─── Article Normalizer ────────────────────────────────────────────────────────

def normalize_entry(entry, source_name: str) -> dict:
    title      = clean_text(getattr(entry, "title", "") or "")
    url        = getattr(entry, "link",  "") or getattr(entry, "id", "")
    summary    = clean_text(getattr(entry, "summary", "") or "")
    published  = getattr(entry, "published", "") or getattr(entry, "updated", "")
    author     = getattr(entry, "author", "")
    tags       = ",".join(t.get("term","") for t in getattr(entry, "tags", []))

    return {
        "url":          url,
        "title":        title,
        "summary":      summary[:500],   # RSS summary (shorter, always available)
        "content":      "",              # filled after full-page scrape
        "author":       author,
        "published_at": published,
        "tags":         tags,
        "source":       source_name,
    }

# ─── Storage ───────────────────────────────────────────────────────────────────

def save_json(articles: list[dict], source: str) -> Path:
    today   = datetime.now(timezone.utc).strftime("%Y-%m-%d")
    out_dir = OUTPUT_DIR / source / today
    out_dir.mkdir(parents=True, exist_ok=True)
    ts      = datetime.now(timezone.utc).strftime("%H%M%S")
    path    = out_dir / f"articles_{ts}.json"
    with open(path, "w", encoding="utf-8") as f:
        json.dump(
            {
                "source":       source,
                "collected_at": datetime.now(timezone.utc).isoformat(),
                "count":        len(articles),
                "articles":     articles,
            },
            f, ensure_ascii=False, indent=2,
        )
    logger.info(f"Saved {len(articles)} articles → {path}")
    return path

def save_articles_to_db(articles: list[dict], region: str):
    conn   = get_connection()
    cursor = conn.cursor()
    inserted = 0
    for a in articles:
        try:
            cursor.execute("""
                INSERT OR IGNORE INTO news_articles
                    (url, title, content, author, source, published_at, region, tags)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?)
            """, (
                a["url"],
                a["title"],
                a.get("content") or a.get("summary", ""),
                a.get("author", ""),
                a["source"],
                a.get("published_at", ""),
                region,
                a.get("tags", ""),
            ))
            inserted += 1
        except Exception as e:
            logger.error(f"DB insert error: {e}")
    conn.commit()
    conn.close()
    logger.info(f"DB: inserted {inserted} articles from {a.get('source','?')}")

# ─── Collector ─────────────────────────────────────────────────────────────────

def collect_source(source_name: str, config: dict) -> list[dict]:
    articles  = []
    seen_urls = set()

    # 1. Pull entries from all RSS feeds for this source
    raw_entries = []
    for rss_url in config["rss_urls"]:
        logger.info(f"[{source_name}] RSS: {rss_url}")
        entries = fetch_rss(rss_url)
        raw_entries.extend(entries)
        time.sleep(1)

    if not raw_entries:
        logger.warning(f"[{source_name}] No RSS entries found — skipping")
        return []

    # 2. Normalize and scrape full body
    for entry in raw_entries[:30]:   # cap at 30 per source run
        a = normalize_entry(entry, source_name)

        if not a["url"] or a["url"] in seen_urls:
            continue
        seen_urls.add(a["url"])

        if not a["title"]:
            continue

        # Scrape full body from article page
        logger.debug(f"  Scraping body: {a['title'][:60]}")
        body = scrape_article_body(a["url"], config["body_sel"], config.get("use_playwright", False))
        a["content"] = body if body else a["summary"]

        articles.append(a)
        time.sleep(2)   # polite delay

    logger.info(f"[{source_name}] Collected {len(articles)} articles")
    return articles

# ─── Entry Point ───────────────────────────────────────────────────────────────

def run():
    init_db()
    total = 0

    for source_name, config in NEWS_SOURCES.items():
        logger.info(f"=== News: scraping {source_name} ===")
        articles = collect_source(source_name, config)
        if articles:
            save_json(articles, source_name)
            save_articles_to_db(articles, config["region"])
        total += len(articles)
        time.sleep(3)

    logger.info(f"News scraper complete. Total articles: {total}")

if __name__ == "__main__":
    run()
