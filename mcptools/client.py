# mcp/client.py
# Central MCP client — used by all collectors
# Handles: Bright Data (Facebook), SocialData (Twitter fallback)
# Install: pip install requests python-dotenv

import os
import time
import logging
import requests
from dotenv import load_dotenv

load_dotenv()
logger = logging.getLogger(__name__)

# ── API Keys from .env ─────────────────────────────────────────────────────────
BRIGHTDATA_API_KEY  = os.getenv("BRIGHTDATA_API_KEY", "")
SOCIALDATA_API_KEY  = os.getenv("SOCIALDATA_API_KEY", "")

# ── Bright Data MCP — Facebook ─────────────────────────────────────────────────
BRIGHTDATA_BASE     = "https://api.brightdata.com/datasets/v3"
FACEBOOK_DATASET_ID = "gd_lyclm20il4r5helnj"   # Bright Data Facebook Pages dataset

def brightdata_trigger_facebook(page_slug: str) -> str | None:
    """
    Trigger a Bright Data snapshot for a Facebook page.
    Returns snapshot_id to poll later, or None on failure.
    """
    if not BRIGHTDATA_API_KEY:
        logger.error("BRIGHTDATA_API_KEY not set in .env")
        return None

    url = f"{BRIGHTDATA_BASE}/trigger?dataset_id={FACEBOOK_DATASET_ID}&include_errors=true"
    payload = [{"url": f"https://www.facebook.com/{page_slug}"}]

    try:
        resp = requests.post(
            url,
            headers={
                "Authorization": f"Bearer {BRIGHTDATA_API_KEY}",
                "Content-Type":  "application/json",
            },
            json=payload,
            timeout=30,
        )
        resp.raise_for_status()
        data        = resp.json()
        snapshot_id = data.get("snapshot_id")
        logger.info(f"Bright Data triggered for {page_slug} → snapshot_id={snapshot_id}")
        return snapshot_id
    except Exception as e:
        logger.error(f"Bright Data trigger error for {page_slug}: {e}")
        return None


def brightdata_poll_facebook(snapshot_id: str, max_wait: int = 120) -> list[dict]:
    """
    Poll Bright Data until snapshot is ready, then return posts.
    Waits up to max_wait seconds (default 2 minutes).
    """
    url = f"{BRIGHTDATA_BASE}/snapshot/{snapshot_id}?format=json"
    waited = 0

    while waited < max_wait:
        try:
            resp = requests.get(
                url,
                headers={"Authorization": f"Bearer {BRIGHTDATA_API_KEY}"},
                timeout=30,
            )
            if resp.status_code == 200:
                data = resp.json()
                logger.info(f"Snapshot {snapshot_id} ready — {len(data)} records")
                return data if isinstance(data, list) else []

            elif resp.status_code == 202:
                logger.debug(f"Snapshot {snapshot_id} still processing... ({waited}s)")
                time.sleep(15)
                waited += 15

            else:
                logger.error(f"Bright Data poll error: {resp.status_code} {resp.text}")
                return []

        except Exception as e:
            logger.error(f"Bright Data poll exception: {e}")
            return []

    logger.error(f"Snapshot {snapshot_id} timed out after {max_wait}s")
    return []


def fetch_facebook_posts_mcp(page_slug: str) -> list[dict]:
    """
    Full flow: trigger → poll → return normalized posts.
    This is what facebook_scraper.py calls.
    """
    snapshot_id = brightdata_trigger_facebook(page_slug)
    if not snapshot_id:
        return []

    raw_posts = brightdata_poll_facebook(snapshot_id)

    # Normalize Bright Data response to your existing post format
    normalized = []
    for post in raw_posts:
        normalized.append({
            "id":         post.get("post_id", ""),
            "text":       post.get("description", post.get("text", ""))[:2000],
            "url":        post.get("url", ""),
            "author":     post.get("page_name", ""),
            "created_at": post.get("date_posted", ""),
            "reactions":  str(post.get("likes", "")),
        })

    return normalized


# ── SocialData MCP — Twitter fallback ─────────────────────────────────────────

SOCIALDATA_BASE = "https://api.socialdata.tools"

def fetch_twitter_mcp(query: str, max_results: int = 20) -> list[dict]:
    """
    Fetch tweets via SocialData API.
    Free tier: 100 requests/month.
    Used as fallback when ALL Nitter instances fail.
    """
    if not SOCIALDATA_API_KEY:
        logger.warning("SOCIALDATA_API_KEY not set — skipping MCP Twitter fallback")
        return []

    try:
        resp = requests.get(
            f"{SOCIALDATA_BASE}/twitter/search",
            headers={
                "Authorization": f"Bearer {SOCIALDATA_API_KEY}",
                "Accept":        "application/json",
            },
            params={
                "query": query,
                "type":  "Latest",
            },
            timeout=20,
        )
        resp.raise_for_status()
        data   = resp.json()
        tweets = data.get("tweets", [])
        logger.info(f"SocialData: got {len(tweets)} tweets for query={query!r}")

        # Normalize to your existing tweet format
        normalized = []
        for t in tweets[:max_results]:
            user = t.get("user", {})
            normalized.append({
                "id":          t.get("id_str", ""),
                "text":        t.get("full_text", t.get("text", ""))[:1000],
                "url":         f"https://twitter.com/i/web/status/{t.get('id_str','')}",
                "user_handle": user.get("screen_name", ""),
                "user_name":   user.get("name", ""),
                "created_at":  t.get("tweet_created_at", ""),
                "source":      "socialdata_mcp",
            })
        return normalized

    except Exception as e:
        logger.error(f"SocialData API error for query={query!r}: {e}")
        return []