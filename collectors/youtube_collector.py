# collectors/youtube_collector.py
# Requires: pip install google-api-python-client youtube-transcript-api python-dotenv pyyaml

import os
import sys
import json
import logging
import time
from datetime import datetime, timezone, timedelta
from pathlib import Path

import yaml
from dotenv import load_dotenv
from googleapiclient.discovery import build
from googleapiclient.errors import HttpError

sys.path.insert(0, str(Path(__file__).parent.parent))
from database.db import get_connection, init_db

load_dotenv()
logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
logger = logging.getLogger(__name__)

# ─── Config ────────────────────────────────────────────────────────────────────

YOUTUBE_API_KEY = os.getenv("YOUTUBE_API_KEY")
OUTPUT_DIR      = Path(os.getenv("DATA_DIR", "data/raw")) / "youtube"
CFG_PATH        = Path(__file__).parent.parent / "config" / "keywords.yaml"

def load_config() -> dict:
    with open(CFG_PATH, encoding="utf-8") as f:
        return yaml.safe_load(f)

# ─── Build search queries from keywords.yaml ───────────────────────────────────

def build_queries(cfg: dict) -> dict[str, list[str]]:
    queries = {}
    yt_cfg  = cfg.get("youtube", {})
    for region, langs in cfg["regions"].items():
        q_list = []
        for lang_terms in langs.values():
            for term in lang_terms[:3]:          # use top-3 per language
                q_list.append(term)
        queries[region] = q_list[:6]             # max 6 queries per region
    return queries

# ─── Storage ───────────────────────────────────────────────────────────────────

def save_json(data: dict, region: str, label: str) -> Path:
    today   = datetime.now(timezone.utc).strftime("%Y-%m-%d")
    out_dir = OUTPUT_DIR / region / today
    out_dir.mkdir(parents=True, exist_ok=True)
    ts   = datetime.now(timezone.utc).strftime("%H%M%S")
    path = out_dir / f"{label}_{ts}.json"
    with open(path, "w", encoding="utf-8") as f:
        json.dump(data, f, ensure_ascii=False, indent=2)
    logger.info(f"Saved → {path}")
    return path

# ─── Transcript Fetcher ────────────────────────────────────────────────────────

def get_transcript(video_id: str, max_chars: int = 10000) -> str:
    """Fetch auto-generated or manual captions as plain text. Free, no API quota."""
    try:
        from youtube_transcript_api import YouTubeTranscriptApi, TranscriptsDisabled, NoTranscriptFound
        transcript_list = YouTubeTranscriptApi.get_transcript(
            video_id, languages=["hi", "en", "gu", "mr"]
        )
        text = " ".join(seg["text"] for seg in transcript_list)
        logger.info(f"  ✓ Transcript fetched ({len(text)} chars) for {video_id}")
        return text[:max_chars]
    except Exception as e:
        logger.debug(f"  No transcript for {video_id}: {e}")
        return ""

# ─── DB Persistence ────────────────────────────────────────────────────────────

def save_video_to_db(video: dict, region: str, query: str):
    conn   = get_connection()
    cursor = conn.cursor()
    try:
        cursor.execute("""
            INSERT OR IGNORE INTO youtube_videos
                (id, title, description, channel_name, published_at, region, query,
                 view_count, like_count, comment_count, transcript, raw_json)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """, (
            video["video_id"],
            video["title"],
            video.get("description", "")[:1000],
            video["channel"],
            video["published_at"],
            region,
            query,
            video.get("view_count", 0),
            video.get("like_count", 0),
            video.get("comment_count", 0),
            video.get("transcript", ""),
            json.dumps(video, ensure_ascii=False),
        ))
        conn.commit()
    except Exception as e:
        logger.error(f"DB insert error for video {video.get('video_id')}: {e}")
    finally:
        conn.close()

# ─── Normalizers ───────────────────────────────────────────────────────────────

def normalize_video(item: dict) -> dict:
    snippet = item.get("snippet", {})
    stats   = item.get("statistics", {})
    vid_id  = item["id"]  # from videos.list(), item["id"] is directly the string ID
    return {
        "video_id":      vid_id,
        "title":         snippet.get("title", ""),
        "description":   snippet.get("description", "")[:500],
        "channel":       snippet.get("channelTitle", ""),
        "published_at":  snippet.get("publishedAt", ""),
        "view_count":    int(stats.get("viewCount", 0) or 0),
        "like_count":    int(stats.get("likeCount", 0) or 0),
        "comment_count": int(stats.get("commentCount", 0) or 0),
        "url":           f"https://www.youtube.com/watch?v={vid_id}",
    }

def normalize_comment(comment_thread: dict) -> dict:
    top = comment_thread["snippet"]["topLevelComment"]["snippet"]
    return {
        "comment_id":   comment_thread["id"],
        "text":         top.get("textDisplay", ""),
        "author":       top.get("authorDisplayName", ""),
        "like_count":   top.get("likeCount", 0),
        "published_at": top.get("publishedAt", ""),
        "reply_count":  comment_thread["snippet"].get("totalReplyCount", 0),
    }

# ─── Core Collector ────────────────────────────────────────────────────────────

def search_videos(youtube, query: str, max_results: int, published_after: str) -> list[str]:
    """Return video IDs — safely guarded against non-video search results."""
    try:
        resp = youtube.search().list(
            q=query,
            part="id",
            type="video",
            maxResults=max_results,
            order="date",                          # ← always fresh content
            publishedAfter=published_after,        # ← last N days only
            regionCode="IN",
        ).execute()
        # BUGFIX: guard with .get("videoId") — search can return channel/playlist items
        ids = [
            item["id"]["videoId"]
            for item in resp.get("items", [])
            if item.get("id", {}).get("videoId")
        ]
        logger.debug(f"  Search '{query}' → {len(ids)} video IDs")
        return ids
    except HttpError as e:
        logger.error(f"YouTube search error: {e}")
        return []

def get_video_details(youtube, video_ids: list[str]) -> list[dict]:
    if not video_ids:
        return []
    try:
        resp = youtube.videos().list(
            id=",".join(video_ids),
            part="snippet,statistics",
        ).execute()
        return [normalize_video(item) for item in resp.get("items", [])]
    except HttpError as e:
        logger.error(f"YouTube video detail error: {e}")
        return []

def get_comments(youtube, video_id: str, max_results: int) -> list[dict]:
    try:
        resp = youtube.commentThreads().list(
            videoId=video_id,
            part="snippet",
            maxResults=max_results,
            order="relevance",
            textFormat="plainText",
        ).execute()
        return [normalize_comment(item) for item in resp.get("items", [])]
    except HttpError as e:
        if e.resp.status in (403, 404):
            logger.debug(f"Comments unavailable for {video_id}")
        else:
            logger.error(f"Comment error for {video_id}: {e}")
        return []

def collect_region(youtube, region: str, queries: list[str], cfg: dict) -> int:
    yt_cfg         = cfg.get("youtube", {})
    max_results    = yt_cfg.get("max_results", 20)
    max_comments   = yt_cfg.get("max_comments", 100)
    max_trans_chars= yt_cfg.get("max_transcript_chars", 10000)
    days_back      = yt_cfg.get("published_after_days", 60)

    published_after = (
        datetime.now(timezone.utc) - timedelta(days=days_back)
    ).strftime("%Y-%m-%dT%H:%M:%SZ")

    total_videos = 0

    for query in queries:
        logger.info(f"[{region}] Searching: {query}")

        video_ids = search_videos(youtube, query, max_results, published_after)
        videos    = get_video_details(youtube, video_ids)

        if not videos:
            logger.info(f"  No videos found for: {query!r}")
            continue

        enriched = []
        for v in videos:
            # Fetch comments
            comments = get_comments(youtube, v["video_id"], max_comments)
            v["comments"] = comments

            # Fetch transcript (free, no API quota)
            v["transcript"] = get_transcript(v["video_id"], max_trans_chars)

            enriched.append(v)
            save_video_to_db(v, region, query)
            time.sleep(0.5)

        save_json(
            {
                "query":        query,
                "region":       region,
                "collected_at": datetime.now(timezone.utc).isoformat(),
                "count":        len(enriched),
                "videos":       enriched,
            },
            region,
            label="videos",
        )
        total_videos += len(enriched)
        logger.info(f"  [{region}] Saved {len(enriched)} videos for: {query!r}")
        time.sleep(2)

    return total_videos

# ─── Entry Point ───────────────────────────────────────────────────────────────

def run():
    if not YOUTUBE_API_KEY:
        raise EnvironmentError("YOUTUBE_API_KEY not set in .env")

    init_db()
    cfg     = load_config()
    queries = build_queries(cfg)
    youtube = build("youtube", "v3", developerKey=YOUTUBE_API_KEY,
                    cache_discovery=False)   # suppress file_cache warning
    grand_total = 0

    for region, q_list in queries.items():
        logger.info(f"=== YouTube: collecting {region} ===")
        count = collect_region(youtube, region, q_list, cfg)
        grand_total += count
        logger.info(f"[{region}] Done — {count} videos collected")

    logger.info(f"YouTube run complete. Total videos: {grand_total}")

if __name__ == "__main__":
    run()
