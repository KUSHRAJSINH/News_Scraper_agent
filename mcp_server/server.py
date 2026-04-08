# mcp_server/server.py
# Political Campaign Intelligence — MCP Server
# Exposes data collection and analysis as LLM-callable tools.
#
# Requires: pip install mcp[cli]
# Run:      python mcp_server/server.py
# Connect:  Add to Claude Desktop config or any MCP-compatible client

import sys
import json
import os
import sqlite3
from pathlib import Path
from datetime import datetime, timezone

from mcp.server.fastmcp import FastMCP

sys.path.insert(0, str(Path(__file__).parent.parent))
from database.db import get_connection, DB_PATH

# ─── MCP Server Setup ─────────────────────────────────────────────────────────

mcp = FastMCP(
    name="political-intelligence",
    instructions=(
        "You are a political campaign intelligence assistant for the Ahmedabad/Sanand region. "
        "Use the available tools to collect fresh data, search the intelligence database, "
        "analyze sentiments, and summarize political trends. Always cite your data source."
    ),
)

# ─────────────────────────────────────────────────────────────────────────────
# TOOL 1: Search Political Data
# LLM Integration Point — results can be fed into an LLM for summarization
# ─────────────────────────────────────────────────────────────────────────────

@mcp.tool()
def search_political_data(
    query: str,
    source: str = "all",
    region: str = "all",
    limit: int = 20,
) -> str:
    """
    Search the political intelligence database for tweets, news articles,
    YouTube videos, and Facebook posts matching a keyword query.

    Args:
        query:  keyword or phrase to search (e.g. "water supply Ahmedabad")
        source: filter by source — 'tweets', 'news', 'youtube', 'facebook', or 'all'
        region: filter by region — 'ahmedabad', 'sanand', 'gujarat_general', or 'all'
        limit:  max number of results to return (default 20)
    """
    conn   = get_connection()
    cursor = conn.cursor()
    results = []
    like    = f"%{query}%"

    def add_results(table: str, text_col: str, title_col: str, source_label: str):
        region_filter = "" if region == "all" else "AND region = ?"
        params        = [like, like]
        if region != "all":
            params.append(region)
        params.append(limit)
        cursor.execute(f"""
            SELECT {title_col}, {text_col}, region, created_at, url
            FROM {table}
            WHERE ({title_col} LIKE ? OR {text_col} LIKE ?)
            {region_filter}
            ORDER BY rowid DESC
            LIMIT ?
        """, params)
        for row in cursor.fetchall():
            results.append({
                "source":  source_label,
                "title":   row[0] or "",
                "text":    (row[1] or "")[:300],
                "region":  row[2] or "",
                "date":    row[3] or "",
                "url":     row[4] or "",
            })

    if source in ("all", "tweets"):
        try:
            add_results("tweets",         "text",    "user_handle", "twitter")
        except Exception as e:
            pass
    if source in ("all", "news"):
        try:
            cursor.execute(f"""
                SELECT title, content, region, published_at, url
                FROM news_articles WHERE (title LIKE ? OR content LIKE ?)
                {"AND region = ?" if region != "all" else ""}
                ORDER BY id DESC LIMIT ?
            """, [like, like] + ([region] if region != "all" else []) + [limit])
            for row in cursor.fetchall():
                results.append({
                    "source": "news",
                    "title":  row[0] or "",
                    "text":   (row[1] or "")[:300],
                    "region": row[2] or "",
                    "date":   row[3] or "",
                    "url":    row[4] or "",
                })
        except Exception:
            pass
    if source in ("all", "youtube"):
        try:
            cursor.execute(f"""
                SELECT title, transcript, region, published_at, id
                FROM youtube_videos WHERE (title LIKE ? OR transcript LIKE ? OR description LIKE ?)
                {"AND region = ?" if region != "all" else ""}
                ORDER BY rowid DESC LIMIT ?
            """, [like, like, like] + ([region] if region != "all" else []) + [limit])
            for row in cursor.fetchall():
                results.append({
                    "source": "youtube",
                    "title":  row[0] or "",
                    "text":   (row[1] or "")[:300],
                    "region": row[2] or "",
                    "date":   row[3] or "",
                    "url":    f"https://www.youtube.com/watch?v={row[4]}" if row[4] else "",
                })
        except Exception:
            pass
    if source in ("all", "facebook"):
        try:
            add_results("facebook_posts", "text", "author", "facebook")
        except Exception:
            pass

    conn.close()

    if not results:
        return f"No results found for query: '{query}' in source: {source}, region: {region}"

    output = [f"Found {len(results)} results for '{query}':\n"]
    for i, r in enumerate(results, 1):
        output.append(
            f"{i}. [{r['source'].upper()}] {r['title'] or r['text'][:60]}\n"
            f"   Region: {r['region']} | Date: {r['date']}\n"
            f"   {r['text'][:200]}...\n"
            f"   URL: {r['url']}\n"
        )
    return "\n".join(output)

# ─────────────────────────────────────────────────────────────────────────────
# TOOL 2: Database Stats
# ─────────────────────────────────────────────────────────────────────────────

@mcp.tool()
def get_database_stats() -> str:
    """
    Return a summary of all collected data in the database:
    row counts per table, latest collection timestamps, breakdown by region.
    """
    conn   = get_connection()
    cursor = conn.cursor()
    lines  = ["=== Political Intelligence Database Stats ===\n"]

    tables = {
        "tweets":          ("text",    "created_at"),
        "youtube_videos":  ("title",   "published_at"),
        "news_articles":   ("title",   "published_at"),
        "facebook_posts":  ("text",    "created_at"),
    }

    for table, (text_col, date_col) in tables.items():
        try:
            cursor.execute(f"SELECT count(*) FROM {table}")
            total = cursor.fetchone()[0]

            cursor.execute(f"SELECT region, count(*) FROM {table} GROUP BY region")
            by_region = cursor.fetchall()

            cursor.execute(f"SELECT max({date_col}) FROM {table}")
            latest = cursor.fetchone()[0] or "none"

            lines.append(f"📊 {table.upper()}: {total} records | Latest: {latest}")
            for r, c in by_region:
                lines.append(f"   {r or 'unknown'}: {c}")
            lines.append("")
        except Exception as e:
            lines.append(f"  ⚠ {table}: {e}\n")

    conn.close()
    return "\n".join(lines)

# ─────────────────────────────────────────────────────────────────────────────
# TOOL 3: Get Trending Topics
# LLM Integration Point — output can be summarized by an LLM
# ─────────────────────────────────────────────────────────────────────────────

@mcp.tool()
def get_trending_topics(region: str = "ahmedabad", days: int = 7, top_n: int = 15) -> str:
    """
    Get the most frequently mentioned political keywords/topics recently
    collected for a given region.

    Args:
        region:  'ahmedabad', 'sanand', 'gujarat_general', or 'all'
        days:    look back window in days (default 7)
        top_n:   number of top topics to return (default 15)
    """
    import re
    from collections import Counter

    STOPWORDS = {
        "the", "a", "an", "is", "in", "of", "to", "and", "for",
        "that", "it", "on", "at", "with", "this", "are", "was",
        "से", "में", "है", "और", "को", "के", "का", "की", "हैं",
        "ने", "पर", "एक", "पर", "New", "new", "about",
    }

    conn   = get_connection()
    cursor = conn.cursor()
    text_corpus = []

    region_filter = "" if region == "all" else "AND region = ?"
    params        = [region] if region != "all" else []

    for table, col in [("tweets","text"), ("news_articles","title"), ("youtube_videos","title")]:
        try:
            cursor.execute(
                f"SELECT {col} FROM {table} WHERE {col} IS NOT NULL {region_filter}",
                params
            )
            text_corpus.extend(row[0] for row in cursor.fetchall() if row[0])
        except Exception:
            pass
    conn.close()

    words = re.findall(r"\b[a-zA-Zα-ωА-Яα-ωu0900-u097F\u0A80-\u0AFF]{3,}\b",
                       " ".join(text_corpus))
    counter = Counter(
        w for w in words
        if w.lower() not in STOPWORDS and len(w) > 3
    )
    top     = counter.most_common(top_n)

    if not top:
        return f"No trending topics found for region '{region}'."

    lines = [f"🔥 Top {top_n} trending topics in {region}:\n"]
    for i, (word, count) in enumerate(top, 1):
        lines.append(f"{i:2}. {word:25} — {count} mentions")
    return "\n".join(lines)

# ─────────────────────────────────────────────────────────────────────────────
# TOOL 4: Run Collector
# ─────────────────────────────────────────────────────────────────────────────

@mcp.tool()
def run_collector(collector: str) -> str:
    """
    Trigger a data collector to fetch fresh data.
    This runs the collector synchronously and returns a summary.

    Args:
        collector: one of 'twitter', 'youtube', 'news', 'facebook', 'public', 'sentiment'
    """
    import importlib
    MAP = {
        "twitter":   "collectors.twitter_collector",
        "youtube":   "collectors.youtube_collector",
        "news":      "collectors.news_scraper",
        "facebook":  "collectors.facebook_scraper",
        "public":    "collectors.public_data_downloader",
        "sentiment": "processors.sentiment_analyzer",
    }
    if collector not in MAP:
        return f"Unknown collector: '{collector}'. Choose from: {', '.join(MAP)}"
    try:
        module = importlib.import_module(MAP[collector])
        module.run()
        return f"✓ Collector '{collector}' completed successfully."
    except Exception as e:
        return f"✗ Collector '{collector}' failed: {e}"

# ─────────────────────────────────────────────────────────────────────────────
# TOOL 5: Analyze Sentiment of Custom Text
# Direct LLM call — routes to configured provider
# ─────────────────────────────────────────────────────────────────────────────

@mcp.tool()
def analyze_text_sentiment(text: str) -> str:
    """
    Analyze the political sentiment of any given text.
    Returns: positive / negative / neutral + score.
    Uses the configured LLM_PROVIDER from .env.

    Args:
        text: political text to analyze (tweet, news snippet, YouTube comment, etc.)
    """
    sys.path.insert(0, str(Path(__file__).parent.parent))
    from processors.sentiment_analyzer import analyze_sentiment
    result = analyze_sentiment(text)
    return (
        f"Sentiment: {result['label'].upper()}\n"
        f"Score:     {result.get('score', 'N/A')}\n"
        f"Provider:  {result.get('provider', 'unknown')}\n"
        f"Text:      {text[:200]}"
    )

# ─────────────────────────────────────────────────────────────────────────────
# TOOL 6: Summarize Latest News (LLM-powered)
# This is where a full LLM call happens to synthesize collected data
# ─────────────────────────────────────────────────────────────────────────────

@mcp.tool()
def summarize_latest_news(region: str = "ahmedabad", limit: int = 10) -> str:
    """
    Fetch the latest news articles for a region and return a structured summary.
    If an LLM provider is configured, uses LLM for smart summarization.

    Args:
        region: 'ahmedabad', 'sanand', or 'gujarat_general'
        limit:  number of articles to summarize (default 10)
    """
    conn   = get_connection()
    cursor = conn.cursor()
    region_filter = "" if region == "all" else "AND region = ?"
    params = ([region] if region != "all" else []) + [limit]
    cursor.execute(f"""
        SELECT title, content, source, published_at, url
        FROM news_articles
        WHERE title IS NOT NULL {region_filter}
        ORDER BY id DESC LIMIT ?
    """, params)
    rows = cursor.fetchall()
    conn.close()

    if not rows:
        return f"No news articles found for region: {region}"

    # Build a text blob for LLM summarization
    articles_text = "\n\n".join(
        f"[{r[2]}] {r[0]}\n{(r[1] or '')[:300]}" for r in rows
    )

    provider = os.getenv("LLM_PROVIDER", "vader")

    if provider == "openai" and os.getenv("OPENAI_API_KEY"):
        import openai
        client = openai.OpenAI(api_key=os.getenv("OPENAI_API_KEY"))
        resp = client.chat.completions.create(
            model="gpt-4o-mini",
            messages=[
                {"role": "system", "content": (
                    f"You are a political analyst for the {region} region in Gujarat, India. "
                    "Summarize the following news articles into 5-7 key political insights. "
                    "Identify: main issues, sentiment, parties mentioned, and strategic implications."
                )},
                {"role": "user", "content": articles_text},
            ],
            max_tokens=800,
        )
        return resp.choices[0].message.content

    elif provider == "gemini" and os.getenv("GEMINI_API_KEY"):
        import google.generativeai as genai
        genai.configure(api_key=os.getenv("GEMINI_API_KEY"))
        model = genai.GenerativeModel("gemini-1.5-flash")
        resp  = model.generate_content(
            f"You are a political analyst for {region}, Gujarat. "
            f"Summarize these news into 5-7 key political insights:\n\n{articles_text}"
        )
        return resp.text

    else:
        # No LLM — return structured list
        lines = [f"Latest {len(rows)} news from {region}:\n"]
        for i, (title, content, source, pub_at, url) in enumerate(rows, 1):
            lines.append(f"{i}. [{source}] {title}")
            lines.append(f"   {pub_at} | {url}\n")
        lines.append("\nTip: Set LLM_PROVIDER=openai or gemini in .env for AI summarization.")
        return "\n".join(lines)

# ─── Entry Point ──────────────────────────────────────────────────────────────

if __name__ == "__main__":
    print("Starting Political Intelligence MCP Server...")
    print(f"Database: {DB_PATH}")
    print("Tools available: search_political_data, get_database_stats, get_trending_topics,")
    print("                 run_collector, analyze_text_sentiment, summarize_latest_news")
    mcp.run()
