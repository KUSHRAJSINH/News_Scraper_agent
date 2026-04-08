import sqlite3
import os
from pathlib import Path
from dotenv import load_dotenv

# Load environment variables
load_dotenv()

DB_PATH = os.getenv("DB_PATH", "database/political_intel.sqlite")

def get_connection():
    """Get a connection to the SQLite database."""
    Path(DB_PATH).parent.mkdir(parents=True, exist_ok=True)
    return sqlite3.connect(DB_PATH)

def init_db():
    """Initialize the database with required tables and run migrations."""
    conn = get_connection()
    cursor = conn.cursor()

    # 1. Tweets Table (Nitter RSS)
    cursor.execute("""
    CREATE TABLE IF NOT EXISTS tweets (
        id TEXT PRIMARY KEY,
        text TEXT,
        user_name TEXT,
        user_handle TEXT,
        created_at TEXT,
        region TEXT,
        query TEXT,
        source TEXT DEFAULT 'nitter_rss',
        sentiment TEXT,
        raw_json TEXT
    )
    """)

    # 2. YouTube Videos Table
    cursor.execute("""
    CREATE TABLE IF NOT EXISTS youtube_videos (
        id TEXT PRIMARY KEY,
        title TEXT,
        description TEXT,
        channel_name TEXT,
        published_at TEXT,
        region TEXT,
        query TEXT,
        view_count INTEGER DEFAULT 0,
        like_count INTEGER DEFAULT 0,
        comment_count INTEGER DEFAULT 0,
        transcript TEXT,
        sentiment TEXT,
        raw_json TEXT
    )
    """)

    # 3. News Articles Table
    cursor.execute("""
    CREATE TABLE IF NOT EXISTS news_articles (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        url TEXT UNIQUE,
        title TEXT,
        content TEXT,
        author TEXT,
        source TEXT,
        published_at TEXT,
        region TEXT,
        sentiment TEXT,
        tags TEXT
    )
    """)

    # 4. Facebook Posts Table
    cursor.execute("""
    CREATE TABLE IF NOT EXISTS facebook_posts (
        id TEXT PRIMARY KEY,
        text TEXT,
        author TEXT,
        url TEXT,
        created_at TEXT,
        region TEXT,
        reactions TEXT,
        sentiment TEXT
    )
    """)

    # ── Migrations: add columns to existing tables safely ──────────────────────
    migrations = [
        ("tweets",          "query",    "TEXT"),
        ("tweets",          "source",   "TEXT DEFAULT 'nitter_rss'"),
        ("tweets",          "sentiment","TEXT"),
        ("youtube_videos",  "query",    "TEXT"),
        ("youtube_videos",  "view_count","INTEGER DEFAULT 0"),
        ("youtube_videos",  "like_count","INTEGER DEFAULT 0"),
        ("youtube_videos",  "comment_count","INTEGER DEFAULT 0"),
        ("youtube_videos",  "transcript","TEXT"),
        ("youtube_videos",  "sentiment","TEXT"),
        ("news_articles",   "author",   "TEXT"),
        ("news_articles",   "tags",     "TEXT"),
        ("facebook_posts",  "reactions","TEXT"),
        ("facebook_posts",  "sentiment","TEXT"),
    ]
    for table, column, col_type in migrations:
        try:
            cursor.execute(f"ALTER TABLE {table} ADD COLUMN {column} {col_type}")
        except Exception:
            pass  # Column already exists — safe to ignore

    conn.commit()
    conn.close()
    print(f"✓ Database initialized at {DB_PATH}")

if __name__ == "__main__":
    init_db()