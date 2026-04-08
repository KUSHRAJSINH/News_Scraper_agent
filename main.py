import argparse
import sys
import os
import logging
from pathlib import Path
from dotenv import load_dotenv

load_dotenv()

sys.path.insert(0, str(Path(__file__).parent))

logging.basicConfig(
    level=os.getenv("LOG_LEVEL", "INFO"),
    format="%(asctime)s [%(levelname)s] %(message)s",
    handlers=[
        logging.StreamHandler(sys.stdout),
        logging.FileHandler("political_ai.log"),
    ],
)
logger = logging.getLogger(__name__)


# ─── DB Check Command ──────────────────────────────────────────────────────────

def check_db():
    """Print a full status report of what's stored in the SQLite database."""
    from database.db import get_connection, DB_PATH

    print(f"\n{'='*55}")
    print(f"  DATABASE CHECK  —  {DB_PATH}")
    print(f"{'='*55}")

    if not Path(DB_PATH).exists():
        print("❌ Database file does NOT exist. Run: python main.py --init-db")
        return

    conn   = get_connection()
    cursor = conn.cursor()

    tables = {
        "tweets": {
            "cols": ["id", "text", "user_handle", "region", "created_at", "source"],
            "preview_col": "text",
        },
        "youtube_videos": {
            "cols": ["id", "title", "region", "published_at", "view_count", "transcript"],
            "preview_col": "title",
        },
        "news_articles": {
            "cols": ["id", "title", "source", "region", "published_at", "url"],
            "preview_col": "title",
        },
        "facebook_posts": {
            "cols": ["id", "text", "author", "region", "created_at"],
            "preview_col": "text",
        },
    }

    for table, meta in tables.items():
        try:
            cursor.execute(f"SELECT count(*) FROM {table}")
            total = cursor.fetchone()[0]
            print(f"\n📋 {table.upper()}: {total} records")

            if total == 0:
                print("   (empty)")
                continue

            # By region
            cursor.execute(f"SELECT region, count(*) FROM {table} GROUP BY region")
            for region, cnt in cursor.fetchall():
                print(f"   {region or 'unknown':<25} {cnt} rows")

            # Sentiment coverage
            cursor.execute(f"SELECT count(*) FROM {table} WHERE sentiment IS NOT NULL")
            analyzed = cursor.fetchone()[0]
            print(f"   Sentiment analyzed:       {analyzed}/{total}")

            # Latest 3 samples
            preview_col = meta["preview_col"]
            cursor.execute(f"SELECT {preview_col} FROM {table} ORDER BY rowid DESC LIMIT 3")
            samples = cursor.fetchall()
            print("   Latest samples:")
            for (s,) in samples:
                snippet = str(s or "")[:80].replace("\n", " ")
                print(f"     • {snippet}")

        except Exception as e:
            print(f"   ⚠ Error reading {table}: {e}")

    # Transcript coverage for YouTube
    try:
        cursor.execute("SELECT count(*) FROM youtube_videos WHERE transcript != ''")
        with_transcript = cursor.fetchone()[0]
        cursor.execute("SELECT count(*) FROM youtube_videos")
        total_yt        = cursor.fetchone()[0]
        print(f"\n🎙  YouTube transcripts:   {with_transcript}/{total_yt} videos")
    except Exception:
        pass

    conn.close()
    print(f"\n{'='*55}\n")


# ─── Collectors ───────────────────────────────────────────────────────────────

def run_collector(name: str):
    try:
        if name == "twitter":
            from collectors.twitter_collector import run
        elif name == "youtube":
            from collectors.youtube_collector import run
        elif name == "news":
            from collectors.news_scraper import run
        elif name == "facebook":
            from collectors.facebook_scraper import run
        elif name == "public":
            from collectors.public_data_downloader import run
        elif name == "sentiment":
            from processors.sentiment_analyzer import run
        else:
            logger.error(f"Unknown collector: {name}")
            return
        logger.info(f"Running {name} collector...")
        run()
        logger.info(f"✓ {name} collector done")
    except Exception as e:
        logger.error(f"✗ {name} collector failed: {e}", exc_info=True)


# ─── Entrypoint ───────────────────────────────────────────────────────────────

def main():
    parser = argparse.ArgumentParser(
        description="Political Campaign AI — Data Collection Orchestrator",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  python main.py --init-db          # create/migrate DB tables
  python main.py --check-db         # verify what's stored in the DB
  python main.py --collector news   # run only news scraper
  python main.py --collector all    # run all collectors
  python main.py --collector sentiment  # run sentiment analysis on stored data
        """,
    )
    parser.add_argument(
        "--collector",
        choices=["twitter", "youtube", "news", "facebook", "public", "sentiment", "all"],
        help="Which collector to run",
    )
    parser.add_argument(
        "--init-db", action="store_true",
        help="Initialize/migrate database tables"
    )
    parser.add_argument(
        "--check-db", action="store_true",
        help="Print a summary of all data stored in the database"
    )

    args = parser.parse_args()

    if args.init_db:
        from database.db import init_db
        init_db()

    if args.check_db:
        check_db()
        return

    if not args.collector:
        parser.print_help()
        return

    COLLECTORS = ["twitter", "youtube", "news", "facebook", "public"]

    if args.collector == "all":
        logger.info("Starting all collectors...")
        for name in COLLECTORS:
            run_collector(name)
        # Run sentiment after all data is collected
        run_collector("sentiment")
    else:
        run_collector(args.collector)

    logger.info("Orchestration complete.")


if __name__ == "__main__":
    main()
