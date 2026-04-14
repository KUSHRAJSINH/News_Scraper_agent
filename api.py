import os
import sys
import sqlite3
import importlib
import contextlib
import re
from collections import Counter
from io import StringIO
from pathlib import Path
from fastapi import FastAPI, HTTPException
from fastapi.middleware.cors import CORSMiddleware
from pydantic import BaseModel
from typing import Optional

# Setup path and env
ROOT = Path(__file__).parent
sys.path.insert(0, str(ROOT))
from dotenv import load_dotenv
load_dotenv(ROOT / ".env")

from database.db import get_connection, DB_PATH, init_db

app = FastAPI(title="Political AI API")

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

TABLES = {
    "tweets":         {"text": "text",      "date": "created_at",  "icon": "🐦"},
    "youtube_videos": {"text": "title",     "date": "published_at","icon": "▶️"},
    "news_articles":  {"text": "title",     "date": "published_at","icon": "📰"},
    "facebook_posts": {"text": "text",      "date": "created_at",  "icon": "👥"},
}

def db_exists() -> bool:
    return Path(DB_PATH).exists()

def capture_run(module_path: str) -> tuple[bool, str]:
    """Import and run module.run(), capturing stdout/stderr."""
    buf = StringIO()
    try:
        module = importlib.import_module(module_path)
        importlib.reload(module)
        with contextlib.redirect_stdout(buf), contextlib.redirect_stderr(buf):
            module.run()
        return True, buf.getvalue()
    except Exception as e:
        return False, f"ERROR: {e}\n{buf.getvalue()}"

@app.get("/api/status")
def get_status():
    return {"status": "online", "db_exists": db_exists(), "db_path": str(DB_PATH)}

@app.get("/api/stats")
def get_table_stats():
    if not db_exists():
        return {}
    conn = get_connection()
    cur  = conn.cursor()
    out  = {}
    for table, meta in TABLES.items():
        try:
            cur.execute(f"SELECT count(*) FROM {table}")
            total = cur.fetchone()[0]
            cur.execute(f"SELECT count(*) FROM {table} WHERE sentiment IS NOT NULL")
            analyzed = cur.fetchone()[0]
            cur.execute(f"SELECT max({meta['date']}) FROM {table}")
            latest = cur.fetchone()[0] or "—"
            cur.execute(f"SELECT region, count(*) FROM {table} GROUP BY region")
            regions = dict(cur.fetchall())
            out[table] = {"total": total, "analyzed": analyzed, "latest": latest, "regions": regions}
        except Exception as e:
            out[table] = {"total": 0, "analyzed": 0, "latest": "—", "regions": {}, "error": str(e)}
    conn.close()
    return out

@app.get("/api/recent")
def get_recent(table: str = "news_articles", limit: int = 5):
    if not db_exists() or table not in TABLES:
        return []
    try:
        conn = get_connection()
        cur  = conn.cursor()
        meta = TABLES[table]
        cur.execute(f"SELECT {meta['text']}, {meta['date']}, region, sentiment FROM {table} ORDER BY rowid DESC LIMIT ?", (limit,))
        rows = cur.fetchall()
        conn.close()
        return [{"text": r[0] or "", "date": r[1] or "", "region": r[2] or "—", "sentiment": r[3] or "neutral"} for r in rows]
    except Exception as e:
        return []

@app.get("/api/trending")
def get_trending(region: str = "all", top_n: int = 20):
    if not db_exists():
        return []
    STOPWORDS = {
        "the","a","an","is","in","of","to","and","for","that","it","on","at",
        "with","this","are","was","from","by","as","be","has","have","had","not",
        "but","or","if","its","also","than","more","their","they","been","will",
        "से","में","है","और","को","के","का","की","हैं","ने","पर","एक","new","about",
        "New","about","said","says","over","after","into","out","up","can","would",
    }
    conn   = get_connection()
    cur    = conn.cursor()
    corpus = []
    rf     = "" if region == "all" else "AND region = ?"
    rp     = [] if region == "all" else [region]
    for table, col in [("tweets","text"),("news_articles","title"),("youtube_videos","title")]:
        try:
            cur.execute(f"SELECT {col} FROM {table} WHERE {col} IS NOT NULL {rf}", rp)
            corpus.extend(r[0] for r in cur.fetchall() if r[0])
        except Exception:
            pass
    conn.close()
    words = re.findall(r"\b[a-zA-Z\u0900-\u097F\u0A80-\u0AFF]{4,}\b", " ".join(corpus))
    c = Counter(w for w in words if w.lower() not in STOPWORDS)
    return [{"word": w, "count": cnt} for w, cnt in c.most_common(top_n)]

@app.get("/api/search")
def search_db_api(query: str, source: str = "all", region: str = "all", limit: int = 20):
    if not db_exists() or not query.strip():
        return []
    conn = get_connection()
    cur  = conn.cursor()
    like = f"%{query}%"
    results = []
    rf = "" if region == "all" else "AND region = ?"

    def rp(*extra): return ([region] if region != "all" else []) + list(extra)

    if source in ("all", "tweets"):
        try:
            cur.execute(f"SELECT user_handle, text, region, created_at, '' FROM tweets WHERE text LIKE ? {rf} ORDER BY rowid DESC LIMIT ?", [like] + rp(limit))
            for r in cur.fetchall():
                results.append({"src":"twitter","title":r[0],"text":r[1],"region":r[2],"date":r[3],"url":r[4]})
        except: pass

    if source in ("all", "news"):
        try:
            cur.execute(f"SELECT title, content, region, published_at, url FROM news_articles WHERE (title LIKE ? OR content LIKE ?) {rf} ORDER BY id DESC LIMIT ?", [like, like] + rp(limit))
            for r in cur.fetchall():
                results.append({"src":"news","title":r[0],"text":r[1],"region":r[2],"date":r[3],"url":r[4]})
        except: pass

    if source in ("all", "youtube"):
        try:
            cur.execute(f"SELECT title, transcript, region, published_at, id FROM youtube_videos WHERE (title LIKE ? OR transcript LIKE ?) {rf} ORDER BY rowid DESC LIMIT ?", [like, like] + rp(limit))
            for r in cur.fetchall():
                results.append({"src":"youtube","title":r[0],"text":r[1],"region":r[2],"date":r[3],"url":f"https://youtube.com/watch?v={r[4]}" if r[4] else ""})
        except: pass

    if source in ("all", "facebook"):
        try:
            cur.execute(f"SELECT author, text, region, created_at, '' FROM facebook_posts WHERE text LIKE ? {rf} ORDER BY rowid DESC LIMIT ?", [like] + rp(limit))
            for r in cur.fetchall():
                results.append({"src":"facebook","title":r[0],"text":r[1],"region":r[2],"date":r[3],"url":r[4]})
        except: pass

    conn.close()
    
    # Sort results by date descending (rough sort)
    results = sorted(results, key=lambda x: str(x.get('date')), reverse=True)[:limit]
    return results

MODULE_MAP = {
    "news":      "collectors.news_scraper",
    "youtube":   "collectors.youtube_collector",
    "twitter":   "collectors.twitter_collector",
    "facebook":  "collectors.facebook_scraper",
    "public":    "collectors.public_data_downloader",
    "sentiment": "processors.sentiment_analyzer",
}

@app.post("/api/run-collector/{name}")
def run_collector(name: str):
    if name not in MODULE_MAP:
        raise HTTPException(status_code=404, detail="Collector not found")
    
    ok, log = capture_run(MODULE_MAP[name])
    return {"success": ok, "log": log, "name": name}

class SentimentRequest(BaseModel):
    text: str

@app.post("/api/analyze-sentiment")
def analyze_sentiment_api(req: SentimentRequest):
    
    try:
        from processors.sentiment_analyzer import analyze_sentiment
        result = analyze_sentiment(req.text)
        return {"success": True, "result": result}
    except Exception as e:
        return {"success": False, "error": str(e)}
class DynamicScrapeRequest(BaseModel):
    area: str

@app.post("/api/dynamic-scrape")
def dynamic_scrape_api(req: DynamicScrapeRequest):
    buf = StringIO()
    try:
        from main import run_dynamic_scrape
        with contextlib.redirect_stdout(buf), contextlib.redirect_stderr(buf):
            run_dynamic_scrape(req.area)
        return {"success": True, "log": buf.getvalue(), "area": req.area}
    except Exception as e:
        return {"success": False, "error": str(e), "log": buf.getvalue()}

@app.get("/api/summarize-news")
def summarize_news(region: str = "all", limit: int = 10):
    if not db_exists():
        return {"success": False, "error": "Database not found"}
    
    conn = get_connection()
    cur  = conn.cursor()
    rf   = "" if region == "all" else "AND region = ?"
    rp   = ([region] if region != "all" else []) + [limit]
    cur.execute(f"SELECT title, content, source, published_at, url FROM news_articles WHERE title IS NOT NULL {rf} ORDER BY id DESC LIMIT ?", rp)
    rows = cur.fetchall()
    conn.close()

    if not rows:
        return {"success": False, "error": f"No news articles found for region: {region}"}

    articles_text = "\\n\\n".join(f"[{r[2]}] {r[0]}\\n{(r[1] or '')[:300]}" for r in rows)
    provider = os.getenv("LLM_PROVIDER", "None")

    if provider == "openai" and os.getenv("OPENAI_API_KEY"):
        try:
            import openai
            client = openai.OpenAI(api_key=os.getenv("OPENAI_API_KEY"))
            resp = client.chat.completions.create(
                model="gpt-4o-mini",
                messages=[
                    {"role":"system","content":f"You are a political analyst for {region}, Gujarat. Summarize into 5-7 key political insights. Identify main issues, sentiment, parties mentioned, and strategic implications."},
                    {"role":"user","content":articles_text},
                ],
                max_tokens=800,
            )
            return {"success": True, "summary": resp.choices[0].message.content, "provider": "openai"}
        except Exception as e:
            return {"success": False, "error": f"OpenAI Error: {e}"}

    elif provider == "gemini" and os.getenv("GEMINI_API_KEY"):
        try:
            import google.generativeai as genai
            genai.configure(api_key=os.getenv("GEMINI_API_KEY"))
            model = genai.GenerativeModel("gemini-1.5-flash")
            resp  = model.generate_content(
                f"You are a political analyst for {region}, Gujarat. "
                f"Summarize these news into 5-7 key political insights:\\n\\n{articles_text}"
            )
            return {"success": True, "summary": resp.text, "provider": "gemini"}
        except Exception as e:
            return {"success": False, "error": f"Gemini Error: {e}"}

    items = [{"title": r[0], "content": r[1], "source": r[2], "published_at": r[3], "url": r[4]} for r in rows]
    return {"success": True, "raw_articles": items, "provider": "none"}


if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=8000)
