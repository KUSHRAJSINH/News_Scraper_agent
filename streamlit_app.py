"""
streamlit_app.py — Political Campaign AI · Workflow Testing Dashboard
Run:  streamlit run streamlit_app.py
"""

import os
import sys
import sqlite3
import importlib
import subprocess
import logging
import time
import re
from pathlib import Path
from datetime import datetime
from collections import Counter
from io import StringIO
import contextlib

import streamlit as st
from dotenv import load_dotenv

# ── Path setup ────────────────────────────────────────────────────────────────
ROOT = Path(__file__).parent
sys.path.insert(0, str(ROOT))
load_dotenv(ROOT / ".env")

from database.db import get_connection, DB_PATH, init_db

# ── Page config ───────────────────────────────────────────────────────────────
st.set_page_config(
    page_title="Political AI · Workflow Dashboard",
    page_icon="🏛️",
    layout="wide",
    initial_sidebar_state="expanded",
)

# ── Global CSS ─────────────────────────────────────────────────────────────────
st.markdown("""
<style>
@import url('https://fonts.googleapis.com/css2?family=Inter:wght@300;400;500;600;700;800&display=swap');

html, body, [class*="css"] { font-family: 'Inter', sans-serif; }

/* ── background ── */
.stApp { background: linear-gradient(135deg, #0d0f1a 0%, #111827 60%, #0a0e1a 100%); }

/* ── sidebar ── */
section[data-testid="stSidebar"] {
    background: linear-gradient(180deg, #0f1629 0%, #131c35 100%);
    border-right: 1px solid rgba(99,179,237,.15);
}
section[data-testid="stSidebar"] .stRadio label { color: #94a3b8 !important; }

/* ── metric cards ── */
.metric-card {
    background: linear-gradient(135deg, rgba(30,41,59,.85), rgba(15,23,42,.9));
    border: 1px solid rgba(99,179,237,.2);
    border-radius: 16px;
    padding: 20px 24px;
    text-align: center;
    backdrop-filter: blur(10px);
    transition: transform .2s, border-color .2s;
}
.metric-card:hover { transform: translateY(-3px); border-color: rgba(99,179,237,.5); }
.metric-number { font-size: 2.4rem; font-weight: 800; color: #63b3ed; line-height: 1; }
.metric-label  { font-size: .8rem; font-weight: 500; color: #64748b; margin-top: 6px; text-transform: uppercase; letter-spacing: .06em; }
.metric-sub    { font-size: .75rem; color: #475569; margin-top: 4px; }

/* ── status badge ── */
.badge {
    display: inline-block; padding: 3px 10px; border-radius: 20px;
    font-size: .72rem; font-weight: 600; letter-spacing: .04em;
}
.badge-green  { background: rgba(16,185,129,.15); color: #10b981; border: 1px solid rgba(16,185,129,.3); }
.badge-red    { background: rgba(239,68,68,.15);  color: #ef4444; border: 1px solid rgba(239,68,68,.3); }
.badge-orange { background: rgba(245,158,11,.15); color: #f59e0b; border: 1px solid rgba(245,158,11,.3); }
.badge-blue   { background: rgba(99,179,237,.15); color: #63b3ed; border: 1px solid rgba(99,179,237,.3); }

/* ── section headings ── */
.section-title {
    font-size: 1.4rem; font-weight: 700; color: #e2e8f0;
    border-left: 4px solid #3b82f6; padding-left: 12px; margin-bottom: 18px;
}
.section-sub { color: #64748b; font-size: .85rem; margin-top: -14px; margin-bottom: 18px; padding-left: 16px; }

/* ── result cards ── */
.result-card {
    background: rgba(15,23,42,.7);
    border: 1px solid rgba(99,179,237,.12);
    border-radius: 12px; padding: 16px 20px; margin-bottom: 12px;
}
.result-source { font-weight: 700; font-size: .75rem; text-transform: uppercase; letter-spacing: .08em; }
.src-twitter  { color: #1d9bf0; }
.src-youtube  { color: #ff0000; }
.src-news     { color: #22c55e; }
.src-facebook { color: #4267b2; }

/* ── log box ── */
.log-box {
    background: #0a0a0f; border: 1px solid #1e293b; border-radius: 10px;
    padding: 16px; font-family: 'Courier New', monospace; font-size: .78rem;
    color: #a3e635; max-height: 340px; overflow-y: auto; white-space: pre-wrap;
}

/* ── sentiment chips ── */
.sent-positive { color: #10b981; font-weight: 700; }
.sent-negative { color: #ef4444; font-weight: 700; }
.sent-neutral  { color: #f59e0b; font-weight: 700; }

/* ── trend item ── */
.trend-row {
    display: flex; align-items: center; gap: 12px;
    padding: 8px 12px; border-radius: 8px;
    background: rgba(30,41,59,.4); margin-bottom: 6px;
}
.trend-rank  { color: #475569; font-size: .85rem; width: 26px; text-align: right; }
.trend-word  { color: #e2e8f0; font-weight: 600; flex: 1; }
.trend-count { color: #63b3ed; font-size: .82rem; }

/* button tweaks */
div.stButton > button {
    background: linear-gradient(135deg, #2563eb, #1d4ed8);
    color: white; border: none; border-radius: 10px;
    font-weight: 600; letter-spacing: .02em;
    transition: all .2s;
}
div.stButton > button:hover {
    background: linear-gradient(135deg, #3b82f6, #2563eb);
    transform: translateY(-1px); box-shadow: 0 4px 15px rgba(59,130,246,.35);
}

/* header gradient */
.hero {
    background: linear-gradient(135deg, #1e3a8a 0%, #1e1b4b 50%, #0f172a 100%);
    border: 1px solid rgba(99,179,237,.2);
    border-radius: 18px; padding: 28px 32px; margin-bottom: 28px;
}
.hero h1 { color: #e2e8f0; font-size: 1.9rem; font-weight: 800; margin: 0; }
.hero p  { color: #64748b; margin: 6px 0 0; font-size: .9rem; }
</style>
""", unsafe_allow_html=True)


# ══════════════════════════════════════════════════════════════════════════════
# Helper utilities
# ══════════════════════════════════════════════════════════════════════════════

TABLES = {
    "tweets":         {"text": "text",      "date": "created_at",  "icon": "🐦"},
    "youtube_videos": {"text": "title",     "date": "published_at","icon": "▶️"},
    "news_articles":  {"text": "title",     "date": "published_at","icon": "📰"},
    "facebook_posts": {"text": "text",      "date": "created_at",  "icon": "👥"},
}

COLLECTORS = ["news", "youtube", "twitter", "facebook", "public", "sentiment"]

COLLECTOR_META = {
    "news":      {"icon": "📰", "label": "News Scraper",         "color": "#22c55e"},
    "youtube":   {"icon": "▶️", "label": "YouTube Collector",    "color": "#ef4444"},
    "twitter":   {"icon": "🐦", "label": "Twitter Collector",    "color": "#1d9bf0"},
    "facebook":  {"icon": "👥", "label": "Facebook Scraper",     "color": "#4267b2"},
    "public":    {"icon": "📂", "label": "Public Data Download", "color": "#a78bfa"},
    "sentiment": {"icon": "🧠", "label": "Sentiment Analyzer",   "color": "#f59e0b"},
}

def db_exists() -> bool:
    return Path(DB_PATH).exists()

def get_table_stats() -> dict:
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

def get_trending(region: str = "all", top_n: int = 20) -> list[tuple[str, int]]:
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
    return c.most_common(top_n)

def search_db(query: str, source: str = "all", region: str = "all", limit: int = 20) -> list[dict]:
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
    return results

def sentiment_badge(label: str) -> str:
    label = (label or "").lower()
    cls = {"positive":"sent-positive","negative":"sent-negative","neutral":"sent-neutral"}.get(label,"sent-neutral")
    icon = {"positive":"▲","negative":"▼","neutral":"●"}.get(label,"●")
    return f'<span class="{cls}">{icon} {label.capitalize()}</span>'

def recent_items(table: str, limit: int = 5) -> list:
    if not db_exists():
        return []
    try:
        conn = get_connection()
        cur  = conn.cursor()
        meta = TABLES[table]
        cur.execute(f"SELECT {meta['text']}, {meta['date']}, region, sentiment FROM {table} ORDER BY rowid DESC LIMIT ?", (limit,))
        rows = cur.fetchall()
        conn.close()
        return rows
    except:
        return []


# ══════════════════════════════════════════════════════════════════════════════
# Sidebar navigation
# ══════════════════════════════════════════════════════════════════════════════

with st.sidebar:
    st.markdown("""
    <div style='text-align:center; padding: 8px 0 20px;'>
        <div style='font-size:2.8rem;'>🏛️</div>
        <div style='font-size:1rem; font-weight:700; color:#e2e8f0;'>Political AI</div>
        <div style='font-size:.72rem; color:#475569;'>Ahmedabad / Sanand Region</div>
    </div>
    """, unsafe_allow_html=True)

    st.markdown("<div style='color:#64748b; font-size:.72rem; font-weight:600; letter-spacing:.08em; margin-bottom:8px;'>NAVIGATION</div>", unsafe_allow_html=True)

    page = st.radio(
        "nav", label_visibility="collapsed",
        options=[
            "🏠  Dashboard",
            "🗄️  Database",
            "⚙️  Run Collectors",
            "📍  Regional Intelligence",
            "🔍  Search Data",
            "🔥  Trending Topics",
            "🧠  Sentiment Lab",
            "📰  News Summary",
        ]
    )

    st.markdown("---")

    # DB status in sidebar
    db_ok = db_exists()
    _db_badge = '<span class="badge badge-green">● Online</span>' if db_ok else '<span class="badge badge-red">● Missing</span>'
    st.markdown(
        f"**DB Status:** {_db_badge}",
        unsafe_allow_html=True
    )
    if db_ok:
        stats = get_table_stats()
        total_rows = sum(v.get("total",0) for v in stats.values())
        st.markdown(f"<div style='color:#64748b; font-size:.78rem;'>Total records: <b style='color:#63b3ed;'>{total_rows:,}</b></div>", unsafe_allow_html=True)

    st.markdown("---")
    st.markdown(f"<div style='color:#374151; font-size:.72rem;'>DB path: <code style='color:#475569;'>{DB_PATH}</code></div>", unsafe_allow_html=True)
    provider = os.getenv("LLM_PROVIDER", "vader")
    st.markdown(f"<div style='color:#374151; font-size:.72rem; margin-top:4px;'>LLM provider: <code style='color:#a78bfa;'>{provider}</code></div>", unsafe_allow_html=True)


# ══════════════════════════════════════════════════════════════════════════════
# PAGE 1 — Dashboard Overview
# ══════════════════════════════════════════════════════════════════════════════

if "Dashboard" in page:
    st.markdown("""
    <div class='hero'>
        <h1>🏛️ Political Campaign Intelligence</h1>
        <p>Real-time monitoring dashboard for Ahmedabad &amp; Sanand political data streams</p>
    </div>
    """, unsafe_allow_html=True)

    # ── Init DB prompt ──
    if not db_exists():
        st.warning("⚠️ Database not found. Initialize it first.")
        if st.button("🗄️ Initialize Database Now"):
            init_db()
            st.success(f"✅ Database initialized at `{DB_PATH}`")
            st.rerun()
        st.stop()

    stats = get_table_stats()
    total_all = sum(v.get("total",0) for v in stats.values())
    analyzed_all = sum(v.get("analyzed",0) for v in stats.values())

    # ── Top KPI row ──
    k1, k2, k3, k4, k5 = st.columns(5)
    with k1:
        st.markdown(f"""
        <div class='metric-card'>
            <div class='metric-number'>{total_all:,}</div>
            <div class='metric-label'>Total Records</div>
        </div>""", unsafe_allow_html=True)
    with k2:
        t = stats.get("tweets",{}).get("total",0)
        st.markdown(f"""
        <div class='metric-card'>
            <div class='metric-number' style='color:#1d9bf0;'>{t:,}</div>
            <div class='metric-label'>🐦 Tweets</div>
        </div>""", unsafe_allow_html=True)
    with k3:
        n = stats.get("news_articles",{}).get("total",0)
        st.markdown(f"""
        <div class='metric-card'>
            <div class='metric-number' style='color:#22c55e;'>{n:,}</div>
            <div class='metric-label'>📰 News Articles</div>
        </div>""", unsafe_allow_html=True)
    with k4:
        y = stats.get("youtube_videos",{}).get("total",0)
        st.markdown(f"""
        <div class='metric-card'>
            <div class='metric-number' style='color:#ef4444;'>{y:,}</div>
            <div class='metric-label'>▶️ YT Videos</div>
        </div>""", unsafe_allow_html=True)
    with k5:
        pct = int(analyzed_all / total_all * 100) if total_all else 0
        st.markdown(f"""
        <div class='metric-card'>
            <div class='metric-number' style='color:#a78bfa;'>{pct}%</div>
            <div class='metric-label'>🧠 Sentiment Done</div>
            <div class='metric-sub'>{analyzed_all}/{total_all} rows</div>
        </div>""", unsafe_allow_html=True)

    st.markdown("<br>", unsafe_allow_html=True)

    # ── Per-source recent items ──
    col_left, col_right = st.columns(2)

    with col_left:
        st.markdown("<div class='section-title'>📰 Latest News</div>", unsafe_allow_html=True)
        rows = recent_items("news_articles", 5)
        if rows:
            for r in rows:
                title, date, region, sent = r
                st.markdown(f"""
                <div class='result-card'>
                    <div style='color:#e2e8f0; font-weight:600; font-size:.88rem;'>{str(title or '')[:80]}</div>
                    <div style='color:#475569; font-size:.75rem; margin-top:4px;'>
                        📍 {region or '—'} &nbsp;|&nbsp; 🕐 {str(date or '')[:16]}
                        &nbsp;|&nbsp; {sentiment_badge(sent)}
                    </div>
                </div>""", unsafe_allow_html=True)
        else:
            st.info("No news articles yet.")

        st.markdown("<div class='section-title' style='margin-top:18px;'>🐦 Latest Tweets</div>", unsafe_allow_html=True)
        rows = recent_items("tweets", 5)
        if rows:
            for r in rows:
                text, date, region, sent = r
                st.markdown(f"""
                <div class='result-card'>
                    <div style='color:#e2e8f0; font-size:.86rem;'>{str(text or '')[:120]}</div>
                    <div style='color:#475569; font-size:.75rem; margin-top:4px;'>
                        📍 {region or '—'} &nbsp;|&nbsp; 🕐 {str(date or '')[:16]}
                        &nbsp;|&nbsp; {sentiment_badge(sent)}
                    </div>
                </div>""", unsafe_allow_html=True)
        else:
            st.info("No tweets yet.")

    with col_right:
        st.markdown("<div class='section-title'>▶️ Latest YouTube Videos</div>", unsafe_allow_html=True)
        rows = recent_items("youtube_videos", 5)
        if rows:
            for r in rows:
                title, date, region, sent = r
                st.markdown(f"""
                <div class='result-card'>
                    <div style='color:#e2e8f0; font-weight:600; font-size:.88rem;'>{str(title or '')[:80]}</div>
                    <div style='color:#475569; font-size:.75rem; margin-top:4px;'>
                        📍 {region or '—'} &nbsp;|&nbsp; 🕐 {str(date or '')[:16]}
                        &nbsp;|&nbsp; {sentiment_badge(sent)}
                    </div>
                </div>""", unsafe_allow_html=True)
        else:
            st.info("No YouTube videos yet.")

        st.markdown("<div class='section-title' style='margin-top:18px;'>🔥 Top Trending (All)</div>", unsafe_allow_html=True)
        trends = get_trending("all", 10)
        if trends:
            max_c = trends[0][1] if trends else 1
            for i, (word, count) in enumerate(trends, 1):
                bar_pct = int(count / max_c * 100)
                st.markdown(f"""
                <div class='trend-row'>
                    <span class='trend-rank'>#{i}</span>
                    <span class='trend-word'>{word}</span>
                    <div style='flex:2; background:rgba(30,41,59,.8); border-radius:4px; height:6px;'>
                        <div style='width:{bar_pct}%; background:linear-gradient(90deg,#2563eb,#7c3aed); height:6px; border-radius:4px;'></div>
                    </div>
                    <span class='trend-count'>{count}</span>
                </div>""", unsafe_allow_html=True)
        else:
            st.info("No trend data yet.")


# ══════════════════════════════════════════════════════════════════════════════
# PAGE 2 — Database
# ══════════════════════════════════════════════════════════════════════════════

elif "Database" in page:
    st.markdown("<div class='section-title'>🗄️ Database Management</div>", unsafe_allow_html=True)

    c1, c2 = st.columns([1, 2])
    with c1:
        if st.button("🔧 Initialize / Migrate DB", use_container_width=True):
            try:
                init_db()
                st.success(f"✅ DB ready at `{DB_PATH}`")
            except Exception as e:
                st.error(f"❌ {e}")

    with c2:
        if db_exists():
            size_kb = Path(DB_PATH).stat().st_size // 1024
            st.markdown(f"<div style='color:#64748b; padding-top:10px;'>📁 <b style='color:#e2e8f0;'>{DB_PATH}</b> &nbsp;·&nbsp; {size_kb} KB</div>", unsafe_allow_html=True)
        else:
            st.warning("Database file not found.")

    if not db_exists():
        st.stop()

    st.markdown("---")
    stats = get_table_stats()

    for table, meta in TABLES.items():
        s    = stats.get(table, {})
        icon = meta["icon"]
        total = s.get("total", 0)
        analyzed = s.get("analyzed", 0)
        latest = s.get("latest", "—")
        regions = s.get("regions", {})
        err = s.get("error")

        with st.expander(f"{icon} **{table.upper()}** — {total:,} records", expanded=total > 0):
            if err:
                st.error(f"Error: {err}")
                continue

            col_a, col_b, col_c = st.columns(3)
            with col_a:
                st.metric("Total Rows", f"{total:,}")
            with col_b:
                st.metric("Sentiment Analyzed", f"{analyzed:,} / {total:,}")
            with col_c:
                st.metric("Latest Entry", str(latest)[:19])

            if regions:
                st.markdown("**By Region:**")
                for region, cnt in regions.items():
                    pct = cnt / total * 100 if total else 0
                    st.markdown(f"`{region or 'unknown'}` — **{cnt}** rows ({pct:.1f}%)")
                    st.progress(pct / 100)

            # Preview rows
            if total > 0:
                st.markdown("**Recent Rows:**")
                rows = recent_items(table, 8)
                import pandas as pd
                df = pd.DataFrame(rows, columns=["Text/Title", "Date", "Region", "Sentiment"])
                df["Text/Title"] = df["Text/Title"].apply(lambda x: str(x or "")[:80])
                st.dataframe(df, use_container_width=True, hide_index=True)


# ══════════════════════════════════════════════════════════════════════════════
# PAGE 3 — Run Collectors
# ══════════════════════════════════════════════════════════════════════════════

elif "Collectors" in page:
    st.markdown("<div class='section-title'>⚙️ Run Data Collectors</div>", unsafe_allow_html=True)
    st.markdown("<div class='section-sub'>Trigger any collector to fetch fresh political data into the database.</div>", unsafe_allow_html=True)

    if not db_exists():
        st.warning("⚠️ Database not initialized. Go to Database tab first.")
        st.stop()

    MODULE_MAP = {
        "news":      "collectors.news_scraper",
        "youtube":   "collectors.youtube_collector",
        "twitter":   "collectors.twitter_collector",
        "facebook":  "collectors.facebook_scraper",
        "public":    "collectors.public_data_downloader",
        "sentiment": "processors.sentiment_analyzer",
    }

    # ── Run ALL button ──
    st.markdown("#### 🚀 Run All Collectors")
    if st.button("▶ Run All  (news → youtube → twitter → facebook → public → sentiment)", use_container_width=True):
        overall_log = ""
        prog = st.progress(0)
        status_area = st.empty()
        for i, name in enumerate(COLLECTORS):
            status_area.markdown(f"⏳ Running **{name}** collector…")
            ok, log = capture_run(MODULE_MAP[name])
            overall_log += f"\n\n{'─'*40}\n[{name.upper()}]\n{log}"
            prog.progress((i + 1) / len(COLLECTORS))
            time.sleep(0.2)
        status_area.success("✅ All collectors finished!")
        st.markdown(f"<div class='log-box'>{overall_log}</div>", unsafe_allow_html=True)
        st.rerun()

    st.markdown("---")
    st.markdown("#### 🎯 Run Individual Collector")

    cols = st.columns(3)
    for idx, name in enumerate(COLLECTORS):
        meta_c = COLLECTOR_META[name]
        with cols[idx % 3]:
            st.markdown(f"""
            <div style='background:rgba(30,41,59,.5); border:1px solid rgba(99,179,237,.15);
                        border-radius:12px; padding:16px; margin-bottom:12px; text-align:center;'>
                <div style='font-size:1.8rem;'>{meta_c['icon']}</div>
                <div style='color:#e2e8f0; font-weight:600; font-size:.9rem;'>{meta_c['label']}</div>
                <div style='color:{meta_c['color']}; font-size:.75rem; margin-top:2px;'>{name}</div>
            </div>""", unsafe_allow_html=True)
            if st.button(f"▶ Run {name}", key=f"run_{name}", use_container_width=True):
                with st.spinner(f"Running {name}…"):
                    ok, log = capture_run(MODULE_MAP[name])
                if ok:
                    st.success(f"✅ {meta_c['label']} completed!")
                else:
                    st.error(f"❌ {meta_c['label']} failed!")
                with st.expander("📋 Output Log", expanded=True):
                    st.markdown(f"<div class='log-box'>{log[:3000]}</div>", unsafe_allow_html=True)


# ══════════════════════════════════════════════════════════════════════════════
# PAGE 4 — Search Data
# ══════════════════════════════════════════════════════════════════════════════

elif "Search" in page:
    st.markdown("<div class='section-title'>🔍 Search Political Intelligence</div>", unsafe_allow_html=True)

    if not db_exists():
        st.warning("⚠️ Database not found.")
        st.stop()

    col1, col2, col3, col4 = st.columns([3, 1, 1, 1])
    with col1:
        query = st.text_input("🔍 Search query", placeholder="e.g. water supply, BJP, infrastructure…", label_visibility="collapsed")
    with col2:
        source = st.selectbox("Source", ["all", "news", "tweets", "youtube", "facebook"], label_visibility="collapsed")
    with col3:
        region = st.selectbox("Region", ["all", "ahmedabad", "sanand", "gujarat_general"], label_visibility="collapsed")
    with col4:
        limit = st.selectbox("Limit", [20, 50, 100], label_visibility="collapsed")

    if query:
        results = search_db(query, source, region, limit)
        st.markdown(f"""<div style='color:#64748b; font-size:.85rem; margin-bottom:12px;'>Found <b style='color:#63b3ed;'>{len(results)}</b> results for <b style='color:#e2e8f0;'>"{query}"</b></div>""", unsafe_allow_html=True)

        SRC_COLOR = {"twitter":"#1d9bf0","news":"#22c55e","youtube":"#ef4444","facebook":"#4267b2"}

        for r in results:
            src_col = SRC_COLOR.get(r["src"], "#64748b")
            title   = str(r.get("title") or "")[:70]
            text    = str(r.get("text")  or "")[:200]
            url     = r.get("url", "")

            st.markdown(f"""
            <div class='result-card'>
                <div style='display:flex; align-items:center; gap:10px; margin-bottom:6px;'>
                    <span class='badge' style='background:rgba(30,41,59,.9); color:{src_col}; border-color:{src_col}40;'>
                        {r['src'].upper()}
                    </span>
                    <span style='color:#e2e8f0; font-weight:600; font-size:.9rem;'>{title}</span>
                </div>
                <div style='color:#94a3b8; font-size:.82rem; line-height:1.5;'>{text}…</div>
                <div style='color:#475569; font-size:.75rem; margin-top:8px;'>
                    📍 {r.get('region') or '—'} &nbsp;·&nbsp; 🕐 {str(r.get('date') or '')[:19]}
                    {('&nbsp;·&nbsp; <a href="' + url + '" style="color:#63b3ed;" target="_blank">🔗 Link</a>') if url else ''}
                </div>
            </div>""", unsafe_allow_html=True)
    else:
        st.markdown("""
        <div style='text-align:center; padding:60px 0; color:#374151;'>
            <div style='font-size:3rem;'>🔍</div>
            <div style='font-size:1rem; margin-top:12px;'>Enter a keyword to search the intelligence database</div>
            <div style='font-size:.8rem; margin-top:6px; color:#1f2937;'>Searches across tweets, news articles, YouTube videos & Facebook posts</div>
        </div>""", unsafe_allow_html=True)


# ══════════════════════════════════════════════════════════════════════════════
# PAGE 5 — Trending Topics
# ══════════════════════════════════════════════════════════════════════════════

elif "Trending" in page:
    st.markdown("<div class='section-title'>🔥 Trending Political Topics</div>", unsafe_allow_html=True)

    colA, colB = st.columns([1, 1])
    with colA:
        region_t = st.selectbox("Region", ["all", "ahmedabad", "sanand", "gujarat_general"])
    with colB:
        top_n_t = st.slider("Number of topics", 5, 50, 20)

    trends = get_trending(region_t, top_n_t)

    if not trends:
        st.info("No data available. Run some collectors first.")
    else:
        max_c = trends[0][1]
        # Split into two columns
        half = len(trends) // 2
        left_trends  = trends[:half] if half else trends
        right_trends = trends[half:] if half < len(trends) else []

        c_l, c_r = st.columns(2)
        for col, chunk in [(c_l, left_trends), (c_r, right_trends)]:
            with col:
                for i, (word, count) in enumerate(chunk, 1 if col == c_l else half + 1):
                    bar = int(count / max_c * 100)
                    medal = "🥇" if i == 1 else "🥈" if i == 2 else "🥉" if i == 3 else f"#{i}"
                    st.markdown(f"""
                    <div style='background:rgba(15,23,42,.7); border:1px solid rgba(99,179,237,.1);
                                border-radius:10px; padding:10px 14px; margin-bottom:8px;'>
                        <div style='display:flex; justify-content:space-between; align-items:center; margin-bottom:5px;'>
                            <span style='color:#e2e8f0; font-weight:600;'>{medal} &nbsp; {word}</span>
                            <span style='color:#63b3ed; font-size:.82rem;'>{count} mentions</span>
                        </div>
                        <div style='background:rgba(30,41,59,.8); border-radius:4px; height:5px;'>
                            <div style='width:{bar}%; background:linear-gradient(90deg,#2563eb,#7c3aed); height:5px; border-radius:4px;'></div>
                        </div>
                    </div>""", unsafe_allow_html=True)

        # Word cloud style display
        st.markdown("---")
        st.markdown("<div style='color:#64748b; font-size:.85rem; margin-bottom:10px;'>All topics at a glance:</div>", unsafe_allow_html=True)
        cloud_html = " &nbsp; ".join(
            f"<span style='color:hsl({(i*37)%360},70%,65%); font-size:{max(0.7, min(1.8, count/max_c*1.8)):.2f}rem; font-weight:600;'>{word}</span>"
            for i, (word, count) in enumerate(trends)
        )
        st.markdown(f"<div style='line-height:2.5; text-align:center; background:rgba(10,10,20,.5); padding:20px; border-radius:12px;'>{cloud_html}</div>", unsafe_allow_html=True)


# ══════════════════════════════════════════════════════════════════════════════
# PAGE 6 — Sentiment Lab
# ══════════════════════════════════════════════════════════════════════════════

elif "Sentiment" in page:
    st.markdown("<div class='section-title'>🧠 Sentiment Analysis Lab</div>", unsafe_allow_html=True)
    st.markdown("<div class='section-sub'>Analyze custom text or run batch sentiment on the full database.</div>", unsafe_allow_html=True)

    provider = os.getenv("LLM_PROVIDER", "vader")
    st.markdown(f"**Active Provider:** `{provider}`")

    # ── Custom text analysis ──
    st.markdown("#### ✍️ Analyze Custom Text")
    custom_text = st.text_area(
        "Enter political text (tweet, news headline, statement…)",
        height=120,
        placeholder="e.g. 'The BJP government has failed to deliver on promises of clean water for Ahmedabad residents…'"
    )

    if st.button("🔬 Analyze Sentiment", use_container_width=False):
        if not custom_text.strip():
            st.warning("Please enter some text.")
        else:
            with st.spinner("Analyzing…"):
                try:
                    from processors.sentiment_analyzer import analyze_sentiment
                    result = analyze_sentiment(custom_text)
                    label    = result.get("label", "neutral").lower()
                    score    = result.get("score")
                    prov_used = result.get("provider", provider)

                    COLOR = {"positive":"#10b981","negative":"#ef4444","neutral":"#f59e0b"}
                    EMOJI = {"positive":"😊","negative":"😠","neutral":"😐"}
                    col_r, col_info = st.columns([1, 2])
                    with col_r:
                        st.markdown(f"""
                        <div style='background:linear-gradient(135deg,rgba(30,41,59,.9),rgba(15,23,42,.95));
                                    border:2px solid {COLOR.get(label,'#475569')}; border-radius:16px;
                                    padding:24px; text-align:center;'>
                            <div style='font-size:2.5rem;'>{EMOJI.get(label,'❓')}</div>
                            <div style='font-size:1.6rem; font-weight:800; color:{COLOR.get(label,'#e2e8f0')}; margin-top:8px;'>
                                {label.upper()}
                            </div>
                            {("<div style='color:#64748b; font-size:.85rem; margin-top:6px;'>Score: " + f"{score:.3f}" + "</div>") if score is not None else ""}
                            <div style='color:#475569; font-size:.75rem; margin-top:4px;'>via {prov_used}</div>
                        </div>""", unsafe_allow_html=True)
                    with col_info:
                        st.markdown(f"""
                        <div style='color:#94a3b8; font-size:.9rem; line-height:1.7; padding:12px 0;'>
                            <b style='color:#e2e8f0;'>Text analyzed:</b><br>
                            <i style='color:#64748b;'>{custom_text[:300]}</i>
                        </div>""", unsafe_allow_html=True)
                except Exception as e:
                    st.error(f"Analysis failed: {e}")

    st.markdown("---")

    # ── Batch DB sentiment ──
    st.markdown("#### 🔄 Batch Sentiment — Run on Database")
    st.markdown("<div style='color:#64748b; font-size:.84rem;'>Updates all rows in the database that don't have sentiment labels yet.</div>", unsafe_allow_html=True)

    if not db_exists():
        st.warning("Database not initialized.")
    else:
        stats = get_table_stats()
        unanalyzed = sum(v.get("total",0) - v.get("analyzed",0) for v in stats.values())
        st.markdown(f"<div style='color:#f59e0b; font-size:.9rem; margin:8px 0;'>⚡ {unanalyzed:,} rows pending sentiment analysis</div>", unsafe_allow_html=True)

        if st.button("🚀 Run Batch Sentiment Analysis", use_container_width=False):
            with st.spinner("Running sentiment analysis on all tables…"):
                ok, log = capture_run("processors.sentiment_analyzer")
            if ok:
                st.success("✅ Batch sentiment analysis complete!")
            else:
                st.error("❌ Something went wrong.")
            with st.expander("📋 Output Log"):
                st.markdown(f"<div class='log-box'>{log[:3000]}</div>", unsafe_allow_html=True)
            st.rerun()

    # ── Sentiment distribution ──
    if db_exists():
        st.markdown("---")
        st.markdown("#### 📊 Sentiment Distribution")
        import pandas as pd
        for table in TABLES:
            try:
                conn = get_connection()
                cur  = conn.cursor()
                cur.execute(f"SELECT sentiment, count(*) FROM {table} WHERE sentiment IS NOT NULL GROUP BY sentiment")
                rows = cur.fetchall()
                conn.close()
                if rows:
                    df = pd.DataFrame(rows, columns=["Sentiment", "Count"])
                    total_s = df["Count"].sum()
                    st.markdown(f"**{TABLES[table]['icon']} {table}**")
                    for _, row in df.iterrows():
                        lbl = str(row["Sentiment"]).lower()
                        pct = row["Count"] / total_s * 100
                        st.markdown(f"{sentiment_badge(lbl)} — {row['Count']:,} ({pct:.1f}%)", unsafe_allow_html=True)
                        st.progress(pct / 100)
            except:
                pass


# ══════════════════════════════════════════════════════════════════════════════
# PAGE 3.5 — Regional Intelligence (Dynamic Search)
# ══════════════════════════════════════════════════════════════════════════════

elif "Regional Intelligence" in page:
    st.markdown("<div class='section-title'>📍 Regional Intelligence (Dynamic Scrape)</div>", unsafe_allow_html=True)
    st.markdown("<div class='section-sub'>Enter any city or area name to perform a localized crawl across YouTube, Twitter, and News.</div>", unsafe_allow_html=True)

    if not db_exists():
        st.warning("⚠️ Database not found.")
        st.stop()

    with st.form("custom_area_form"):
        area_input = st.text_input("Target City/Area Name", placeholder="e.g. Dholka, Bavla, Rajkot, Surat...")
        submitted = st.form_submit_button("🔍 Start Local Intelligence Scrape")

    if submitted:
        if not area_input.strip():
            st.warning("Please enter a city or area name.")
        else:
            st.info(f"🚀 Initializing intelligence gathering for: **{area_input}**")
            
            # Using progress bars for each stage
            status_container = st.empty()
            
            try:
                # 1. YouTube
                status_container.markdown("⏳ Step 1/4: Scraping **YouTube** videos...")
                from collectors.youtube_collector import run_custom as run_yt
                yt_count = run_yt(area_input)
                st.write(f"✅ YouTube: Found {yt_count} videos.")

                # 2. Twitter
                status_container.markdown("⏳ Step 2/4: Scraping **Twitter** trends...")
                from collectors.twitter_collector import run_custom as run_tw
                tw_count = run_tw(area_input)
                st.write(f"✅ Twitter: Found {tw_count} tweets.")

                # 3. News
                status_container.markdown("⏳ Step 3/4: Filtering **News** RSS feeds...")
                from collectors.news_scraper import run_custom as run_ns
                ns_count = run_ns(area_input)
                st.write(f"✅ News: Found {ns_count} related articles.")

                # 4. Sentiment
                status_container.markdown("⏳ Step 4/4: Running **Sentiment Analysis** on new data...")
                from processors.sentiment_analyzer import run as run_sent
                run_sent()
                st.write("✅ Sentiment Analysis: Complete.")

                status_container.success(f"🎊 Local intelligence gathered for **{area_input}**! Check the Database or Search tabs to view results.")
                
                if st.button("🔄 Refresh Dashboard"):
                    st.rerun()

            except Exception as e:
                st.error(f"❌ Scrape failed: {e}")
                st.exception(e)

# ══════════════════════════════════════════════════════════════════════════════
# PAGE 7 — News Summary
# ══════════════════════════════════════════════════════════════════════════════

elif "News" in page:
    st.markdown("<div class='section-title'>📰 News Summary (LLM-Powered)</div>", unsafe_allow_html=True)

    if not db_exists():
        st.warning("⚠️ Database not found.")
        st.stop()

    provider = os.getenv("LLM_PROVIDER", "vader")
    region_n = st.selectbox("Region", ["ahmedabad", "sanand", "gujarat_general", "all"])
    limit_n  = st.slider("Articles to summarize", 5, 30, 10)

    if st.button("📝 Generate Summary", use_container_width=False):
        conn = get_connection()
        cur  = conn.cursor()
        rf   = "" if region_n == "all" else "AND region = ?"
        rp   = ([region_n] if region_n != "all" else []) + [limit_n]
        cur.execute(f"SELECT title, content, source, published_at, url FROM news_articles WHERE title IS NOT NULL {rf} ORDER BY id DESC LIMIT ?", rp)
        rows = cur.fetchall()
        conn.close()

        if not rows:
            st.warning(f"No news articles found for region: {region_n}")
        else:
            articles_text = "\n\n".join(f"[{r[2]}] {r[0]}\n{(r[1] or '')[:300]}" for r in rows)

            if provider == "openai" and os.getenv("OPENAI_API_KEY"):
                with st.spinner("Summarizing with OpenAI GPT…"):
                    try:
                        import openai
                        client = openai.OpenAI(api_key=os.getenv("OPENAI_API_KEY"))
                        resp = client.chat.completions.create(
                            model="gpt-4o-mini",
                            messages=[
                                {"role":"system","content":f"You are a political analyst for {region_n}, Gujarat. Summarize into 5-7 key political insights. Identify main issues, sentiment, parties mentioned, and strategic implications."},
                                {"role":"user","content":articles_text},
                            ],
                            max_tokens=800,
                        )
                        summary = resp.choices[0].message.content
                        st.markdown(f"""
                        <div style='background:rgba(15,23,42,.8); border:1px solid rgba(99,179,237,.2);
                                    border-radius:12px; padding:24px; line-height:1.8; color:#e2e8f0;'>
                            {summary}
                        </div>""", unsafe_allow_html=True)
                    except Exception as e:
                        st.error(f"OpenAI error: {e}")

            elif provider == "gemini" and os.getenv("GEMINI_API_KEY"):
                with st.spinner("Summarizing with Gemini…"):
                    try:
                        import google.generativeai as genai
                        genai.configure(api_key=os.getenv("GEMINI_API_KEY"))
                        model = genai.GenerativeModel("gemini-1.5-flash")
                        resp  = model.generate_content(
                            f"You are a political analyst for {region_n}, Gujarat. "
                            f"Summarize these news into 5-7 key political insights:\n\n{articles_text}"
                        )
                        st.markdown(f"""
                        <div style='background:rgba(15,23,42,.8); border:1px solid rgba(99,179,237,.2);
                                    border-radius:12px; padding:24px; line-height:1.8; color:#e2e8f0;'>
                            {resp.text}
                        </div>""", unsafe_allow_html=True)
                    except Exception as e:
                        st.error(f"Gemini error: {e}")

            else:
                # Fallback: structured list
                st.markdown(f"<div style='color:#f59e0b; font-size:.84rem; margin-bottom:12px;'>💡 No LLM configured — showing raw list. Set <code>LLM_PROVIDER=openai</code> or <code>gemini</code> in .env for AI summaries.</div>", unsafe_allow_html=True)
                for i, (title, content, source, pub_at, url) in enumerate(rows, 1):
                    st.markdown(f"""
                    <div class='result-card'>
                        <div style='display:flex; gap:10px; align-items:center;'>
                            <span class='badge badge-green'>{source or 'unknown'}</span>
                            <span style='color:#e2e8f0; font-weight:600;'>{str(title or '')[:100]}</span>
                        </div>
                        <div style='color:#64748b; font-size:.78rem; margin-top:6px;'>{str(pub_at or '')[:19]}</div>
                        <div style='color:#94a3b8; font-size:.82rem; margin-top:6px;'>{str(content or '')[:200]}…</div>
                        {"<a href='" + url + "' style='color:#63b3ed; font-size:.78rem;' target='_blank'>🔗 Read more</a>" if url else ""}
                    </div>""", unsafe_allow_html=True)
    