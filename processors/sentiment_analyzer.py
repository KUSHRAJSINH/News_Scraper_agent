# processors/sentiment_analyzer.py
# Multilingual sentiment for Hindi, Gujarati, and English political text.
# LLM Integration Point #1 — can swap the HF model for an OpenAI/Gemini call.
# Requires: pip install vaderSentiment transformers torch

import os
import sys
import json
import logging
from pathlib import Path
from dotenv import load_dotenv

load_dotenv()
logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
logger = logging.getLogger(__name__)

sys.path.insert(0, str(Path(__file__).parent.parent))
from database.db import get_connection

# ─── LLM Provider Config ──────────────────────────────────────────────────────
# Set LLM_PROVIDER in .env to switch backends:
#   "hf"     → HuggingFace transformers (local, free, needs GPU/RAM)
#   "openai" → OpenAI API (GPT-4o-mini, paid but cheap)
#   "gemini" → Google Gemini API (free tier available)
#   "vader"  → VADER (English only, fully offline, no model download)

LLM_PROVIDER   = os.getenv("LLM_PROVIDER", "vader")      # default: no download required
OPENAI_API_KEY = os.getenv("OPENAI_API_KEY", "")
GEMINI_API_KEY = os.getenv("GEMINI_API_KEY", "")
GROQ_API_KEY   = os.getenv("GROQ_API_KEY", "")
OPEN_ROUTER_API_KEY = os.getenv("OPEN_ROUTER_API_KEY", "")

# ─── VADER (English, no model download) ───────────────────────────────────────

_vader = None
def get_vader():
    global _vader
    if _vader is None:
        from vaderSentiment.vaderSentiment import SentimentIntensityAnalyzer
        _vader = SentimentIntensityAnalyzer()
    return _vader

def analyze_vader(text: str) -> dict:
    scores = get_vader().polarity_scores(text)
    compound = scores["compound"]
    if compound >= 0.05:
        label = "positive"
    elif compound <= -0.05:
        label = "negative"
    else:
        label = "neutral"
    return {"label": label, "score": compound, "provider": "vader"}

# ─── HuggingFace (multilingual, local) ────────────────────────────────────────

_hf_pipeline = None
def get_hf_pipeline():
    global _hf_pipeline
    if _hf_pipeline is None:
        from transformers import pipeline
        logger.info("Loading HuggingFace sentiment model (first run may be slow)...")
        _hf_pipeline = pipeline(
            "text-classification",
            model="cardiffnlp/twitter-xlm-roberta-base-sentiment",
            top_k=1,
        )
    return _hf_pipeline

def analyze_hf(text: str) -> dict:
    pipe    = get_hf_pipeline()
    result  = pipe(text[:512])[0][0]    # truncate to model max length
    label   = result["label"].lower()   # "positive" / "neutral" / "negative"
    score   = round(result["score"], 4)
    return {"label": label, "score": score, "provider": "hf_xlm_roberta"}

# ─── OpenAI (GPT-4o-mini — cheap, good for Gujarati/Hindi) ───────────────────
# def analyze_openai(text: str) -> dict:
#     import openai
#     client = openai.OpenAI(api_key=OPENAI_API_KEY)
#     resp = client.chat.completions.create(
#         model="gpt-4o-mini",
#         messages=[
#             {"role": "system", "content": (
#                 "Analyze the political sentiment of the following text. "
#                 "Respond with exactly one word: positive, negative, or neutral."
#             )},
#             {"role": "user", "content": text[:1000]},
#         ],
#         max_tokens=5,
#         temperature=0,
#     )
#     label = resp.choices[0].message.content.strip().lower()
#     if label not in ("positive", "negative", "neutral"):
#         label = "neutral"
#     return {"label": label, "score": None, "provider": "openai_gpt4o_mini"}
# ─── OpenAI (GPT-4o-mini — cheap, good for Gujarati/Hindi) ───────────────────

# ─── Gemini (free tier) ───────────────────────────────────────────────────────
# def analyze_gemini(text: str) -> dict:
#     import google.generativeai as genai
#     genai.configure(api_key=GEMINI_API_KEY)
#     model = genai.GenerativeModel("gemini-1.5-flash")
#     resp  = model.generate_content(
#         f"Analyze the political sentiment. Reply with exactly: positive, negative, or neutral.\n\nText: {text[:1000]}"
#     )
#     label = resp.text.strip().lower()
#     if label not in ("positive", "negative", "neutral"):
#         label = "neutral"
#     return {"label": label, "score": None, "provider": "gemini_flash"}

# ─── Groq (llama-3.1-8b-instant) ─────────────────────────────────────────────

def analyze_groq(text: str) -> dict:
    import requests
    if not GROQ_API_KEY:
        return {"label": "neutral", "score": None, "provider": "groq_failed_no_key"}
    
    try:
        url = "https://api.groq.com/openai/v1/chat/completions"
        headers = {
            "Authorization": f"Bearer {GROQ_API_KEY}",
            "Content-Type": "application/json"
        }
        payload = {
            "model": "llama-3.1-8b-instant",
            "messages": [
                {"role": "system", "content": "Analyze the political sentiment. Respond with exactly one word: positive, negative, or neutral."},
                {"role": "user", "content": text[:1000]}
            ],
            "temperature": 0,
            "max_tokens": 10
        }
        resp = requests.post(url, headers=headers, json=payload, timeout=10)
        data = resp.json()
        label = data["choices"][0]["message"]["content"].strip().lower()
        if label not in ("positive", "negative", "neutral"):
            label = "neutral"
        return {"label": label, "score": None, "provider": "groq_llama31"}
    except Exception as e:
        logger.error(f"Groq error: {e}")
        return {"label": "neutral", "score": None, "provider": "groq_error"}

# ─── OpenRouter (arcee-ai/trinity-large-preview:free) ────────────────────────

def analyze_openrouter(text: str) -> dict:
    import requests
    if not OPEN_ROUTER_API_KEY:
        return {"label": "neutral", "score": None, "provider": "openrouter_failed_no_key"}

    try:
        url = "https://openrouter.ai/api/v1/chat/completions"
        headers = {
            "Authorization": f"Bearer {OPEN_ROUTER_API_KEY}",
            "Content-Type": "application/json"
        }
        payload = {
            "model": "arcee-ai/trinity-large-preview:free",
            "messages": [
                {"role": "system", "content": "Analyze the political sentiment. Respond with exactly one word: positive, negative, or neutral."},
                {"role": "user", "content": text[:1000]}
            ],
            "temperature": 0,
            "max_tokens": 10
        }
        resp = requests.post(url, headers=headers, json=payload, timeout=10)
        data = resp.json()
        label = data["choices"][0]["message"]["content"].strip().lower()
        if label not in ("positive", "negative", "neutral"):
            label = "neutral"
        return {"label": label, "score": None, "provider": "openrouter_trinity"}
    except Exception as e:
        logger.error(f"OpenRouter error: {e}")
        return {"label": "neutral", "score": None, "provider": "openrouter_error"}

# ─── Router ───────────────────────────────────────────────────────────────────

def analyze_sentiment(text: str) -> dict:
    """Route to the configured LLM provider."""
    if not text or len(text.strip()) < 5:
        return {"label": "neutral", "score": 0.0, "provider": "none"}
    try:
        
        if LLM_PROVIDER == "openrouter" and OPEN_ROUTER_API_KEY:
            return analyze_openrouter(text)
        elif LLM_PROVIDER == "groq" and GROQ_API_KEY:
            return analyze_groq(text)
        elif LLM_PROVIDER == "hf":
            return analyze_hf(text)
        else:
            return analyze_vader(text)
    except Exception as e:
        logger.error(f"Sentiment analysis error: {e} — falling back to VADER")
        return analyze_vader(text)

# ─── Batch Processor ──────────────────────────────────────────────────────────

def run_batch(table: str, text_column: str, batch_size: int = 500):
    """Update sentiment column for all rows that haven't been analyzed yet."""
    conn   = get_connection()
    cursor = conn.cursor()

    cursor.execute(f"SELECT id, {text_column} FROM {table} WHERE sentiment IS NULL LIMIT ?", (batch_size,))
    rows = cursor.fetchall()
    logger.info(f"[{table}] Processing {len(rows)} rows for sentiment...")

    updated = 0
    for row_id, text in rows:
        if not text:
            continue
        result = analyze_sentiment(text)
        cursor.execute(
            f"UPDATE {table} SET sentiment = ? WHERE id = ?",
            (result["label"], row_id)
        )
        updated += 1

    conn.commit()
    conn.close()
    logger.info(f"[{table}] Updated sentiment for {updated} rows (provider: {LLM_PROVIDER})")

def run():
    """Run sentiment analysis on all tables."""
    logger.info(f"=== Sentiment Analyzer (provider: {LLM_PROVIDER}) ===")
    run_batch("tweets",         "text")
    run_batch("news_articles",  "content")
    # YouTube fallback: use title if transcript is empty/null
    run_batch("youtube_videos", "COALESCE(NULLIF(transcript, ''), title)")
    run_batch("facebook_posts", "text")
    logger.info("Sentiment analysis complete.")

if __name__ == "__main__":
    run()
