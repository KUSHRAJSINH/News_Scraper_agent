# collectors/public_data_downloader.py
# Requires: pip install requests pdfplumber pandas python-dotenv tqdm
#
# Downloads public datasets from:
#   • ECI (Election Commission of India) — election result PDFs/CSVs
#   • data.gov.in — census, demographic, infrastructure datasets
#   • Open government data portals for Gujarat

import os
import sys
import csv
import logging
import time
from datetime import datetime, timezone
from pathlib import Path
from urllib.parse import urlparse

import requests
import pandas as pd
from tqdm import tqdm
from dotenv import load_dotenv

load_dotenv()
logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
logger = logging.getLogger(__name__)

# ─── Config ────────────────────────────────────────────────────────────────────

OUTPUT_DIR = Path(os.getenv("DATA_DIR", "data/raw")) / "public"

HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (compatible; PoliticalDataBot/1.0; +research-use)"
    )
}

# ─── Dataset Registry ──────────────────────────────────────────────────────────
# Each entry:
#   url        ─ direct download link
#   filename   ─ filename to save under OUTPUT_DIR / category /
#   category   ─ subdirectory name
#   type       ─ "csv" | "pdf" | "json"
#   notes      ─ human-readable description

DATASETS = [
    # ── ECI Results (Gujarat 2022 State Assembly) ──────────────────────────────
    {
        "url": "https://results.eci.gov.in/Result2022/partywiseresult-S07.htm",
        "filename":   "eci_gujarat_assembly_2022_partywise.html",
        "category":   "eci",
        "type":       "html",
        "notes":      "ECI Gujarat 2022 Assembly party-wise results page",
    },
    {
        "url": "https://affidavit.eci.gov.in/",
        "filename":   "eci_affidavit_portal.html",
        "category":   "eci",
        "type":       "html",
        "notes":      "ECI candidate affidavit portal for financial disclosures",
    },

    # ── data.gov.in — Census & Demographics ───────────────────────────────────
    {
        "url": (
            "https://api.data.gov.in/resource/4a819e2c-0c25-4e84-a6c6-0b8f99d13fe2"
            "?api-key=579b464db66ec23bdd000001cdd3946e44ce4aae38d975ea6dfdb9a1"
            "&format=csv&limit=500&filters%5Bstate%5D=Gujarat"
        ),
        "filename":   "census_2011_gujarat_population.csv",
        "category":   "census",
        "type":       "csv",
        "notes":      "Census 2011 population data filtered to Gujarat",
    },
    {
        "url": (
            "https://api.data.gov.in/resource/a823c2e5-fb44-4f3a-a5e0-b3dcf9c6b7d2"
            "?api-key=579b464db66ec23bdd000001cdd3946e44ce4aae38d975ea6dfdb9a1"
            "&format=csv&limit=500&filters%5Bstate%5D=Gujarat"
        ),
        "filename":   "gujarat_literacy_rate_2011.csv",
        "category":   "census",
        "type":       "csv",
        "notes":      "Literacy rate by district, Gujarat, Census 2011",
    },
    {
        "url": (
            "https://api.data.gov.in/resource/6176ee09-3d56-4a3b-8115-21841a1d3fb5"
            "?api-key=579b464db66ec23bdd000001cdd3946e44ce4aae38d975ea6dfdb9a1"
            "&format=csv&limit=200&filters%5Bstate_ut%5D=Gujarat"
        ),
        "filename":   "gujarat_voter_registration_2022.csv",
        "category":   "election",
        "type":       "csv",
        "notes":      "Voter registration data 2022, Gujarat",
    },

    # ── Ahmedabad / Sanand specific ───────────────────────────────────────────
    {
        "url": (
            "https://api.data.gov.in/resource/7c8a3aa7-6b9f-4f88-a87a-f8c3f1e2bb04"
            "?api-key=579b464db66ec23bdd000001cdd3946e44ce4aae38d975ea6dfdb9a1"
            "&format=csv&limit=200&filters%5Bdistrict%5D=Ahmedabad"
        ),
        "filename":   "ahmedabad_infrastructure_projects.csv",
        "category":   "infrastructure",
        "type":       "csv",
        "notes":      "Government infrastructure projects in Ahmedabad district",
    },
    {
        "url": (
            "https://api.data.gov.in/resource/f1b0a70e-c3a2-4f25-b6e3-ea54d1c95820"
            "?api-key=579b464db66ec23bdd000001cdd3946e44ce4aae38d975ea6dfdb9a1"
            "&format=csv&limit=200&filters%5Bdistrict%5D=Ahmedabad"
        ),
        "filename":   "ahmedabad_crime_stats.csv",
        "category":   "crime",
        "type":       "csv",
        "notes":      "NCRB crime statistics for Ahmedabad",
    },
]

# ─── Downloader ────────────────────────────────────────────────────────────────

def download_file(url: str, dest_path: Path, timeout: int = 30) -> bool:
    """Stream-download a file with a progress bar. Returns True on success."""
    try:
        resp = requests.get(url, headers=HEADERS, stream=True, timeout=timeout)
        resp.raise_for_status()

        total = int(resp.headers.get("content-length", 0))
        dest_path.parent.mkdir(parents=True, exist_ok=True)

        with open(dest_path, "wb") as f, tqdm(
            desc=dest_path.name,
            total=total,
            unit="iB",
            unit_scale=True,
            unit_divisor=1024,
            leave=False,
        ) as bar:
            for chunk in resp.iter_content(chunk_size=8192):
                f.write(chunk)
                bar.update(len(chunk))

        logger.info(f"✓ Downloaded: {dest_path}")
        return True

    except requests.HTTPError as e:
        logger.warning(f"HTTP error {e.response.status_code} for {url}")
    except Exception as e:
        logger.error(f"Download failed for {url}: {e}")
    return False

# ─── PDF to CSV converter ──────────────────────────────────────────────────────

def pdf_to_csv(pdf_path: Path) -> Path | None:
    """Extract tables from a PDF and save as CSV. Requires pdfplumber."""
    try:
        import pdfplumber
    except ImportError:
        logger.warning("pdfplumber not installed — skipping PDF→CSV conversion")
        return None

    csv_path = pdf_path.with_suffix(".csv")
    rows = []

    try:
        with pdfplumber.open(pdf_path) as pdf:
            for page in pdf.pages:
                tables = page.extract_tables()
                for table in tables:
                    rows.extend(table)

        if not rows:
            logger.warning(f"No tables found in {pdf_path.name}")
            return None

        with open(csv_path, "w", newline="", encoding="utf-8") as f:
            writer = csv.writer(f)
            writer.writerows(rows)

        logger.info(f"PDF → CSV: {csv_path}")
        return csv_path

    except Exception as e:
        logger.error(f"PDF extraction failed for {pdf_path}: {e}")
        return None

# ─── CSV preview ───────────────────────────────────────────────────────────────

def preview_csv(csv_path: Path, rows: int = 5):
    """Log a quick preview of a downloaded CSV."""
    try:
        df = pd.read_csv(csv_path, nrows=rows)
        logger.info(f"Preview of {csv_path.name}:\n{df.to_string(index=False)}")
    except Exception as e:
        logger.warning(f"Could not preview CSV {csv_path}: {e}")

# ─── Entry Point ───────────────────────────────────────────────────────────────

def run():
    logger.info(f"=== Public Data Downloader — {len(DATASETS)} datasets ===")
    downloaded = 0
    failed     = 0

    for dataset in DATASETS:
        url      = dataset["url"]
        filename = dataset["filename"]
        category = dataset["category"]
        dtype    = dataset["type"]
        notes    = dataset.get("notes", "")

        dest_path = OUTPUT_DIR / category / filename

        if dest_path.exists():
            logger.info(f"Already exists, skipping: {filename}")
            downloaded += 1
            continue

        logger.info(f"Downloading [{category}] {filename}")
        logger.debug(f"  Notes: {notes}")

        ok = download_file(url, dest_path)

        if ok:
            downloaded += 1
            if dtype == "pdf":
                pdf_to_csv(dest_path)
            elif dtype == "csv":
                preview_csv(dest_path)
        else:
            failed += 1

        time.sleep(2)   # polite delay between requests

    logger.info(
        f"\nDownload complete. Success: {downloaded}, Failed: {failed}"
    )
    logger.info(f"Files saved to: {OUTPUT_DIR.resolve()}")

if __name__ == "__main__":
    run()
