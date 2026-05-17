#!/usr/bin/env python3
"""
iShares ETF Holdings Pipeline  —  v2.0
========================================
Downloads iShares ETF holdings CSVs and loads them into SQLite.

Key improvements over original:
  • Direct URL construction from Product_URL  — no HTML scraping
  • Concurrent downloads via ThreadPoolExecutor
  • Dynamic CSV header/footer detection       — no hardcoded skiprows
  • Quantity → Shares mapping as per DB schema
  • Batch upserts (INSERT OR REPLACE)         — no delete/re-insert cycle
  • SQLite WAL mode + tuned PRAGMAs for speed
  • Rotating log files + console output
  • Retry with exponential backoff
  • Cross-platform (Windows / Mac / Linux)

Usage:
  python pipeline.py                             # all tickers, yesterday
  python pipeline.py --date 20260514             # specific date
  python pipeline.py --ticker IVV QQQ SPY        # specific tickers
  python pipeline.py --update-meta               # refresh ishare_meta table
  python pipeline.py --no-download               # reprocess existing CSVs
  python pipeline.py --workers 20                # tune concurrency
"""

# ─── Stdlib ────────────────────────────────────────────────────────────────────
import argparse
import io
import logging
import logging.handlers
import os
import re
import sqlite3
import sys
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime, timedelta
from pathlib import Path
from typing import List, Optional, Tuple

# ─── Third-party ───────────────────────────────────────────────────────────────
import pandas as pd
import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry


# ══════════════════════════════════════════════════════════════════════════════
# CONFIGURATION
# ══════════════════════════════════════════════════════════════════════════════

ROOT         = Path(__file__).parent
DOWNLOAD_DIR = ROOT / "Download"
DB_DIR       = ROOT / "db"
LOG_DIR      = ROOT / "Log"
DB_PATH      = DB_DIR / "meta.db"
TICKER_FILE  = ROOT / "Folder_Ishares.csv"

# iShares Ajax endpoint ID (same across all regional sites)
ISHARES_AJAX_ID = "1467271812594"

# CSV columns to keep (drop Currency, FX Rate, Market Currency, Accrual Date)
CSV_KEEP_COLS = [
    "Ticker", "Name", "Sector", "Asset Class",
    "Market Value", "Weight (%)", "Notional Value",
    "Quantity", "Price", "Location", "Exchange",
]

# CSV column → DB column rename (includes the Quantity→Shares mapping)
RENAME_MAP = {
    "Asset Class":    "Asset_Class",
    "Market Value":   "Market_Value",
    "Weight (%)":     "Weight",
    "Notional Value": "Notional_Value",
    "Quantity":       "Shares",   # ← requested: map CSV Quantity → DB Shares
}

# Columns to convert from comma-formatted strings to floats
NUMERIC_COLS = ["Market_Value", "Weight", "Notional_Value", "Shares", "Price"]

# DB table name
TABLE_NAME = "holdings"

# All DB columns in insert order (matches schema from screenshot)
DB_COLS = [
    "Etf_id", "Created_date", "Etf", "Ticker", "Name", "Sector",
    "Asset_Class", "Market_Value", "Weight", "Notional_Value",
    "Shares", "Price", "Location", "Exchange",
]

# HTTP settings
REQUEST_TIMEOUT = 30
MAX_RETRIES     = 3
BACKOFF_FACTOR  = 1.5
RATE_LIMIT_SLEEP = 0.3   # seconds between requests per worker

DEFAULT_WORKERS = 10


# ══════════════════════════════════════════════════════════════════════════════
# LOGGING
# ══════════════════════════════════════════════════════════════════════════════

def setup_logging(date: str) -> logging.Logger:
    LOG_DIR.mkdir(parents=True, exist_ok=True)
    logger = logging.getLogger("ishares_pipeline")
    logger.setLevel(logging.DEBUG)
    logger.handlers.clear()

    fmt = logging.Formatter(
        "%(levelname)-8s %(asctime)s  [%(funcName)s]  %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S",
    )

    # Per-run log file
    fh = logging.FileHandler(LOG_DIR / f"{date}_pipeline.log", encoding="utf-8")
    fh.setLevel(logging.DEBUG)
    fh.setFormatter(fmt)

    # Rotating archive (10 MB × 5 files)
    rh = logging.handlers.RotatingFileHandler(
        LOG_DIR / "pipeline_archive.log",
        maxBytes=10 * 1024 * 1024,
        backupCount=5,
        encoding="utf-8",
    )
    rh.setLevel(logging.INFO)
    rh.setFormatter(fmt)

    # Console — INFO and above
    ch = logging.StreamHandler(sys.stdout)
    ch.setLevel(logging.INFO)
    ch.setFormatter(fmt)

    logger.addHandler(fh)
    logger.addHandler(rh)
    logger.addHandler(ch)
    return logger


# ══════════════════════════════════════════════════════════════════════════════
# HTTP SESSION  (shared across thread pool)
# ══════════════════════════════════════════════════════════════════════════════

def build_session() -> requests.Session:
    session = requests.Session()
    retry = Retry(
        total=MAX_RETRIES,
        backoff_factor=BACKOFF_FACTOR,
        status_forcelist=[429, 500, 502, 503, 504],
        allowed_methods=["GET"],
    )
    adapter = HTTPAdapter(max_retries=retry)
    session.mount("https://", adapter)
    session.mount("http://",  adapter)
    session.headers.update({
        "User-Agent":      "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36",
        "Accept-Encoding": "identity",
        "Accept":          "text/html,application/xhtml+xml,*/*;q=0.8",
        "Accept-Language": "en-US,en;q=0.9",
    })
    return session


# ══════════════════════════════════════════════════════════════════════════════
# URL CONSTRUCTION
# ══════════════════════════════════════════════════════════════════════════════

def build_download_url(product_url: str, date: str) -> str:
    """
    Append the iShares Ajax endpoint to a product page URL.

    Product URL:  https://www.ishares.com/us/products/239726/ishares-core-sp-500-etf
    Download URL: .../239726/ishares-core-sp-500-etf/1467271812594.ajax
                  ?fileType=csv&dataType=fund&asOfDate=20260514

    Works for US, UK, DE, CH regional URLs — same Ajax ID everywhere.
    """
    base = product_url.rstrip("/")
    return f"{base}/{ISHARES_AJAX_ID}.ajax?fileType=csv&dataType=fund&asOfDate={date}"


# ══════════════════════════════════════════════════════════════════════════════
# CSV DOWNLOAD
# ══════════════════════════════════════════════════════════════════════════════

def download_holdings(
    session:     requests.Session,
    ticker:      str,
    product_url: str,
    date:        str,
    csv_dir:     Path,
    fund_name:   str,
    logger:      logging.Logger,
) -> Optional[Path]:
    """
    Download holdings CSV for one ETF. Returns saved path or None.
    Skips if the file already exists on disk.
    """
    safe_fund = re.sub(r'[\\/*?:"<>|]', "_", fund_name)
    filename  = f"{ticker}_{safe_fund}_{date}.csv"
    dest      = csv_dir / filename

    if dest.exists():
        logger.debug(f"[{ticker}] Already on disk — skipping download")
        return dest

    url = build_download_url(product_url, date)
    logger.debug(f"[{ticker}] GET {url}")

    try:
        resp = session.get(url, timeout=REQUEST_TIMEOUT)
        if resp.status_code == 200 and len(resp.content) > 200:
            dest.write_bytes(resp.content)
            logger.info(f"[{ticker}] ✓ Downloaded {len(resp.content):,} bytes → {filename}")
            return dest
        logger.warning(f"[{ticker}] ✗ HTTP {resp.status_code} ({len(resp.content)} bytes)")
        return None
    except requests.RequestException as exc:
        logger.error(f"[{ticker}] ✗ Request error: {exc}")
        return None
    finally:
        time.sleep(RATE_LIMIT_SLEEP)


# ══════════════════════════════════════════════════════════════════════════════
# CSV PARSING
# ══════════════════════════════════════════════════════════════════════════════

def _find_header_line(lines: List[str]) -> int:
    """Find the row index containing column headers (Ticker, Name, Sector)."""
    for i, line in enumerate(lines):
        if "Ticker" in line and "Name" in line and "Sector" in line:
            return i
    raise ValueError("Cannot find header row — CSV structure may have changed")


def _is_data_row(ticker_val: str) -> bool:
    """
    True if this row is a real holding row (not a footer / blank / BOM line).
    Rules:
      • Ticker must be non-null
      • Ticker must be ≤ 20 characters (real tickers are short)
      • Ticker must not start with known garbage chars
    """
    if not isinstance(ticker_val, str):
        return False
    t = ticker_val.strip()
    if not t or len(t) > 20:
        return False
    # Footer rows start with long prose or special chars
    bad_starts = ("©", "Â", "The ", "Holdings", "Please", "CAREFULLY",
                  "This ", "http", "None", "Â©")
    return not any(t.startswith(b) for b in bad_starts)


def _clean_numeric(series: pd.Series) -> pd.Series:
    """Strip thousands commas and cast to float."""
    return pd.to_numeric(
        series.astype(str).str.replace(",", "", regex=False).str.strip(),
        errors="coerce",
    )


def parse_holdings_csv(
    path:       Path,
    etf_ticker: str,
    date:       str,
    logger:     logging.Logger,
) -> Optional[pd.DataFrame]:
    """
    Parse an iShares holdings CSV into a DB-ready DataFrame.

    Handles:
      • Dynamic header detection (no hardcoded skiprows)
      • Footer boilerplate trimmed via row-level validation
      • Comma-formatted numbers → float
      • Quantity → Shares rename
      • Etf_id composite PK construction
      • Funds with no Sector (crypto/alternative ETFs like ETHA)
    """
    try:
        raw  = path.read_bytes()
        text = raw.decode("ISO-8859-1")
        lines = text.splitlines()

        header_idx = _find_header_line(lines)
        data_text  = "\n".join(lines[header_idx:])

        df = pd.read_csv(
            io.StringIO(data_text),
            dtype=str,
            on_bad_lines="skip",
        )

        # ── Drop footer / blank / BOM rows ────────────────────────────────────
        df = df[df["Ticker"].apply(_is_data_row)].copy()

        if df.empty:
            logger.warning(f"[{etf_ticker}] No valid holdings after filtering")
            return None

        # ── Keep only relevant columns (handle missing ones gracefully) ────────
        available = [c for c in CSV_KEEP_COLS if c in df.columns]
        df = df[available].copy()

        # ── Rename CSV → DB columns (includes Quantity → Shares) ──────────────
        df.rename(columns=RENAME_MAP, inplace=True)

        # ── Numeric conversion ─────────────────────────────────────────────────
        for col in NUMERIC_COLS:
            if col in df.columns:
                df[col] = _clean_numeric(df[col])

        # ── Derived columns ────────────────────────────────────────────────────
        df["Etf"]          = etf_ticker
        df["Created_date"] = date

        def safe(col: str) -> pd.Series:
            return (
                df[col].fillna("NA").astype(str).str.strip()
                if col in df.columns
                else pd.Series(["NA"] * len(df), index=df.index)
            )

        df["Etf_id"] = (
            safe("Etf")          + "_" +
            safe("Created_date") + "_" +
            safe("Ticker")       + "_" +
            safe("Exchange")     + "_" +
            safe("Location")     + "_" +
            safe("Asset_Class")  + "_" +
            safe("Sector")
        )

        df.drop_duplicates(subset=["Etf_id"], inplace=True)

        # ── Remove rows with null Sector (true nulls only — not "-") ──────────
        # Note: "-" is a valid sector value for crypto/alternative funds (ETHA)
        if "Sector" in df.columns:
            df = df[df["Sector"].notna()]

        logger.info(f"[{etf_ticker}] Parsed {len(df):,} holdings from {path.name}")
        return df

    except Exception as exc:
        logger.error(f"[{etf_ticker}] Parse error: {exc}", exc_info=True)
        return None


# ══════════════════════════════════════════════════════════════════════════════
# DATABASE
# ══════════════════════════════════════════════════════════════════════════════

def init_db(db_path: Path, logger: logging.Logger) -> sqlite3.Connection:
    """Open DB, apply PRAGMAs, create schema if needed."""
    db_path.parent.mkdir(parents=True, exist_ok=True)
    conn = sqlite3.connect(str(db_path), check_same_thread=False)
    conn.executescript("""
        PRAGMA journal_mode = WAL;
        PRAGMA synchronous  = NORMAL;
        PRAGMA temp_store   = MEMORY;
        PRAGMA cache_size   = -32000;
    """)
    conn.execute(f"""
        CREATE TABLE IF NOT EXISTS {TABLE_NAME} (
            Etf_id         TEXT PRIMARY KEY NOT NULL UNIQUE,
            Created_date   TEXT NOT NULL,
            Etf            TEXT NOT NULL,
            Ticker         TEXT NOT NULL,
            Name           TEXT,
            Sector         TEXT,
            Asset_Class    TEXT,
            Market_Value   REAL,
            Weight         REAL,
            Notional_Value REAL,
            Shares         REAL,
            Price          REAL,
            Location       TEXT,
            Exchange       TEXT
        )
    """)
    # Legacy table alias so old queries still work
    conn.execute("""
        CREATE VIEW IF NOT EXISTS test AS SELECT * FROM holdings
    """)
    conn.commit()
    logger.debug(f"DB ready: {db_path}")
    return conn


def upsert_holdings(
    conn:   sqlite3.Connection,
    df:     pd.DataFrame,
    ticker: str,
    logger: logging.Logger,
) -> int:
    """Batch-upsert holdings using INSERT OR REPLACE. Returns rows written."""
    for col in DB_COLS:
        if col not in df.columns:
            df[col] = None

    rows = [
        tuple(
            None if pd.isna(row[col]) else row[col]
            for col in DB_COLS
        )
        for _, row in df[DB_COLS].iterrows()
    ]
    placeholders = ", ".join(["?"] * len(DB_COLS))
    cols_str     = ", ".join(DB_COLS)
    conn.executemany(
        f"INSERT OR REPLACE INTO {TABLE_NAME} ({cols_str}) VALUES ({placeholders})",
        rows,
    )
    conn.commit()
    logger.info(f"[{ticker}] ✓ Upserted {len(rows):,} rows")
    return len(rows)


def update_ishare_meta(
    conn:        sqlite3.Connection,
    ticker_file: Path,
    logger:      logging.Logger,
) -> None:
    """Refresh the ishare_meta reference table from Folder_Ishares.csv."""
    logger.info("Refreshing ishare_meta table...")
    df = pd.read_csv(ticker_file)[["Ticker", "Fund", "ISIN", "Product_ID", "Product_URL"]]
    df.to_sql("ishare_meta", conn, if_exists="replace", index=False)
    conn.commit()
    logger.info(f"ishare_meta: {len(df):,} rows written")


# ══════════════════════════════════════════════════════════════════════════════
# WORKER  (runs in each thread-pool thread)
# ══════════════════════════════════════════════════════════════════════════════

def process_one(
    session:     requests.Session,
    ticker:      str,
    fund:        str,
    product_url: str,
    date:        str,
    csv_dir:     Path,
    db_path:     Path,
    skip_dl:     bool,
    logger:      logging.Logger,
) -> Tuple[str, bool, str]:
    """Download + parse + insert one ETF. Returns (ticker, success, message)."""

    # ── Locate / download CSV ─────────────────────────────────────────────────
    if skip_dl:
        matches  = list(csv_dir.glob(f"{ticker}_*_{date}.csv"))
        csv_path = matches[0] if matches else None
        if csv_path is None:
            return ticker, False, "No CSV on disk (--no-download mode)"
    else:
        csv_path = download_holdings(
            session, ticker, product_url, date, csv_dir, fund, logger
        )
        if csv_path is None:
            return ticker, False, "Download failed"

    # ── Parse CSV ─────────────────────────────────────────────────────────────
    df = parse_holdings_csv(csv_path, ticker, date, logger)
    if df is None or df.empty:
        return ticker, False, "Parse returned no data"

    # ── DB upsert (each worker opens its own connection — WAL allows this) ───
    conn = sqlite3.connect(str(db_path), check_same_thread=False)
    conn.execute("PRAGMA journal_mode = WAL;")
    try:
        n = upsert_holdings(conn, df, ticker, logger)
        return ticker, True, f"{n:,} rows upserted"
    except Exception as exc:
        logger.error(f"[{ticker}] DB error: {exc}", exc_info=True)
        return ticker, False, f"DB error: {exc}"
    finally:
        conn.close()


# ══════════════════════════════════════════════════════════════════════════════
# MAIN PIPELINE
# ══════════════════════════════════════════════════════════════════════════════

def run(
    date:          str,
    ticker_list:   Optional[List[str]],
    ticker_file:   Path,
    update_meta:   bool,
    skip_download: bool,
    workers:       int,
    logger:        logging.Logger,
) -> None:
    t0 = time.time()
    logger.info("═" * 70)
    logger.info(f"Pipeline start  date={date}  workers={workers}  skip_dl={skip_download}")

    # ── Setup directories ─────────────────────────────────────────────────────
    csv_dir = DOWNLOAD_DIR / date
    csv_dir.mkdir(parents=True, exist_ok=True)

    # ── Load ticker universe ──────────────────────────────────────────────────
    logger.info(f"Loading tickers from: {ticker_file}")
    master = pd.read_csv(ticker_file)

    required = {"Ticker", "Fund", "Product_URL"}
    missing  = required - set(master.columns)
    if missing:
        logger.error(f"Ticker file missing required columns: {missing}")
        sys.exit(1)

    master = master.dropna(subset=["Product_URL"])

    if ticker_list:
        master = master[master["Ticker"].isin(ticker_list)]
        logger.info(f"Filtered to {len(master)} specified tickers")

    if master.empty:
        logger.error("No tickers to process — exiting")
        sys.exit(1)

    logger.info(f"Processing {len(master):,} ETFs")

    # ── Init DB and optionally refresh meta ───────────────────────────────────
    conn_main = init_db(DB_PATH, logger)
    if update_meta:
        update_ishare_meta(conn_main, ticker_file, logger)
    conn_main.close()

    # ── Concurrent download + parse + insert ──────────────────────────────────
    session   = build_session()
    ok_count  = 0
    err_count = 0

    with ThreadPoolExecutor(max_workers=workers) as pool:
        futures = {
            pool.submit(
                process_one,
                session,
                str(row["Ticker"]),
                str(row["Fund"]),
                str(row["Product_URL"]),
                date,
                csv_dir,
                DB_PATH,
                skip_download,
                logger,
            ): str(row["Ticker"])
            for _, row in master.iterrows()
        }

        for future in as_completed(futures):
            try:
                etf, success, msg = future.result()
                if success:
                    ok_count += 1
                    logger.info(f"  ✓ [{etf}]  {msg}")
                else:
                    err_count += 1
                    logger.warning(f"  ✗ [{etf}]  {msg}")
            except Exception as exc:
                err_count += 1
                logger.error(f"  ✗ [{futures[future]}]  EXCEPTION: {exc}", exc_info=True)

    elapsed = time.time() - t0
    logger.info("═" * 70)
    logger.info(
        f"Done in {elapsed:.1f}s  |  "
        f"✓ {ok_count} succeeded  ✗ {err_count} failed  |  "
        f"Total: {ok_count + err_count}"
    )


# ══════════════════════════════════════════════════════════════════════════════
# CLI
# ══════════════════════════════════════════════════════════════════════════════

def main() -> None:
    yesterday = (datetime.today() - timedelta(days=1)).strftime("%Y%m%d")

    parser = argparse.ArgumentParser(
        description="iShares ETF Holdings Pipeline",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  python pipeline.py                            # all ETFs, yesterday
  python pipeline.py --date 20260514            # specific date
  python pipeline.py --ticker IVV QQQ SPY       # specific tickers
  python pipeline.py --update-meta              # also refresh ishare_meta
  python pipeline.py --no-download --date 20260514  # reprocess saved CSVs
        """,
    )
    parser.add_argument("--date", default=yesterday,
                        help="Holdings date YYYYMMDD (default: yesterday)")
    parser.add_argument("--ticker", nargs="+", default=None,
                        help="Ticker list e.g. --ticker IVV QQQ SPY")
    parser.add_argument("--ticker-file", default=str(TICKER_FILE),
                        help="Path to Folder_Ishares.csv")
    parser.add_argument("--update-meta", action="store_true",
                        help="Refresh ishare_meta reference table")
    parser.add_argument("--no-download", action="store_true",
                        help="Skip downloads; reprocess CSVs already on disk")
    parser.add_argument("--workers", type=int, default=DEFAULT_WORKERS,
                        help=f"Concurrent threads (default: {DEFAULT_WORKERS})")
    args = parser.parse_args()

    logger = setup_logging(args.date)
    run(
        date          = args.date,
        ticker_list   = args.ticker,
        ticker_file   = Path(args.ticker_file),
        update_meta   = args.update_meta,
        skip_download = args.no_download,
        workers       = args.workers,
        logger        = logger,
    )


if __name__ == "__main__":
    main()
