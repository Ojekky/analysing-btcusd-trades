"""
binance_ingest.py
-----------------
Ingests BTCUSDT 1h spot klines from data.binance.vision into
Azure Synapse Analytics via dlt, staging through ADLS2.

Credentials live in .dlt/secrets.toml and .dlt/config.toml — do NOT
hardcode them here.

Run modes
---------
  # Full backfill from 2023-01-01 to last completed month
  python binance_ingest.py --mode backfill

  # Incremental: last full calendar month only
  python binance_ingest.py --mode incremental

  # Dry-run: download + parse but do NOT load into Synapse
  python binance_ingest.py --mode backfill --dry-run
"""

import argparse
import io
import zipfile
from datetime import datetime, date
from typing import Iterator

import dlt
import pandas as pd
import requests

# ---------------------------------------------------------------------------
# CONSTANTS  (only non-secret config lives here)
# ---------------------------------------------------------------------------

SYMBOL        = "BTCUSDT"
INTERVAL      = "1h"
BASE_URL      = "https://data.binance.vision/data/spot/monthly/klines"
BACKFILL_FROM = date(2023, 1, 1)    # adjust start year as needed

COLUMNS = [
    "open_time",
    "open",
    "high",
    "low",
    "close",
    "volume",
    "close_time",
    "quote_volume",
    "count",
    "taker_buy_base",
    "taker_buy_quote",
    "ignore",           # dropped before yield
]

# ---------------------------------------------------------------------------
# HELPERS
# ---------------------------------------------------------------------------

def _detect_time_unit(series: pd.Series) -> str:
    """
    Return 'us' (microseconds) or 'ms' (milliseconds).
    Binance changed SPOT timestamps to microseconds from 2025-01-01 onwards.
    """
    sample = str(int(series.dropna().iloc[0]))
    return "us" if len(sample) > 13 else "ms"


def _months_in_range(start: date, end: date) -> list:
    """Return a list of (year, month) tuples from start to end inclusive."""
    result = []
    year, month = start.year, start.month
    while (year, month) <= (end.year, end.month):
        result.append((year, month))
        month += 1
        if month > 12:
            month = 1
            year += 1
    return result


def fetch_monthly_klines(symbol: str, interval: str, year: int, month: int):
    """
    Download and parse one monthly kline zip from data.binance.vision.
    Returns a cleaned DataFrame, or None if the file is unavailable.
    """
    file_name = f"{symbol}-{interval}-{year}-{month:02d}.zip"
    url = f"{BASE_URL}/{symbol}/{interval}/{file_name}"

    print(f"  [fetch] {file_name}", end=" ... ", flush=True)

    # ── Download ─────────────────────────────────────────────────────────────
    try:
        resp = requests.get(url, timeout=60)
    except requests.RequestException as exc:
        print(f"CONNECTION ERROR — {exc}")
        return None

    if resp.status_code == 404:
        print("not available (404), skipping")
        return None
    if resp.status_code != 200:
        print(f"HTTP {resp.status_code}, skipping")
        return None

    # ── Unzip + parse ────────────────────────────────────────────────────────
    try:
        with zipfile.ZipFile(io.BytesIO(resp.content)) as zf:
            with zf.open(zf.namelist()[0]) as f:
                df = pd.read_csv(f, header=None, names=COLUMNS)
    except Exception as exc:
        print(f"PARSE ERROR — {exc}")
        return None

    # ── Basic cleaning ───────────────────────────────────────────────────────
    df = df.dropna(how="all")
    if df.empty:
        print("empty file, skipping")
        return None

    df = df.drop(columns=["ignore"])

    # ── Timestamps ───────────────────────────────────────────────────────────
    # Convert epoch integers → Python datetime (maps to DATETIME2 in Synapse,
    # avoiding the NVARCHAR(MAX) type that breaks distribution keys).
    unit = _detect_time_unit(df["open_time"])
    for col in ("open_time", "close_time"):
        df[col] = pd.to_datetime(df[col], unit=unit, errors="coerce")

    bad = df["open_time"].isna().sum()
    if bad:
        print(f"dropping {bad} unparseable row(s) ... ", end="", flush=True)
        df = df.dropna(subset=["open_time"])

    if df.empty:
        print("all rows invalid after timestamp parse, skipping")
        return None

    # ── Numeric coercion ─────────────────────────────────────────────────────
    float_cols = ["open", "high", "low", "close", "volume",
                  "quote_volume", "taker_buy_base", "taker_buy_quote"]
    df[float_cols] = df[float_cols].apply(pd.to_numeric, errors="coerce")
    df["count"] = pd.to_numeric(df["count"], errors="coerce").astype("Int64")

    # ── Convert Timestamps → plain Python datetime ───────────────────────────
    # dlt serialises pd.Timestamp as string in some Synapse code paths;
    # plain datetime objects reliably map to DATETIME2.
    df["open_time"]  = df["open_time"].dt.to_pydatetime()
    df["close_time"] = df["close_time"].dt.to_pydatetime()

    df = df.reset_index(drop=True)
    print(f"OK ({len(df):,} rows)")
    return df


# ---------------------------------------------------------------------------
# DLT RESOURCE
# ---------------------------------------------------------------------------

@dlt.resource(
    name="btcusdt_1h_klines",
    write_disposition="merge",
    primary_key="open_time",        # one candle per hour — open_time is unique
)
def btcusdt_klines(months: list) -> Iterator[list]:
    """
    Iterate over (year, month) pairs, fetch each month and yield
    a batch of records. Batching by calendar month keeps memory flat
    regardless of how wide the date range is.
    """
    for year, month in months:
        df = fetch_monthly_klines(SYMBOL, INTERVAL, year, month)
        if df is not None:
            yield df.to_dict(orient="records")


# ---------------------------------------------------------------------------
# PIPELINE
# ---------------------------------------------------------------------------

def run_pipeline(mode: str, dry_run: bool = False) -> None:
    """
    Build the month list for the requested mode, then run the dlt pipeline.

    mode
    ----
    backfill    : BACKFILL_FROM → last fully completed calendar month
    incremental : previous calendar month only (safe to run on a schedule)
    """
    today = date.today()

    # Last *completed* month (never includes the current in-progress month)
    if today.month == 1:
        last_completed = date(today.year - 1, 12, 1)
    else:
        last_completed = date(today.year, today.month - 1, 1)

    if mode == "backfill":
        start = BACKFILL_FROM
        end   = last_completed
        print(f"\n[INFO] BACKFILL mode: {start} → {end}")
    elif mode == "incremental":
        start = end = last_completed
        print(f"\n[INFO] INCREMENTAL mode: {start.strftime('%Y-%m')}")
    else:
        raise ValueError(f"Unknown mode '{mode}'. Use: backfill | incremental")

    months = _months_in_range(start, end)
    print(f"[INFO] {len(months)} month(s) queued\n")

    # ── Dry run ──────────────────────────────────────────────────────────────
    if dry_run:
        print("[DRY RUN] Parsing only — Synapse will NOT be touched.\n")
        total = 0
        for year, month in months:
            df = fetch_monthly_klines(SYMBOL, INTERVAL, year, month)
            if df is not None:
                total += len(df)
        print(f"\n[DRY RUN] Done — {total:,} rows parsed successfully.")
        return

    # ── Live load ────────────────────────────────────────────────────────────
    pipeline = dlt.pipeline(
        pipeline_name = "binance_btcusdt_pipeline",
        destination   = "synapse",
        dataset_name  = "gold_btc_data",
        staging       = "filesystem",   # stage to ADLS2 first (required for Synapse)
    )

    print("[INFO] Running dlt pipeline …\n")
    load_info = pipeline.run(btcusdt_klines(months=months))

    print("\n" + "=" * 60)
    print(load_info)
    print("=" * 60)


# ---------------------------------------------------------------------------
# CLI
# ---------------------------------------------------------------------------

def _build_parser() -> argparse.ArgumentParser:
    p = argparse.ArgumentParser(
        description="Ingest BTCUSDT 1h klines → ADLS2 → Synapse via dlt",
        formatter_class=argparse.RawTextHelpFormatter,
    )
    p.add_argument(
        "--mode",
        choices=["backfill", "incremental"],
        default="backfill",
        help=(
            "backfill    : load BACKFILL_FROM → last completed month\n"
            "incremental : load previous calendar month only"
        ),
    )
    p.add_argument(
        "--dry-run",
        action="store_true",
        help="Fetch and parse without loading into Synapse",
    )
    return p


if __name__ == "__main__":
    args = _build_parser().parse_args()
    run_pipeline(mode=args.mode, dry_run=args.dry_run)
