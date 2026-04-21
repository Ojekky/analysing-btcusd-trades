"""
binance_ingest.py
-----------------
Ingests BTCUSDT 1h spot klines from data.binance.vision into
Azure Synapse Analytics via dlt.

Credentials live in .dlt/secrets.toml and .dlt/config.toml — do NOT
hardcode them here.

Run modes
---------
  # Load one specific month (test connectivity / manual step-through)
  python binance_ingest.py --mode single --year 2023 --month 1

  # Backfill: loads each month one at a time, prints result per month
  python binance_ingest.py --mode backfill

  # Incremental: previous calendar month only (for scheduled runs)
  python binance_ingest.py --mode incremental

  # Any mode with --dry-run: download + parse but do NOT touch Synapse
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
# CONSTANTS
# ---------------------------------------------------------------------------

SYMBOL        = "BTCUSDT"
INTERVAL      = "1h"
BASE_URL      = "https://data.binance.vision/data/spot/monthly/klines"
BACKFILL_FROM = date(2023, 1, 1)    # adjust start date as needed

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

    try:
        with zipfile.ZipFile(io.BytesIO(resp.content)) as zf:
            with zf.open(zf.namelist()[0]) as f:
                df = pd.read_csv(f, header=None, names=COLUMNS)
    except Exception as exc:
        print(f"PARSE ERROR — {exc}")
        return None

    df = df.dropna(how="all")
    if df.empty:
        print("empty file, skipping")
        return None

    df = df.drop(columns=["ignore"])

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

    float_cols = ["open", "high", "low", "close", "volume",
                  "quote_volume", "taker_buy_base", "taker_buy_quote"]
    df[float_cols] = df[float_cols].apply(pd.to_numeric, errors="coerce")
    df["count"] = pd.to_numeric(df["count"], errors="coerce").astype("Int64")

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
    primary_key="open_time",
)
def btcusdt_klines(year: int, month: int) -> Iterator[list]:
    """Fetch and yield a single month of kline records."""
    df = fetch_monthly_klines(SYMBOL, INTERVAL, year, month)
    if df is not None:
        yield df.to_dict(orient="records")


# ---------------------------------------------------------------------------
# CORE: load one month through its own pipeline call
# ---------------------------------------------------------------------------

def load_one_month(year: int, month: int) -> bool:
    """
    Run a full dlt pipeline for exactly one month.
    Returns True on success, False on failure.
    Prints the same status block you see in single mode.
    """
    pipeline = dlt.pipeline(
        pipeline_name = "binance_btcusdt_pipeline",
        destination   = "synapse",
        dataset_name  = "gold_btc_data",
    )

    try:
        load_info = pipeline.run(btcusdt_klines(year=year, month=month))
        print("=" * 60)
        print(load_info)
        print("=" * 60 + "\n")
        return True
    except Exception as exc:
        print("=" * 60)
        print(f"  [FAILED] {year}-{month:02d} — {exc}")
        print("=" * 60 + "\n")
        return False


# ---------------------------------------------------------------------------
# PIPELINE RUNNER
# ---------------------------------------------------------------------------

def run_pipeline(mode: str, dry_run: bool = False,
                 year: int = None, month: int = None) -> None:

    today = date.today()

    if today.month == 1:
        last_completed = date(today.year - 1, 12, 1)
    else:
        last_completed = date(today.year, today.month - 1, 1)

    # ── Build month list ─────────────────────────────────────────────────────
    if mode == "single":
        if not year or not month:
            raise ValueError(
                "--mode single requires both --year and --month.\n"
                "Example: python binance_ingest.py --mode single --year 2023 --month 1"
            )
        months = [(year, month)]
        print(f"\n[INFO] SINGLE mode: {year}-{month:02d} (1 month)")

    elif mode == "backfill":
        months = _months_in_range(BACKFILL_FROM, last_completed)
        print(f"\n[INFO] BACKFILL mode: {BACKFILL_FROM} → {last_completed}")

    elif mode == "incremental":
        months = [(last_completed.year, last_completed.month)]
        print(f"\n[INFO] INCREMENTAL mode: {last_completed.strftime('%Y-%m')}")

    else:
        raise ValueError(f"Unknown mode '{mode}'. Use: single | backfill | incremental")

    print(f"[INFO] {len(months)} month(s) queued\n")

    # ── Dry run ──────────────────────────────────────────────────────────────
    if dry_run:
        print("[DRY RUN] Parsing only — Synapse will NOT be touched.\n")
        total = 0
        for y, m in months:
            df = fetch_monthly_klines(SYMBOL, INTERVAL, y, m)
            if df is not None:
                total += len(df)
        print(f"\n[DRY RUN] Done — {total:,} rows parsed successfully.")
        return

    # ── Live load — one pipeline call per month ───────────────────────────────
    # Each month is loaded independently so you see a result line per month
    # and a failure in one month does not affect the others.
    failed = []

    for y, m in months:
        print(f"[INFO] Loading {y}-{m:02d} …")
        success = load_one_month(y, m)
        if not success:
            failed.append(f"{y}-{m:02d}")

    # ── Final summary ─────────────────────────────────────────────────────────
    print("=" * 60)
    print(f"SUMMARY: {len(months) - len(failed)}/{len(months)} months loaded successfully")
    if failed:
        print(f"FAILED months — re-run these manually with --mode single:")
        for f in failed:
            yr, mo = f.split("-")
            print(f"  python binance_ingest.py --mode single --year {yr} --month {int(mo)}")
    print("=" * 60)


# ---------------------------------------------------------------------------
# CLI
# ---------------------------------------------------------------------------

def _build_parser() -> argparse.ArgumentParser:
    p = argparse.ArgumentParser(
        description="Ingest BTCUSDT 1h klines from Binance → Synapse via dlt",
        formatter_class=argparse.RawTextHelpFormatter,
    )
    p.add_argument(
        "--mode",
        choices=["single", "backfill", "incremental"],
        required=True,
        help=(
            "single      : load one specific month (use --year and --month)\n"
            "backfill    : load each month individually from BACKFILL_FROM → today\n"
            "incremental : load previous calendar month only"
        ),
    )
    p.add_argument(
        "--year",
        type=int,
        default=None,
        help="Year for single mode, e.g. --year 2023",
    )
    p.add_argument(
        "--month",
        type=int,
        default=None,
        help="Month for single mode (1-12), e.g. --month 1",
    )
    p.add_argument(
        "--dry-run",
        action="store_true",
        help="Fetch and parse without loading into Synapse",
    )
    return p


if __name__ == "__main__":
    args = _build_parser().parse_args()
    run_pipeline(
        mode    = args.mode,
        dry_run = args.dry_run,
        year    = args.year,
        month   = args.month,
    )
