# Automated Intraday Pattern Mining for BTC/USDT

A high-performance data engineering pipeline designed to identify recurring, time-based price behaviors and volatility clusters in the BTC/USDT market.

## 📌 Project Overview
Markets are not random. The BTC/USDT pair exhibits distinct "intraday seasonality"—consistent bullish or bearish tendencies at specific hours—driven by regional session opens (Asia, Europe, and the US). 

This project automates the ingestion, transformation, and visualization of historical 1h kline data to answer:
* **Which specific hours** does BTC/USDT consistently rise or fall?
* **Which trading sessions** exhibit the highest volatility vs. liquidity?
* **How can traders exploit** these predictable temporal patterns?

## 🏗️ Architecture & Tech Stack
The project follows a Modern Data Stack (MDS) architecture deployed on **Microsoft Azure**.

![alt text](image.png)

* **Source:** [Binance Public Data](https://github.com/binance/binance-public-data/tree/master) (BTC/USDT 1h klines via `data.binance.vision`).
* **Ingestion:** `dlt` (Data Load Tool) running on GitHub Actions/Codespaces for automated extraction and schema normalization.
* **Storage:** **Azure Data Lake Storage (ADLS Gen2)** for raw Parquet files (Bronze/Silver layers).
* **Warehouse:** **Azure Synapse Analytics** (Dedicated SQL Pool) for high-performance analytical queries.
* **Transformation:** `dbt-synapse` to model hourly average returns, session-based volatility, and buy/sell pressure ratios.
* **Infrastructure:** **Terraform** for reproducible Infrastructure as Code (IaC) of all Azure resources.
* **Visualization:** **Power BI** dashboard featuring hourly return heatmaps and session volatility metrics.

## 🎯 Problem Statement
The 24/7 nature of the Bitcoin market creates a **signal-to-noise problem**. Most traders apply static strategies regardless of the time of day, ignoring that a move at 04:00 UTC (Asian Session) has different liquidity and volume characteristics than the same move at 14:00 UTC (US Open). 

**Intraday Pattern Mining for BTC/USDT** solves this by:
1.  **Bridging the Context Gap:** Providing "Time-of-Day" analytics that lagging indicators (RSI/MACD) miss.
2.  **Optimizing Execution:** Identifying "Golden Hours" of volatility to reduce slippage and improve entry timing.
3.  **Automating Research:** Replacing manual CSV analysis with a scalable, cloud-native SQL pipeline.

## 🚀 Key Insights Provided
* **Hourly Expected Value (EV):** The mean return for each of the 24 hours in a day across multi-year datasets.
* **Session Volatility Profiles:** Comparative analysis of range expansion across Asian, European, and US trading windows.
* **Regional Handover Deltas:** Tracking price behavior during the "power hour" overlaps between major global financial hubs.

## 🛠️ Performance Optimization
To ensure the project remains cost-effective and performant within Azure Synapse:
* **Partitioning:** Tables are partitioned by **Month** to optimize historical lookbacks.
* **Clustering:** Data is clustered by **Hour** to accelerate time-series mining queries.
* **Parquet Storage:** Using columnar storage in ADLS Gen2 to minimize I/O and storage costs.


## ⚙️ Data Ingestion

### Overview
Data is ingested from [Binance's public data archive](https://data.binance.vision) using a custom Python pipeline (`binance_ingest.py`) built on top of [dlt (Data Load Tool)](https://dlthub.com). The pipeline downloads monthly BTCUSDT 1h kline zip files, parses and cleans them, and loads them directly into Azure Synapse Analytics via `INSERT INTO ... SELECT ... UNION` statements.

**Key design decisions:**
- `write_disposition="merge"` with `primary_key="open_time"` ensures re-runs are safe — no duplicate candles.
- Timestamps are converted to Python `datetime` objects (maps to `DATETIME2` in Synapse) to avoid `NVARCHAR(MAX)` type issues.
- Binance changed SPOT timestamps from milliseconds to microseconds from January 2025 onwards — the pipeline auto-detects the unit per file.
- `loader_file_format = "insert_values"` with `batch_size = 50` is used to stay within Synapse's 32,500 literal-per-query limit.
- Each month is loaded through its own isolated pipeline call so any failure is visible immediately without affecting other months.

### Schema loaded into Synapse
Table: `gold_btc_data.btcusdt_1h_klines`

| Column | Type | Description |
|---|---|---|
| `open_time` | DATETIME2 | Candle open timestamp (primary key) |
| `open` | FLOAT | Opening price |
| `high` | FLOAT | Highest price in period |
| `low` | FLOAT | Lowest price in period |
| `close` | FLOAT | Closing price |
| `volume` | FLOAT | Base asset volume (BTC) |
| `close_time` | DATETIME2 | Candle close timestamp |
| `quote_volume` | FLOAT | Quote asset volume (USDT) |
| `count` | BIGINT | Number of trades |
| `taker_buy_base` | FLOAT | Taker buy base asset volume |
| `taker_buy_quote` | FLOAT | Taker buy quote asset volume |

### dlt configuration

`.dlt/config.toml` (committed — no secrets):
```toml
[runtime]
batch_size = 50

[load]
loader_file_format = "insert_values"
```

`.dlt/secrets.toml` (gitignored — never commit):
```toml
[destination.synapse]
credentials = "synapse://<user>:<password>@<workspace>.sql.azuresynapse.net:1433/<pool>"
```

### Running the pipeline

**One-time historical backfill (2023-01-01 → present):**
```bash
python binance_ingest.py --mode backfill
```
Loads each month individually with its own pipeline call, printing a status line per month:
```
[INFO] Loading 2023-01 …
  [fetch] BTCUSDT-1h-2023-01.zip ... OK (744 rows)
============================================================
Pipeline binance_btcusdt_pipeline load step completed in 14.86 seconds
1 load package(s) were loaded to destination synapse and into dataset gold_btc_data
Load package 177xxx is LOADED and contains no failed jobs
============================================================
```

**Monthly incremental load (previous calendar month only):**
```bash
python binance_ingest.py --mode incremental
```

**Single month (for testing or reloading a specific month):**
```bash
python binance_ingest.py --mode single --year 2024 --month 6
```

**Dry run (fetch and parse without loading into Synapse):**
```bash
python binance_ingest.py --mode backfill --dry-run
```

### Automated monthly scheduling
The incremental load is triggered automatically on the **2nd of every month at 06:00 UTC** via GitHub Actions (`.github/workflows/binance_incremental.yml`). The Synapse connection string is stored as a GitHub repository secret (`SYNAPSE_CONNECTION_STRING`).

To trigger a manual run at any time:
1. Go to the repo → **Actions** tab
2. Select **Binance Monthly Incremental Ingest**
3. Click **Run workflow**

---

## 🛠️ Performance Optimisation
To ensure the project remains cost-effective and performant within Azure Synapse:
* **Partitioning:** Tables are partitioned by **Month** to optimise historical lookbacks.
* **Clustering:** Data is clustered by **Hour** to accelerate time-series mining queries.
* **Columnar storage:** Parquet in ADLS Gen2 minimises I/O and storage costs for staging.

---

*Developed as a project for Data Engineering Zoomcamp 2026 — [DataTalks.Club](https://github.com/DataTalksClub).*










---
*Developed as a project for Data Engineering Zoomcamp 2026 Project [DATATALKS CLUB](https://github.com/DataTalksClub).*


