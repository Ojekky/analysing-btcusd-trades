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


















---
*Developed as a project for Data Engineering Project for [DATATALKS CLUB](https://github.com/DataTalksClub).*
