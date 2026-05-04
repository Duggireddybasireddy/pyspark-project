A production-grade **PySpark ETL pipeline** implementing the **Medallion Architecture** (Raw → Standard → Distribution) to process large-scale retail datasets — including sales, returns, web transactions, and catalog data — with structured logging, modular job execution, and shell-based orchestration.

---
## Tech Stack

| Tool | Purpose |
|---|---|
| **Apache Spark (PySpark)** | Distributed data processing |
| **Python 3.8+** | Pipeline scripting and orchestration |
| **YAML** | Configuration management |
| **Shell Scripts (Bash)** | Spark-submit automation & file operations |
| **Parquet** | Columnar storage format (intermediate layers) |
| **CSV** | Final output format for analytical consumption |
| **Python logging** | Runtime observability |

---

## 📌 Table of Contents

- [Overview](#overview)
- [Architecture](#architecture)
- [Project Structure](#project-structure)
- [Data Flow](#data-flow)
- [Layers Explained](#layers-explained)
- [Configuration](#configuration)
- [Running the Pipeline](#running-the-pipeline)
- [Logging](#logging)
- [Output](#output)

---

## Overview

This pipeline ingests raw **Parquet files**, processes them through multiple transformation layers, and produces clean, analytics-ready **CSV outputs** for downstream reporting and analysis.

The pipeline handles six core retail data domains:

| Domain | Description |
|---|---|
| `sales` | In-store sales transactions |
| `returns` | In-store return records |
| `web_sales` | Online sales transactions |
| `web_returns` | Online return records |
| `catalog_sales` | Catalog-based sales |
| `catalog_returns` | Catalog-based returns |

---

## Architecture

The pipeline follows the **Medallion Architecture** pattern:

```
Raw Layer (Bronze)
    └── Ingest Parquet → Convert to CSV

Standard Layer (Silver)
    └── Type casting, null handling, data cleansing

Distribution Layer (Gold)
    └── Cross-source joins → Final analytical datasets → CSV output
```

---

## Project Structure

```
main/
│
├── src/
│   ├── raw/                        # Bronze layer: Parquet → CSV ingestion
│   │   └── convert.py
│   │
│   ├── std/                        # Silver layer: Data standardization scripts
│   │   ├── std_sales.py
│   │   ├── std_returns.py
│   │   └── ...
│   │
│   ├── dist/                       # Gold layer: Join logic and final outputs
│   │   ├── sales.py
│   │   ├── returns.py
│   │   ├── sales_web.py
│   │   ├── returns_web.py
│   │   ├── sales_catalog.py
│   │   └── returns_catalog.py
│   │
│   ├── common/                     # Shared utilities
│   │   ├── spark_session.py        # Centralized SparkSession factory
│   │   └── logger.py               # Python logging module wrapper
│   │
│   ├── config/
│   |   └── config.yaml             # Pipeline configuration (paths, settings)
│   |
│   |
|   ├── scripts/                        # Shell scripts for spark-submit & file ops
│       ├── raw.sh                      # Submits Bronze job
│       ├── std.sh                      # Submits Silver job
│       ├── dist.sh                     # Submits Gold job
│       └── final.sh                    # Master script: runs all layers in order
│
├── data/                           # Local testing data (not for production)
│   └── raw/
│
├── logs/                           # Runtime logs (auto-generated)
│
└── README.md
```

---

## Data Flow

```
[Parquet Files]
      │
      ▼
┌─────────────┐
│  RAW LAYER  │  ← convert.py
│  (Bronze)   │    Parquet → CSV
└──────┬──────┘
       │
       ▼
┌─────────────┐
│  STD LAYER  │  ← std_*.py scripts
│  (Silver)   │    Type casting
│             │    Null replacement (numeric cols)
│             │    Drop null SK columns
└──────┬──────┘
       │
       ▼
┌─────────────┐
│  DIST LAYER │  ← dist_*.py scripts
│   (Gold)    │    Cross-source joins
│             │    Final transformations
│             │    → CSV output (analytics-ready)
└─────────────┘
```

---

## Layers Explained

### 🥉 Raw Layer (Bronze)
- Reads source **Parquet files** from the input directory
- Converts them to **CSV format** for downstream processing
- No business logic applied — raw ingestion only
- Entry point: `src/raw/convert.py`

### 🥈 Standard Layer (Silver)
- Loads CSVs produced by the Raw layer
- Performs **data type casting** to enforce schema consistency
- **Replaces null values** in numeric columns with appropriate defaults (e.g., `0` or mean values)
- **Drops columns** where surrogate key (SK) fields contain nulls
- Saves processed data back as **Parquet** for efficient downstream reads
- Entry points: `src/std/std_*.py`

### 🥇 Distribution Layer (Gold)
- Reads Parquet outputs from the Standard layer
- **Joins related data sources** (e.g., sales + returns, web + catalog)
- Produces six final analytical datasets:
  - `sales`, `returns`, `web_sales`, `web_returns`, `catalog_sales`, `catalog_returns`
- Writes final results as **CSV files** ready for reporting and analysis
- Entry points: `src/dist/dist_*.py`

---


## Configuration

All pipeline settings are managed via `src/config/config.yaml`:

Update paths and Spark settings to match your environment before running.

---

## Running the Pipeline

### Option 1: Run All Layers (Recommended)

```bash
bash scripts/final.sh
```

This sequentially executes:
1. `raw.sh` → Bronze ingestion
2. `std.sh` → Silver standardization
3. `dist.sh` → Gold distribution & joins

### Option 2: Run Individual Layers

```bash
# Bronze layer only
bash scripts/raw.sh

# Silver layer only
bash scripts/std.sh

# Gold layer only
bash scripts/dist.sh
```

## Logging

Runtime logs are automatically written to the `logs/` directory.

The `logger.py` module in `src/common/` wraps Python's built-in `logging` module and provides:

- Timestamped log entries
- Configurable log levels (`DEBUG`, `INFO`, `WARNING`, `ERROR`)
- Separate log files per pipeline run

**Sample log output:**

```
2025-08-01 10:23:45 | INFO  | raw.ingest_parquet  | Starting Parquet ingestion...
2025-08-01 10:23:47 | INFO  | raw.ingest_parquet  | Successfully converted 12 files to CSV
2025-08-01 10:24:10 | INFO  | std.std_sales        | Null values replaced in 3 numeric columns
2025-08-01 10:25:30 | INFO  | dist.dist_sales      | Join completed. Output rows: 1,245,890
```

---

## Output

Final analytical CSV files are saved in the distribution output directory (configured in `config.yaml`):

| File | Description |
|---|---|
| `sales.csv` | Final in-store sales data |
| `returns.csv` | Final in-store returns data |
| `web_sales.csv` | Final online sales data |
| `web_returns.csv` | Final online returns data |
| `catalog_sales.csv` | Final catalog sales data |
| `catalog_returns.csv` | Final catalog returns data |

These files are ready for consumption by BI tools, analytical databases, or ML pipelines.

---


