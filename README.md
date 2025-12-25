# Week-2 Project

Data workflow project for cleaning, transforming, and analyzing orders data.

## Setup

Clone the repository:

```bash
git clone https://github.com/Mohammed-Balkhair-hub/Week-2_Project.git
cd Week-2_Project
```

Install `uv` from https://docs.astral.sh/uv/. Python 3.12 is required (see `.python-version`).

From the project root, install dependencies:

```bash
uv sync
```

This creates a virtual environment in `.venv/` and installs all packages from `pyproject.toml`.



## Project Structure

```
ROOT/
├── data/
│   ├── raw/          # Immutable input data
│   ├── cache/        # Cached API responses
│   ├── processed/    # Clean, analysis-ready outputs
│   └── external/     # Reference data
├── src/
│   └── data_workflow/  # Package with all functions
│       └── __init__.py
├── scripts/          # Run scripts
├── reports/
│   └── figures/      # Exported charts
├── pyproject.toml    # Dependencies
├── README.md         # Project documentation
└── .gitignore        # Git ignore file
```

## Pipeline Overview

The ETL pipeline is in `src/data_workflow/etl.py`. It has 5 main functions:

1. **`load_inputs(path)`** - Reads raw CSV files (orders and users) from `data/raw/`
2. **`transform(orders_raw, users)`** - Cleans and enriches the data:
   - Validates columns and data quality
   - Enforces schema (correct data types)
   - Normalizes text fields
   - Handles missing values
   - Parses dates and extracts time parts
   - Joins orders with users
   - Handles outliers
3. **`load_outputs(...)`** - Writes cleaned data to parquet files and generates missingness report
4. **`write_run_meta(path, analytics)`** - Saves run metadata (timestamp, row counts, data quality metrics)
5. **`run_etl(root)`** - Runs the complete pipeline from start to finish

## Running the Pipeline

Run the full ETL pipeline:

```bash
uv run python scripts/run_etl.py
```

This will:
- Load raw data from `data/raw/`
- Clean and transform it
- Write outputs to `data/processed/`
- Generate reports in `reports/`

You can also run individual day scripts:

- Day 1 (load): `uv run python scripts/run_day1_load.py`
- Day 2 (clean): `uv run python scripts/run_day2_clean.py`
- Day 3 (build analytics): `uv run python scripts/run_day3_build_analytics.py`

## Package Modules

The `src/data_workflow/` package is organized by function:

- **`io.py`** - Read/write CSV and parquet files
- **`quality.py`** - Data quality checks (column requirements, uniqueness, ranges)
- **`transformers.py`** - Data cleaning functions (schema, normalization, datetime parsing, outlier handling)
- **`joins.py`** - Safe join functions with validation
- **`utils.py`** - Statistical utilities (bootstrap)
- **`viz.py`** - Plotly visualization functions
- **`config.py`** - Path configuration
- **`etl.py`** - Main ETL pipeline functions

## Results

Processed outputs are in `data/processed/`:
- `orders_clean.parquet` - Cleaned orders data
- `users.parquet` - Users data
- `analytics_table.parquet` - Final analytics-ready table
- `_run_meta.json` - Run metadata

Reports are in `reports/`:
- `missingness_orders.csv` - Missing value report
- `summary.md` - Analysis findings and caveats
- `figures/` - Exported charts

For detailed findings and data quality notes, see `reports/summary.md`.
