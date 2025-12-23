# Week-2 Project

## Quick setup
- Install `uv` (https://docs.astral.sh/uv/); Python `3.12` is expected (see `.python-version`).
- From the project root, install deps into `.venv`:
  - ```bash
    uv sync
    ```
- Optional: activate the virtualenv:
  - ```bash
    source .venv/bin/activate
    ```

## Run the scripts
- Day 1 (load):
  - ```bash
    uv run python scripts/run_day1_load.py
    ```
- Day 2 (clean):
  - ```bash
    uv run python scripts/run_day2_clean.py
    ```

Processed outputs are written under `data/processed/` and reports under `reports/`.
