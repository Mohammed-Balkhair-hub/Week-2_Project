import json
from datetime import datetime, timezone
from pathlib import Path
import pandas as pd

from data_workflow.io import read_orders_csv, read_users_csv, write_parquet, write_csv
from data_workflow.joins import safe_left_join
from data_workflow.quality import assert_non_empty, assert_unique_key, require_columns, assert_in_range
from data_workflow.transformers import *
from data_workflow.config import make_paths


def load_inputs(path):
    orders = read_orders_csv(path.raw / "orders.csv")
    users = read_users_csv(path.raw / "users.csv")
    return orders, users


def transform(orders_raw, users):
    require_columns(orders_raw, ["order_id", "user_id", "amount", "quantity", "created_at", "status"])
    require_columns(users, ["user_id", "country", "signup_date"])
    assert_non_empty(orders_raw, "orders")
    assert_non_empty(users, "users")
    
    orders = enforce_schema(orders_raw)
    
    status_clean = normalize_text(orders["status"])
    orders_clean = orders.assign(status_clean=status_clean)
    
    orders_clean = add_missing_flags(orders_clean, cols=["amount", "quantity"])
    
    assert_in_range(orders_clean["amount"], lo=0, name="amount")
    assert_in_range(orders_clean["quantity"], lo=0, name="quantity")
    
    orders_clean = parse_datetime(orders_clean, "created_at")
    orders_clean = add_time_parts(orders_clean, "created_at")
    
    users['user_id'] = users["user_id"].astype("string") 
    assert_unique_key(users, "user_id")
    
    orders_clean['user_id'] = orders_clean['user_id'].astype("string")
    users['user_id'] = users['user_id'].astype("string")# i need to unified the types of user id becuase it will cause error  if i want to merge tables
    joined = safe_left_join(orders_clean, users, on="user_id", validate="many_to_one")
    assert len(joined) == len(orders_clean)
    
    joined['amount'] = winsorize(joined['amount'], lo=0.01, hi=0.99)
    joined = add_outlier_flag(joined, "amount", k=1.2)
    
    return orders, orders_clean, joined


def load_outputs(orders, orders_clean, analytics, users, path, root):
    write_parquet(users, path.processed / "users.parquet")
    write_parquet(orders_clean, path.processed / "orders_clean.parquet")
    write_parquet(analytics, path.processed / "analytics_table.parquet")
    
    orders_report=missingness_report(orders).reset_index()
    orders_report=orders_report.rename(columns={"index": "column"})
    write_csv(orders_report, root / "reports" / "missingness_orders.csv")


def write_run_meta(path, analytics):
    missing_created_at = int(analytics["created_at"].isna().sum())
    country_match_rate = 1.0 - float(analytics["country"].isna().mean())
    
    meta = {
        "timestamp_utc": datetime.now(timezone.utc).isoformat(),
        "rows_out": int(len(analytics)),
        "missing_created_at": missing_created_at,
        "country_match_rate": country_match_rate,
    }
    
    run_meta_path = path.processed / "_run_meta.json"
    run_meta_path.parent.mkdir(parents=True, exist_ok=True)
    run_meta_path.write_text(json.dumps(meta, indent=2), encoding="utf-8")


def run_etl(root):
    path=make_paths(root)
    orders_raw, users=load_inputs(path)
    orders, orders_clean, analytics=transform(orders_raw, users)
    load_outputs(orders, orders_clean, analytics, users, path, root)
    write_run_meta(path, analytics)

