"""Small helpers to load and save data frames."""

import pandas as pd
from pathlib import Path

# Common NA markers used when reading CSV files
na_values = ["", "NA", "N/A", "null", "None"]


def read_orders_csv(path: Path) -> pd.DataFrame:
    """Read the orders CSV with basic dtypes and NA handling.

    Parameters
    ----------
    path : Path
        Location of `orders.csv`.
    """
    df = pd.read_csv(
        path,
        dtype={"order_id": "string", "user_id": "string"},
        na_values=na_values,
        keep_default_na=True,
    )
    return df


def read_users_csv(path: Path) -> pd.DataFrame:
    """Read the users CSV with basic dtypes and NA handling.

    Parameters
    ----------
    path : Path
        Location of `users.csv`.
    """
    df = pd.read_csv(
        path,
        dtype={"order_id": "string"},
        na_values=na_values,
        keep_default_na=True,
    )
    return df


def write_parquet(df: pd.DataFrame, path: Path) -> None:
    """Write a DataFrame to parquet, creating parent folders if needed.

    Parameters
    ----------
    df : pd.DataFrame
        Data to save.
    path : Path
        Output parquet path.
    """
    path.parent.mkdir(parents=True, exist_ok=True)
    df.to_parquet(path, index=False)


def write_csv(df: pd.DataFrame, path: Path) -> None:
    """Write a DataFrame to CSV, creating parent folders if needed.

    Parameters
    ----------
    df : pd.DataFrame
        Data to save.
    path : Path
        Output CSV path.
    """
    path.parent.mkdir(parents=True, exist_ok=True)
    df.to_csv(path, index=False)


def read_parquet(path: Path) -> pd.DataFrame:
    """Read a parquet file into a DataFrame.

    Parameters
    ----------
    path : Path
        Parquet file path.
    """
    return pd.read_parquet(path)