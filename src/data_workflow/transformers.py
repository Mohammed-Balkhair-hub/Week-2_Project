"""Transformers for cleaning and enriching tabular data."""

import pandas as pd


def enforce_schema(df: pd.DataFrame) -> pd.DataFrame:
    """Cast order columns to the expected dtypes.

    Parameters
    ----------
    df : pd.DataFrame
        Orders table with raw types.
    """
    df = df.assign(
        order_id=df["order_id"].astype("string"),
        user_id=df["user_id"].astype("string"),
        amount=pd.to_numeric(df["amount"], errors="coerce").astype("Float64"),
        quantity=pd.to_numeric(df["quantity"], errors="coerce").astype("Int64"),
    )
    return df


def missingness_report(df: pd.DataFrame) -> pd.DataFrame:
    """Summarise missing values per column.

    Parameters
    ----------
    df : pd.DataFrame
        Table to inspect.
    """
    return (
        df.isna()
        .sum()
        .rename("n_missing")
        .to_frame()
        .assign(p_missing=lambda t: t["n_missing"] / len(df))
        .sort_values("p_missing", ascending=False)
    )


def add_missing_flags(df: pd.DataFrame, cols: list[str]) -> pd.DataFrame:
    """Add boolean `<col>__isna` flags for selected columns.

    Parameters
    ----------
    df : pd.DataFrame
        Input table.
    cols : list[str]
        Columns to flag for missingness.
    """
    out = df.copy()
    for c in cols:
        out[f"{c}__isna"] = out[c].isna()
    return out


def normalize_text(s: pd.Series) -> pd.Series:
    """Trim, lowercase, and collapse whitespace in a text Series.

    Parameters
    ----------
    s : pd.Series
        Text values to normalise.
    """
    s = s.str.strip().str.casefold().str.replace(r"\s+", " ", regex=True)
    return s


def apply_mapping(s: pd.Series, mapping: dict[str, str]) -> pd.Series:
    """Map values using a dict, leaving unmapped values unchanged.

    Parameters
    ----------
    s : pd.Series
        Input values.
    mapping : dict[str, str]
        Old → new value mapping.
    """
    return s.map(lambda x: mapping.get(x, x))


def dedupe_keep_latest(df: pd.DataFrame, key_cols: list[str], ts_col: str) -> pd.DataFrame:
    """Drop duplicates, keeping the latest row per key based on a timestamp.

    Parameters
    ----------
    df : pd.DataFrame
        Input table.
    key_cols : list[str]
        Columns that define the key.
    ts_col : str
        Timestamp column used to pick the latest row.
    """
    df = df.sort_values(by=[ts_col], ascending=False)
    df = df.drop_duplicates(keep="first", ignore_index=True)
    return df


def parse_datetime(df: pd.DataFrame, col: str, *, utc: bool = True) -> pd.DataFrame:
    """Convert a text column to pandas datetime.

    Parameters
    ----------
    df : pd.DataFrame
        Input table.
    col : str
        Name of the column to parse.
    utc : bool, default True
        If True, convert to UTC-aware timestamps.
    """
    dt = pd.to_datetime(df[col], errors="coerce", utc=utc)
    return df.assign(**{col: dt})


def add_time_parts(df: pd.DataFrame, ts_col: str) -> pd.DataFrame:
    """Add simple time features (date, year, month, dow, hour) from a timestamp.

    Parameters
    ----------
    df : pd.DataFrame
        Input table.
    ts_col : str
        Timestamp column to expand.
    """
    ts = df[ts_col]
    return df.assign(
        date=ts.dt.date,
        year=ts.dt.year,
        month=ts.dt.to_period("M").astype("string"),
        dow=ts.dt.day_name(),
        hour=ts.dt.hour,
    )


def iqr_bounds(s: pd.Series, k: float = 1.5) -> tuple[float, float]:
    """Compute lower/upper bounds using the IQR rule.

    Parameters
    ----------
    s : pd.Series
        Numeric values.
    k : float, default 1.5
        IQR multiplier (larger → wider bounds).
    """
    q1 = s.quantile(0.25)
    q3 = s.quantile(0.75)
    iqr = q3 - q1
    return float(q1 - k * iqr), float(q3 + k * iqr)


def winsorize(s: pd.Series, lo: float = 0.01, hi: float = 0.99) -> pd.Series:
    """Clip extreme values based on lower/upper quantiles.

    Parameters
    ----------
    s : pd.Series
        Numeric values.
    lo : float, default 0.01
        Lower quantile in [0, 1].
    hi : float, default 0.99
        Upper quantile in [0, 1].
    """
    a, b = s.quantile(lo), s.quantile(hi)
    return s.clip(lower=a, upper=b)


def add_outlier_flag(df: pd.DataFrame, col: str, *, k: float = 1.5) -> pd.DataFrame:
    """Add a `{col}__is_outlier` flag using IQR bounds.

    Parameters
    ----------
    df : pd.DataFrame
        Input table.
    col : str
        Numeric column to flag.
    k : float, default 1.5
        IQR multiplier used to define outliers.
    """
    s = df[col]
    lo, hi = iqr_bounds(s.dropna(), k=k)
    flag = (s < lo) | (s > hi)
    return df.assign(**{f"{col}__is_outlier": flag})