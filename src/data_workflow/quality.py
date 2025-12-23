"""Simple data quality assertion helpers."""

import pandas as pd


def require_columns(df: pd.DataFrame, cols: list[str]) -> None:
    """Assert that all required column names exist in the DataFrame.

    Parameters
    ----------
    df : pd.DataFrame
        Input table to check.
    cols : list[str]
        Column names that must be present.
    """
    missing = [c for c in cols if c not in df.columns]
    assert not missing, f"{missing} is missing"


def assert_non_empty(df: pd.DataFrame, name: str = "df") -> None:
    """Assert that the DataFrame has at least one row.

    Parameters
    ----------
    df : pd.DataFrame
        Input table to check.
    name : str
        Name used in the error message.
    """
    assert len(df) > 0, f"{name} is empty"


def assert_unique_key(df: pd.DataFrame, key: str, *, allow_na: bool = False) -> None:
    """Assert that a column can be used as a unique key (no duplicates).

    Parameters
    ----------
    df : pd.DataFrame
        Input table to check.
    key : str
        Column that should be unique.
    allow_na : bool, default False
        If False, NAs are not allowed in the key.
    """
    if not allow_na:
        assert df[key].notna().all(), f"{key} contains NA"
    dup = df[key].duplicated(keep=False) & df[key].notna()
    assert not dup.any(), f"{key} not unique; {dup.sum()} duplicate rows"


def assert_in_range(s: pd.Series, lo=None, hi=None, name: str = "value") -> None:
    """Assert that values lie between optional lower/upper bounds.

    Parameters
    ----------
    s : pd.Series
        Series of numeric values.
    lo : float, optional
        Minimum allowed value (inclusive).
    hi : float, optional
        Maximum allowed value (inclusive).
    name : str
        Label used in the error message.
    """
    x = s.dropna()
    if lo is not None:
        assert (x >= lo).all(), f"{name} below {lo}"
    if hi is not None:
        assert (x <= hi).all(), f"{name} above {hi}"