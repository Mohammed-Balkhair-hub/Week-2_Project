"""Safe join helpers built on top of pandas.merge."""

import pandas as pd
from pandas import DataFrame


def safe_left_join(
    left: DataFrame,
    right: DataFrame,
    on: str | list[str],
    *,
    validate: str = "many_to_one",
    suffixes: tuple[str, str] = ("_left", "_right"),
) -> DataFrame:
    """Left join with a `validate` check to prevent join explosions.

    Parameters
    ----------
    left : DataFrame
        Left-hand table; all its rows are kept.
    right : DataFrame
        Right-hand table to join onto `left`.
    on : str | list[str]
        Join key column(s).
    validate : str, default "many_to_one"
        pandas merge `validate` rule (e.g. "many_to_one").
    suffixes : tuple[str, str], default ("_left", "_right")
        Suffixes for overlapping column names.
    """
    return pd.merge(
        left,
        right,
        how="left",
        on=on,
        validate=validate,
        suffixes=suffixes,
    )