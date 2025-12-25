import pandas as pd
from pandas import DataFrame


def safe_left_join(left: DataFrame, right: DataFrame, on: str | list[str], *,validate: str = "many_to_one",suffixes: tuple[str, str] = ("_left", "_right"), ) -> DataFrame:
    return pd.merge(left,right,how="left",on=on,validate=validate,suffixes=suffixes,) # i mean... however