import pandas as pd
from pathlib import Path







def read_orders_csv(path: Path) -> pd.DataFrame:
    df=pd.read_csv(
    path,
    dtype={"order_id": "string", "user_id": "string"},
    na_values=na_values,
    keep_default_na=True,
)
    return df







na_values=["", "NA", "N/A", "null", "None"]

def read_users_csv(path: Path) -> pd.DataFrame:
    df=pd.read_csv(
    path,
    dtype={"order_id": "string"},
    na_values=na_values,
    keep_default_na=True,
)
    return df
 



def write_parquet(df: pd.DataFrame, path: Path) -> None:

    path.parent.mkdir(parents=True, exist_ok=True)
    df.to_parquet(path,index=False)# need a cache check later


def write_csv(df: pd.DataFrame, path: Path) -> None:

    path.parent.mkdir(parents=True, exist_ok=True)
    df.to_csv(path,index=False)# need a cache check later





def read_parquet(path: Path) -> pd.DataFrame:
    return pd.read_parquet(path)
