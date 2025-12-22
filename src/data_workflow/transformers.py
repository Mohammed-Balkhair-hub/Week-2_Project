import pandas as pd




def enforce_schema(df : pd.DataFrame) -> pd.DataFrame:
    df =df.assign(
    order_id=df["order_id"].astype("string"),
    user_id=df["user_id"].astype("string"),
    amount=pd.to_numeric(df["amount"], errors="coerce").astype("Float64"),
    quantity=pd.to_numeric(df["quantity"], errors="coerce").astype("Int64"),
    )
    return df




import pandas as pd

def missingness_report(df: pd.DataFrame) -> pd.DataFrame:
    return (
        df.isna().sum()
        .rename("n_missing")
        .to_frame()
        .assign(p_missing=lambda t: t["n_missing"] / len(df))
        .sort_values("p_missing", ascending=False)
    )

def add_missing_flags(df: pd.DataFrame, cols: list[str]) -> pd.DataFrame:
    out = df.copy()
    for c in cols:
        out[f"{c}__isna"] = out[c].isna()
    return out



def normalize_text(s: pd.Series) -> pd.Series:


    s=s.str.strip().str.casefold().str.replace(r'\s+',' ',regex=True)
    return s


def apply_mapping(s: pd.Series, mapping: dict[str, str]) -> pd.Series:
    return s.map(lambda x: mapping.get(x, x)) # second x to reamin those dont have a key unchanged




def dedupe_keep_latest(df: pd.DataFrame, key_cols: list[str], ts_col: str) -> pd.DataFrame:
    df=df.sort_values(by=[ts_col])
    df=df.drop_duplicates( keep='first')