import numpy as np
import pandas as pd

def bootstrap_diff_means(a: pd.Series, b: pd.Series, *, n_boot: int = 2000, seed: int = 0) -> dict[str, float]:
    """Compute bootstrap confidence interval for difference in means."""
    a_clean = pd.to_numeric(a, errors="coerce").dropna().to_numpy()
    b_clean = pd.to_numeric(b, errors="coerce").dropna().to_numpy()
    
    assert len(a_clean) > 0 and len(b_clean) > 0, "Empty group after cleaning"
    
    rng = np.random.default_rng(seed)
    diffs = []
    for _ in range(n_boot):
        sa = rng.choice(a_clean, size=len(a_clean), replace=True)
        sb = rng.choice(b_clean, size=len(b_clean), replace=True)
        diffs.append(sa.mean() - sb.mean())
    
    diffs = np.array(diffs)
    return {
        "diff_mean": float(a_clean.mean() - b_clean.mean()),
        "ci_low": float(np.quantile(diffs, 0.025)),
        "ci_high": float(np.quantile(diffs, 0.975)),
    }