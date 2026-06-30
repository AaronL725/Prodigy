from __future__ import annotations

import pandas as pd


FACTOR_COLUMNS = ["timestamp", "symbol", "factor_name", "value"]


def factor_frame(source: pd.DataFrame, factor_name: str, value: pd.Series) -> pd.DataFrame:
    return pd.DataFrame(
        {
            "timestamp": source["timestamp"],
            "symbol": source["symbol"],
            "factor_name": factor_name,
            "value": value.astype("float64"),
        }
    )[FACTOR_COLUMNS]
