import json
from pathlib import Path

import pandas as pd

from prodigy.data.parquet_store import write_daily_partition


NOTEBOOKS = [
    "00_data_check.ipynb",
    "01_example_momentum_factor.ipynb",
    "02_example_funding_factor.ipynb",
    "03_example_volatility_factor.ipynb",
    "99_combine_example_factors.ipynb",
]


def load_source(path):
    data = json.loads(path.read_text())
    return "\n".join("".join(cell.get("source", [])) for cell in data["cells"])


def load_code_cells(path):
    data = json.loads(path.read_text())
    return [
        "".join(cell.get("source", []))
        for cell in data["cells"]
        if cell.get("cell_type") == "code"
    ]


def test_research_notebooks_exist_and_use_shared_data_backtester():
    root = Path("research/notebooks")
    for name in NOTEBOOKS:
        path = root / name
        assert path.exists(), name
        source = load_source(path)
        assert "load_ohlcv" in source
        assert "Backtester" in source


def test_combine_notebook_builds_example_features():
    source = load_source(Path("research/notebooks/99_combine_example_factors.ipynb"))

    assert "example_momentum" in source
    assert "example_funding" in source
    assert "example_volatility" in source
    assert "example_features.parquet.gzip" in source


def test_combine_notebook_feature_pipeline_executes_on_fixture(tmp_path, monkeypatch):
    notebook_path = Path("research/notebooks/99_combine_example_factors.ipynb").resolve()
    notebook_dir = tmp_path / "research" / "notebooks"
    notebook_dir.mkdir(parents=True)

    ts = pd.date_range("2024-01-01", periods=96, freq="15min", tz="UTC")
    close = pd.Series([100 + i * 0.1 for i in range(len(ts))])
    ohlcv = pd.DataFrame(
        {
            "timestamp": ts,
            "symbol": ["ETH/USDT:USDT"] * len(ts),
            "open": close,
            "high": close + 1,
            "low": close - 1,
            "close": close,
            "volume": [10.0] * len(ts),
        }
    )
    funding = pd.DataFrame(
        {
            "timestamp": ts,
            "symbol": ["ETH/USDT:USDT"] * len(ts),
            "funding_time": ts,
            "funding_rate": [0.0001 + (i % 5) * 0.00001 for i in range(len(ts))],
            "raw_symbol": ["ETHUSDT"] * len(ts),
        }
    )

    write_daily_partition(
        ohlcv,
        tmp_path / "data",
        "bitget",
        "ETH/USDT:USDT",
        "ohlcv",
        "2024-01-01",
        "15m",
    )
    write_daily_partition(
        funding,
        tmp_path / "data",
        "bitget",
        "ETH/USDT:USDT",
        "funding_rates",
        "2024-01-01",
    )

    monkeypatch.chdir(notebook_dir)
    namespace = {}
    for source in load_code_cells(notebook_path):
        exec(compile(source, str(notebook_path), "exec"), namespace)
        if "features.to_parquet" in source:
            break

    output = tmp_path / "data" / "processed" / "example_features.parquet.gzip"
    assert output.exists()
    example_features = pd.read_parquet(output)
    assert not example_features.empty
    assert {
        "example_momentum",
        "example_funding",
        "example_volatility",
    }.issubset(example_features.columns)
