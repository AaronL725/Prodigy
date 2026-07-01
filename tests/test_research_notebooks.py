import json
from pathlib import Path


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
