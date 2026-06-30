from pathlib import Path

from prodigy.config import load_config


def test_load_default_config():
    cfg = load_config(Path("configs/default.toml"))

    assert cfg["trading"]["enabled_symbols"] == ["ETH/USDT:USDT"]
    assert cfg["trading"]["leverage"] == 5
    assert cfg["risk"]["total_notional_cap_x_equity"] == 5.0
    assert cfg["risk"]["per_order_cap_fraction_of_total"] == 0.10
    assert cfg["execution"]["open_maker_timeout_seconds"] == 15


def test_config_rejects_missing_top_level_section(tmp_path):
    path = tmp_path / "bad.toml"
    path.write_text("[trading]\nleverage = 5\n")

    try:
        load_config(path)
    except ValueError as exc:
        message = str(exc)
    else:
        message = ""

    assert "missing config section: risk" in message
