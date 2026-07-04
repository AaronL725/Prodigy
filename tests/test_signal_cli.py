from prodigy.cli.signal import build_parser
from prodigy.config import load_config


def test_signal_parser_supports_once_and_defaults():
    args = build_parser().parse_args(["--once"])

    assert args.once is True
    assert args.daemon is False
    assert args.db == "var/prodigy.sqlite"
    assert args.data_root == "data"
    assert args.signal_source == "example-factors"


def test_signal_parser_rejects_once_and_daemon_together():
    parser = build_parser()

    try:
        parser.parse_args(["--once", "--daemon"])
    except SystemExit as exc:
        assert exc.code != 0
    else:
        raise AssertionError("parser should reject --once and --daemon together")


def test_default_config_has_signal_section():
    cfg = load_config("configs/default.toml")

    assert cfg["signal"]["enabled_symbols"] == ["ETH/USDT:USDT"]
    assert cfg["signal"]["exchange_symbols"]["ETH/USDT:USDT"] == "ETHUSDT"
    assert cfg["signal"]["timeframe"] == "15m"
    assert cfg["signal"]["signal_source"] == "example-factors"
    assert cfg["signal"]["max_state_age_secs"] == 120
    assert cfg["signal"]["poll_interval_secs"] == 30
    assert cfg["signal"]["entry_threshold"] == 0.6
    assert cfg["signal"]["exit_threshold"] == 0.2
    assert cfg["signal"]["min_order_fraction"] == 0.05
    assert cfg["signal"]["max_order_fraction"] == 0.10
    assert cfg["signal"]["max_holding_bars"] == 96
