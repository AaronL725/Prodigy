from prodigy.cli.signal import build_parser
from prodigy.config import load_config


def test_signal_parser_supports_once_and_defaults():
    args = build_parser().parse_args(["--once"])

    assert args.once is True
    assert args.daemon is False
    assert args.db == "var/prodigy.sqlite"
    assert args.data_root == "data"
    # --signal-source defaults to None so the config's signal_source is honored
    # when the flag is omitted (resolved + validated in resolve_signal_source).
    assert args.signal_source is None


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
    assert cfg["signal"]["total_notional_cap"] == 10_000


def test_signal_parser_supports_bounded_daemon_loop():
    args = build_parser().parse_args(["--daemon", "--max-loops", "1"])

    assert args.daemon is True
    assert args.max_loops == 1


def test_build_signal_daemon_config_reads_thresholds():
    from prodigy.cli.signal import build_signal_daemon_config

    signal_cfg = {
        "entry_threshold": 0.7,
        "exit_threshold": 0.3,
        "min_order_fraction": 0.06,
        "max_order_fraction": 0.12,
        "max_holding_bars": 48,
        "max_state_age_secs": 60,
        "total_notional_cap": 12345.0,
    }
    cfg = build_signal_daemon_config(signal_cfg)

    assert cfg.total_notional_cap == 12345.0
    assert cfg.entry_threshold == 0.7
    assert cfg.exit_threshold == 0.3
    assert cfg.min_order_fraction == 0.06
    assert cfg.max_order_fraction == 0.12
    assert cfg.max_holding_bars == 48


def test_default_config_round_trips_through_build_signal_daemon_config():
    from prodigy.cli.signal import build_signal_daemon_config

    signal_cfg = load_config("configs/default.toml")["signal"]
    built = build_signal_daemon_config(signal_cfg)

    assert built.entry_threshold == 0.6
    assert built.max_holding_bars == 96


def test_daemon_loop_exits_on_stop_flag_and_writes_shutdown_event(tmp_path):
    # Spec: --daemon "exits cleanly on SIGINT or SIGTERM after writing a shutdown
    # event when possible." The loop checks an injected stop flag between
    # iterations; on stop it writes a 'signal' shutdown event and returns.
    from prodigy.cli.signal import build_parser, run_daemon_loop
    from prodigy.db import connect, init_db

    db_path = tmp_path / "prodigy.sqlite"
    with connect(db_path) as conn:
        init_db(conn)
        # fresh-enough equity snapshot so run_once is not skipped as stale
        conn.execute(
            """
            insert into equity_snapshots (
              snapshot_id, created_at, equity, available_margin, unrealized_pnl, realized_pnl_24h
            ) values ('s1', '2026-07-04T10:15:30Z', 1000, 1000, 0, 0)
            """
        )
        conn.commit()

    args = build_parser().parse_args(["--daemon", "--db", str(db_path)])
    signal_cfg = load_config("configs/default.toml")["signal"]

    iterations = {"n": 0}

    def stop_flag() -> bool:
        # Stop after the first completed iteration.
        return iterations["n"] >= 1

    def refresh_data() -> None:
        pass

    def score_loader():
        import pandas as pd

        from prodigy.signals.daemon import expected_closed_bar_ts

        iterations["n"] += 1
        return 1.0, expected_closed_bar_ts(pd.Timestamp("2026-07-04T10:16:00Z"), "15m")

    rc = run_daemon_loop(
        args,
        signal_cfg=signal_cfg,
        db_path=db_path,
        source="dummy-cycle",
        refresh_data=refresh_data,
        score_loader=score_loader,
        stop_flag=stop_flag,
        now_factory=lambda: __import__("pandas").Timestamp("2026-07-04T10:16:00Z"),
        sleep=lambda _s: None,
        clock=lambda: __import__("pandas").Timestamp("2026-07-04T10:16:00Z"),
    )

    assert rc == 0
    with connect(db_path) as conn:
        evt = conn.execute(
            "select severity, component, message from events where component = 'signal' and message like 'shutdown%'"
        ).fetchone()
    assert evt is not None, "daemon loop must write a shutdown event on stop"


def test_daemon_loop_stops_without_extra_run_once_after_sleep(tmp_path):
    # Spec: after SIGINT/SIGTERM the daemon stops accepting new intents / opening
    # new orders — it must NOT start another run_once. The stop flag can fire
    # during sleep; the loop must check it at the top of the next iteration (and
    # right after sleep) instead of barreling into another run_once.
    from prodigy.cli.signal import build_parser, run_daemon_loop
    from prodigy.db import connect, init_db

    db_path = tmp_path / "prodigy.sqlite"
    with connect(db_path) as conn:
        init_db(conn)
        conn.execute(
            """
            insert into equity_snapshots (
              snapshot_id, created_at, equity, available_margin, unrealized_pnl, realized_pnl_24h
            ) values ('s1', '2026-07-04T10:15:30Z', 1000, 1000, 0, 0)
            """
        )
        conn.commit()

    args = build_parser().parse_args(["--daemon", "--db", str(db_path)])
    signal_cfg = load_config("configs/default.toml")["signal"]

    score_calls = {"n": 0}
    now_calls = {"n": 0}  # counts run_once invocations (now_factory is called first thing)
    stop = {"v": False}

    def refresh_data() -> None:
        pass

    def score_loader():
        import pandas as pd

        from prodigy.signals.daemon import expected_closed_bar_ts

        score_calls["n"] += 1
        return 1.0, expected_closed_bar_ts(
            pd.Timestamp("2026-07-04T10:16:00Z"), "15m"
        )

    def now_factory():
        import pandas as pd

        now_calls["n"] += 1
        return pd.Timestamp("2026-07-04T10:16:00Z")

    def sleeping(_secs):
        # Signal fires while the loop is sleeping after iteration 1.
        stop["v"] = True

    rc = run_daemon_loop(
        args,
        signal_cfg=signal_cfg,
        db_path=db_path,
        source="dummy-cycle",
        refresh_data=refresh_data,
        score_loader=score_loader,
        stop_flag=lambda: stop["v"],
        now_factory=now_factory,
        sleep=sleeping,
        clock=lambda: __import__("pandas").Timestamp("2026-07-04T10:16:00Z"),
    )

    assert rc == 0
    # now_factory is invoked once per run_once; a stop flag during sleep must
    # not let the loop start a second run_once.
    assert now_calls["n"] == 1, (
        f"stop flag during sleep must not trigger a second run_once (got {now_calls['n']})"
    )


def test_resolve_signal_source_prefers_cli_then_config():
    from prodigy.cli.signal import resolve_signal_source

    # CLI flag wins over config.
    assert resolve_signal_source("dummy-cycle", "example-factors") == "dummy-cycle"
    # No CLI flag -> fall back to config value.
    assert resolve_signal_source(None, "example-factors") == "example-factors"


def test_resolve_signal_source_rejects_unknown_source():
    from prodigy.cli.signal import resolve_signal_source

    # A typo must NOT silently fall back to example-factors scoring while
    # keeping the typo as the source/idempotency key — reject it loudly.
    import pytest

    with pytest.raises(ValueError):
        resolve_signal_source("example-factor", "example-factors")
    with pytest.raises(ValueError):
        resolve_signal_source(None, "bogus-source")
