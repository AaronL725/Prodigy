import pandas as pd

from prodigy.research.signals import SignalParams, score_to_lot_signals
from prodigy.research.simulator import BacktestParams, simulate_lots


def prices():
    ts = pd.date_range("2026-07-01", periods=12, freq="15min", tz="UTC")
    return pd.DataFrame(
        {
            "timestamp": ts,
            "symbol": ["ETH/USDT:USDT"] * len(ts),
            "open": [100, 101, 102, 103, 104, 105, 104, 103, 102, 101, 100, 99],
            "high": [101, 102, 103, 104, 105, 106, 105, 104, 103, 102, 101, 100],
            "low": [99, 100, 101, 102, 103, 104, 103, 102, 101, 100, 99, 98],
            "close": [101, 102, 103, 104, 105, 104, 103, 102, 101, 100, 99, 98],
            "volume": [10] * len(ts),
        }
    )


def test_simulator_opens_and_closes_lot_with_fees():
    scores = pd.DataFrame(
        {
            "timestamp": pd.date_range("2026-07-01", periods=3, freq="15min", tz="UTC"),
            "symbol": ["ETH/USDT:USDT"] * 3,
            "score": [0.8, 0.0, -0.2],
        }
    )
    signals = score_to_lot_signals(scores, SignalParams(total_notional_cap=10_000))

    result = simulate_lots(prices(), pd.DataFrame(), signals, BacktestParams(initial_equity=1000))

    assert len(result.trades) == 1
    assert result.trades.iloc[0]["exit_reason"] == "opposite_signal"
    assert result.equity_curve["equity"].iloc[-1] != 1000


def test_simulator_stop_loss_closes_lot():
    scores = pd.DataFrame(
        {
            "timestamp": [pd.Timestamp("2026-07-01T00:00:00Z")],
            "symbol": ["ETH/USDT:USDT"],
            "score": [1.0],
        }
    )
    signals = score_to_lot_signals(scores, SignalParams(total_notional_cap=10_000))
    params = BacktestParams(initial_equity=1000, stop_loss_position_notional_fraction=0.01)

    result = simulate_lots(prices(), pd.DataFrame(), signals, params)

    assert "stop_loss" in set(result.trades["exit_reason"])


def _rising_prices(n: int = 5) -> pd.DataFrame:
    # Deterministic gently-rising close so a long carries a small positive gross.
    ts = pd.date_range("2026-07-01", periods=n, freq="15min", tz="UTC")
    closes = [100.0 + i for i in range(n)]
    return pd.DataFrame(
        {
            "timestamp": ts,
            "symbol": ["ETH/USDT:USDT"] * n,
            "open": closes,
            "high": [c + 0.5 for c in closes],
            "low": [c - 0.5 for c in closes],
            "close": closes,
            "volume": [10] * n,
        }
    )


def _open_and_close_signals(open_ts, close_ts, score=0.8) -> pd.DataFrame:
    # Open a LONG at open_ts, close it via opposite signal at close_ts.
    return pd.DataFrame(
        {
            "timestamp": [open_ts, close_ts],
            "symbol": ["ETH/USDT:USDT", "ETH/USDT:USDT"],
            "action": ["open", "close"],
            "side": ["long", "long"],
            "score": [score, -0.8],
            "notional": [1000.0, 1000.0],
            "lot_id": ["lot-000001", "lot-000001"],
            "reason": ["open_threshold", "close_opposite"],
        }
    )


def test_funding_cost_charges_long_on_positive_funding():
    # Construction: rising 5-bar prices; long opened at bar0, closed at bar4 by an
    # opposite signal. One funding row with positive funding_rate inside the
    # holding window. Run twice (funded vs empty funding); only funding differs.
    # Stop/trailing set so they cannot fire; only funding drives the difference.
    prices_df = _rising_prices(5)
    open_ts = prices_df["timestamp"].iloc[0]
    close_ts = prices_df["timestamp"].iloc[-1]
    signals = _open_and_close_signals(open_ts, close_ts)
    params = BacktestParams(
        initial_equity=1000,
        stop_loss_position_notional_fraction=1.0,  # disable stop-loss
        trailing_start_position_notional_fraction=1.0,  # disable trailing
    )
    funding_funded = pd.DataFrame(
        {
            "timestamp": [prices_df["timestamp"].iloc[2]],
            "symbol": ["ETH/USDT:USDT"],
            "funding_rate": [0.001],  # positive funding: longs pay shorts
        }
    )

    funded = simulate_lots(prices_df, funding_funded, signals, params)
    unfunded = simulate_lots(prices_df, pd.DataFrame(), signals, params)

    long_pnl_funded = float(funded.trades.iloc[0]["pnl"])
    long_pnl_unfunded = float(unfunded.trades.iloc[0]["pnl"])
    # Positive funding -> long's realized PnL is LOWER with funding than without.
    assert long_pnl_funded < long_pnl_unfunded


def test_holding_review_extends_by_one_hour_with_confirming_score():
    # Construction: 104 bars (15m). A long opens at bar0 with score 0.8.
    # max_holding_hours=24 -> review at bar 96. At bar96 a confirming
    # same-direction open signal (score 0.8, action="open") is present, so the
    # review extends the lot by extension_hours=1h (4 bars) -> next review at
    # bar 100. With confirming score the lot stays open past bar 96 (exit bar >
    # 96); without a confirming score the lot closes at bar 96 with reason
    # "holding". Signals are written directly (not via score_to_lot_signals) for
    # deterministic control of which bars carry signals.
    n = 104
    ts = pd.date_range("2026-07-01", periods=n, freq="15min", tz="UTC")
    closes = [100.0 + i * 0.01 for i in range(n)]  # near-flat so trailing/stop never fire
    prices_df = pd.DataFrame(
        {
            "timestamp": ts,
            "symbol": ["ETH/USDT:USDT"] * n,
            "open": closes,
            "high": [c + 0.001 for c in closes],
            "low": [c - 0.001 for c in closes],
            "close": closes,
            "volume": [10] * n,
        }
    )
    params = BacktestParams(
        initial_equity=1000,
        stop_loss_position_notional_fraction=1.0,
        trailing_start_position_notional_fraction=1.0,
    )

    def signals_with(confirming_score_at_96):
        rows = [
            {
                "timestamp": ts[0],
                "symbol": "ETH/USDT:USDT",
                "action": "open",
                "side": "long",
                "score": 0.8,
                "notional": 1000.0,
                "lot_id": "lot-000001",
                "reason": "open_threshold",
            }
        ]
        if confirming_score_at_96 is not None:
            rows.append(
                {
                    "timestamp": ts[96],
                    "symbol": "ETH/USDT:USDT",
                    "action": "open",
                    "side": "long",
                    "score": confirming_score_at_96,
                    "notional": 1000.0,
                    "lot_id": "lot-000002",
                    "reason": "open_threshold",
                }
            )
        return pd.DataFrame(
            rows,
            columns=[
                "timestamp",
                "symbol",
                "action",
                "side",
                "score",
                "notional",
                "lot_id",
                "reason",
            ],
        )

    # WITH confirming score at bar96: the review extends the lot by 1h (4 bars),
    # so the next review is at bar 100. No confirming score at bar 100 -> the lot
    # closes there with reason "holding". The decisive evidence: exit bar (100) is
    # strictly greater than the review bar (96), proving the 1h extension fired
    # rather than a close-at-review. (Old buggy code reset open_bar=i, which would
    # have postponed the next review to bar 96+96=192, far beyond our 104 bars, so
    # the lot would have stayed open at data end -> no trade row at all.)
    result_confirmed = simulate_lots(prices_df, pd.DataFrame(), signals_with(0.8), params)
    lot1_confirmed = result_confirmed.trades[
        result_confirmed.trades["lot_id"] == "lot-000001"
    ]
    assert not lot1_confirmed.empty, "lot-000001 should close within data (at bar 100)"
    exit_bar_confirmed = ts.get_loc(pd.Timestamp(lot1_confirmed.iloc[0]["exit_ts"]))
    assert exit_bar_confirmed == 100
    assert exit_bar_confirmed > 96

    # WITHOUT confirming score at bar96: lot-000001 closes at bar96 reason "holding".
    result_no_confirm = simulate_lots(
        prices_df, pd.DataFrame(), signals_with(None), params
    )
    lot1_no = result_no_confirm.trades[
        result_no_confirm.trades["lot_id"] == "lot-000001"
    ].iloc[0]
    assert lot1_no["exit_reason"] == "holding"
    assert ts.get_loc(pd.Timestamp(lot1_no["exit_ts"])) == 96
