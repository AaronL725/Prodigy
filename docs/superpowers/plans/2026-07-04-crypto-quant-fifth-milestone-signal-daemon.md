# Crypto Quant Fifth Milestone Signal Daemon Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Build `prodigy-signal`, a thin Python daemon that writes demo `open`/`close` `trade_intents` from closed 15m example-factor signals, while Rust remains the only Bitget execution component.

**Architecture:** Keep M5 small. Python refreshes existing official-free parquet data, evaluates a closed-bar score, reads SQLite authority state, and writes an idempotent intent. Rust keeps execution ownership; M5 adds only the minimal Rust change for `action=close` to resolve and close the full current exchange position.

**Tech Stack:** Python 3.11+, pandas, sqlite3, existing `prodigy` modules, Rust `prodigy-executor`, SQLite WAL.

---

## Reference Spec

- `docs/superpowers/specs/2026-07-04-crypto-quant-fifth-milestone-signal-daemon-design.md`

Hard constraints:

- Do not implement live trading.
- Do not add Redis, Kafka, FastAPI, actor framework, event bus, or a separate service.
- Do not make Python call Bitget account, position, order, or execution APIs.
- Do not write `reverse`, `reduce`, or `cancel` intents.
- Do not create a new processed-bar table; use existing `executor_state`.
- Use TDD for each task.
- Keep changes small and local.

## File Map

- Modify: `pyproject.toml` - add `prodigy-signal` console script.
- Modify: `configs/default.toml` - add `[signal]` defaults.
- Modify: `src/prodigy/signals/intents.py` - add a no-commit insert helper while preserving existing `write_trade_intent`.
- Create: `src/prodigy/signals/state.py` - SQLite state reads and `executor_state` marker helpers.
- Create: `src/prodigy/signals/daemon.py` - signal config, closed-bar scoring, decision logic, `run_once`, daemon loop.
- Create: `src/prodigy/cli/signal.py` - CLI parser and entrypoint.
- Modify: `crates/executor/src/executor.rs` - full-position close sizing for `action=close`.
- Test: `tests/test_trade_intents.py`
- Create: `tests/test_signal_state.py`
- Create: `tests/test_signal_daemon.py`
- Create: `tests/test_signal_cli.py`
- Modify: `tests/test_executor_integration.py` only if needed for an end-to-end M5 smoke test.

## Task 1: Add Signal Config And CLI Skeleton

**Files:**
- Modify: `pyproject.toml`
- Modify: `configs/default.toml`
- Create: `src/prodigy/cli/signal.py`
- Create: `tests/test_signal_cli.py`

- [ ] **Step 1: Write failing CLI/config tests**

Create `tests/test_signal_cli.py`:

```python
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
```

- [ ] **Step 2: Run tests to verify they fail**

Run:

```bash
mamba run -n quantmamba python -m pytest -q tests/test_signal_cli.py
```

Expected: FAIL because `prodigy.cli.signal` and `[signal]` do not exist.

- [ ] **Step 3: Add config defaults**

Modify `configs/default.toml`:

```toml
[signal]
enabled_symbols = ["ETH/USDT:USDT"]
exchange_symbols = { "ETH/USDT:USDT" = "ETHUSDT" }
timeframe = "15m"
signal_source = "example-factors"
max_state_age_secs = 120
poll_interval_secs = 30
entry_threshold = 0.60
exit_threshold = 0.20
min_order_fraction = 0.05
max_order_fraction = 0.10
max_holding_bars = 96
```

Modify `src/prodigy/config.py`:

```python
REQUIRED_SECTIONS = (
    "trading",
    "risk",
    "execution",
    "fees",
    "model",
    "telegram",
    "signal",
)
```

- [ ] **Step 4: Add CLI skeleton**

Create `src/prodigy/cli/signal.py`:

```python
from __future__ import annotations

import argparse


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(prog="prodigy-signal")
    mode = parser.add_mutually_exclusive_group()
    mode.add_argument("--once", action="store_true")
    mode.add_argument("--daemon", action="store_true")
    parser.add_argument("--config", default="configs/default.toml")
    parser.add_argument("--db", default="var/prodigy.sqlite")
    parser.add_argument("--data-root", default="data")
    parser.add_argument("--signal-source", default="example-factors")
    parser.add_argument("--max-loops", type=int)
    return parser


def main(argv: list[str] | None = None) -> int:
    args = build_parser().parse_args(argv)
    if not args.once and not args.daemon:
        args.once = True
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
```

Modify `pyproject.toml`:

```toml
[project.scripts]
prodigy-data = "prodigy.cli.data:main"
prodigy-ml = "prodigy.cli.ml:main"
prodigy-signal = "prodigy.cli.signal:main"
```

- [ ] **Step 5: Run tests**

Run:

```bash
mamba run -n quantmamba python -m pytest -q tests/test_signal_cli.py
```

Expected: PASS.

- [ ] **Step 6: Commit**

```bash
git add pyproject.toml configs/default.toml src/prodigy/config.py src/prodigy/cli/signal.py tests/test_signal_cli.py
git commit -m "feat: add signal daemon cli skeleton"
```

## Task 2: Add Transaction-Safe Intent And Executor State Helpers

**Files:**
- Modify: `src/prodigy/signals/intents.py`
- Create: `src/prodigy/signals/state.py`
- Modify: `tests/test_trade_intents.py`
- Create: `tests/test_signal_state.py`

- [ ] **Step 1: Write failing no-commit intent test**

Append to `tests/test_trade_intents.py`:

```python
from prodigy.signals.intents import insert_trade_intent


def test_insert_trade_intent_does_not_commit(tmp_path):
    db_path = tmp_path / "prodigy.sqlite"
    intent = TradeIntent(
        intent_id="intent-no-commit",
        created_at="2026-07-04T00:00:00Z",
        symbol="ETHUSDT",
        side="long",
        action="open",
        target_notional=100.0,
        max_order_notional=100.0,
        source="test",
        reason="transaction test",
        model_version="m5-test",
    )

    with connect(db_path) as conn:
        init_db(conn)
        conn.execute("begin")
        insert_trade_intent(conn, intent)
        conn.rollback()
        row = conn.execute(
            "select intent_id from trade_intents where intent_id = ?",
            (intent.intent_id,),
        ).fetchone()

    assert row is None
```

- [ ] **Step 2: Write failing executor_state helper tests**

Create `tests/test_signal_state.py`:

```python
from prodigy.db import connect, init_db
from prodigy.signals.state import (
    get_executor_state,
    has_unfinished_system_order,
    has_unresolved_intent,
    is_manual_override_active,
    set_executor_state,
    signal_processed_key,
)


def test_signal_processed_key_uses_exchange_symbol():
    assert (
        signal_processed_key("example-factors", "ETHUSDT", "15m", "2026-07-04T10:15:00Z")
        == "signal_processed:example-factors:ETHUSDT:15m:2026-07-04T10:15:00Z"
    )


def test_executor_state_round_trip(tmp_path):
    db_path = tmp_path / "prodigy.sqlite"
    with connect(db_path) as conn:
        init_db(conn)
        set_executor_state(conn, "signal_processed:test", "no_signal", "2026-07-04T00:00:00Z")
        row = get_executor_state(conn, "signal_processed:test")

    assert row == "no_signal"


def test_pending_intent_blocks_symbol(tmp_path):
    db_path = tmp_path / "prodigy.sqlite"
    with connect(db_path) as conn:
        init_db(conn)
        conn.execute(
            """
            insert into trade_intents (
              intent_id, created_at, symbol, side, action, target_notional,
              max_order_notional, status, source, reason, model_version
            ) values ('i1', '2026-07-04T00:00:00Z', 'ETHUSDT', 'long', 'open',
                      100, 100, 'pending', 'test', 'x', 'm')
            """
        )
        conn.commit()

        assert has_unresolved_intent(conn, "ETHUSDT") is True
        assert has_unresolved_intent(conn, "BTCUSDT") is False


def test_manual_override_blocks_symbol(tmp_path):
    db_path = tmp_path / "prodigy.sqlite"
    with connect(db_path) as conn:
        init_db(conn)
        set_executor_state(conn, "manual_override:ETHUSDT", "active", "2026-07-04T00:00:00Z")
        assert is_manual_override_active(conn, "ETHUSDT") is True
        assert is_manual_override_active(conn, "BTCUSDT") is False


def test_unfinished_system_order_blocks_symbol(tmp_path):
    db_path = tmp_path / "prodigy.sqlite"
    with connect(db_path) as conn:
        init_db(conn)
        conn.execute(
            """
            insert into trade_intents (
              intent_id, created_at, symbol, side, action, target_notional,
              max_order_notional, status, source, reason, model_version
            ) values ('i1', '2026-07-04T00:00:00Z', 'ETHUSDT', 'long', 'open',
                      100, 100, 'accepted', 'test', 'x', 'm')
            """
        )
        conn.execute(
            """
            insert into orders (
              order_id, client_oid, intent_id, symbol, side, action, order_type,
              status, price, size, filled_size, created_at, updated_at
            ) values ('o1', 'c1', 'i1', 'ETHUSDT', 'buy', 'open', 'limit',
                      'submitted', 100, 0.1, 0, '2026-07-04T00:00:00Z', '2026-07-04T00:00:00Z')
            """
        )
        conn.commit()

        assert has_unfinished_system_order(conn, "ETHUSDT") is True
        assert has_unfinished_system_order(conn, "BTCUSDT") is False
```

- [ ] **Step 3: Run tests to verify they fail**

Run:

```bash
mamba run -n quantmamba python -m pytest -q tests/test_trade_intents.py tests/test_signal_state.py
```

Expected: FAIL because helpers do not exist.

- [ ] **Step 4: Add no-commit insert helper**

Modify `src/prodigy/signals/intents.py`:

```python
def insert_trade_intent(conn: sqlite3.Connection, intent: TradeIntent) -> None:
    conn.execute(
        """
        insert into trade_intents (
          intent_id, created_at, symbol, side, action, target_notional,
          max_order_notional, status, source, reason, model_version
        ) values (?, ?, ?, ?, ?, ?, ?, 'pending', ?, ?, ?)
        """,
        (
            intent.intent_id,
            intent.created_at,
            intent.symbol,
            intent.side,
            intent.action,
            intent.target_notional,
            intent.max_order_notional,
            intent.source,
            intent.reason,
            intent.model_version,
        ),
    )


def write_trade_intent(conn: sqlite3.Connection, intent: TradeIntent) -> None:
    insert_trade_intent(conn, intent)
    conn.commit()
```

- [ ] **Step 5: Add SQLite state helpers**

Create `src/prodigy/signals/state.py`:

```python
from __future__ import annotations

import sqlite3


def signal_processed_key(source: str, symbol: str, timeframe: str, closed_bar_ts: str) -> str:
    return f"signal_processed:{source}:{symbol}:{timeframe}:{closed_bar_ts}"


def get_executor_state(conn: sqlite3.Connection, key: str) -> str | None:
    row = conn.execute("select value from executor_state where key = ?", (key,)).fetchone()
    return None if row is None else str(row["value"])


def set_executor_state(
    conn: sqlite3.Connection,
    key: str,
    value: str,
    updated_at: str,
) -> None:
    conn.execute(
        """
        insert into executor_state (key, value, updated_at)
        values (?, ?, ?)
        on conflict(key) do update set
          value = excluded.value,
          updated_at = excluded.updated_at
        """,
        (key, value, updated_at),
    )


def has_unresolved_intent(conn: sqlite3.Connection, symbol: str) -> bool:
    row = conn.execute(
        """
        select 1 from trade_intents
        where symbol = ? and status in ('pending', 'accepted')
        limit 1
        """,
        (symbol,),
    ).fetchone()
    return row is not None


def has_unfinished_system_order(conn: sqlite3.Connection, symbol: str) -> bool:
    row = conn.execute(
        """
        select 1 from orders
        where symbol = ?
          and intent_id is not null
          and status not in ('filled', 'cancelled', 'canceled', 'rejected',
                             'failed', 'externally_cancelled', 'externally_closed')
        limit 1
        """,
        (symbol,),
    ).fetchone()
    return row is not None


def is_manual_override_active(conn: sqlite3.Connection, symbol: str) -> bool:
    return get_executor_state(conn, f"manual_override:{symbol}") == "active"
```

- [ ] **Step 6: Run tests**

Run:

```bash
mamba run -n quantmamba python -m pytest -q tests/test_trade_intents.py tests/test_signal_state.py
```

Expected: PASS.

- [ ] **Step 7: Commit**

```bash
git add src/prodigy/signals/intents.py src/prodigy/signals/state.py tests/test_trade_intents.py tests/test_signal_state.py
git commit -m "feat: add transaction-safe signal state helpers"
```

## Task 3: Add Closed-Bar Scoring And Decision Logic

**Files:**
- Create: `src/prodigy/signals/daemon.py`
- Create: `tests/test_signal_daemon.py`

- [ ] **Step 1: Write failing closed-bar and score tests**

Create `tests/test_signal_daemon.py`:

```python
import pandas as pd

from prodigy.signals.daemon import (
    PositionState,
    SignalDaemonConfig,
    SignalDecision,
    combine_example_score,
    decide_intent,
    latest_closed_bar,
)


def test_latest_closed_bar_ignores_open_bar():
    frame = pd.DataFrame(
        {
            "timestamp": pd.to_datetime(
                ["2026-07-04T10:00:00Z", "2026-07-04T10:15:00Z"],
                utc=True,
            ),
            "symbol": ["ETH/USDT:USDT", "ETH/USDT:USDT"],
            "close": [100.0, 101.0],
        }
    )

    row = latest_closed_bar(frame, now=pd.Timestamp("2026-07-04T10:29:59Z"), timeframe="15m")

    assert row["timestamp"] == pd.Timestamp("2026-07-04T10:00:00Z")


def test_combine_example_score_clips_to_range():
    features = pd.DataFrame(
        {
            "example_momentum": [2.0],
            "example_funding": [-0.5],
            "example_volatility": [0.5],
        }
    )

    assert combine_example_score(features.iloc[-1]) == 0.6666666666666666


def test_decide_opens_when_score_crosses_threshold():
    cfg = SignalDaemonConfig(total_notional_cap=10_000)

    decision = decide_intent(score=0.8, position=None, holding_bars=0, cfg=cfg)

    assert decision == SignalDecision(action="open", side="long", target_notional=750.0, reason="open_threshold")


def test_decide_reverse_signal_closes_existing_position_only():
    cfg = SignalDaemonConfig(total_notional_cap=10_000)
    position = PositionState(side="long", unrealized_pnl=10.0)

    decision = decide_intent(score=-0.8, position=position, holding_bars=3, cfg=cfg)

    assert decision == SignalDecision(action="close", side="long", target_notional=0.0, reason="close_opposite")


def test_decide_holding_expiry_profit_and_loss_thresholds():
    cfg = SignalDaemonConfig(total_notional_cap=10_000, max_holding_bars=96)

    profit_hold = decide_intent(
        score=0.3,
        position=PositionState(side="long", unrealized_pnl=1.0),
        holding_bars=96,
        cfg=cfg,
    )
    profit_close = decide_intent(
        score=0.1,
        position=PositionState(side="long", unrealized_pnl=1.0),
        holding_bars=96,
        cfg=cfg,
    )
    loss_close = decide_intent(
        score=0.3,
        position=PositionState(side="long", unrealized_pnl=-1.0),
        holding_bars=96,
        cfg=cfg,
    )

    assert profit_hold is None
    assert profit_close == SignalDecision(action="close", side="long", target_notional=0.0, reason="holding_expiry_profit")
    assert loss_close == SignalDecision(action="close", side="long", target_notional=0.0, reason="holding_expiry_loss")
```

- [ ] **Step 2: Run tests to verify they fail**

Run:

```bash
mamba run -n quantmamba python -m pytest -q tests/test_signal_daemon.py
```

Expected: FAIL because `prodigy.signals.daemon` does not exist.

- [ ] **Step 3: Add minimal decision module**

Create `src/prodigy/signals/daemon.py`:

```python
from __future__ import annotations

from dataclasses import dataclass

import pandas as pd


@dataclass(frozen=True)
class SignalDaemonConfig:
    total_notional_cap: float
    entry_threshold: float = 0.6
    exit_threshold: float = 0.2
    min_order_fraction: float = 0.05
    max_order_fraction: float = 0.10
    max_holding_bars: int = 96
    profit_hold_score_threshold: float = 0.2
    loss_hold_score_threshold: float = 0.4


@dataclass(frozen=True)
class PositionState:
    side: str
    unrealized_pnl: float


@dataclass(frozen=True)
class SignalDecision:
    action: str
    side: str
    target_notional: float
    reason: str


def latest_closed_bar(frame: pd.DataFrame, now: pd.Timestamp, timeframe: str) -> pd.Series:
    if frame.empty:
        raise ValueError("no OHLCV rows available")
    now = pd.Timestamp(now).tz_convert("UTC") if pd.Timestamp(now).tzinfo else pd.Timestamp(now, tz="UTC")
    cutoff = now.floor(timeframe) - pd.Timedelta(timeframe)
    closed = frame[pd.to_datetime(frame["timestamp"], utc=True) <= cutoff]
    if closed.empty:
        raise ValueError("no closed bar available")
    return closed.sort_values("timestamp").iloc[-1]


def combine_example_score(row: pd.Series) -> float:
    cols = ["example_momentum", "example_funding", "example_volatility"]
    values = [float(row[c]) for c in cols if c in row and pd.notna(row[c])]
    if not values:
        return 0.0
    return max(min(sum(values) / len(values), 1.0), -1.0)


def _notional(score: float, cfg: SignalDaemonConfig) -> float:
    mag = min(max(abs(score), cfg.entry_threshold), 1.0)
    span = 1.0 - cfg.entry_threshold
    fraction = cfg.min_order_fraction + (
        cfg.max_order_fraction - cfg.min_order_fraction
    ) * ((mag - cfg.entry_threshold) / span if span else 0.0)
    return cfg.total_notional_cap * fraction


def decide_intent(
    score: float,
    position: PositionState | None,
    holding_bars: int,
    cfg: SignalDaemonConfig,
) -> SignalDecision | None:
    if position is not None:
        if position.side == "long" and score <= -cfg.exit_threshold:
            return SignalDecision("close", "long", 0.0, "close_opposite")
        if position.side == "short" and score >= cfg.exit_threshold:
            return SignalDecision("close", "short", 0.0, "close_opposite")
        if holding_bars >= cfg.max_holding_bars:
            threshold = (
                cfg.profit_hold_score_threshold
                if position.unrealized_pnl >= 0
                else cfg.loss_hold_score_threshold
            )
            if abs(score) < threshold:
                reason = "holding_expiry_profit" if position.unrealized_pnl >= 0 else "holding_expiry_loss"
                return SignalDecision("close", position.side, 0.0, reason)
        return None

    if abs(score) >= cfg.entry_threshold:
        side = "long" if score > 0 else "short"
        return SignalDecision("open", side, _notional(score, cfg), "open_threshold")

    return None
```

- [ ] **Step 4: Run tests**

Run:

```bash
mamba run -n quantmamba python -m pytest -q tests/test_signal_daemon.py
```

Expected: PASS.

- [ ] **Step 5: Commit**

```bash
git add src/prodigy/signals/daemon.py tests/test_signal_daemon.py
git commit -m "feat: add closed-bar signal decisions"
```

## Task 4: Wire SQLite State Gates And Atomic Bar Processing

**Files:**
- Modify: `src/prodigy/signals/daemon.py`
- Modify: `tests/test_signal_daemon.py`

- [ ] **Step 1: Add failing run-once tests**

Append to `tests/test_signal_daemon.py`:

```python
from prodigy.db import connect, init_db
from prodigy.signals.daemon import process_decision
from prodigy.signals.state import get_executor_state, signal_processed_key


def test_process_decision_writes_intent_and_marker_in_one_transaction(tmp_path):
    db_path = tmp_path / "prodigy.sqlite"
    key = signal_processed_key("dummy-cycle", "ETHUSDT", "15m", "2026-07-04T10:15:00Z")

    with connect(db_path) as conn:
        init_db(conn)
        decision = SignalDecision("open", "long", 100.0, "open_threshold")
        process_decision(
            conn=conn,
            decision=decision,
            processed_key=key,
            created_at="2026-07-04T10:15:01Z",
            symbol="ETHUSDT",
            source="dummy-cycle",
            model_version="dummy-cycle",
        )

        intent = conn.execute("select action, side, target_notional from trade_intents").fetchone()
        marker = get_executor_state(conn, key)

    assert dict(intent) == {"action": "open", "side": "long", "target_notional": 100.0}
    assert marker == "open_intent_written"


def test_process_decision_close_uses_zero_notional(tmp_path):
    db_path = tmp_path / "prodigy.sqlite"
    key = signal_processed_key("dummy-cycle", "ETHUSDT", "15m", "2026-07-04T10:15:00Z")

    with connect(db_path) as conn:
        init_db(conn)
        process_decision(
            conn=conn,
            decision=SignalDecision("close", "long", 0.0, "close_opposite"),
            processed_key=key,
            created_at="2026-07-04T10:15:01Z",
            symbol="ETHUSDT",
            source="dummy-cycle",
            model_version="dummy-cycle",
        )
        intent = conn.execute("select action, side, target_notional, max_order_notional from trade_intents").fetchone()

    assert dict(intent) == {
        "action": "close",
        "side": "long",
        "target_notional": 0.0,
        "max_order_notional": 0.0,
    }
```

- [ ] **Step 2: Run tests to verify they fail**

Run:

```bash
mamba run -n quantmamba python -m pytest -q tests/test_signal_daemon.py::test_process_decision_writes_intent_and_marker_in_one_transaction tests/test_signal_daemon.py::test_process_decision_close_uses_zero_notional
```

Expected: FAIL because `process_decision` does not exist.

- [ ] **Step 3: Implement atomic process helper**

Append to `src/prodigy/signals/daemon.py`:

```python
import sqlite3
import uuid

from prodigy.signals.intents import TradeIntent, insert_trade_intent
from prodigy.signals.state import set_executor_state


def process_decision(
    conn: sqlite3.Connection,
    decision: SignalDecision,
    processed_key: str,
    created_at: str,
    symbol: str,
    source: str,
    model_version: str,
) -> None:
    outcome = "open_intent_written" if decision.action == "open" else "close_intent_written"
    intent = TradeIntent(
        intent_id=f"{source}-{symbol}-{uuid.uuid4().hex[:12]}",
        created_at=created_at,
        symbol=symbol,
        side=decision.side,
        action=decision.action,
        target_notional=decision.target_notional,
        max_order_notional=decision.target_notional if decision.action == "open" else 0.0,
        source=source,
        reason=decision.reason,
        model_version=model_version,
    )
    with conn:
        insert_trade_intent(conn, intent)
        set_executor_state(conn, processed_key, outcome, created_at)
```

- [ ] **Step 4: Run tests**

Run:

```bash
mamba run -n quantmamba python -m pytest -q tests/test_signal_daemon.py
```

Expected: PASS.

- [ ] **Step 5: Commit**

```bash
git add src/prodigy/signals/daemon.py tests/test_signal_daemon.py
git commit -m "feat: write signal intents atomically"
```

## Task 5: Implement Run Once With Data Refresh, Gates, And Idempotency

**Files:**
- Modify: `src/prodigy/signals/daemon.py`
- Modify: `src/prodigy/cli/signal.py`
- Modify: `tests/test_signal_daemon.py`
- Modify: `tests/test_signal_cli.py`

- [ ] **Step 1: Add failing skip/gate tests**

Append to `tests/test_signal_daemon.py`:

```python
from prodigy.signals.daemon import RunOnceConfig, run_once
from prodigy.signals.state import set_executor_state


def test_run_once_skips_when_state_is_stale(tmp_path):
    db_path = tmp_path / "prodigy.sqlite"
    with connect(db_path) as conn:
        init_db(conn)

    result = run_once(
        RunOnceConfig(
            db_path=db_path,
            data_root=tmp_path / "data",
            research_symbol="ETH/USDT:USDT",
            exchange_symbol="ETHUSDT",
            source="dummy-cycle",
            now=pd.Timestamp("2026-07-04T10:16:00Z"),
            refresh_data=lambda: None,
            score_loader=lambda: 1.0,
        )
    )

    assert result == "skipped_stale_state"
    with connect(db_path) as conn:
        assert conn.execute("select count(*) from trade_intents").fetchone()[0] == 0
        key = signal_processed_key("dummy-cycle", "ETHUSDT", "15m", "2026-07-04T10:00:00Z")
        assert get_executor_state(conn, key) is None


def test_run_once_is_idempotent_per_closed_bar(tmp_path):
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

    cfg = RunOnceConfig(
        db_path=db_path,
        data_root=tmp_path / "data",
        research_symbol="ETH/USDT:USDT",
        exchange_symbol="ETHUSDT",
        source="dummy-cycle",
        now=pd.Timestamp("2026-07-04T10:16:00Z"),
        refresh_data=lambda: None,
        score_loader=lambda: 1.0,
    )

    assert run_once(cfg) == "open_intent_written"
    assert run_once(cfg) == "already_processed"

    with connect(db_path) as conn:
        assert conn.execute("select count(*) from trade_intents").fetchone()[0] == 1


def test_run_once_skips_manual_override(tmp_path):
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
        set_executor_state(conn, "manual_override:ETHUSDT", "active", "2026-07-04T10:15:30Z")
        conn.commit()

    result = run_once(
        RunOnceConfig(
            db_path=db_path,
            data_root=tmp_path / "data",
            research_symbol="ETH/USDT:USDT",
            exchange_symbol="ETHUSDT",
            source="dummy-cycle",
            now=pd.Timestamp("2026-07-04T10:16:00Z"),
            refresh_data=lambda: None,
            score_loader=lambda: 1.0,
        )
    )

    assert result == "skipped_manual_override"
    with connect(db_path) as conn:
        key = signal_processed_key("dummy-cycle", "ETHUSDT", "15m", "2026-07-04T10:00:00Z")
        assert get_executor_state(conn, key) is None
```

- [ ] **Step 2: Run tests to verify they fail**

Run:

```bash
mamba run -n quantmamba python -m pytest -q tests/test_signal_daemon.py::test_run_once_skips_when_state_is_stale tests/test_signal_daemon.py::test_run_once_is_idempotent_per_closed_bar tests/test_signal_daemon.py::test_run_once_skips_manual_override
```

Expected: FAIL because `RunOnceConfig` and `run_once` do not exist.

- [ ] **Step 3: Implement run_once minimally**

Add to `src/prodigy/signals/daemon.py`:

```python
from pathlib import Path
from typing import Callable

from prodigy.db import connect, init_db
from prodigy.signals.state import (
    get_executor_state,
    has_unfinished_system_order,
    has_unresolved_intent,
    is_manual_override_active,
    set_executor_state,
    signal_processed_key,
)


@dataclass(frozen=True)
class RunOnceConfig:
    db_path: str | Path
    data_root: str | Path
    research_symbol: str
    exchange_symbol: str
    source: str
    now: pd.Timestamp
    refresh_data: Callable[[], None]
    score_loader: Callable[[], float]
    signal_cfg: SignalDaemonConfig = SignalDaemonConfig(total_notional_cap=10_000)
    max_state_age_secs: int = 120
    timeframe: str = "15m"


def _latest_equity_snapshot_age_secs(conn: sqlite3.Connection, now: pd.Timestamp) -> float | None:
    row = conn.execute(
        "select created_at from equity_snapshots order by created_at desc limit 1"
    ).fetchone()
    if row is None:
        return None
    created = pd.Timestamp(row["created_at"])
    created = created.tz_localize("UTC") if created.tzinfo is None else created.tz_convert("UTC")
    now = pd.Timestamp(now)
    now = now.tz_localize("UTC") if now.tzinfo is None else now.tz_convert("UTC")
    return (now - created).total_seconds()


def _position_state(conn: sqlite3.Connection, symbol: str) -> PositionState | None:
    row = conn.execute(
        "select side, unrealized_pnl from positions where symbol = ?",
        (symbol,),
    ).fetchone()
    if row is None:
        return None
    return PositionState(side=str(row["side"]), unrealized_pnl=float(row["unrealized_pnl"]))


def run_once(cfg: RunOnceConfig) -> str:
    now = pd.Timestamp(cfg.now)
    now = now.tz_localize("UTC") if now.tzinfo is None else now.tz_convert("UTC")
    closed_ts = (now.floor(cfg.timeframe) - pd.Timedelta(cfg.timeframe)).isoformat().replace("+00:00", "Z")
    key = signal_processed_key(cfg.source, cfg.exchange_symbol, cfg.timeframe, closed_ts)

    with connect(cfg.db_path) as conn:
        init_db(conn)
        if get_executor_state(conn, key) is not None:
            return "already_processed"
        age = _latest_equity_snapshot_age_secs(conn, now)
        if age is None or age > cfg.max_state_age_secs:
            return "skipped_stale_state"
        if is_manual_override_active(conn, cfg.exchange_symbol):
            return "skipped_manual_override"
        if has_unresolved_intent(conn, cfg.exchange_symbol):
            return "skipped_pending_intent"
        if has_unfinished_system_order(conn, cfg.exchange_symbol):
            return "skipped_pending_order"

    cfg.refresh_data()
    score = cfg.score_loader()

    with connect(cfg.db_path) as conn:
        init_db(conn)
        position = _position_state(conn, cfg.exchange_symbol)
        decision = decide_intent(score, position, holding_bars=0, cfg=cfg.signal_cfg)
        if decision is None:
            set_executor_state(conn, key, "no_signal", now.isoformat())
            conn.commit()
            return "no_signal"
        process_decision(
            conn=conn,
            decision=decision,
            processed_key=key,
            created_at=now.isoformat().replace("+00:00", "Z"),
            symbol=cfg.exchange_symbol,
            source=cfg.source,
            model_version=cfg.source,
        )
        return "open_intent_written" if decision.action == "open" else "close_intent_written"
```

This implementation intentionally keeps `holding_bars=0`; Task 6 replaces that with a real SQLite-derived value.

- [ ] **Step 4: Run tests**

Run:

```bash
mamba run -n quantmamba python -m pytest -q tests/test_signal_daemon.py
```

Expected: PASS.

- [ ] **Step 5: Commit**

```bash
git add src/prodigy/signals/daemon.py tests/test_signal_daemon.py
git commit -m "feat: add idempotent signal run once"
```

## Task 6: Add Example-Factors Source And CLI Wiring

**Files:**
- Modify: `src/prodigy/signals/daemon.py`
- Modify: `src/prodigy/cli/signal.py`
- Modify: `tests/test_signal_daemon.py`
- Modify: `tests/test_signal_cli.py`

- [ ] **Step 1: Add failing example-score loader test**

Append to `tests/test_signal_daemon.py`:

```python
from prodigy.data.parquet_store import write_daily_partition
from prodigy.signals.daemon import load_example_score


def test_load_example_score_reads_parquet_and_uses_closed_bar(tmp_path):
    ts = pd.date_range("2026-07-04T09:00:00Z", periods=8, freq="15min")
    ohlcv = pd.DataFrame(
        {
            "timestamp": ts,
            "symbol": ["ETH/USDT:USDT"] * len(ts),
            "open": [100, 101, 102, 103, 104, 105, 106, 107],
            "high": [101, 102, 103, 104, 105, 106, 107, 108],
            "low": [99, 100, 101, 102, 103, 104, 105, 106],
            "close": [100, 102, 104, 106, 108, 110, 112, 114],
            "volume": [10] * len(ts),
        }
    )
    funding = pd.DataFrame(
        {
            "timestamp": ts,
            "symbol": ["ETH/USDT:USDT"] * len(ts),
            "funding_rate": [0.0001] * len(ts),
        }
    )
    write_daily_partition(
        ohlcv,
        data_root=tmp_path,
        exchange="bitget",
        symbol="ETH/USDT:USDT",
        dataset="ohlcv",
        date="2026-07-04",
        timeframe="15m",
    )
    write_daily_partition(
        funding,
        data_root=tmp_path,
        exchange="bitget",
        symbol="ETH/USDT:USDT",
        dataset="funding_rates",
        date="2026-07-04",
    )

    score = load_example_score(
        data_root=tmp_path,
        research_symbol="ETH/USDT:USDT",
        now=pd.Timestamp("2026-07-04T11:00:00Z"),
        timeframe="15m",
    )

    assert -1.0 <= score <= 1.0
```

- [ ] **Step 2: Run test to verify it fails**

Run:

```bash
mamba run -n quantmamba python -m pytest -q tests/test_signal_daemon.py::test_load_example_score_reads_parquet_and_uses_closed_bar
```

Expected: FAIL because `load_example_score` does not exist.

- [ ] **Step 3: Implement example score loader**

Append to `src/prodigy/signals/daemon.py`:

```python
from prodigy.data.parquet_store import load_funding_rates, load_ohlcv
from prodigy.factors.examples import (
    example_funding_factor,
    example_momentum_factor,
    example_volatility_factor,
)


def load_example_score(
    data_root: str | Path,
    research_symbol: str,
    now: pd.Timestamp,
    timeframe: str = "15m",
) -> float:
    now = pd.Timestamp(now).tz_convert("UTC") if pd.Timestamp(now).tzinfo else pd.Timestamp(now, tz="UTC")
    start = now - pd.Timedelta(days=7)
    end = now + pd.Timedelta(days=1)
    ohlcv = load_ohlcv(data_root, research_symbol, start, end, timeframe)
    funding = load_funding_rates(data_root, research_symbol, start, end)
    closed = latest_closed_bar(ohlcv, now, timeframe)
    closed_ts = closed["timestamp"]

    momentum = example_momentum_factor(ohlcv).rename(columns={"value": "example_momentum"})
    volatility = example_volatility_factor(ohlcv).rename(columns={"value": "example_volatility"})
    features = momentum[["timestamp", "symbol", "example_momentum"]].merge(
        volatility[["timestamp", "symbol", "example_volatility"]],
        on=["timestamp", "symbol"],
        how="left",
    )
    if not funding.empty:
        funding_factor = example_funding_factor(funding).rename(columns={"value": "example_funding"})
        features = features.merge(
            funding_factor[["timestamp", "symbol", "example_funding"]],
            on=["timestamp", "symbol"],
            how="left",
        )
    row = features[features["timestamp"] == closed_ts].iloc[-1]
    return combine_example_score(row)
```

- [ ] **Step 4: Wire CLI main to run_once**

Modify `src/prodigy/cli/signal.py`:

```python
from __future__ import annotations

import argparse
import time

import pandas as pd

from prodigy.config import load_config
from prodigy.data.backfill import run_backfill
from prodigy.signals.daemon import RunOnceConfig, load_example_score, run_once


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(prog="prodigy-signal")
    mode = parser.add_mutually_exclusive_group()
    mode.add_argument("--once", action="store_true")
    mode.add_argument("--daemon", action="store_true")
    parser.add_argument("--config", default="configs/default.toml")
    parser.add_argument("--db", default="var/prodigy.sqlite")
    parser.add_argument("--data-root", default="data")
    parser.add_argument("--signal-source", default="example-factors")
    parser.add_argument("--max-loops", type=int)
    return parser


def main(argv: list[str] | None = None) -> int:
    args = build_parser().parse_args(argv)
    if not args.once and not args.daemon:
        args.once = True
    cfg = load_config(args.config)
    signal_cfg = cfg["signal"]
    research_symbol = signal_cfg["enabled_symbols"][0]
    exchange_symbol = signal_cfg["exchange_symbols"][research_symbol]
    timeframe = signal_cfg["timeframe"]

    def refresh_data() -> None:
        now = pd.Timestamp.now(tz="UTC")
        run_backfill(
            symbol=research_symbol,
            start=(now - pd.Timedelta(days=7)).strftime("%Y-%m-%d"),
            # ponytail: run_backfill treats end as an exclusive boundary; using
            # tomorrow includes today's intraday closed bars without changing the
            # data layer in M5.
            end=(now + pd.Timedelta(days=1)).strftime("%Y-%m-%d"),
            timeframe=timeframe,
            data_root=args.data_root,
            db_path=args.db,
        )

    def score_loader() -> float:
        if args.signal_source == "dummy-cycle":
            return 1.0
        return load_example_score(args.data_root, research_symbol, pd.Timestamp.now(tz="UTC"), timeframe)

    loops = args.max_loops if args.max_loops is not None else (1 if args.once else None)
    count = 0
    while loops is None or count < loops:
        result = run_once(
            RunOnceConfig(
                db_path=args.db,
                data_root=args.data_root,
                research_symbol=research_symbol,
                exchange_symbol=exchange_symbol,
                source=args.signal_source,
                now=pd.Timestamp.now(tz="UTC"),
                refresh_data=refresh_data,
                score_loader=score_loader,
            )
        )
        print(result)
        count += 1
        if args.once:
            break
        time.sleep(int(signal_cfg["poll_interval_secs"]))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
```

- [ ] **Step 5: Run tests**

Run:

```bash
mamba run -n quantmamba python -m pytest -q tests/test_signal_daemon.py tests/test_signal_cli.py
```

Expected: PASS.

- [ ] **Step 6: Commit**

```bash
git add src/prodigy/signals/daemon.py src/prodigy/cli/signal.py tests/test_signal_daemon.py tests/test_signal_cli.py
git commit -m "feat: wire example signal source"
```

## Task 7: Add Rust Full-Position Close Semantics

**Files:**
- Modify: `crates/executor/src/executor.rs`

- [ ] **Step 1: Write failing Rust unit tests**

Add tests in the existing `#[cfg(test)]` module in `crates/executor/src/executor.rs`:

```rust
#[test]
fn close_order_request_uses_base_size_not_target_notional() {
    let cfg = test_cfg();
    let intent = TradeIntent {
        intent_id: "close-intent".to_string(),
        created_at: "2026-07-04T00:00:00Z".to_string(),
        symbol: "ETHUSDT".to_string(),
        side: "long".to_string(),
        action: "close".to_string(),
        target_notional: 0.0,
        max_order_notional: 0.0,
        source: "test".to_string(),
        reason: None,
        model_version: None,
    };
    let market = MarketUpdate {
        symbol: "ETHUSDT".to_string(),
        best_bid: 2000.0,
        best_ask: 2001.0,
        exchange_ts_ms: None,
        local_received_at_ms: 1,
    };

    let order = build_order_request_for_base(
        &cfg,
        &intent,
        &market,
        0.03,
        OrderMode::Taker,
        1,
    );

    assert_eq!(order.size, "0.03");
    assert_eq!(order.side, "sell");
    assert_eq!(order.reduce_only.as_deref(), Some("YES"));
}

#[test]
fn position_row_matches_close_side_and_full_size() {
    let row = serde_json::json!({
        "symbol": "ETHUSDT",
        "holdSide": "long",
        "total": "0.04",
        "available": "0"
    });

    assert_eq!(position_row_close_base_for_side(&row, "ETHUSDT", "long"), Some(0.04));
    assert_eq!(position_row_close_base_for_side(&row, "ETHUSDT", "short"), None);
}
```

- [ ] **Step 2: Run tests to verify they fail**

Run:

```bash
cargo test -q -p prodigy-executor close_order_request_uses_base_size_not_target_notional position_row_matches_close_side_and_full_size
```

Expected: FAIL because the new helpers do not exist.

- [ ] **Step 3: Add base-sized order builder**

Modify `crates/executor/src/executor.rs` near `build_order_request`:

```rust
pub fn build_order_request_for_base(
    cfg: &ExecutorConfig,
    intent: &TradeIntent,
    market: &MarketUpdate,
    base_size: f64,
    mode: OrderMode,
    attempt: u32,
) -> PlaceOrderRequest {
    let side = order_side(&intent.action, &intent.side);
    let price = match mode {
        OrderMode::Maker if side == "buy" => Some(format_price(market.best_bid)),
        OrderMode::Maker => Some(format_price(market.best_ask)),
        OrderMode::Taker => None,
    };
    let client_oid = format!(
        "pdgy-{}-{attempt}-{}",
        intent.intent_id,
        crate::bitget::now_ms()
    );

    PlaceOrderRequest {
        symbol: cfg.bitget_symbol.clone(),
        product_type: cfg.product_type.clone(),
        margin_mode: cfg.margin_mode.clone(),
        margin_coin: cfg.margin_coin.clone(),
        size: format_size(base_size),
        price,
        side: side.to_string(),
        order_type: if mode == OrderMode::Maker { "limit" } else { "market" }.to_string(),
        force: if mode == OrderMode::Maker { Some("post_only".to_string()) } else { None },
        client_oid,
        reduce_only: if intent.action == "close" { Some("YES".to_string()) } else { None },
    }
}
```

Then simplify existing `build_order_request` so it delegates:

```rust
pub fn build_order_request(
    cfg: &ExecutorConfig,
    intent: &TradeIntent,
    market: &MarketUpdate,
    approved_notional: f64,
    mode: OrderMode,
    attempt: u32,
) -> PlaceOrderRequest {
    let side = order_side(&intent.action, &intent.side);
    let reference_price = reference_price(mode, side, market);
    build_order_request_for_base(
        cfg,
        intent,
        market,
        approved_notional / reference_price,
        mode,
        attempt,
    )
}
```

- [ ] **Step 4: Add close position-size resolver**

Modify the existing `position_row_closeable` area in `crates/executor/src/executor.rs`:

```rust
fn position_row_close_base_for_side(
    row: &serde_json::Value,
    symbol: &str,
    side: &str,
) -> Option<f64> {
    let (size, hold_side) = position_row_closeable(row, symbol)?;
    if hold_side == side {
        Some(size)
    } else {
        None
    }
}
```

Inside `process_one_intent`, before `maker_ref`/`target_base` sizing, resolve close base:

```rust
let close_target_base = if intent.action == "close" {
    let positions = rest
        .get_with_query(
            "/api/v2/mix/position/all-position",
            &[
                ("productType", cfg.product_type.clone()),
                ("marginCoin", cfg.margin_coin.clone()),
            ],
        )
        .await?;
    let rows = positions
        .get("data")
        .and_then(serde_json::Value::as_array)
        .cloned()
        .unwrap_or_default();
    let base = rows
        .iter()
        .find_map(|row| position_row_close_base_for_side(row, &cfg.bitget_symbol, &intent.side))
        .unwrap_or(0.0);
    if base <= DUST_BASE {
        db::fail_intent(conn, &intent.intent_id, "close requested but no matching exchange position exists")?;
        return Ok(());
    }
    Some(base)
} else {
    None
};
```

Then set target base:

```rust
let target_base = if let Some(base) = close_target_base {
    base
} else if maker_ref > 0.0 {
    approved / maker_ref
} else {
    return Err(anyhow::anyhow!("reference price not positive; cannot size order"));
};
```

When placing maker/taker attempts, use base sizing for close:

```rust
let order = if intent.action == "close" {
    build_order_request_for_base(
        cfg,
        &intent,
        &place_market,
        remaining_base(target_base, cumulative_filled_base),
        OrderMode::Maker,
        attempt,
    )
} else {
    build_order_request(
        cfg,
        &intent,
        &place_market,
        remaining_notional,
        OrderMode::Maker,
        attempt,
    )
};
```

Apply the same pattern in the taker placement branch.

- [ ] **Step 5: Run targeted Rust tests**

Run:

```bash
cargo test -q -p prodigy-executor close_order_request_uses_base_size_not_target_notional position_row_matches_close_side_and_full_size taker_close_long_is_reduce_only_sell_market
```

Expected: PASS.

- [ ] **Step 6: Commit**

```bash
git add crates/executor/src/executor.rs
git commit -m "fix: close intents use full exchange position size"
```

## Task 8: Add Bounded Daemon Loop And End-To-End Smoke Tests

**Files:**
- Modify: `src/prodigy/cli/signal.py`
- Modify: `src/prodigy/signals/daemon.py`
- Create or modify: `tests/test_signal_cli.py`
- Create or modify: `tests/test_executor_integration.py`

- [ ] **Step 1: Add CLI bounded-loop test**

Append to `tests/test_signal_cli.py`:

```python
def test_signal_parser_supports_bounded_daemon_loop():
    args = build_parser().parse_args(["--daemon", "--max-loops", "1"])

    assert args.daemon is True
    assert args.max_loops == 1
```

- [ ] **Step 2: Add Python/Rust SQLite handoff smoke test**

Append to `tests/test_executor_integration.py`:

```python
import pandas as pd

from prodigy.signals.daemon import RunOnceConfig, run_once


def test_signal_run_once_writes_intent_for_executor(tmp_path):
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

    result = run_once(
        RunOnceConfig(
            db_path=db_path,
            data_root=tmp_path / "data",
            research_symbol="ETH/USDT:USDT",
            exchange_symbol="ETHUSDT",
            source="dummy-cycle",
            now=pd.Timestamp("2026-07-04T10:16:00Z"),
            refresh_data=lambda: None,
            score_loader=lambda: 1.0,
        )
    )

    with connect(db_path) as conn:
        row = conn.execute(
            "select status, symbol, action, side from trade_intents"
        ).fetchone()

    assert result == "open_intent_written"
    assert dict(row) == {
        "status": "pending",
        "symbol": "ETHUSDT",
        "action": "open",
        "side": "long",
    }
```

- [ ] **Step 3: Run smoke tests**

Run:

```bash
mamba run -n quantmamba python -m pytest -q tests/test_signal_cli.py tests/test_executor_integration.py::test_signal_run_once_writes_intent_for_executor
```

Expected: PASS after prior tasks; fix only direct import issues.

- [ ] **Step 4: Commit**

```bash
git add src/prodigy/cli/signal.py src/prodigy/signals/daemon.py tests/test_signal_cli.py tests/test_executor_integration.py
git commit -m "test: cover signal daemon sqlite handoff"
```

## Task 9: Final Verification And Scope Audit

**Files:**
- No planned source edits unless verification exposes a defect.

- [ ] **Step 1: Run Python tests**

Run:

```bash
mamba run -n quantmamba python -m pytest -q
```

Expected: all Python tests pass.

- [ ] **Step 2: Run Rust tests**

Run:

```bash
cargo test -q
```

Expected: all Rust tests pass, including Bitget demo tests when credentials are available.

- [ ] **Step 3: Run format and lint checks**

Run:

```bash
cargo fmt --check
cargo clippy --all-targets --all-features -- -D warnings
git diff --check
```

Expected: all exit 0.

- [ ] **Step 4: Scope scan**

Run:

```bash
rg -n "FastAPI|Redis|Kafka|event-bus|event bus|actor|/stop|/resume|/close_all|TradingMode::Live|ws.bitget.com" src crates tests configs docs
```

Expected:

- no new Redis/Kafka/FastAPI/event-bus/actor implementation;
- Telegram remote controls remain absent or explicitly rejected;
- live trading remains rejected;
- Python signal code does not call Bitget account, order, position, or execution endpoints.

- [ ] **Step 5: Commit final fixes if verification required changes**

If Step 1-4 required changes:

```bash
git add <changed-files>
git commit -m "chore: finalize fifth milestone signal daemon"
```

If no changes were needed, do not create an empty commit.

## Claude Code Handoff Prompt

Use this prompt in a fresh Claude Code context:

```text
You are working in /Users/aaronliang/Documents/Projects/Prodigy.

Use @Superpowers. Use subagent-driven-development. Use TDD. Use @Ponytail full.

Implement M5 from:
- docs/superpowers/specs/2026-07-04-crypto-quant-fifth-milestone-signal-daemon-design.md
- docs/superpowers/plans/2026-07-04-crypto-quant-fifth-milestone-signal-daemon.md

Important constraints:
- M5 is not real alpha. It proves the demo automatic trading loop.
- Python writes only open/close trade_intents.
- Python must not call Bitget account/order/position/execution APIs.
- Rust remains the only execution component.
- Add only the minimal Rust change so action=close resolves current exchange position size and sends reduce-only full-position close.
- Python close intent should use target_notional=0 and max_order_notional=0; Rust ignores these for close sizing.
- Write trade_intent and signal_processed executor_state marker in one SQLite transaction.
- Use executor_state key signal_processed:{source}:{symbol}:{timeframe}:{closed_bar_ts}; do not add a processed-bar table.
- Default research/config symbol is ETH/USDT:USDT; executor/exchange symbol is ETHUSDT.
- Default signal source is example-factors; dummy-cycle is explicit test source only.
- Do not add Redis/Kafka/FastAPI/actor/event-bus.
- Do not implement live trading or Telegram remote controls.

Execute the plan task-by-task. For each task:
1. Write the failing tests first.
2. Run the targeted tests and confirm failure.
3. Implement the minimal code.
4. Run targeted tests and confirm pass.
5. Commit the task.
6. Pause for review after each task if a reviewer is present.

Final verification:
- mamba run -n quantmamba python -m pytest -q
- cargo test -q
- cargo fmt --check
- cargo clippy --all-targets --all-features -- -D warnings
- git diff --check
- scope scan for live trading, Telegram controls, and forbidden services.
```
