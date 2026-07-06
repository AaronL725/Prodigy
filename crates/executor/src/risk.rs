use crate::types::TradeIntent;

#[derive(Debug, Clone, Copy)]
pub struct RiskParams {
    pub total_notional_cap_x_equity: f64,
    pub trading_suspension_unrealized_loss_x_equity: f64,
    pub min_available_margin_fraction: f64,
}

impl Default for RiskParams {
    fn default() -> Self {
        Self {
            total_notional_cap_x_equity: 5.0,
            trading_suspension_unrealized_loss_x_equity: 0.10,
            min_available_margin_fraction: 0.05,
        }
    }
}

#[derive(Debug, Clone, Copy)]
pub struct AccountRiskSnapshot {
    pub equity: f64,
    pub available_margin: f64,
    pub unrealized_pnl_24h: f64,
    pub gross_notional: f64,
    pub market_is_fresh: bool,
    pub private_state_is_ready: bool,
    pub margin_danger: bool,
}

#[derive(Debug, Clone, Copy, PartialEq)]
pub struct RiskDecision {
    pub approved_notional: f64,
}

pub fn check_intent(
    intent: &TradeIntent,
    account: &AccountRiskSnapshot,
    params: &RiskParams,
) -> Result<RiskDecision, String> {
    // Opening actions create new exposure and are subject to the full risk gate.
    // Close/reduce/cancel are risk-REDUCING and bypass the open-only limits
    // (M4 risk priority: de-risk outranks new-opening limits). They still must
    // not run on a busted (equity<=0) account.
    // ponytail: inlined here (not imported from executor.rs) because risk.rs
    // cannot depend on executor.rs — executor depends on risk.
    let is_open = matches!(intent.action.as_str(), "open" | "add" | "reverse");

    if is_open && !account.private_state_is_ready {
        return Err("private account state is not ready".to_string());
    }
    if is_open && !account.market_is_fresh {
        return Err("market data is stale".to_string());
    }
    if account.equity <= 0.0 {
        return Err("equity is not positive".to_string());
    }
    if is_open && account.available_margin < account.equity * params.min_available_margin_fraction {
        return Err("available margin is too low".to_string());
    }
    if is_open && account.margin_danger {
        return Err("margin danger".to_string());
    }

    let suspended = account.unrealized_pnl_24h
        <= -account.equity * params.trading_suspension_unrealized_loss_x_equity;
    if suspended && is_open {
        return Err("trading suspended by 24h unrealized loss".to_string());
    }

    // Opens are clipped by the total-notional cap (new exposure). Close/reduce/cancel
    // are NOT new exposure — they use the intent's notional directly, uncapped, so a
    // cap-exhausted account can still de-risk.
    let approved = if is_open {
        let total_cap = account.equity * params.total_notional_cap_x_equity;
        let remaining = (total_cap - account.gross_notional).max(0.0);
        intent
            .target_notional
            .min(intent.max_order_notional)
            .min(remaining)
    } else {
        intent.target_notional.min(intent.max_order_notional)
    };
    if approved <= 0.0 && is_open {
        return Err("notional cap reached".to_string());
    }

    Ok(RiskDecision {
        approved_notional: approved.max(0.0),
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::types::TradeIntent;

    fn intent(action: &str, target: f64, max_order: f64) -> TradeIntent {
        TradeIntent {
            intent_id: "i1".to_string(),
            symbol: "ETH/USDT:USDT".to_string(),
            side: "long".to_string(),
            action: action.to_string(),
            target_notional: target,
            max_order_notional: max_order,
        }
    }

    #[test]
    fn clips_order_notional_to_intent_and_cap() {
        let account = AccountRiskSnapshot {
            equity: 1_000.0,
            available_margin: 500.0,
            unrealized_pnl_24h: 0.0,
            gross_notional: 1_000.0,
            market_is_fresh: true,
            private_state_is_ready: true,
            margin_danger: false,
        };
        let params = RiskParams::default();

        let decision = check_intent(&intent("open", 900.0, 600.0), &account, &params);

        assert_eq!(decision.unwrap().approved_notional, 600.0);
    }

    #[test]
    fn blocks_new_opening_when_unrealized_loss_threshold_hit() {
        let account = AccountRiskSnapshot {
            equity: 1_000.0,
            available_margin: 500.0,
            unrealized_pnl_24h: -100.0,
            gross_notional: 0.0,
            market_is_fresh: true,
            private_state_is_ready: true,
            margin_danger: false,
        };

        let err = check_intent(
            &intent("open", 100.0, 100.0),
            &account,
            &RiskParams::default(),
        )
        .unwrap_err();

        assert!(err.contains("trading suspended"));
    }

    #[test]
    fn allows_close_during_trading_suspension() {
        let account = AccountRiskSnapshot {
            equity: 1_000.0,
            available_margin: 500.0,
            unrealized_pnl_24h: -100.0,
            gross_notional: 500.0,
            market_is_fresh: true,
            private_state_is_ready: true,
            margin_danger: false,
        };

        let decision = check_intent(
            &intent("close", 100.0, 100.0),
            &account,
            &RiskParams::default(),
        );

        assert!(decision.is_ok());
    }

    #[test]
    fn margin_danger_blocks_open_but_allows_close_cancel() {
        let account = AccountRiskSnapshot {
            equity: 1_000.0,
            available_margin: 500.0,
            unrealized_pnl_24h: 0.0,
            gross_notional: 500.0,
            market_is_fresh: true,
            private_state_is_ready: true,
            margin_danger: true,
        };

        let err = check_intent(
            &intent("open", 100.0, 100.0),
            &account,
            &RiskParams::default(),
        )
        .unwrap_err();
        assert!(err.contains("margin danger"));

        assert!(check_intent(
            &intent("close", 100.0, 100.0),
            &account,
            &RiskParams::default(),
        )
        .is_ok());
        assert!(check_intent(
            &intent("cancel", 0.0, 0.0),
            &account,
            &RiskParams::default(),
        )
        .is_ok());
    }

    #[test]
    fn blocks_open_when_total_notional_cap_is_exhausted() {
        // gross_notional already at the equity*cap_x ceiling → remaining cap 0 →
        // a new open is blocked even though margin/equity/market are healthy.
        let account = AccountRiskSnapshot {
            equity: 1_000.0,
            available_margin: 1_000.0,
            unrealized_pnl_24h: 0.0,
            gross_notional: 5_000.0, // == equity 1000 * cap_x 5 → remaining 0
            market_is_fresh: true,
            private_state_is_ready: true,
            margin_danger: false,
        };

        let err = check_intent(
            &intent("open", 100.0, 100.0),
            &account,
            &RiskParams::default(),
        )
        .unwrap_err();

        assert!(err.contains("notional cap reached"));
    }

    #[test]
    fn close_is_allowed_when_total_notional_cap_exhausted() {
        // Cap exhausted (remaining 0): a close must still size its full notional.
        let account = AccountRiskSnapshot {
            equity: 1_000.0,
            available_margin: 1_000.0,
            unrealized_pnl_24h: 0.0,
            gross_notional: 5_000.0, // == cap → remaining 0
            market_is_fresh: true,
            private_state_is_ready: true,
            margin_danger: false,
        };

        let decision = check_intent(
            &intent("close", 200.0, 200.0),
            &account,
            &RiskParams::default(),
        )
        .unwrap();

        assert_eq!(decision.approved_notional, 200.0);

        // Open on the same cap-exhausted account IS still blocked.
        let err = check_intent(
            &intent("open", 200.0, 200.0),
            &account,
            &RiskParams::default(),
        )
        .unwrap_err();
        assert!(err.contains("notional cap reached"));
    }

    #[test]
    fn close_is_allowed_when_available_margin_low() {
        // available_margin below the 5% floor would block an open; close must
        // still go through (closing REDUCES margin usage — that's the point).
        let account = AccountRiskSnapshot {
            equity: 1_000.0,
            available_margin: 10.0, // < 5% of 1000 = 50
            unrealized_pnl_24h: 0.0,
            gross_notional: 0.0,
            market_is_fresh: true,
            private_state_is_ready: true,
            margin_danger: false,
        };

        let decision = check_intent(
            &intent("close", 100.0, 100.0),
            &account,
            &RiskParams::default(),
        )
        .unwrap();

        assert_eq!(decision.approved_notional, 100.0);

        let err = check_intent(
            &intent("open", 100.0, 100.0),
            &account,
            &RiskParams::default(),
        )
        .unwrap_err();
        assert!(err.contains("available margin is too low"));
    }

    #[test]
    fn close_ignores_stale_market_and_unready_private_state() {
        // Private WS down + stale market would block open; close bypasses both
        // (close must be possible during a WS outage — REST still works).
        let account = AccountRiskSnapshot {
            equity: 1_000.0,
            available_margin: 1_000.0,
            unrealized_pnl_24h: 0.0,
            gross_notional: 0.0,
            market_is_fresh: false,
            private_state_is_ready: false,
            margin_danger: false,
        };

        let decision = check_intent(
            &intent("close", 100.0, 100.0),
            &account,
            &RiskParams::default(),
        )
        .unwrap();

        assert_eq!(decision.approved_notional, 100.0);

        let err = check_intent(
            &intent("open", 100.0, 100.0),
            &account,
            &RiskParams::default(),
        )
        .unwrap_err();
        assert!(err.contains("private account state is not ready"));
    }

    #[test]
    fn close_still_blocked_when_equity_not_positive() {
        // Bust guard applies to ALL actions — a busted (equity<=0) account is
        // already liquidated; close must not run there either.
        let account = AccountRiskSnapshot {
            equity: 0.0,
            available_margin: 0.0,
            unrealized_pnl_24h: 0.0,
            gross_notional: 0.0,
            market_is_fresh: true,
            private_state_is_ready: true,
            margin_danger: false,
        };

        let err = check_intent(
            &intent("close", 100.0, 100.0),
            &account,
            &RiskParams::default(),
        )
        .unwrap_err();

        assert!(err.contains("equity is not positive"));
    }
}
