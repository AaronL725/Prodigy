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
    if !account.private_state_is_ready {
        return Err("private account state is not ready".to_string());
    }
    if !account.market_is_fresh && intent.action == "open" {
        return Err("market data is stale".to_string());
    }
    if account.equity <= 0.0 {
        return Err("equity is not positive".to_string());
    }
    if account.available_margin < account.equity * params.min_available_margin_fraction {
        return Err("available margin is too low".to_string());
    }

    let suspended = account.unrealized_pnl_24h
        <= -account.equity * params.trading_suspension_unrealized_loss_x_equity;
    if suspended && intent.action == "open" {
        return Err("trading suspended by 24h unrealized loss".to_string());
    }

    let total_cap = account.equity * params.total_notional_cap_x_equity;
    let remaining = (total_cap - account.gross_notional).max(0.0);
    let approved = intent
        .target_notional
        .min(intent.max_order_notional)
        .min(remaining);
    if approved <= 0.0 && intent.action == "open" {
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
        };

        let decision = check_intent(
            &intent("close", 100.0, 100.0),
            &account,
            &RiskParams::default(),
        );

        assert!(decision.is_ok());
    }
}
