use std::collections::HashSet;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum InterventionKind {
    Open,
    Add,
    Reduce,
    Close,
    Cancel,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ExchangeIntervention {
    pub symbol: String,
    pub matched_local_client_oid: bool,
    pub kind: InterventionKind,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ManualOverrideDecision {
    Entered(String),
    Cleared(String),
    NoChange,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ManualRiskAction {
    DoNothing,
    AllowEmergencyReduce,
}

#[derive(Debug, Default, Clone)]
pub struct ManualOverrideState {
    blocked_symbols: HashSet<String>,
}

impl ManualOverrideState {
    pub fn enter(&mut self, symbol: &str) {
        self.blocked_symbols.insert(symbol.to_string());
    }

    pub fn is_blocked_for_open(&self, symbol: &str) -> bool {
        self.blocked_symbols.contains(symbol)
    }

    fn clear(&mut self, symbol: &str) -> bool {
        self.blocked_symbols.remove(symbol)
    }
}

pub fn apply_exchange_intervention(
    state: &mut ManualOverrideState,
    event: ExchangeIntervention,
) -> ManualOverrideDecision {
    if event.matched_local_client_oid {
        return ManualOverrideDecision::NoChange;
    }
    if state.is_blocked_for_open(&event.symbol) {
        return ManualOverrideDecision::NoChange;
    }
    state.enter(&event.symbol);
    ManualOverrideDecision::Entered(event.symbol)
}

pub fn maybe_clear_manual_override(
    state: &mut ManualOverrideState,
    symbol: &str,
    position_notional: f64,
    open_order_count: usize,
) -> ManualOverrideDecision {
    if position_notional == 0.0 && open_order_count == 0 && state.clear(symbol) {
        return ManualOverrideDecision::Cleared(symbol.to_string());
    }
    ManualOverrideDecision::NoChange
}

pub fn risk_action_for_manual_position(
    manual_notional: f64,
    normal_cap: f64,
    margin_danger: bool,
) -> ManualRiskAction {
    if margin_danger {
        return ManualRiskAction::AllowEmergencyReduce;
    }
    let _ = (manual_notional, normal_cap);
    ManualRiskAction::DoNothing
}

pub fn classify_external_status(
    kind: InterventionKind,
    was_system_owned: bool,
) -> Option<&'static str> {
    if !was_system_owned {
        return None;
    }
    match kind {
        InterventionKind::Cancel => Some("externally_cancelled"),
        InterventionKind::Close | InterventionKind::Reduce => Some("externally_closed"),
        InterventionKind::Open | InterventionKind::Add => None,
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn unmatched_exchange_change_enters_symbol_manual_override() {
        let mut state = ManualOverrideState::default();
        let event = ExchangeIntervention {
            symbol: "ETH/USDT:USDT".to_string(),
            matched_local_client_oid: false,
            kind: InterventionKind::Open,
        };

        let decision = apply_exchange_intervention(&mut state, event);

        assert_eq!(
            decision,
            ManualOverrideDecision::Entered("ETH/USDT:USDT".to_string())
        );
        assert!(state.is_blocked_for_open("ETH/USDT:USDT"));
    }

    #[test]
    fn matched_system_change_does_not_enter_manual_override() {
        let mut state = ManualOverrideState::default();
        let event = ExchangeIntervention {
            symbol: "ETH/USDT:USDT".to_string(),
            matched_local_client_oid: true,
            kind: InterventionKind::Open,
        };

        let decision = apply_exchange_intervention(&mut state, event);

        assert_eq!(decision, ManualOverrideDecision::NoChange);
        assert!(!state.is_blocked_for_open("ETH/USDT:USDT"));
    }

    #[test]
    fn override_clears_only_when_position_and_open_orders_are_zero() {
        let mut state = ManualOverrideState::default();
        state.enter("ETH/USDT:USDT");

        assert_eq!(
            maybe_clear_manual_override(&mut state, "ETH/USDT:USDT", 10.0, 0),
            ManualOverrideDecision::NoChange
        );
        assert!(state.is_blocked_for_open("ETH/USDT:USDT"));

        assert_eq!(
            maybe_clear_manual_override(&mut state, "ETH/USDT:USDT", 0.0, 0),
            ManualOverrideDecision::Cleared("ETH/USDT:USDT".to_string())
        );
        assert!(!state.is_blocked_for_open("ETH/USDT:USDT"));
    }

    #[test]
    fn cap_breach_alone_does_not_force_reduce_manual_position() {
        let action = risk_action_for_manual_position(20_000.0, 5_000.0, false);

        assert_eq!(action, ManualRiskAction::DoNothing);
    }

    #[test]
    fn margin_danger_can_force_reduce_manual_position() {
        let action = risk_action_for_manual_position(20_000.0, 5_000.0, true);

        assert_eq!(action, ManualRiskAction::AllowEmergencyReduce);
    }

    #[test]
    fn system_order_manual_cancel_is_external_cancel() {
        assert_eq!(
            classify_external_status(InterventionKind::Cancel, true),
            Some("externally_cancelled")
        );
    }

    #[test]
    fn system_position_manual_close_is_external_close() {
        assert_eq!(
            classify_external_status(InterventionKind::Close, true),
            Some("externally_closed")
        );
    }
}
