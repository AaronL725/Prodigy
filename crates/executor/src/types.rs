#[derive(Debug, Clone, PartialEq)]
pub struct TradeIntent {
    pub intent_id: String,
    pub symbol: String,
    pub side: String,
    pub action: String,
    pub target_notional: f64,
    pub max_order_notional: f64,
}
