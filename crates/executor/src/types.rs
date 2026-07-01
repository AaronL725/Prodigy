#[derive(Debug, Clone, PartialEq)]
pub struct TradeIntent {
    pub intent_id: String,
    pub symbol: String,
    pub side: String,
    pub action: String,
    pub target_notional: f64,
    pub max_order_notional: f64,
}

#[derive(Debug, Clone, PartialEq)]
pub struct OrderRecord {
    pub order_id: String,
    pub exchange_order_id: Option<String>,
    pub client_oid: String,
    pub intent_id: Option<String>,
    pub symbol: String,
    pub side: String,
    pub action: String,
    pub order_type: String,
    pub status: String,
    pub price: Option<f64>,
    pub size: f64,
    pub filled_size: f64,
    pub attempt: i64,
    pub raw_json: String,
    pub last_error: Option<String>,
}

#[derive(Debug, Clone, PartialEq)]
pub struct FillRecord {
    pub fill_id: String,
    pub order_id: String,
    pub trade_id: Option<String>,
    pub client_oid: Option<String>,
    pub symbol: String,
    pub side: String,
    pub price: f64,
    pub size: f64,
    pub fee: f64,
    pub created_at: String,
    pub raw_json: String,
}

#[derive(Debug, Clone, PartialEq)]
pub struct PositionRecord {
    pub symbol: String,
    pub side: String,
    pub notional: f64,
    pub entry_price: f64,
    pub unrealized_pnl: f64,
    pub ownership: String,
    pub opened_at: Option<String>,
    pub adopted_at: Option<String>,
    pub source_intent_id: Option<String>,
    pub raw_json: String,
}

#[derive(Debug, Clone, PartialEq)]
pub struct MarketUpdate {
    pub symbol: String,
    pub best_bid: f64,
    pub best_ask: f64,
    pub exchange_ts_ms: i64,
}

#[derive(Debug, Clone, Default, PartialEq)]
pub struct PrivateWsUpdate {
    pub orders: Vec<OrderRecord>,
    pub fills: Vec<FillRecord>,
    pub positions: Vec<PositionRecord>,
}
