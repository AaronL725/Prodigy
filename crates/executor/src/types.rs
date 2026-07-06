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
pub struct ControlCommand {
    pub command_id: String,
    pub command: String,
    pub requested_by: String,
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

/// Equity snapshot parsed from a private-WS `account` event. A fast cache update;
/// the REST reconcile path remains the source of truth.
#[derive(Debug, Clone, PartialEq)]
pub struct AccountSnapshotUpdate {
    pub equity: f64,
    pub available_margin: f64,
    pub unrealized_pnl: f64,
}

#[derive(Debug, Clone, Default, PartialEq)]
pub struct PrivateWsUpdate {
    pub orders: Vec<OrderRecord>,
    pub fills: Vec<FillRecord>,
    pub positions: Vec<PositionRecord>,
    pub account: Option<AccountSnapshotUpdate>,
    /// True when a private-WS `{"event":"login","code":"0",...}` ack was parsed.
    /// The WS loop waits for this before treating private state as ready.
    pub login_ack: bool,
    /// Set when a private-WS `{"event":"error",...}` or a non-zero login code is
    /// parsed (auth/subscribe failure). Carries the exchange message so the loop
    /// can emit a `websocket_auth_failed` event and stay not-ready until reconnect.
    pub auth_error: Option<String>,
    /// Private-WS `{"event":"subscribe","code":0,...}` ack channel, when present.
    /// Readiness waits for subscribe acks, not for the first real data push.
    pub subscribe_ack_channel: Option<String>,
}
