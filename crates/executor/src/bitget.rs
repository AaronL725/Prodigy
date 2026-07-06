use anyhow::{bail, Context, Result};
use base64::engine::general_purpose::STANDARD;
use base64::Engine;
use hmac::{Hmac, Mac};
use reqwest::header::{HeaderMap, HeaderName, HeaderValue, CONTENT_TYPE};
use serde::Serialize;
use serde_json::Value;
use sha2::Sha256;
use std::collections::HashMap;
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use crate::config::ExecutorConfig;
use crate::types::{
    AccountSnapshotUpdate, MarketUpdate, OrderRecord, PositionRecord, PrivateWsUpdate,
};

type HmacSha256 = Hmac<Sha256>;

pub fn now_ms() -> String {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .expect("system clock before unix epoch")
        .as_millis()
        .to_string()
}

pub fn now_seconds() -> String {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .expect("system clock before unix epoch")
        .as_secs()
        .to_string()
}

pub fn sign(secret: &str, timestamp: &str, method: &str, path: &str, body: &str) -> String {
    let payload = format!("{timestamp}{method}{path}{body}");
    let mut mac = HmacSha256::new_from_slice(secret.as_bytes()).expect("hmac accepts key");
    mac.update(payload.as_bytes());
    STANDARD.encode(mac.finalize().into_bytes())
}

pub fn websocket_sign(secret: &str, timestamp: &str) -> String {
    sign(secret, timestamp, "GET", "/user/verify", "")
}

pub fn signed_headers(
    cfg: &ExecutorConfig,
    timestamp: &str,
    method: &str,
    path: &str,
    body: &str,
) -> Result<HashMap<String, String>> {
    cfg.validate_demo_only()?;
    let mut headers = HashMap::new();
    headers.insert("ACCESS-KEY".to_string(), cfg.secrets.api_key.clone());
    headers.insert(
        "ACCESS-SIGN".to_string(),
        sign(&cfg.secrets.api_secret, timestamp, method, path, body),
    );
    headers.insert(
        "ACCESS-PASSPHRASE".to_string(),
        cfg.secrets.passphrase.clone(),
    );
    headers.insert("ACCESS-TIMESTAMP".to_string(), timestamp.to_string());
    headers.insert("locale".to_string(), "en-US".to_string());
    headers.insert("Content-Type".to_string(), "application/json".to_string());
    headers.insert("PAPTRADING".to_string(), "1".to_string());
    Ok(headers)
}

fn to_headermap(headers: HashMap<String, String>) -> Result<HeaderMap> {
    let mut map = HeaderMap::new();
    for (key, value) in headers {
        let name = HeaderName::from_bytes(key.as_bytes()).with_context(|| key.clone())?;
        map.insert(name, HeaderValue::from_str(&value)?);
    }
    map.insert(CONTENT_TYPE, HeaderValue::from_static("application/json"));
    Ok(map)
}

#[derive(Debug, Clone)]
pub struct BitgetRestClient {
    cfg: ExecutorConfig,
    client: reqwest::Client,
}

impl BitgetRestClient {
    pub fn new(cfg: ExecutorConfig) -> Result<Self> {
        cfg.validate_demo_only()?;
        Ok(Self {
            cfg,
            client: reqwest::Client::builder()
                .timeout(bitget_rest_timeout())
                .build()?,
        })
    }

    /// Read-only accessors for the configured symbol/product (reconcile + reset use them).
    pub fn product_type(&self) -> &str {
        &self.cfg.product_type
    }

    pub fn margin_coin(&self) -> &str {
        &self.cfg.margin_coin
    }

    pub fn display_symbol(&self) -> &str {
        &self.cfg.symbol
    }

    pub fn bitget_symbol(&self) -> &str {
        &self.cfg.bitget_symbol
    }

    pub async fn get(&self, path: &str, query: &[(&str, String)]) -> Result<Value> {
        let query_string = if query.is_empty() {
            String::new()
        } else {
            let joined = query
                .iter()
                .map(|(k, v)| format!("{k}={v}"))
                .collect::<Vec<_>>()
                .join("&");
            format!("?{joined}")
        };
        let request_path = format!("{path}{query_string}");
        let timestamp = now_ms();
        let headers = to_headermap(signed_headers(
            &self.cfg,
            &timestamp,
            "GET",
            &request_path,
            "",
        )?)?;
        let url = format!("{}{}", self.cfg.rest_base_url, request_path);
        let response = self.client.get(url).headers(headers).send().await?;
        parse_bitget_response(response).await
    }

    pub async fn post_json<T: Serialize>(&self, path: &str, body: &T) -> Result<Value> {
        let body_text = serde_json::to_string(body)?;
        let timestamp = now_ms();
        let headers = to_headermap(signed_headers(
            &self.cfg, &timestamp, "POST", path, &body_text,
        )?)?;
        let url = format!("{}{}", self.cfg.rest_base_url, path);
        let response = self
            .client
            .post(url)
            .headers(headers)
            .body(body_text)
            .send()
            .await?;
        parse_bitget_response(response).await
    }

    pub async fn cancel_order(&self, request: &CancelOrderRequest) -> Result<Value> {
        self.post_json("/api/v2/mix/order/cancel-order", request)
            .await
    }

    pub async fn cancel_all_orders(&self) -> Result<Value> {
        // ponytail: Bitget v2 mix has no working "cancel all by productType" endpoint
        // (cancel-all-order 404s). Query pending orders, then cancel each by clientOid
        // through the single-cancel endpoint (verified working in the demo tests).
        // Best-effort per order: a 404 on an already-gone order must not abort the rest.
        let pending = self
            .get(
                "/api/v2/mix/order/orders-pending",
                &[
                    ("productType", self.cfg.product_type.clone()),
                    ("marginCoin", self.cfg.margin_coin.clone()),
                ],
            )
            .await?;
        let mut cancelled = 0u32;
        for row in pending
            .get("data")
            .and_then(Value::as_array)
            .into_iter()
            .flatten()
        {
            if row.get("symbol").and_then(Value::as_str) != Some(self.cfg.bitget_symbol.as_str()) {
                continue;
            }
            let client_oid = row
                .get("clientOid")
                .and_then(Value::as_str)
                .unwrap_or("")
                .to_string();
            if client_oid.is_empty() {
                continue;
            }
            let req = CancelOrderRequest {
                symbol: self.cfg.bitget_symbol.clone(),
                product_type: self.cfg.product_type.clone(),
                margin_coin: self.cfg.margin_coin.clone(),
                client_oid: client_oid.clone(),
            };
            if self.cancel_order(&req).await.is_ok() {
                cancelled += 1;
            }
        }
        Ok(serde_json::json!({ "cancelled": cancelled }))
    }

    pub async fn get_account_snapshot(&self) -> Result<Value> {
        self.get(
            "/api/v2/mix/account/account",
            &[
                ("symbol", self.cfg.bitget_symbol.clone()),
                ("productType", self.cfg.product_type.clone()),
                ("marginCoin", self.cfg.margin_coin.clone()),
            ],
        )
        .await
    }

    /// Set the leverage for the configured symbol. Called once at startup so the
    /// demo account runs at the configured leverage (default 5x).
    pub async fn set_leverage(&self, leverage: u32) -> Result<Value> {
        #[derive(Serialize)]
        #[serde(rename_all = "camelCase")]
        struct SetLeverage<'a> {
            symbol: &'a str,
            product_type: &'a str,
            margin_coin: &'a str,
            leverage: String,
        }
        self.post_json(
            "/api/v2/mix/account/set-leverage",
            &SetLeverage {
                symbol: &self.cfg.bitget_symbol,
                product_type: &self.cfg.product_type,
                margin_coin: &self.cfg.margin_coin,
                leverage: leverage.to_string(),
            },
        )
        .await
    }

    /// Poll a single order by client_oid. Returns the `data` object from
    /// GET /api/v2/mix/order/detail. A canceled or filled order is still
    /// returned here (it is historical, not deleted), so this is the correct
    /// endpoint for terminal-state polling of both fills and cancellations.
    // ponytail: return the raw `data` Value and let the pure classifier
    // (classify_order_poll) read status/state + baseVolume. Keeping the field
    // extraction in the caller avoids a second status struct and reuses the
    // one glue function the wiring test covers. Detail's status key is
    // inconsistent across Bitget's docs (table says `status`, example shows
    // `state`), so callers read both.
    pub async fn get_order_detail(&self, client_oid: &str) -> Result<Value> {
        let response = self
            .get(
                "/api/v2/mix/order/detail",
                &[
                    ("symbol", self.cfg.bitget_symbol.clone()),
                    ("productType", self.cfg.product_type.clone()),
                    ("clientOid", client_oid.to_string()),
                ],
            )
            .await?;
        Ok(response.get("data").cloned().unwrap_or(Value::Null))
    }

    /// Fetch the demo order book depth (best bid/ask + sizes) for the configured
    /// symbol via GET /api/v2/mix/market/merge-depth. The signed `get` carries the
    /// `paptrading: 1` header, so this returns the DEMO book (not live) — used to
    /// judge whether the demo book is actually tradable before an order test.
    pub async fn merge_depth(&self) -> Result<DepthSnapshot> {
        let resp = self
            .get(
                "/api/v2/mix/market/merge-depth",
                &[
                    ("productType", self.cfg.product_type.clone()),
                    ("symbol", self.cfg.bitget_symbol.clone()),
                    ("limit", "5".to_string()),
                    ("precision", "scale0".to_string()),
                ],
            )
            .await?;
        let data = resp
            .get("data")
            .ok_or_else(|| anyhow::anyhow!("merge-depth missing data"))?;
        // asks/bids are arrays of [price, size] levels; best = first. ask asc, bid desc.
        let best_level = |side: &str| -> Option<&Value> {
            data.get(side)
                .and_then(Value::as_array)
                .and_then(|rows| rows.first())
        };
        let parse = |lvl: Option<&Value>| -> Option<(f64, f64)> {
            let lvl = lvl?.as_array()?;
            let price = num_or_str_f64(lvl.first()?)?;
            let size = num_or_str_f64(lvl.get(1)?)?;
            Some((price, size))
        };
        Ok(DepthSnapshot {
            best_bid: parse(best_level("bids")),
            best_ask: parse(best_level("asks")),
        })
    }
}

fn bitget_rest_timeout() -> Duration {
    Duration::from_secs(15)
}

/// Read a depth level field that Bitget serializes as either a JSON number
/// (merge-depth: `1977.31`) or a string (ticker/order-detail: `"1977.31"`).
fn num_or_str_f64(v: &Value) -> Option<f64> {
    v.as_f64()
        .or_else(|| v.as_str().and_then(|s| s.parse().ok()))
}

/// Best bid/ask (price, base size) snapshot from merge-depth. Either side is
/// None when the book has no level at that depth.
#[derive(Debug, Clone, Copy, PartialEq)]
pub struct DepthSnapshot {
    pub best_bid: Option<(f64, f64)>,
    pub best_ask: Option<(f64, f64)>,
}

/// Decide whether a demo book is tradable for a market order test. The demo
/// ETHUSDT book is often phantom-liquid: it publishes a best ask/bid that sit
/// far apart (spread many × a real market) and beyond the exchange price-limit
/// band, so a market order is accepted then cancelled by Bitget with no fill.
/// A spread larger than `max_spread_fraction` of the mid (default 2%), or a
/// missing/zero-size top level, marks the book non-tradable. Pure so the test
/// can assert the classifier and the threshold is named, not magic.
pub fn book_tradable(depth: &DepthSnapshot, max_spread_fraction: f64) -> bool {
    // Require a positive price AND positive size on both top levels — a level
    // with price but zero size is phantom and won't fill.
    let valid = |lvl: Option<(f64, f64)>| lvl.filter(|(p, s)| *p > 0.0 && *s > 0.0);
    let bid = match valid(depth.best_bid) {
        Some(v) => v.0,
        None => return false,
    };
    let ask = match valid(depth.best_ask) {
        Some(v) => v.0,
        None => return false,
    };
    if ask <= bid {
        return false; // crossed/inverted — treat cautiously.
    }
    let mid = (bid + ask) / 2.0;
    (ask - bid) / mid <= max_spread_fraction
}

async fn parse_bitget_response(response: reqwest::Response) -> Result<Value> {
    let status = response.status();
    let value: Value = response.json().await?;
    if !status.is_success() {
        bail!("bitget http status {status}: {value}");
    }
    let code = value.get("code").and_then(|v| v.as_str()).unwrap_or("");
    if code != "00000" && code != "0" {
        bail!("bitget api error: {value}");
    }
    Ok(value)
}

#[derive(Debug, Clone, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct PlaceOrderRequest {
    pub symbol: String,
    pub product_type: String,
    pub margin_mode: String,
    pub margin_coin: String,
    pub size: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub price: Option<String>,
    pub side: String,
    pub order_type: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub force: Option<String>,
    pub client_oid: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub reduce_only: Option<String>,
}

#[derive(Debug, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct CancelOrderRequest {
    pub symbol: String,
    pub product_type: String,
    pub margin_coin: String,
    pub client_oid: String,
}

pub fn parse_public_ws_message(text: &str) -> Result<Option<MarketUpdate>> {
    if text == "pong" {
        return Ok(None);
    }
    let value: Value = serde_json::from_str(text)?;
    let channel = value
        .pointer("/arg/channel")
        .and_then(Value::as_str)
        .unwrap_or("");
    if channel != "books5" && channel != "books1" {
        return Ok(None);
    }
    let data = value
        .get("data")
        .and_then(Value::as_array)
        .and_then(|rows| rows.first())
        .context("missing books data")?;
    let bid = data
        .get("bids")
        .and_then(Value::as_array)
        .and_then(|rows| rows.first())
        .and_then(Value::as_array)
        .context("missing best bid")?;
    let ask = data
        .get("asks")
        .and_then(Value::as_array)
        .and_then(|rows| rows.first())
        .and_then(Value::as_array)
        .context("missing best ask")?;
    Ok(Some(MarketUpdate {
        symbol: value
            .pointer("/arg/instId")
            .and_then(Value::as_str)
            .unwrap_or("")
            .to_string(),
        best_bid: parse_f64(bid.first(), "best bid")?,
        best_ask: parse_f64(ask.first(), "best ask")?,
        exchange_ts_ms: value.get("ts").and_then(Value::as_i64).unwrap_or(0),
    }))
}

pub fn parse_private_ws_message(text: &str, cfg: &ExecutorConfig) -> Result<PrivateWsUpdate> {
    if text == "pong" {
        return Ok(PrivateWsUpdate::default());
    }
    let value: Value = serde_json::from_str(text)?;
    let mut update = PrivateWsUpdate::default();

    // Connection-control events arrive WITHOUT an arg.channel: the login ack
    // ({"event":"login","code":0,...}) confirms auth; any {"event":"error",...}
    // or a non-zero login code is an auth/subscribe failure. The WS loop gates
    // private-state readiness on the ack and emits websocket_auth_failed on the
    // error, so the parser must surface both. Data messages (orders/positions/
    // account) have no top-level "event" and fall through to the channel dispatch.
    // ponytail: Bitget serializes `code` as a NUMBER on the login ack
    // ({"event":"login","code":0}) but as a STRING on REST/error bodies; accept
    // both so a numeric ack isn't mis-read as a missing/failed code.
    if let Some(event) = value.get("event").and_then(Value::as_str) {
        if event == "login" {
            if ws_code(&value) == "0" {
                update.login_ack = true;
            } else {
                update.auth_error = Some(format!(
                    "login code {}: {}",
                    ws_code(&value),
                    value.get("msg").and_then(Value::as_str).unwrap_or("")
                ));
            }
            return Ok(update);
        }
        if event == "subscribe" {
            let code = ws_code(&value);
            if code.is_empty() || code == "0" {
                update.subscribe_ack_channel = value
                    .pointer("/arg/channel")
                    .and_then(Value::as_str)
                    .map(str::to_string);
            } else {
                update.auth_error = Some(format!(
                    "subscribe code {}: {}",
                    code,
                    value.get("msg").and_then(Value::as_str).unwrap_or("")
                ));
            }
            return Ok(update);
        }
        if event == "error" {
            update.auth_error = Some(format!(
                "code {}: {}",
                ws_code(&value),
                value.get("msg").and_then(Value::as_str).unwrap_or("")
            ));
            return Ok(update);
        }
        // Other control events (e.g. subscribe ack) carry no private data; return
        // the empty update so the read loop skips it.
        return Ok(update);
    }

    let channel = value
        .pointer("/arg/channel")
        .and_then(Value::as_str)
        .unwrap_or("");
    let data = value
        .get("data")
        .and_then(Value::as_array)
        .cloned()
        .unwrap_or_default();

    if channel == "orders" {
        for row in data {
            let order_id = str_field(&row, "orderId");
            let client_oid = str_field(&row, "clientOid");
            update.orders.push(OrderRecord {
                order_id: order_id.clone(),
                exchange_order_id: Some(order_id),
                client_oid,
                intent_id: None,
                symbol: resolve_symbol(cfg, &str_field(&row, "instId")),
                side: str_field(&row, "side"),
                action: str_field(&row, "tradeSide"),
                order_type: str_field(&row, "orderType"),
                status: str_field(&row, "status"),
                price: row
                    .get("price")
                    .and_then(Value::as_str)
                    .and_then(|v| v.parse().ok()),
                size: row
                    .get("size")
                    .and_then(Value::as_str)
                    .and_then(|v| v.parse().ok())
                    .unwrap_or(0.0),
                filled_size: row
                    .get("accBaseVolume")
                    .and_then(Value::as_str)
                    .and_then(|v| v.parse().ok())
                    .unwrap_or(0.0),
                attempt: 1,
                raw_json: row.to_string(),
                last_error: None,
            });
        }
    } else if channel == "positions" {
        for row in data {
            let size = row.get("total").and_then(num_or_str_f64).unwrap_or(0.0);
            if size.abs() < 1e-12 {
                continue; // no open position
            }
            let entry_price = row
                .get("openPriceAvg")
                .and_then(num_or_str_f64)
                .unwrap_or(0.0);
            update.positions.push(PositionRecord {
                symbol: resolve_symbol(cfg, &str_field(&row, "instId")),
                side: str_field(&row, "holdSide"),
                notional: size.abs() * entry_price,
                entry_price,
                unrealized_pnl: row
                    .get("unrealizedPL")
                    .and_then(num_or_str_f64)
                    .unwrap_or(0.0),
                ownership: "system".to_string(),
                opened_at: None,
                adopted_at: None,
                source_intent_id: None,
                raw_json: row.to_string(),
            });
        }
    } else if channel == "account" {
        // ponytail: a single account message can carry multiple marginCoin rows;
        // the configured marginCoin is the one the risk gate cares about. Take the
        // first matching row, fall back to the first row if none matches. equity is
        // accountEquity on REST and either equity/accountEquity on WS — accept both.
        let row = data
            .iter()
            .find(|r| str_field(r, "marginCoin") == cfg.margin_coin)
            .or_else(|| data.first());
        if let Some(row) = row {
            let equity = row
                .get("accountEquity")
                .and_then(num_or_str_f64)
                .or_else(|| row.get("equity").and_then(num_or_str_f64));
            let available = row.get("available").and_then(num_or_str_f64);
            if let (Some(equity), Some(available)) = (equity, available) {
                update.account = Some(AccountSnapshotUpdate {
                    equity,
                    available_margin: available,
                    unrealized_pnl: row
                        .get("unrealizedPL")
                        .and_then(num_or_str_f64)
                        .unwrap_or(0.0),
                });
            }
        }
    }

    Ok(update)
}

/// Map a private-WS message instId to the SQLite execution-layer symbol. The
/// execution layer (orders/positions/manual_override) uses ONE symbol — the
/// exchange symbol (bitget_symbol, e.g. "ETHUSDT") — matching what REST
/// reconcile, trade_intents, and the Python signal daemon all read. Returning
/// the display symbol (ETH/USDT:USDT) here split the convention: WS-written
/// positions/orders landed under a different key than REST reconcile's, and the
/// Python daemon (which reads ETHUSDT) couldn't see them. A foreign instId
/// falls through to the raw string — never silently relabel a non-configured
/// symbol.
fn resolve_symbol(cfg: &ExecutorConfig, inst_id: &str) -> String {
    if inst_id == cfg.bitget_symbol {
        cfg.bitget_symbol.clone()
    } else {
        inst_id.to_string()
    }
}

fn str_field(value: &Value, key: &str) -> String {
    value
        .get(key)
        .and_then(Value::as_str)
        .unwrap_or("")
        .to_string()
}

/// Read a connection-control `code` field as a string regardless of whether
/// Bitget serialized it as a number (login ack: `"code":0`) or a string
/// (error/REST bodies: `"code":"30001"`). Empty string when absent.
fn ws_code(value: &Value) -> String {
    match value.get("code") {
        Some(Value::String(s)) => s.clone(),
        Some(Value::Number(n)) => n.to_string(),
        Some(other) => other.to_string(),
        None => String::new(),
    }
}

fn parse_f64(value: Option<&Value>, name: &str) -> Result<f64> {
    value
        .and_then(Value::as_str)
        .context(name.to_string())?
        .parse::<f64>()
        .with_context(|| format!("parse {name}"))
}

/// Private WS login payload: signs `GET /user/verify` with the per-connection
/// timestamp. Pure so the one-shot verify and the long-running WS loop build the
/// identical login (and the test pins the signature path).
pub fn private_login_message(cfg: &ExecutorConfig, timestamp: &str) -> serde_json::Value {
    serde_json::json!({
        "op": "login",
        "args": [{
            "apiKey": cfg.secrets.api_key,
            "passphrase": cfg.secrets.passphrase,
            "timestamp": timestamp,
            "sign": websocket_sign(&cfg.secrets.api_secret, timestamp)
        }]
    })
}

/// Private WS subscribe payload for orders/positions/account (orders+positions
/// key on instId, account keys on coin; "default" carries every symbol for the
/// product type). Pure so the one-shot verify and the long-running WS loop build
/// the identical subscription. Sent after login.
pub fn private_subscribe_message(cfg: &ExecutorConfig) -> serde_json::Value {
    serde_json::json!({
        "op": "subscribe",
        "args": [
            {"instType": cfg.product_type, "channel": "orders", "instId": "default"},
            {"instType": cfg.product_type, "channel": "positions", "instId": "default"},
            {"instType": cfg.product_type, "channel": "account", "coin": "default"}
        ]
    })
}

#[derive(Debug, Default)]
pub struct PrivateSubscribeAcks {
    orders: bool,
    positions: bool,
    account: bool,
}

impl PrivateSubscribeAcks {
    pub fn record(&mut self, update: &PrivateWsUpdate) {
        match update.subscribe_ack_channel.as_deref() {
            Some("orders") => self.orders = true,
            Some("positions") => self.positions = true,
            Some("account") => self.account = true,
            _ => {}
        }
    }

    pub fn ready(&self) -> bool {
        self.orders && self.positions && self.account
    }
}

/// Public books5 subscribe payload for the configured symbol. Pure so the one-shot
/// verify and the long-running WS loop build the identical subscription.
pub fn public_books5_subscribe_message(cfg: &ExecutorConfig) -> serde_json::Value {
    serde_json::json!({
        "op": "subscribe",
        "args": [{
            "instType": cfg.product_type,
            "channel": "books5",
            "instId": cfg.bitget_symbol
        }]
    })
}

pub fn public_ws_message_confirms_subscription(text: &str, cfg: &ExecutorConfig) -> Result<bool> {
    if text == "pong" {
        return Ok(false);
    }
    let value: Value = serde_json::from_str(text)?;
    if let Some(event) = value.get("event").and_then(Value::as_str) {
        if event == "error" {
            bail!(
                "public websocket subscription failed: code {}: {}",
                ws_code(&value),
                value.get("msg").and_then(Value::as_str).unwrap_or("")
            );
        }
        if event == "subscribe" {
            let code = ws_code(&value);
            if !code.is_empty() && code != "0" {
                bail!(
                    "public websocket subscription failed: code {}: {}",
                    code,
                    value.get("msg").and_then(Value::as_str).unwrap_or("")
                );
            }
            let channel = value
                .pointer("/arg/channel")
                .and_then(Value::as_str)
                .unwrap_or("");
            let inst_id = value
                .pointer("/arg/instId")
                .and_then(Value::as_str)
                .unwrap_or("");
            return Ok(matches!(channel, "books5" | "books1") && inst_id == cfg.bitget_symbol);
        }
        return Ok(false);
    }
    Ok(parse_public_ws_message(text)?
        .is_some_and(|m| m.symbol == cfg.bitget_symbol && m.best_bid > 0.0 && m.best_ask > 0.0))
}

pub async fn verify_public_ws_connects(cfg: &ExecutorConfig) -> Result<()> {
    cfg.validate_demo_only()?;
    let (mut socket, _) = tokio_tungstenite::connect_async(&cfg.public_ws_url).await?;
    use futures_util::{SinkExt, StreamExt};
    let msg = public_books5_subscribe_message(cfg);
    socket
        .send(tokio_tungstenite::tungstenite::Message::Text(
            msg.to_string(),
        ))
        .await?;
    let deadline = std::time::Instant::now() + Duration::from_secs(10);
    while std::time::Instant::now() < deadline {
        let remaining = deadline.saturating_duration_since(std::time::Instant::now());
        let msg = tokio::time::timeout(remaining, socket.next())
            .await?
            .ok_or_else(|| anyhow::anyhow!("public websocket closed before subscribe ack"))??;
        let Ok(text) = msg.into_text() else {
            continue;
        };
        if public_ws_message_confirms_subscription(&text, cfg)? {
            return Ok(());
        }
    }
    bail!("public websocket subscribe ack timed out")
}

pub async fn verify_private_ws_connects(cfg: &ExecutorConfig) -> Result<()> {
    cfg.validate_demo_only()?;
    let (mut socket, _) = tokio_tungstenite::connect_async(&cfg.private_ws_url).await?;
    use futures_util::{SinkExt, StreamExt};
    use tokio_tungstenite::tungstenite::Message;
    let timestamp = now_seconds();
    let login = private_login_message(cfg, &timestamp);
    socket.send(Message::Text(login.to_string())).await?;
    let msg = tokio::time::timeout(std::time::Duration::from_secs(10), socket.next())
        .await?
        .ok_or_else(|| anyhow::anyhow!("private websocket closed"))??;
    let text = msg.into_text()?;
    // ponytail: reuse the daemon's parser instead of a string-contains probe so
    // the one-shot verify and the long-running loop judge login the same way —
    // including Bitget's NUMERIC login code ({"event":"login","code":0}), which
    // a "contains \"login\"" check accepts but a future stricter check could
    // diverge on. Surfaces the real code+msg on failure instead of raw text.
    let update = parse_private_ws_message(&text, cfg)?;
    if let Some(detail) = update.auth_error {
        bail!("private websocket login failed: {detail}");
    }
    if !update.login_ack {
        bail!("private websocket login not acked: {text}");
    }
    socket
        .send(Message::Text(private_subscribe_message(cfg).to_string()))
        .await?;
    let deadline = std::time::Instant::now() + Duration::from_secs(10);
    let mut acks = PrivateSubscribeAcks::default();
    while std::time::Instant::now() < deadline {
        let remaining = deadline.saturating_duration_since(std::time::Instant::now());
        let msg = tokio::time::timeout(remaining, socket.next())
            .await?
            .ok_or_else(|| anyhow::anyhow!("private websocket closed before subscribe ack"))??;
        let Ok(text) = msg.into_text() else {
            continue;
        };
        let update = parse_private_ws_message(&text, cfg)?;
        if let Some(detail) = update.auth_error {
            bail!("private websocket subscribe failed: {detail}");
        }
        acks.record(&update);
        if acks.ready() {
            return Ok(());
        }
    }
    bail!("private websocket subscribe ack timed out")
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn rest_signature_matches_hmac_sha256_base64() {
        let sig = sign(
            "secret",
            "16273667805456",
            "POST",
            "/api/v2/mix/order/place-order",
            r#"{"symbol":"ETHUSDT"}"#,
        );

        assert_eq!(sig, "7q0EikaFI6vj9FuQRddouLkfjADl2NLTCej2t5t/QZY=");
    }

    #[test]
    fn websocket_signature_uses_user_verify_path() {
        let sig = websocket_sign("secret", "1538054050");

        assert_eq!(sig, "QW9NDIxQTfljkfSeZydUIsfx+5D1GgkIbDzvrCplpp4=");
    }

    #[test]
    fn demo_rest_headers_include_paptrading() {
        let cfg = crate::config::ExecutorConfig::demo_for_tests();
        let headers = signed_headers(&cfg, "1", "GET", "/api/v2/mix/account/account", "").unwrap();

        assert_eq!(headers.get("PAPTRADING").unwrap(), "1");
        assert_eq!(headers.get("ACCESS-KEY").unwrap(), "key");
    }

    #[test]
    fn parses_books5_snapshot_into_best_bid_ask() {
        let raw = r#"{
          "action":"snapshot",
          "arg":{"instType":"USDT-FUTURES","channel":"books5","instId":"ETHUSDT"},
          "data":[{"bids":[["3000.1","2"]],"asks":[["3000.2","3"]],"ts":"1760461517285"}],
          "ts":1760461517285
        }"#;

        let update = parse_public_ws_message(raw).unwrap().unwrap();

        assert_eq!(update.symbol, "ETHUSDT");
        assert_eq!(update.best_bid, 3000.1);
        assert_eq!(update.best_ask, 3000.2);
    }

    #[test]
    fn public_ws_verify_requires_subscribe_ack_or_valid_market() {
        let cfg = ExecutorConfig::demo_for_tests();
        assert!(!public_ws_message_confirms_subscription("pong", &cfg).unwrap());
        assert!(public_ws_message_confirms_subscription(
            r#"{"event":"subscribe","arg":{"channel":"books5","instId":"ETHUSDT"}}"#,
            &cfg,
        )
        .unwrap());
        assert!(public_ws_message_confirms_subscription(
            r#"{
              "action":"snapshot",
              "arg":{"instType":"USDT-FUTURES","channel":"books5","instId":"ETHUSDT"},
              "data":[{"bids":[["3000.1","2"]],"asks":[["3000.2","3"]]}],
              "ts":1760461517285
            }"#,
            &cfg,
        )
        .unwrap());
        let err = public_ws_message_confirms_subscription(
            r#"{"event":"error","code":"30001","msg":"bad subscribe"}"#,
            &cfg,
        )
        .unwrap_err();
        assert!(err.to_string().contains("bad subscribe"));
    }

    #[test]
    fn parses_private_order_message() {
        let raw = r#"{
          "action":"snapshot",
          "arg":{"instType":"USDT-FUTURES","instId":"default","channel":"orders"},
          "data":[{
            "orderId":"123",
            "clientOid":"client-1",
            "instId":"ETHUSDT",
            "side":"buy",
            "orderType":"limit",
            "status":"live",
            "price":"3000",
            "size":"0.01",
            "accBaseVolume":"0"
          }]
        }"#;

        let cfg = ExecutorConfig::demo_for_tests();
        let update = parse_private_ws_message(raw, &cfg).unwrap();

        assert_eq!(update.orders.len(), 1);
        assert_eq!(update.orders[0].client_oid, "client-1");
        assert_eq!(update.orders[0].status, "live");
        // Execution-layer symbol: instId ETHUSDT resolves to the exchange symbol
        // (bitget_symbol), matching REST reconcile / trade_intents / the Python daemon.
        assert_eq!(update.orders[0].symbol, "ETHUSDT");
    }

    #[test]
    fn private_subscribe_message_covers_orders_positions_account() {
        let cfg = ExecutorConfig::demo_for_tests();
        let msg = private_subscribe_message(&cfg);
        let text = msg.to_string();

        assert!(text.contains("\"op\":\"subscribe\""));
        assert!(text.contains("\"channel\":\"orders\""));
        assert!(text.contains("\"channel\":\"positions\""));
        assert!(text.contains("\"channel\":\"account\""));
        assert!(text.contains("\"instType\":\"USDT-FUTURES\""));
    }

    #[test]
    fn parses_private_subscribe_ack_channel() {
        let cfg = ExecutorConfig::demo_for_tests();
        let update = parse_private_ws_message(
            r#"{"event":"subscribe","code":0,"arg":{"channel":"orders"}}"#,
            &cfg,
        )
        .unwrap();

        assert_eq!(update.subscribe_ack_channel.as_deref(), Some("orders"));
        assert!(update.auth_error.is_none());
    }

    #[test]
    fn parses_private_subscribe_ack_without_code_as_success() {
        let cfg = ExecutorConfig::demo_for_tests();
        let update = parse_private_ws_message(
            r#"{"event":"subscribe","arg":{"channel":"positions"}}"#,
            &cfg,
        )
        .unwrap();

        assert_eq!(update.subscribe_ack_channel.as_deref(), Some("positions"));
        assert!(update.auth_error.is_none());
    }

    #[test]
    fn private_subscribe_ack_tracker_requires_all_channels() {
        let mut acks = PrivateSubscribeAcks::default();
        for channel in ["orders", "positions"] {
            let update = PrivateWsUpdate {
                subscribe_ack_channel: Some(channel.to_string()),
                ..PrivateWsUpdate::default()
            };
            acks.record(&update);
        }
        assert!(!acks.ready(), "two private channel acks are not enough");

        let update = PrivateWsUpdate {
            subscribe_ack_channel: Some("account".to_string()),
            ..PrivateWsUpdate::default()
        };
        acks.record(&update);

        assert!(acks.ready());
    }

    #[test]
    fn private_verify_sends_subscribe_and_requires_ack() {
        let source = include_str!("bitget.rs");
        let start = source
            .find("pub async fn verify_private_ws_connects")
            .expect("verify_private_ws_connects exists");
        let tail = &source[start..];
        let end = tail
            .find("#[cfg(test)]")
            .expect("test module follows verify");
        let body = &tail[..end];

        assert!(body.contains("private_subscribe_message"));
        assert!(body.contains("PrivateSubscribeAcks"));
        assert!(body.contains(".ready()"));
    }

    #[test]
    fn bitget_rest_client_has_timeout() {
        let source = include_str!("bitget.rs");

        assert!(source.contains(".timeout(bitget_rest_timeout())"));
    }

    #[test]
    fn parses_private_position_message() {
        let raw = r#"{
          "action":"snapshot",
          "arg":{"instType":"USDT-FUTURES","instId":"default","channel":"positions"},
          "data":[{
            "instId":"ETHUSDT",
            "holdSide":"long",
            "total":"0.05",
            "openPriceAvg":"3000",
            "unrealizedPL":"1.5"
          }]
        }"#;

        let cfg = ExecutorConfig::demo_for_tests();
        let update = parse_private_ws_message(raw, &cfg).unwrap();

        assert_eq!(update.positions.len(), 1);
        let pos = &update.positions[0];
        assert_eq!(pos.symbol, "ETHUSDT");
        assert_eq!(pos.side, "long");
        assert!((pos.entry_price - 3000.0).abs() < 1e-9);
        assert!((pos.notional - 150.0).abs() < 1e-9);
        assert!((pos.unrealized_pnl - 1.5).abs() < 1e-9);
    }

    #[test]
    fn parses_private_account_message() {
        let raw = r#"{
          "action":"snapshot",
          "arg":{"instType":"USDT-FUTURES","instId":"default","channel":"account"},
          "data":[{
            "marginCoin":"USDT",
            "available":"500",
            "accountEquity":"1000",
            "unrealizedPL":"-2"
          }]
        }"#;

        let cfg = ExecutorConfig::demo_for_tests();
        let update = parse_private_ws_message(raw, &cfg).unwrap();

        let acct = update.account.expect("account snapshot parsed");
        assert!((acct.equity - 1000.0).abs() < 1e-9);
        assert!((acct.available_margin - 500.0).abs() < 1e-9);
        assert!((acct.unrealized_pnl - (-2.0)).abs() < 1e-9);
    }

    #[test]
    fn parses_private_login_ack() {
        // Bitget's real login ack serializes code as a NUMBER: {"event":"login","code":0}.
        // The WS loop waits for this before treating private state as ready, so the
        // parser must set login_ack for the numeric form (and the string form too).
        let cfg = ExecutorConfig::demo_for_tests();
        let update = parse_private_ws_message(r#"{"event":"login","code":0}"#, &cfg).unwrap();
        assert!(update.login_ack);
        assert!(update.auth_error.is_none());
        // defensive: a string "0" must also ack (some clients/gateways vary)
        let update_s = parse_private_ws_message(r#"{"event":"login","code":"0"}"#, &cfg).unwrap();
        assert!(update_s.login_ack);
    }

    #[test]
    fn parses_private_login_failure_code_as_auth_error() {
        // A login response with a non-zero code (numeric, like the ack). Must
        // surface as auth_error so the loop emits websocket_auth_failed and stays
        // not-ready; login_ack must NOT be set.
        let cfg = ExecutorConfig::demo_for_tests();
        let update = parse_private_ws_message(
            r#"{"event":"login","code":30001,"msg":"sign invalid"}"#,
            &cfg,
        )
        .unwrap();
        assert!(!update.login_ack);
        let err = update.auth_error.expect("auth error parsed");
        assert!(err.contains("30001"));
        assert!(err.contains("sign invalid"));
    }

    #[test]
    fn parses_private_error_event_as_auth_error() {
        // A generic {"event":"error",...} (string code, as on REST/error bodies).
        let cfg = ExecutorConfig::demo_for_tests();
        let update = parse_private_ws_message(
            r#"{"event":"error","code":"30005","msg":"login timeout"}"#,
            &cfg,
        )
        .unwrap();
        assert!(!update.login_ack);
        let err = update.auth_error.expect("auth error parsed");
        assert!(err.contains("30005"));
        assert!(err.contains("login timeout"));
    }

    #[test]
    fn public_books5_subscribe_payload_targets_demo_symbol() {
        let cfg = ExecutorConfig::demo_for_tests();
        let msg = public_books5_subscribe_message(&cfg);
        let text = msg.to_string();

        assert!(text.contains("\"op\":\"subscribe\""));
        assert!(text.contains("\"channel\":\"books5\""));
        assert!(text.contains("\"instId\":\"ETHUSDT\""));
        assert!(text.contains("\"instType\":\"USDT-FUTURES\""));
    }

    #[test]
    fn private_login_payload_uses_websocket_signature() {
        let cfg = ExecutorConfig::demo_for_tests();
        let msg = private_login_message(&cfg, "1538054050");
        let text = msg.to_string();

        assert!(text.contains("\"op\":\"login\""));
        assert!(text.contains("\"apiKey\":\"key\""));
        assert!(text.contains("\"passphrase\":\"pass\""));
        assert!(text.contains("\"timestamp\":\"1538054050\""));
        assert!(text.contains(&websocket_sign("secret", "1538054050")));
    }

    #[test]
    fn book_tradable_flags_wide_phantom_spread_and_missing_levels() {
        // A real market: tight spread, both sides have size → tradable.
        let tight = DepthSnapshot {
            best_bid: Some((1703.97, 51.0)),
            best_ask: Some((1703.98, 36.0)),
        };
        assert!(book_tradable(&tight, 0.02));

        // The phantom demo book: ask 1977 / bid 1740 → ~12.5% spread, far beyond
        // any real market → not tradable (a market order would be cancelled).
        let phantom = DepthSnapshot {
            best_bid: Some((1740.66, 0.04)),
            best_ask: Some((1977.31, 1.30)),
        };
        assert!(!book_tradable(&phantom, 0.02));

        // Missing one side → not tradable.
        let one_sided = DepthSnapshot {
            best_bid: Some((1703.97, 51.0)),
            best_ask: None,
        };
        assert!(!book_tradable(&one_sided, 0.02));

        // Top level with zero size → not tradable.
        let zero_size = DepthSnapshot {
            best_bid: Some((1703.97, 0.0)),
            best_ask: Some((1703.98, 36.0)),
        };
        assert!(!book_tradable(&zero_size, 0.02));
    }
}
