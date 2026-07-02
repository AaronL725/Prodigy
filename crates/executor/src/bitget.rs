use anyhow::{bail, Context, Result};
use base64::engine::general_purpose::STANDARD;
use base64::Engine;
use hmac::{Hmac, Mac};
use reqwest::header::{HeaderMap, HeaderName, HeaderValue, CONTENT_TYPE};
use serde::Serialize;
use serde_json::Value;
use sha2::Sha256;
use std::collections::HashMap;
use std::time::{SystemTime, UNIX_EPOCH};

use crate::config::ExecutorConfig;
use crate::types::{MarketUpdate, OrderRecord, PrivateWsUpdate};

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
            client: reqwest::Client::builder().build()?,
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
        let rows = pending
            .get("data")
            .and_then(Value::as_array)
            .cloned()
            .unwrap_or_default();
        let mut cancelled = 0u32;
        for row in rows {
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

#[derive(Debug, Serialize)]
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

pub fn parse_private_ws_message(text: &str) -> Result<PrivateWsUpdate> {
    if text == "pong" {
        return Ok(PrivateWsUpdate::default());
    }
    let value: Value = serde_json::from_str(text)?;
    let channel = value
        .pointer("/arg/channel")
        .and_then(Value::as_str)
        .unwrap_or("");
    let data = value
        .get("data")
        .and_then(Value::as_array)
        .cloned()
        .unwrap_or_default();
    let mut update = PrivateWsUpdate::default();

    if channel == "orders" {
        for row in data {
            let order_id = str_field(&row, "orderId");
            let client_oid = str_field(&row, "clientOid");
            update.orders.push(OrderRecord {
                order_id: order_id.clone(),
                exchange_order_id: Some(order_id),
                client_oid,
                intent_id: None,
                symbol: "ETH/USDT:USDT".to_string(),
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
    }

    Ok(update)
}

fn str_field(value: &Value, key: &str) -> String {
    value
        .get(key)
        .and_then(Value::as_str)
        .unwrap_or("")
        .to_string()
}

fn parse_f64(value: Option<&Value>, name: &str) -> Result<f64> {
    value
        .and_then(Value::as_str)
        .context(name.to_string())?
        .parse::<f64>()
        .with_context(|| format!("parse {name}"))
}

pub async fn verify_public_ws_connects(cfg: &ExecutorConfig) -> Result<()> {
    cfg.validate_demo_only()?;
    let (mut socket, _) = tokio_tungstenite::connect_async(&cfg.public_ws_url).await?;
    use futures_util::{SinkExt, StreamExt};
    let msg = serde_json::json!({
        "op": "subscribe",
        "args": [{
            "instType": cfg.product_type,
            "channel": "books5",
            "instId": cfg.bitget_symbol
        }]
    });
    socket
        .send(tokio_tungstenite::tungstenite::Message::Text(
            msg.to_string(),
        ))
        .await?;
    let msg = tokio::time::timeout(std::time::Duration::from_secs(10), socket.next())
        .await?
        .ok_or_else(|| anyhow::anyhow!("public websocket closed"))??;
    let text = msg.into_text()?;
    if text.contains("\"event\":\"error\"") {
        bail!("public websocket subscription failed: {text}");
    }
    Ok(())
}

pub async fn verify_private_ws_connects(cfg: &ExecutorConfig) -> Result<()> {
    cfg.validate_demo_only()?;
    let (mut socket, _) = tokio_tungstenite::connect_async(&cfg.private_ws_url).await?;
    use futures_util::{SinkExt, StreamExt};
    let timestamp = now_seconds();
    let login = serde_json::json!({
        "op": "login",
        "args": [{
            "apiKey": cfg.secrets.api_key,
            "passphrase": cfg.secrets.passphrase,
            "timestamp": timestamp,
            "sign": websocket_sign(&cfg.secrets.api_secret, &timestamp)
        }]
    });
    socket
        .send(tokio_tungstenite::tungstenite::Message::Text(
            login.to_string(),
        ))
        .await?;
    let msg = tokio::time::timeout(std::time::Duration::from_secs(10), socket.next())
        .await?
        .ok_or_else(|| anyhow::anyhow!("private websocket closed"))??;
    let text = msg.into_text()?;
    if text.contains("\"event\":\"error\"") || !text.contains("\"login\"") {
        bail!("private websocket login failed: {text}");
    }
    Ok(())
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

        let update = parse_private_ws_message(raw).unwrap();

        assert_eq!(update.orders.len(), 1);
        assert_eq!(update.orders[0].client_oid, "client-1");
        assert_eq!(update.orders[0].status, "live");
    }
}
