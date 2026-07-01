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
}
