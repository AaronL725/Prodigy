use anyhow::{bail, Context, Result};
use std::collections::HashMap;
use std::fs;
use std::path::{Path, PathBuf};

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum TradingMode {
    Demo,
    Live,
}

#[derive(Clone, PartialEq, Eq)]
pub struct DemoSecrets {
    pub api_key: String,
    pub api_secret: String,
    pub passphrase: String,
}

impl std::fmt::Debug for DemoSecrets {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("DemoSecrets")
            .field("api_key", &"<redacted>")
            .field("api_secret", &"<redacted>")
            .field("passphrase", &"<redacted>")
            .finish()
    }
}

#[derive(Debug, Clone)]
pub struct ExecutorConfig {
    pub mode: TradingMode,
    pub db_path: PathBuf,
    pub symbol: String,
    pub bitget_symbol: String,
    pub product_type: String,
    pub margin_coin: String,
    pub margin_mode: String,
    pub leverage: u32,
    pub rest_base_url: String,
    pub public_ws_url: String,
    pub private_ws_url: String,
    pub open_maker_timeout_secs: u64,
    pub close_maker_timeout_secs: u64,
    pub stale_market_data_secs: u64,
    pub reconcile_interval_secs: u64,
    pub total_notional_cap_x_equity: f64,
    pub trading_suspension_unrealized_loss_x_equity: f64,
    pub test_reset_demo_state: bool,
    pub secrets: DemoSecrets,
    pub telegram_bot_token: Option<String>,
    pub telegram_chat_id: Option<String>,
}

impl ExecutorConfig {
    pub fn demo_for_tests() -> Self {
        Self {
            mode: TradingMode::Demo,
            db_path: PathBuf::from("var/prodigy.sqlite"),
            symbol: "ETH/USDT:USDT".to_string(),
            bitget_symbol: "ETHUSDT".to_string(),
            product_type: "USDT-FUTURES".to_string(),
            margin_coin: "USDT".to_string(),
            margin_mode: "crossed".to_string(),
            leverage: 5,
            rest_base_url: "https://api.bitget.com".to_string(),
            public_ws_url: "wss://wspap.bitget.com/v2/ws/public".to_string(),
            private_ws_url: "wss://wspap.bitget.com/v2/ws/private".to_string(),
            open_maker_timeout_secs: 15,
            close_maker_timeout_secs: 8,
            stale_market_data_secs: 3,
            reconcile_interval_secs: 10,
            total_notional_cap_x_equity: 5.0,
            trading_suspension_unrealized_loss_x_equity: 0.10,
            test_reset_demo_state: false,
            secrets: DemoSecrets {
                api_key: "key".to_string(),
                api_secret: "secret".to_string(),
                passphrase: "pass".to_string(),
            },
            telegram_bot_token: None,
            telegram_chat_id: None,
        }
    }

    pub fn validate_demo_only(&self) -> Result<()> {
        if self.mode != TradingMode::Demo {
            bail!("third milestone executor only supports Bitget demo mode");
        }
        if self.secrets.api_key.trim().is_empty()
            || self.secrets.api_secret.trim().is_empty()
            || self.secrets.passphrase.trim().is_empty()
        {
            bail!("missing Bitget demo API credentials");
        }
        if !self.public_ws_url.contains("wspap.bitget.com")
            || !self.private_ws_url.contains("wspap.bitget.com")
        {
            bail!("demo executor must use Bitget demo websocket URLs");
        }
        Ok(())
    }
}

pub fn parse_env_text(input: &str) -> HashMap<String, String> {
    let mut values = HashMap::new();
    for raw_line in input.lines() {
        let line = raw_line.trim();
        if line.is_empty() || line.starts_with('#') {
            continue;
        }
        if let Some((key, value)) = line.split_once('=') {
            let clean = value
                .trim()
                .trim_matches('"')
                .trim_matches('\'')
                .to_string();
            values.insert(key.trim().to_string(), clean);
        }
    }
    values
}

pub fn load_env_file(path: &Path) -> Result<HashMap<String, String>> {
    if !path.exists() {
        return Ok(HashMap::new());
    }
    let text = fs::read_to_string(path).with_context(|| format!("read {}", path.display()))?;
    Ok(parse_env_text(&text))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn env_file_parser_reads_plain_key_values_and_ignores_comments() {
        let input = r#"
        # local secrets
        BITGET_DEMO_API_KEY=key-1
        BITGET_DEMO_API_SECRET="secret-1"
        BITGET_DEMO_API_PASSPHRASE='pass-1'
        "#;

        let parsed = parse_env_text(input);

        assert_eq!(parsed.get("BITGET_DEMO_API_KEY").unwrap(), "key-1");
        assert_eq!(parsed.get("BITGET_DEMO_API_SECRET").unwrap(), "secret-1");
        assert_eq!(parsed.get("BITGET_DEMO_API_PASSPHRASE").unwrap(), "pass-1");
    }

    #[test]
    fn live_mode_is_rejected_for_third_milestone() {
        let cfg = ExecutorConfig {
            mode: TradingMode::Live,
            ..ExecutorConfig::demo_for_tests()
        };

        assert!(cfg.validate_demo_only().is_err());
    }

    #[test]
    fn demo_config_requires_demo_credentials() {
        let cfg = ExecutorConfig {
            secrets: DemoSecrets {
                api_key: String::new(),
                api_secret: "secret".to_string(),
                passphrase: "pass".to_string(),
            },
            ..ExecutorConfig::demo_for_tests()
        };

        assert!(cfg.validate_demo_only().is_err());
    }

    #[test]
    fn demo_config_rejects_non_demo_websocket_urls() {
        let cfg = ExecutorConfig {
            public_ws_url: "wss://ws.bitget.com/v2/ws/public".to_string(),
            ..ExecutorConfig::demo_for_tests()
        };
        assert!(cfg.validate_demo_only().is_err());

        let cfg_private = ExecutorConfig {
            private_ws_url: "wss://ws.bitget.com/v2/ws/private".to_string(),
            ..ExecutorConfig::demo_for_tests()
        };
        assert!(cfg_private.validate_demo_only().is_err());
    }

    #[test]
    fn demo_secrets_debug_redacts_values() {
        let secrets = DemoSecrets {
            api_key: "real-key".to_string(),
            api_secret: "real-secret".to_string(),
            passphrase: "real-pass".to_string(),
        };
        let formatted = format!("{:?}", secrets);
        assert!(!formatted.contains("real-key"));
        assert!(!formatted.contains("real-secret"));
        assert!(!formatted.contains("real-pass"));
        assert!(formatted.contains("<redacted>"));
    }
}
