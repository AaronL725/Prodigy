use anyhow::{bail, Context, Result};
use std::collections::HashMap;
use std::fs;
use std::path::{Path, PathBuf};

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum TradingMode {
    Demo,
    Live,
}

impl TradingMode {
    pub fn as_str(self) -> &'static str {
        match self {
            TradingMode::Demo => "demo",
            TradingMode::Live => "live",
        }
    }
}

#[derive(Clone, PartialEq, Eq)]
pub struct BitgetSecrets {
    pub api_key: String,
    pub api_secret: String,
    pub passphrase: String,
}

pub type DemoSecrets = BitgetSecrets;

impl std::fmt::Debug for BitgetSecrets {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("BitgetSecrets")
            .field("api_key", &"<redacted>")
            .field("api_secret", &"<redacted>")
            .field("passphrase", &"<redacted>")
            .finish()
    }
}

#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct LiveSafety {
    pub enabled: bool,
    pub confirm_phrase: Option<String>,
}

pub const LIVE_CONFIRM_PHRASE: &str = "I_UNDERSTAND_THIS_CAN_TRADE_REAL_MONEY";

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
    pub secrets: BitgetSecrets,
    pub live_safety: LiveSafety,
    pub telegram_bot_token: Option<String>,
    pub telegram_allowed_user_ids: Vec<String>,
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
            secrets: BitgetSecrets {
                api_key: "key".to_string(),
                api_secret: "secret".to_string(),
                passphrase: "pass".to_string(),
            },
            live_safety: LiveSafety::default(),
            telegram_bot_token: None,
            telegram_allowed_user_ids: Vec::new(),
            telegram_chat_id: None,
        }
    }

    pub fn live_for_tests() -> Self {
        Self {
            mode: TradingMode::Live,
            public_ws_url: "wss://ws.bitget.com/v2/ws/public".to_string(),
            private_ws_url: "wss://ws.bitget.com/v2/ws/private".to_string(),
            secrets: BitgetSecrets {
                api_key: String::new(),
                api_secret: String::new(),
                passphrase: String::new(),
            },
            live_safety: LiveSafety::default(),
            ..Self::demo_for_tests()
        }
    }

    pub fn validate_for_runtime(&self) -> Result<()> {
        self.validate_urls_for_mode()?;
        if self.mode == TradingMode::Live && self.test_reset_demo_state {
            bail!("test reset demo state is only supported in demo mode (--test-reset-demo-state)");
        }
        if self.secrets.api_key.trim().is_empty()
            || self.secrets.api_secret.trim().is_empty()
            || self.secrets.passphrase.trim().is_empty()
        {
            bail!(
                "missing Bitget {} credentials",
                match self.mode {
                    TradingMode::Demo => "demo",
                    TradingMode::Live => "live",
                }
            );
        }
        if self.mode == TradingMode::Live {
            if !self.live_safety.enabled {
                bail!("live trading enable flag is required");
            }
            if self.live_safety.confirm_phrase.as_deref() != Some(LIVE_CONFIRM_PHRASE) {
                bail!("live confirmation phrase is required");
            }
        }
        Ok(())
    }

    pub fn validate_for_dry_validate(&self) -> Result<()> {
        if self.mode != TradingMode::Live {
            bail!("dry validation is only for live mode");
        }
        self.validate_urls_for_mode()
    }

    pub fn validate_urls_for_mode(&self) -> Result<()> {
        match self.mode {
            TradingMode::Demo => {
                if self.public_ws_url != "wss://wspap.bitget.com/v2/ws/public"
                    || self.private_ws_url != "wss://wspap.bitget.com/v2/ws/private"
                {
                    bail!("demo profile must use Bitget demo websocket URLs");
                }
            }
            TradingMode::Live => {
                if self.public_ws_url != "wss://ws.bitget.com/v2/ws/public"
                    || self.private_ws_url != "wss://ws.bitget.com/v2/ws/private"
                {
                    bail!("live profile must use Bitget live websocket URLs");
                }
            }
        }
        Ok(())
    }

    pub fn validate_demo_only(&self) -> Result<()> {
        if self.mode != TradingMode::Demo {
            bail!("prodigy executor only supports Bitget demo mode");
        }
        self.validate_for_runtime()
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

pub fn parse_allowed_user_ids(value: &str) -> Vec<String> {
    value
        .split(',')
        .map(str::trim)
        .filter(|v| !v.is_empty())
        .map(ToString::to_string)
        .collect()
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
    fn allowed_user_ids_parser_trims_and_drops_empty_values() {
        assert_eq!(
            parse_allowed_user_ids(" 123, ,456,789 "),
            vec!["123".to_string(), "456".to_string(), "789".to_string()]
        );
    }

    #[test]
    fn live_mode_is_rejected() {
        let cfg = ExecutorConfig {
            mode: TradingMode::Live,
            ..ExecutorConfig::demo_for_tests()
        };

        assert!(cfg.validate_demo_only().is_err());
    }

    #[test]
    fn demo_runtime_validation_requires_demo_ws_and_demo_creds() {
        let cfg = ExecutorConfig::demo_for_tests();

        cfg.validate_for_runtime().unwrap();

        let live_ws = ExecutorConfig {
            public_ws_url: "wss://ws.bitget.com/v2/ws/public".to_string(),
            ..ExecutorConfig::demo_for_tests()
        };
        assert!(live_ws
            .validate_for_runtime()
            .unwrap_err()
            .to_string()
            .contains("demo websocket"));
    }

    #[test]
    fn live_runtime_validation_requires_enable_confirm_and_live_creds() {
        let cfg = ExecutorConfig::live_for_tests();
        let err = cfg.validate_for_runtime().unwrap_err().to_string();
        assert!(err.contains("live credentials") || err.contains("live trading enable"));

        let enabled = ExecutorConfig {
            live_safety: LiveSafety {
                enabled: true,
                confirm_phrase: Some("I_UNDERSTAND_THIS_CAN_TRADE_REAL_MONEY".to_string()),
            },
            secrets: BitgetSecrets {
                api_key: "live-key".to_string(),
                api_secret: "live-secret".to_string(),
                passphrase: "live-pass".to_string(),
            },
            ..ExecutorConfig::live_for_tests()
        };
        enabled.validate_for_runtime().unwrap();
    }

    #[test]
    fn live_runtime_validation_rejects_test_reset_demo_state() {
        let cfg = ExecutorConfig {
            test_reset_demo_state: true,
            live_safety: LiveSafety {
                enabled: true,
                confirm_phrase: Some(LIVE_CONFIRM_PHRASE.to_string()),
            },
            secrets: BitgetSecrets {
                api_key: "live-key".to_string(),
                api_secret: "live-secret".to_string(),
                passphrase: "live-pass".to_string(),
            },
            ..ExecutorConfig::live_for_tests()
        };

        assert!(cfg
            .validate_for_runtime()
            .unwrap_err()
            .to_string()
            .contains("test reset"));

        ExecutorConfig {
            test_reset_demo_state: true,
            ..ExecutorConfig::demo_for_tests()
        }
        .validate_for_runtime()
        .unwrap();
    }

    #[test]
    fn live_runtime_validation_rejects_spoofed_ws_host() {
        let cfg = ExecutorConfig {
            public_ws_url: "wss://ws.bitget.com.evil/v2/ws/public".to_string(),
            live_safety: LiveSafety {
                enabled: true,
                confirm_phrase: Some(LIVE_CONFIRM_PHRASE.to_string()),
            },
            secrets: BitgetSecrets {
                api_key: "live-key".to_string(),
                api_secret: "live-secret".to_string(),
                passphrase: "live-pass".to_string(),
            },
            ..ExecutorConfig::live_for_tests()
        };

        assert!(cfg
            .validate_for_runtime()
            .unwrap_err()
            .to_string()
            .contains("live websocket"));
    }

    #[test]
    fn live_dry_validation_requires_no_demo_or_live_credentials() {
        let cfg = ExecutorConfig {
            secrets: BitgetSecrets {
                api_key: String::new(),
                api_secret: String::new(),
                passphrase: String::new(),
            },
            ..ExecutorConfig::live_for_tests()
        };

        cfg.validate_for_dry_validate().unwrap();
    }

    #[test]
    fn bitget_secrets_debug_redacts_values_for_live_too() {
        let secrets = BitgetSecrets {
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
