use anyhow::{bail, Result};
use prodigy_executor::config::{
    load_env_file, parse_allowed_user_ids, BitgetSecrets, ExecutorConfig, LiveSafety, TradingMode,
};
use prodigy_executor::executor;
use std::env;
use std::path::{Path, PathBuf};

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum RunMode {
    Once,
    Daemon,
}

#[derive(Debug)]
struct ParsedExecutorArgs {
    cfg: ExecutorConfig,
    run_mode: RunMode,
    max_runtime_ms: Option<u64>,
    dry_validate: bool,
}

#[tokio::main]
async fn main() -> Result<()> {
    let parsed = parse_args_and_config()?;
    if parsed.dry_validate {
        parsed.cfg.validate_for_dry_validate()?;
        prodigy_executor::daemon::run_live_dry_validate(parsed.cfg).await
    } else {
        parsed.cfg.validate_for_runtime()?;
        match parsed.run_mode {
            RunMode::Once => executor::run_once_or_loop(parsed.cfg).await,
            RunMode::Daemon => {
                prodigy_executor::daemon::run_daemon(
                    parsed.cfg,
                    prodigy_executor::daemon::DaemonOptions {
                        max_runtime: parsed.max_runtime_ms.map(std::time::Duration::from_millis),
                    },
                )
                .await
            }
        }
    }
}

fn parse_args_and_config() -> Result<ParsedExecutorArgs> {
    parse_args_from(env::args())
}

// Production entry point for arg parsing. Loads the `.env.local` file, overlays
// real process env vars (real env wins over the file, matching prior behavior),
// then delegates to the pure `parse_args_from_env`. Kept as a thin wrapper so
// the actual CLI parsing can be unit-tested with an injected env map and no
// filesystem/secret dependency.
fn parse_args_from<I, S>(args: I) -> Result<ParsedExecutorArgs>
where
    I: IntoIterator<Item = S>,
    S: Into<String>,
{
    let mut env_file = load_env_file(&find_env_local())?;
    for key in [
        "BITGET_DEMO_API_KEY",
        "BITGET_DEMO_API_SECRET",
        "BITGET_DEMO_SECRET_KEY",
        "BITGET_DEMO_API_PASSPHRASE",
        "BITGET_DEMO_PASSPHRASE",
        "BITGET_LIVE_API_KEY",
        "BITGET_LIVE_API_SECRET",
        "BITGET_LIVE_API_PASSPHRASE",
        "PRODIGY_LIVE_TRADING_ENABLED",
        "PRODIGY_LIVE_CONFIRM",
        "TELEGRAM_BOT_TOKEN",
        "TELEGRAM_ALLOWED_USER_IDS",
        "TELEGRAM_CHAT_ID",
    ] {
        if let Ok(value) = env::var(key) {
            env_file.insert(key.to_string(), value);
        }
    }
    parse_args_from_env(args, &env_file)
}

// Pure CLI parsing + secret resolution against an injected env map. Reads NO
// filesystem and NO real process env — everything comes from `env_file`. This
// makes the parse-mode unit tests hermetic.
fn parse_args_from_env<I, S>(
    args: I,
    env_file: &std::collections::HashMap<String, String>,
) -> Result<ParsedExecutorArgs>
where
    I: IntoIterator<Item = S>,
    S: Into<String>,
{
    let mut cfg = ExecutorConfig::demo_for_tests();

    let mut run_mode = RunMode::Once;
    let mut max_runtime_ms: Option<u64> = None;
    let mut dry_validate = false;
    let mut explicit_once = false;
    let mut explicit_daemon = false;

    let mut args = args.into_iter().map(|s| s.into()).skip(1);
    while let Some(arg) = args.next() {
        match arg.as_str() {
            "--db" => {
                cfg.db_path = args
                    .next()
                    .unwrap_or_else(|| "var/prodigy.sqlite".to_string())
                    .into()
            }
            "--once" => {
                explicit_once = true;
                run_mode = RunMode::Once;
            }
            "--daemon" => {
                explicit_daemon = true;
                run_mode = RunMode::Daemon;
            }
            "--max-runtime-ms" => {
                let value = args
                    .next()
                    .ok_or_else(|| anyhow::anyhow!("--max-runtime-ms requires a value"))?;
                max_runtime_ms = Some(value.parse().map_err(|_| {
                    anyhow::anyhow!("--max-runtime-ms must be a non-negative integer")
                })?);
            }
            "--test-reset-demo-state" => cfg.test_reset_demo_state = true,
            "--dry-validate" => dry_validate = true,
            "--mode" => {
                let value = args.next().unwrap_or_else(|| "demo".to_string());
                let db_path = cfg.db_path.clone();
                let test_reset_demo_state = cfg.test_reset_demo_state;
                cfg = match value.as_str() {
                    "demo" => ExecutorConfig::demo_for_tests(),
                    "live" => ExecutorConfig::live_for_tests(),
                    other => bail!("unsupported mode: {other}"),
                };
                cfg.db_path = db_path;
                cfg.test_reset_demo_state = test_reset_demo_state;
            }
            other => bail!("unknown argument: {other}"),
        }
    }

    if explicit_once && explicit_daemon {
        bail!("cannot use --once and --daemon together");
    }
    if dry_validate && cfg.mode != TradingMode::Live {
        bail!("--dry-validate requires --mode live");
    }
    if cfg.mode == TradingMode::Live && run_mode == RunMode::Once && !dry_validate {
        bail!("live mode requires --daemon or --dry-validate");
    }
    if cfg.mode == TradingMode::Live && cfg.test_reset_demo_state {
        bail!("test reset demo state is only supported in demo mode (--test-reset-demo-state)");
    }

    cfg.secrets = match cfg.mode {
        TradingMode::Demo => BitgetSecrets {
            api_key: read_secret(&["BITGET_DEMO_API_KEY"], env_file)?,
            // ponytail: .env.local ships two naming conventions; accept either so the
            // demo creds load regardless of which key the operator set.
            api_secret: read_secret(
                &["BITGET_DEMO_API_SECRET", "BITGET_DEMO_SECRET_KEY"],
                env_file,
            )?,
            passphrase: read_secret(
                &["BITGET_DEMO_API_PASSPHRASE", "BITGET_DEMO_PASSPHRASE"],
                env_file,
            )?,
        },
        TradingMode::Live if dry_validate => BitgetSecrets {
            api_key: read_optional(&["BITGET_LIVE_API_KEY"], env_file).unwrap_or_default(),
            api_secret: read_optional(&["BITGET_LIVE_API_SECRET"], env_file).unwrap_or_default(),
            passphrase: read_optional(&["BITGET_LIVE_API_PASSPHRASE"], env_file)
                .unwrap_or_default(),
        },
        TradingMode::Live => BitgetSecrets {
            api_key: read_secret(&["BITGET_LIVE_API_KEY"], env_file)?,
            api_secret: read_secret(&["BITGET_LIVE_API_SECRET"], env_file)?,
            passphrase: read_secret(&["BITGET_LIVE_API_PASSPHRASE"], env_file)?,
        },
    };
    cfg.live_safety = LiveSafety {
        enabled: read_optional(&["PRODIGY_LIVE_TRADING_ENABLED"], env_file).as_deref() == Some("1"),
        confirm_phrase: read_optional(&["PRODIGY_LIVE_CONFIRM"], env_file),
    };
    cfg.telegram_bot_token = read_optional(&["TELEGRAM_BOT_TOKEN"], env_file);
    cfg.telegram_allowed_user_ids = read_optional(&["TELEGRAM_ALLOWED_USER_IDS"], env_file)
        .map(|v| parse_allowed_user_ids(&v))
        .unwrap_or_default();
    cfg.telegram_chat_id = read_optional(&["TELEGRAM_CHAT_ID"], env_file);
    Ok(ParsedExecutorArgs {
        cfg,
        run_mode,
        max_runtime_ms,
        dry_validate,
    })
}

// ponytail: cargo runs tests with CWD = crate dir, but .env.local lives at the
// workspace root. Walk up to find it, mirroring the integration-test helper in
// tests/bitget_demo.rs. Also lets `main()` run from any subdir.
fn find_env_local() -> PathBuf {
    let mut dir = match env::current_dir() {
        Ok(d) => d,
        Err(_) => return Path::new(".env.local").to_path_buf(),
    };
    loop {
        let candidate = dir.join(".env.local");
        if candidate.exists() {
            return candidate;
        }
        if !dir.pop() {
            return Path::new(".env.local").to_path_buf();
        }
    }
}

fn read_secret(
    keys: &[&str],
    env_file: &std::collections::HashMap<String, String>,
) -> Result<String> {
    read_optional(keys, env_file).ok_or_else(|| anyhow::anyhow!("missing one of {:?}", keys))
}

fn read_optional(
    keys: &[&str],
    env_file: &std::collections::HashMap<String, String>,
) -> Option<String> {
    keys.iter()
        .filter_map(|k| env_file.get(*k).cloned())
        .find(|v| !v.is_empty())
}

#[cfg(test)]
mod tests {
    use super::*;
    use prodigy_executor::config::TradingMode;
    use std::collections::HashMap;

    // Injected fake demo creds so the parse tests never touch `.env.local` or
    // real machine secrets. These are obviously-fake test values.
    fn fake_env() -> HashMap<String, String> {
        let mut m = HashMap::new();
        m.insert("BITGET_DEMO_API_KEY".into(), "test-key".into());
        m.insert("BITGET_DEMO_API_SECRET".into(), "test-secret".into());
        m.insert("BITGET_DEMO_API_PASSPHRASE".into(), "test-pass".into());
        m
    }

    #[test]
    fn parses_once_mode_by_default() {
        let parsed = parse_args_from_env(["prodigy-executor"], &fake_env()).unwrap();

        assert_eq!(parsed.run_mode, RunMode::Once);
        assert_eq!(
            parsed.cfg.db_path,
            std::path::PathBuf::from("var/prodigy.sqlite")
        );
    }

    #[test]
    fn parses_daemon_mode_and_db_path() {
        let parsed = parse_args_from_env(
            [
                "prodigy-executor",
                "--daemon",
                "--db",
                "/tmp/prodigy-test.sqlite",
            ],
            &fake_env(),
        )
        .unwrap();

        assert_eq!(parsed.run_mode, RunMode::Daemon);
        assert_eq!(
            parsed.cfg.db_path,
            std::path::PathBuf::from("/tmp/prodigy-test.sqlite")
        );
    }

    #[test]
    fn parses_telegram_allowed_user_ids_without_chat_id() {
        let mut env = fake_env();
        env.insert("TELEGRAM_BOT_TOKEN".into(), "test-token".into());
        env.insert("TELEGRAM_ALLOWED_USER_IDS".into(), "123, 456".into());

        let parsed = parse_args_from_env(["prodigy-executor", "--daemon"], &env).unwrap();

        assert_eq!(parsed.cfg.telegram_bot_token.as_deref(), Some("test-token"));
        assert_eq!(parsed.cfg.telegram_allowed_user_ids, vec!["123", "456"]);
        assert!(parsed.cfg.telegram_chat_id.is_none());
    }

    #[test]
    fn ignores_live_key_names_and_loads_demo_credentials_only() {
        let mut env = fake_env();
        env.insert("BITGET_LIVE_API_KEY".into(), "live-key".into());
        env.insert("BITGET_LIVE_API_SECRET".into(), "live-secret".into());
        env.insert("BITGET_LIVE_API_PASSPHRASE".into(), "live-pass".into());

        let parsed = parse_args_from_env(["prodigy-executor", "--daemon"], &env).unwrap();

        assert_eq!(parsed.cfg.mode, TradingMode::Demo);
        assert_eq!(parsed.cfg.secrets.api_key, "test-key");
        assert_eq!(parsed.cfg.secrets.api_secret, "test-secret");
        assert_eq!(parsed.cfg.secrets.passphrase, "test-pass");
    }

    #[test]
    fn parses_live_mode_with_live_credentials_and_enable_flags() {
        let mut env = fake_env();
        env.insert("BITGET_LIVE_API_KEY".into(), "live-key".into());
        env.insert("BITGET_LIVE_API_SECRET".into(), "live-secret".into());
        env.insert("BITGET_LIVE_API_PASSPHRASE".into(), "live-pass".into());
        env.insert("PRODIGY_LIVE_TRADING_ENABLED".into(), "1".into());
        env.insert(
            "PRODIGY_LIVE_CONFIRM".into(),
            "I_UNDERSTAND_THIS_CAN_TRADE_REAL_MONEY".into(),
        );

        let parsed =
            parse_args_from_env(["prodigy-executor", "--mode", "live", "--daemon"], &env).unwrap();

        assert_eq!(parsed.cfg.mode, TradingMode::Live);
        assert_eq!(parsed.cfg.secrets.api_key, "live-key");
        assert!(parsed.cfg.live_safety.enabled);
    }

    #[test]
    fn live_mode_requires_daemon_or_dry_validate() {
        let mut env = fake_env();
        env.insert("BITGET_LIVE_API_KEY".into(), "live-key".into());
        env.insert("BITGET_LIVE_API_SECRET".into(), "live-secret".into());
        env.insert("BITGET_LIVE_API_PASSPHRASE".into(), "live-pass".into());
        env.insert("PRODIGY_LIVE_TRADING_ENABLED".into(), "1".into());
        env.insert(
            "PRODIGY_LIVE_CONFIRM".into(),
            "I_UNDERSTAND_THIS_CAN_TRADE_REAL_MONEY".into(),
        );

        let err = parse_args_from_env(["prodigy-executor", "--mode", "live"], &env).unwrap_err();

        assert!(err.to_string().contains("--daemon"));
    }

    #[test]
    fn rejects_live_mode_with_test_reset_demo_state() {
        let mut env = fake_env();
        env.insert("BITGET_LIVE_API_KEY".into(), "live-key".into());
        env.insert("BITGET_LIVE_API_SECRET".into(), "live-secret".into());
        env.insert("BITGET_LIVE_API_PASSPHRASE".into(), "live-pass".into());

        let err = parse_args_from_env(
            [
                "prodigy-executor",
                "--daemon",
                "--test-reset-demo-state",
                "--mode",
                "live",
            ],
            &env,
        )
        .unwrap_err();

        assert!(err.to_string().contains("--test-reset-demo-state"));
    }

    #[test]
    fn demo_mode_accepts_test_reset_demo_state() {
        let parsed = parse_args_from_env(
            [
                "prodigy-executor",
                "--mode",
                "demo",
                "--test-reset-demo-state",
            ],
            &fake_env(),
        )
        .unwrap();

        assert_eq!(parsed.cfg.mode, TradingMode::Demo);
        assert!(parsed.cfg.test_reset_demo_state);
    }

    #[test]
    fn live_dry_validate_parses_without_any_bitget_credentials() {
        let env = std::collections::HashMap::new();

        let parsed = parse_args_from_env(
            ["prodigy-executor", "--mode", "live", "--dry-validate"],
            &env,
        )
        .unwrap();

        assert_eq!(parsed.cfg.mode, TradingMode::Live);
        assert!(parsed.dry_validate);
        assert!(parsed.cfg.secrets.api_key.is_empty());
    }

    #[test]
    fn dry_validate_requires_live_mode_before_demo_credentials() {
        let err = parse_args_from_env(["prodigy-executor", "--dry-validate"], &HashMap::new())
            .unwrap_err();

        assert!(err
            .to_string()
            .contains("--dry-validate requires --mode live"));
    }

    #[test]
    fn rejects_once_and_daemon_together() {
        let err = parse_args_from_env(["prodigy-executor", "--once", "--daemon"], &HashMap::new())
            .unwrap_err();

        assert!(err
            .to_string()
            .contains("cannot use --once and --daemon together"));
    }

    #[test]
    fn parses_bounded_daemon_runtime_for_tests() {
        let parsed = parse_args_from_env(
            ["prodigy-executor", "--daemon", "--max-runtime-ms", "1500"],
            &fake_env(),
        )
        .unwrap();

        assert_eq!(parsed.run_mode, RunMode::Daemon);
        assert_eq!(parsed.max_runtime_ms, Some(1500));
    }

    #[test]
    fn rejects_unsupported_mode_before_execution() {
        let err = parse_args_from_env(["prodigy-executor", "--mode", "paper"], &HashMap::new())
            .unwrap_err();

        assert!(err.to_string().contains("unsupported mode: paper"));
    }
}
