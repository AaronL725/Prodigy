use anyhow::{bail, Result};
use prodigy_executor::config::{
    load_env_file, parse_allowed_user_ids, DemoSecrets, ExecutorConfig,
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
}

#[tokio::main]
async fn main() -> Result<()> {
    let parsed = parse_args_and_config()?;
    parsed.cfg.validate_demo_only()?;
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
            "--mode" => {
                let value = args.next().unwrap_or_else(|| "demo".to_string());
                if value != "demo" {
                    bail!("prodigy executor only supports --mode demo");
                }
            }
            other => bail!("unknown argument: {other}"),
        }
    }

    if explicit_once && explicit_daemon {
        bail!("cannot use --once and --daemon together");
    }

    cfg.secrets = DemoSecrets {
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
    fn rejects_live_mode_before_execution() {
        let err = parse_args_from_env(["prodigy-executor", "--mode", "live"], &HashMap::new())
            .unwrap_err();

        assert!(err.to_string().contains("only supports --mode demo"));
    }
}
