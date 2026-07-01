use anyhow::{bail, Result};
use prodigy_executor::config::{load_env_file, DemoSecrets, ExecutorConfig};
use prodigy_executor::executor;
use std::env;
use std::path::Path;

#[tokio::main]
async fn main() -> Result<()> {
    let cfg = parse_args_and_config()?;
    cfg.validate_demo_only()?;
    executor::run_once_or_loop(cfg).await
}

fn parse_args_and_config() -> Result<ExecutorConfig> {
    let mut cfg = ExecutorConfig::demo_for_tests();
    let env_file = load_env_file(Path::new(".env.local"))?;

    let mut args = env::args().skip(1);
    while let Some(arg) = args.next() {
        match arg.as_str() {
            "--db" => {
                cfg.db_path = args
                    .next()
                    .unwrap_or_else(|| "var/prodigy.sqlite".into())
                    .into()
            }
            "--once" => {}
            "--test-reset-demo-state" => cfg.test_reset_demo_state = true,
            "--mode" => {
                let value = args.next().unwrap_or_else(|| "demo".into());
                if value != "demo" {
                    bail!("third milestone executor only supports --mode demo");
                }
            }
            other => bail!("unknown argument: {other}"),
        }
    }

    cfg.secrets = DemoSecrets {
        api_key: read_secret("BITGET_DEMO_API_KEY", &env_file)?,
        api_secret: read_secret("BITGET_DEMO_API_SECRET", &env_file)?,
        passphrase: read_secret("BITGET_DEMO_API_PASSPHRASE", &env_file)?,
    };
    cfg.telegram_bot_token = read_optional("TELEGRAM_BOT_TOKEN", &env_file);
    cfg.telegram_chat_id = read_optional("TELEGRAM_CHAT_ID", &env_file);
    Ok(cfg)
}

fn read_secret(key: &str, env_file: &std::collections::HashMap<String, String>) -> Result<String> {
    read_optional(key, env_file).ok_or_else(|| anyhow::anyhow!("missing {key}"))
}

fn read_optional(
    key: &str,
    env_file: &std::collections::HashMap<String, String>,
) -> Option<String> {
    env::var(key).ok().or_else(|| env_file.get(key).cloned())
}
