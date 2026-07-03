use anyhow::Result;
use std::time::Duration;

use crate::config::ExecutorConfig;

#[derive(Debug, Clone, Default)]
pub struct DaemonOptions {
    pub max_runtime: Option<Duration>,
}

pub async fn run_daemon(cfg: ExecutorConfig, options: DaemonOptions) -> Result<()> {
    cfg.validate_demo_only()?;
    if let Some(max_runtime) = options.max_runtime {
        tokio::time::sleep(max_runtime).await;
        return Ok(());
    }
    futures_util::future::pending::<()>().await;
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::config::{ExecutorConfig, TradingMode};

    #[test]
    fn daemon_options_default_runs_forever() {
        let options = DaemonOptions::default();

        assert!(options.max_runtime.is_none());
    }

    #[tokio::test]
    async fn daemon_rejects_non_demo_mode_before_opening_db() {
        let cfg = ExecutorConfig {
            mode: TradingMode::Live,
            ..ExecutorConfig::demo_for_tests()
        };

        let err = run_daemon(
            cfg,
            DaemonOptions {
                max_runtime: Some(std::time::Duration::from_millis(1)),
            },
        )
        .await
        .unwrap_err();

        assert!(err.to_string().contains("demo"));
    }
}
