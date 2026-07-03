use anyhow::Result;
use std::time::Duration;

use crate::config::ExecutorConfig;

#[derive(Debug, Clone)]
pub struct DaemonOptions {
    pub max_runtime: Option<Duration>,
}

pub async fn run_daemon(_cfg: ExecutorConfig, options: DaemonOptions) -> Result<()> {
    if let Some(max_runtime) = options.max_runtime {
        tokio::time::sleep(max_runtime).await;
    }
    Ok(())
}
