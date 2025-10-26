use std::{env, net::SocketAddr, path::PathBuf, time::Duration};

use anyhow::{Context, Result};

/// Runtime configuration derived from environment variables.
#[derive(Debug, Clone)]
pub struct Config {
    pub listen_addr: SocketAddr,
    pub rocksdb_path: PathBuf,
    pub flush: FlushConfig,
}

#[derive(Debug, Clone)]
pub struct FlushConfig {
    pub max_batch: usize,
    pub interval: Duration,
}

impl Config {
    pub fn load() -> Result<Self> {
        let listen_addr: SocketAddr = env::var("EVENTDBX_GRAPH_LISTEN")
            .unwrap_or_else(|_| "0.0.0.0:6464".to_string())
            .parse()
            .context("invalid EVENTDBX_GRAPH_LISTEN socket address")?;

        let rocksdb_path = match env::var("EVENTDBX_GRAPH_PATH") {
            Ok(path) => PathBuf::from(path),
            Err(_) => {
                let home = dirs::home_dir()
                    .context("failed to resolve home directory for rocksdb path")?;
                home.join(".eventdbx/graphdb")
            }
        };

        let max_batch = env::var("EVENTDBX_GRAPH_FLUSH_MAX_BATCH")
            .ok()
            .and_then(|v| v.parse().ok())
            .filter(|value: &usize| *value > 0)
            .unwrap_or(256);

        let flush_millis = env::var("EVENTDBX_GRAPH_FLUSH_INTERVAL_MS")
            .ok()
            .and_then(|v| v.parse::<u64>().ok())
            .filter(|value| *value > 0)
            .unwrap_or(1_000);

        Ok(Self {
            listen_addr,
            rocksdb_path,
            flush: FlushConfig {
                max_batch,
                interval: Duration::from_millis(flush_millis),
            },
        })
    }
}
