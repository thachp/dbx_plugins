use std::{env, path::PathBuf};

use anyhow::{Context, Result};

/// Runtime configuration resolved from environment variables.
#[derive(Debug, Clone)]
pub struct Config {
    pub index_path: PathBuf,
    pub commit_after_events: usize,
    pub writer_heap_size_bytes: usize,
}

impl Config {
    pub fn load() -> Result<Self> {
        let index_path = match env::var("EVENTDBX_SEARCH_INDEX_PATH") {
            Ok(path) => PathBuf::from(path),
            Err(_) => {
                let home =
                    dirs::home_dir().context("failed to resolve home directory for index path")?;
                home.join(".eventdbx").join("search-index")
            }
        };

        let commit_after_events = env::var("EVENTDBX_SEARCH_COMMIT")
            .ok()
            .and_then(|value| value.parse::<usize>().ok())
            .filter(|value| *value > 0)
            .unwrap_or(100);

        let writer_heap_size_bytes = env::var("EVENTDBX_SEARCH_WRITER_MB")
            .ok()
            .and_then(|value| value.parse::<usize>().ok())
            .filter(|value| *value > 0)
            .map(|mb| mb * 1024 * 1024)
            .unwrap_or(64 * 1024 * 1024);

        Ok(Self {
            index_path,
            commit_after_events,
            writer_heap_size_bytes,
        })
    }
}
