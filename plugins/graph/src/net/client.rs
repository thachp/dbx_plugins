#![allow(dead_code)]

use anyhow::Result;

pub struct GraphClient;

impl GraphClient {
    pub async fn connect(_addr: &str) -> Result<Self> {
        anyhow::bail!("GraphClient not yet implemented");
    }
}
