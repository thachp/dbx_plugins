use async_trait::async_trait;

use crate::model::{EdgeQuery, GraphEdge};

pub mod storage;

pub use storage::RocksGraph;

#[async_trait]
pub trait GraphStorage: Send + Sync {
    async fn upsert_edge(&self, edge: GraphEdge) -> anyhow::Result<String>;
    async fn delete_edge(&self, edge_id: &str, fallback: (&str, &str, &str)) -> anyhow::Result<()>;
    async fn query_edges(&self, query: EdgeQuery) -> anyhow::Result<Vec<GraphEdge>>;
    async fn flush(&self) -> anyhow::Result<()>;
}
