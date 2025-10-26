use async_trait::async_trait;

use crate::model::{EdgeQuery, GraphEdge};

pub mod storage;

#[allow(unused_imports)]
pub use storage::{
    GraphNeighbor, GraphSnapshot, NeighborDirection, RocksGraph, SnapshotOrientation,
};

#[async_trait]
pub trait GraphStorage: Send + Sync {
    async fn upsert_edge(&self, edge: GraphEdge) -> anyhow::Result<String>;
    async fn delete_edge(&self, edge_id: &str, fallback: (&str, &str, &str)) -> anyhow::Result<()>;
    async fn query_edges(&self, query: EdgeQuery) -> anyhow::Result<Vec<GraphEdge>>;
    #[cfg_attr(not(test), allow(dead_code))]
    async fn neighbors(
        &self,
        node: &str,
        direction: crate::model::QueryDirection,
    ) -> anyhow::Result<Vec<GraphNeighbor>>;
    #[cfg_attr(not(test), allow(dead_code))]
    async fn export_snapshot(&self) -> anyhow::Result<GraphSnapshot>;
    async fn flush(&self) -> anyhow::Result<()>;
}
