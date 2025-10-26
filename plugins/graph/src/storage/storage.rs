use std::{
    collections::{HashMap, HashSet, VecDeque},
    path::Path,
    sync::Arc,
};

use anyhow::{bail, Context, Result};
use async_trait::async_trait;
use parking_lot::Mutex;
use rocksdb::{
    ColumnFamilyDescriptor, DBWithThreadMode, IteratorMode, MultiThreaded, Options, WriteBatch,
};
use serde::{Deserialize, Serialize};
use serde_json::{self, Value};
use snowflake::ProcessUniqueId;
use tracing::warn;

use super::GraphStorage;
use crate::model::{EdgeOp, EdgeQuery, GraphEdge, QueryDirection};

const CF_DEFAULT: &str = "default";
const CF_EDGES: &str = "edges";
const CF_OUT: &str = "out";
const CF_IN: &str = "in";

#[derive(Debug, Clone, Serialize, Deserialize)]
struct StoredEdge {
    id: String,
    from: String,
    to: String,
    relation: String,
    tags: Vec<String>,
    weight: Option<f64>,
    confidence: Option<f64>,
    properties_json: String,
}

impl From<StoredEdge> for GraphEdge {
    fn from(edge: StoredEdge) -> Self {
        let properties = serde_json::from_str(&edge.properties_json).unwrap_or_else(|err| {
            warn!(error = ?err, "failed to parse stored edge properties; defaulting to null");
            Value::Null
        });
        GraphEdge {
            id: Some(edge.id),
            from: edge.from,
            to: edge.to,
            relation: edge.relation,
            tags: edge.tags,
            weight: edge.weight,
            confidence: edge.confidence,
            properties,
            op: EdgeOp::Upsert,
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct AdjacencyEntry {
    id: String,
    relation: String,
    #[serde(default)]
    neighbor: String,
    #[serde(default)]
    weight: Option<f64>,
}

#[cfg_attr(not(test), allow(dead_code))]
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum NeighborDirection {
    Outbound,
    Inbound,
}

#[cfg_attr(not(test), allow(dead_code))]
#[derive(Debug, Clone)]
pub struct GraphNeighbor {
    pub edge_id: String,
    pub relation: String,
    pub target: String,
    pub weight: f64,
    pub direction: NeighborDirection,
}

#[cfg_attr(not(test), allow(dead_code))]
#[derive(Debug, Clone)]
pub struct SnapshotEdge {
    pub edge_id: String,
    pub relation: String,
    pub target: String,
    pub weight: f64,
    pub tags: Vec<String>,
    pub confidence: Option<f64>,
    pub direction: NeighborDirection,
    pub properties: Value,
}

#[cfg_attr(not(test), allow(dead_code))]
#[derive(Debug, Clone)]
pub struct GraphSnapshot {
    adjacency: HashMap<String, Vec<SnapshotEdge>>,
}

#[cfg_attr(not(test), allow(dead_code))]
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SnapshotOrientation {
    Outbound,
    Inbound,
    Undirected,
}

impl GraphSnapshot {
    /// Returns a reference to the internal adjacency map keyed by node id.
    #[cfg_attr(not(test), allow(dead_code))]
    pub fn adjacency_map(&self) -> &HashMap<String, Vec<SnapshotEdge>> {
        &self.adjacency
    }

    /// Produces a matrix-style adjacency representation suitable for graph algorithms.
    ///
    /// The returned tuple contains a stable ordering of node identifiers and a parallel
    /// adjacency list where each entry is a `(neighbor_index, weight)` pair. This mirrors the
    /// input expected by `xgraph` clustering algorithms—map node identifiers to contiguous
    /// indices, then hand the adjacency matrix to the desired routine.
    #[cfg_attr(not(test), allow(dead_code))]
    pub fn to_weighted_adjacency(
        &self,
        orientation: SnapshotOrientation,
    ) -> (Vec<String>, Vec<Vec<(usize, f64)>>) {
        let mut node_ids: Vec<_> = self.adjacency.keys().cloned().collect();
        node_ids.sort();
        let index_lookup: HashMap<_, _> = node_ids
            .iter()
            .enumerate()
            .map(|(idx, id)| (id.clone(), idx))
            .collect();

        let adjacency = node_ids
            .iter()
            .map(|node| {
                self.adjacency
                    .get(node)
                    .into_iter()
                    .flat_map(|edges| edges.iter())
                    .filter(|edge| orientation.includes(edge.direction))
                    .filter_map(|edge| {
                        index_lookup
                            .get(&edge.target)
                            .copied()
                            .map(|neighbor_idx| (neighbor_idx, edge.weight))
                    })
                    .collect::<Vec<_>>()
            })
            .collect::<Vec<_>>();

        (node_ids, adjacency)
    }
}

impl SnapshotOrientation {
    #[cfg_attr(not(test), allow(dead_code))]
    fn includes(self, direction: NeighborDirection) -> bool {
        match self {
            SnapshotOrientation::Outbound => direction == NeighborDirection::Outbound,
            SnapshotOrientation::Inbound => direction == NeighborDirection::Inbound,
            SnapshotOrientation::Undirected => direction == NeighborDirection::Outbound,
        }
    }
}

pub struct RocksGraph {
    db: DBWithThreadMode<MultiThreaded>,
    write_lock: Mutex<()>,
}

impl RocksGraph {
    pub fn open(path: impl AsRef<Path>) -> Result<Arc<Self>> {
        let path = path.as_ref();
        let mut opts = Options::default();
        opts.create_if_missing(true);
        opts.create_missing_column_families(true);
        opts.increase_parallelism(num_cpus::get() as i32);
        opts.optimize_level_style_compaction(512 * 1024 * 1024);

        let cf_descriptors = vec![
            ColumnFamilyDescriptor::new(CF_DEFAULT, Options::default()),
            ColumnFamilyDescriptor::new(CF_EDGES, Options::default()),
            ColumnFamilyDescriptor::new(CF_OUT, Options::default()),
            ColumnFamilyDescriptor::new(CF_IN, Options::default()),
        ];

        let db = DBWithThreadMode::open_cf_descriptors(&opts, path, cf_descriptors)
            .with_context(|| format!("failed to open RocksDB at {}", path.display()))?;

        Ok(Arc::new(Self {
            db,
            write_lock: Mutex::new(()),
        }))
    }

    fn cf_edges(&self) -> Arc<rocksdb::BoundColumnFamily<'_>> {
        self.db
            .cf_handle(CF_EDGES)
            .expect("edges column family missing")
    }

    fn cf_out(&self) -> Arc<rocksdb::BoundColumnFamily<'_>> {
        self.db
            .cf_handle(CF_OUT)
            .expect("out column family missing")
    }

    fn cf_in(&self) -> Arc<rocksdb::BoundColumnFamily<'_>> {
        self.db.cf_handle(CF_IN).expect("in column family missing")
    }

    fn serialize<T: Serialize + ?Sized>(value: &T) -> Result<Vec<u8>> {
        bincode::serialize(value).context("failed to serialize value")
    }

    fn deserialize<T: for<'de> Deserialize<'de>>(bytes: &[u8]) -> Result<T> {
        bincode::deserialize(bytes).context("failed to deserialize value")
    }

    fn ensure_edge_id(edge: &mut GraphEdge) -> String {
        if let Some(id) = edge.id.take() {
            id
        } else {
            ProcessUniqueId::new().to_string()
        }
    }

    fn build_stored(edge_id: String, edge: GraphEdge) -> Result<StoredEdge> {
        let properties_json = serde_json::to_string(&edge.properties)
            .context("failed to serialize edge properties to JSON")?;
        Ok(StoredEdge {
            id: edge_id,
            from: edge.from,
            to: edge.to,
            relation: edge.relation,
            tags: edge.tags,
            weight: edge.weight,
            confidence: edge.confidence,
            properties_json,
        })
    }

    fn get_edge(&self, id: &str) -> Result<Option<StoredEdge>> {
        let cf = self.cf_edges();
        match self.db.get_cf(&cf, id.as_bytes())? {
            Some(bytes) => Ok(Some(Self::deserialize(&bytes)?)),
            None => Ok(None),
        }
    }

    fn save_edge(&self, batch: &mut WriteBatch, edge: &StoredEdge) -> Result<()> {
        let cf = self.cf_edges();
        let bytes = Self::serialize(edge)?;
        batch.put_cf(&cf, edge.id.as_bytes(), bytes);
        Ok(())
    }

    fn remove_edge_entry(
        &self,
        batch: &mut WriteBatch,
        edge_id: &str,
    ) -> Result<Option<StoredEdge>> {
        let cf = self.cf_edges();
        let existing = self.get_edge(edge_id)?;
        batch.delete_cf(&cf, edge_id.as_bytes());
        Ok(existing)
    }

    fn load_adj_list(
        &self,
        cf: Arc<rocksdb::BoundColumnFamily<'_>>,
        node: &str,
    ) -> Result<Vec<AdjacencyEntry>> {
        match self.db.get_cf(&cf, node.as_bytes())? {
            Some(bytes) => Self::deserialize(&bytes),
            None => Ok(Vec::new()),
        }
    }

    fn store_adj_list(
        &self,
        batch: &mut WriteBatch,
        cf: Arc<rocksdb::BoundColumnFamily<'_>>,
        node: &str,
        entries: &[AdjacencyEntry],
    ) -> Result<()> {
        if entries.is_empty() {
            batch.delete_cf(&cf, node.as_bytes());
        } else {
            let bytes = Self::serialize(entries)?;
            batch.put_cf(&cf, node.as_bytes(), bytes);
        }
        Ok(())
    }

    fn upsert_adj_entry(
        &self,
        batch: &mut WriteBatch,
        cf: Arc<rocksdb::BoundColumnFamily<'_>>,
        node: &str,
        entry: AdjacencyEntry,
    ) -> Result<()> {
        let mut entries = self.load_adj_list(cf.clone(), node)?;
        if let Some(existing) = entries.iter_mut().find(|item| item.id == entry.id) {
            *existing = entry;
        } else {
            entries.push(entry);
        }
        self.store_adj_list(batch, cf, node, &entries)
    }

    fn remove_adj_entry(
        &self,
        batch: &mut WriteBatch,
        cf: Arc<rocksdb::BoundColumnFamily<'_>>,
        node: &str,
        edge_id: &str,
    ) -> Result<bool> {
        let mut entries = self.load_adj_list(cf.clone(), node)?;
        let len_before = entries.len();
        entries.retain(|entry| entry.id != edge_id);
        let changed = entries.len() != len_before;
        if changed {
            self.store_adj_list(batch, cf, node, &entries)?;
        }
        Ok(changed)
    }

    fn find_edge_by_fallback(
        &self,
        from: &str,
        relation: &str,
        to: &str,
    ) -> Result<Option<String>> {
        let cf = self.cf_out();
        let entries = self.load_adj_list(cf.clone(), from)?;
        for entry in entries {
            if entry.relation == relation {
                if let Some(edge) = self.get_edge(&entry.id)? {
                    if edge.to == to {
                        return Ok(Some(entry.id));
                    }
                }
            }
        }
        Ok(None)
    }

    fn query_direction(
        &self,
        node: &str,
        direction: QueryDirection,
        relation: &Option<String>,
    ) -> Result<Vec<StoredEdge>> {
        let mut ids = Vec::new();
        if matches!(direction, QueryDirection::Outbound | QueryDirection::Both) {
            let cf = self.cf_out();
            ids.extend(self.collect_edges_from_cf(cf.clone(), node, relation)?);
        }
        if matches!(direction, QueryDirection::Inbound | QueryDirection::Both) {
            let cf = self.cf_in();
            ids.extend(self.collect_edges_from_cf(cf.clone(), node, relation)?);
        }
        ids.sort();
        ids.dedup();

        let mut edges = Vec::with_capacity(ids.len());
        for id in ids {
            if let Some(edge) = self.get_edge(&id)? {
                edges.push(edge);
            }
        }
        Ok(edges)
    }

    fn collect_edges_from_cf(
        &self,
        cf: Arc<rocksdb::BoundColumnFamily<'_>>,
        node: &str,
        relation: &Option<String>,
    ) -> Result<Vec<String>> {
        let entries = self.load_adj_list(cf, node)?;
        Ok(entries
            .into_iter()
            .filter(|entry| match relation {
                Some(filter) => &entry.relation == filter,
                None => true,
            })
            .map(|entry| entry.id)
            .collect())
    }

    #[cfg_attr(not(test), allow(dead_code))]
    fn collect_neighbors_from_cf(
        &self,
        cf: Arc<rocksdb::BoundColumnFamily<'_>>,
        node: &str,
        direction: NeighborDirection,
        seen: &mut HashSet<(String, NeighborDirection)>,
        output: &mut Vec<GraphNeighbor>,
    ) -> Result<()> {
        let entries = self.load_adj_list(cf, node)?;
        for entry in entries {
            if !seen.insert((entry.id.clone(), direction)) {
                continue;
            }

            if let Some((target, weight)) = self.resolve_adjacent(&entry, direction)? {
                output.push(GraphNeighbor {
                    edge_id: entry.id,
                    relation: entry.relation,
                    target,
                    weight,
                    direction,
                });
            }
        }
        Ok(())
    }

    #[cfg_attr(not(test), allow(dead_code))]
    fn resolve_adjacent(
        &self,
        entry: &AdjacencyEntry,
        direction: NeighborDirection,
    ) -> Result<Option<(String, f64)>> {
        let mut neighbor = if entry.neighbor.is_empty() {
            None
        } else {
            Some(entry.neighbor.clone())
        };
        let mut weight = entry.weight;

        if neighbor.is_some() && weight.is_some() {
            return Ok(neighbor.map(|target| (target, weight.unwrap_or(1.0))));
        }

        match self.get_edge(&entry.id)? {
            Some(edge) => {
                if neighbor.is_none() {
                    neighbor = Some(match direction {
                        NeighborDirection::Outbound => edge.to.clone(),
                        NeighborDirection::Inbound => edge.from.clone(),
                    });
                }
                if weight.is_none() {
                    weight = Some(edge.weight.unwrap_or(1.0));
                }
                Ok(neighbor.map(|target| (target, weight.unwrap_or(1.0))))
            }
            None => {
                warn!(
                    edge_id = entry.id,
                    "adjacency entry points to missing edge payload"
                );
                Ok(None)
            }
        }
    }

    #[cfg_attr(not(test), allow(dead_code))]
    fn neighbors_internal(
        &self,
        node: &str,
        direction: QueryDirection,
    ) -> Result<Vec<GraphNeighbor>> {
        if node.is_empty() {
            bail!("neighbors query requires a node id");
        }

        let mut neighbors = Vec::new();
        let mut seen = HashSet::new();

        if matches!(direction, QueryDirection::Outbound | QueryDirection::Both) {
            let cf = self.cf_out();
            self.collect_neighbors_from_cf(
                cf,
                node,
                NeighborDirection::Outbound,
                &mut seen,
                &mut neighbors,
            )?;
        }

        if matches!(direction, QueryDirection::Inbound | QueryDirection::Both) {
            let cf = self.cf_in();
            self.collect_neighbors_from_cf(
                cf,
                node,
                NeighborDirection::Inbound,
                &mut seen,
                &mut neighbors,
            )?;
        }

        neighbors.sort_by(|a, b| a.edge_id.cmp(&b.edge_id));
        Ok(neighbors)
    }

    #[cfg_attr(not(test), allow(dead_code))]
    fn build_snapshot(&self) -> Result<GraphSnapshot> {
        let cf = self.cf_edges();
        let mut adjacency: HashMap<String, Vec<SnapshotEdge>> = HashMap::new();
        let iter = self.db.iterator_cf(&cf, IteratorMode::Start);

        for item in iter {
            let (_key, value) = item?;
            let stored: StoredEdge = Self::deserialize(&value)?;
            let weight = stored.weight.unwrap_or(1.0);
            let properties: Value =
                serde_json::from_str(&stored.properties_json).unwrap_or(Value::Null);

            adjacency
                .entry(stored.from.clone())
                .or_default()
                .push(SnapshotEdge {
                    edge_id: stored.id.clone(),
                    relation: stored.relation.clone(),
                    target: stored.to.clone(),
                    weight,
                    tags: stored.tags.clone(),
                    confidence: stored.confidence,
                    direction: NeighborDirection::Outbound,
                    properties: properties.clone(),
                });

            adjacency
                .entry(stored.to.clone())
                .or_default()
                .push(SnapshotEdge {
                    edge_id: stored.id.clone(),
                    relation: stored.relation.clone(),
                    target: stored.from.clone(),
                    weight,
                    tags: stored.tags.clone(),
                    confidence: stored.confidence,
                    direction: NeighborDirection::Inbound,
                    properties: properties.clone(),
                });
        }

        Ok(GraphSnapshot { adjacency })
    }
}

#[async_trait]
impl GraphStorage for RocksGraph {
    async fn upsert_edge(&self, mut edge: GraphEdge) -> Result<String> {
        if edge.from.is_empty() || edge.to.is_empty() || edge.relation.is_empty() {
            bail!("edge must contain non-empty from, to, and relation fields");
        }

        let edge_id = Self::ensure_edge_id(&mut edge);
        let stored = Self::build_stored(edge_id.clone(), edge)?;
        let entry_out = AdjacencyEntry {
            id: edge_id.clone(),
            relation: stored.relation.clone(),
            neighbor: stored.to.clone(),
            weight: stored.weight,
        };
        let entry_in = AdjacencyEntry {
            id: edge_id.clone(),
            relation: stored.relation.clone(),
            neighbor: stored.from.clone(),
            weight: stored.weight,
        };

        let _guard = self.write_lock.lock();
        let mut batch = WriteBatch::default();
        self.save_edge(&mut batch, &stored)?;
        let cf_out = self.cf_out();
        self.upsert_adj_entry(&mut batch, cf_out.clone(), &stored.from, entry_out)?;
        let cf_in = self.cf_in();
        self.upsert_adj_entry(&mut batch, cf_in.clone(), &stored.to, entry_in)?;
        self.db.write(batch)?;

        Ok(edge_id)
    }

    async fn delete_edge(&self, edge_id: &str, fallback: (&str, &str, &str)) -> Result<()> {
        let _guard = self.write_lock.lock();
        let mut batch = WriteBatch::default();
        let existing = if !edge_id.is_empty() {
            self.remove_edge_entry(&mut batch, edge_id)?
        } else {
            if let Some(found_id) =
                self.find_edge_by_fallback(fallback.0, fallback.1, fallback.2)?
            {
                self.remove_edge_entry(&mut batch, &found_id)?
            } else {
                return Ok(());
            }
        };

        if let Some(edge) = existing {
            let cf_out = self.cf_out();
            self.remove_adj_entry(&mut batch, cf_out.clone(), &edge.from, &edge.id)?;
            let cf_in = self.cf_in();
            self.remove_adj_entry(&mut batch, cf_in.clone(), &edge.to, &edge.id)?;
            self.db.write(batch)?;
        }

        Ok(())
    }

    async fn query_edges(&self, query: EdgeQuery) -> Result<Vec<GraphEdge>> {
        if query.node.is_empty() {
            bail!("query requires a node id");
        }

        let mut visited_nodes = HashSet::new();
        let mut visited_edges = HashSet::new();
        let mut queue = VecDeque::new();
        let mut results = Vec::new();

        let max_depth = query.max_depth.max(1);
        let relation = query.relation.clone();
        let limit = query.limit.unwrap_or(u32::MAX) as usize;

        visited_nodes.insert(query.node.clone());
        queue.push_back((query.node.clone(), 0u16));

        while let Some((node, depth)) = queue.pop_front() {
            let edges = self.query_direction(&node, query.direction, &relation)?;
            for stored_edge in edges {
                if !visited_edges.insert(stored_edge.id.clone()) {
                    continue;
                }

                let graph_edge: GraphEdge = stored_edge.clone().into();
                results.push(graph_edge);
                if results.len() >= limit {
                    return Ok(results);
                }

                if depth + 1 < max_depth {
                    let mut next_nodes = Vec::new();
                    match query.direction {
                        QueryDirection::Inbound => next_nodes.push(stored_edge.from.clone()),
                        QueryDirection::Outbound => next_nodes.push(stored_edge.to.clone()),
                        QueryDirection::Both => {
                            if stored_edge.from != node {
                                next_nodes.push(stored_edge.from.clone());
                            }
                            if stored_edge.to != node {
                                next_nodes.push(stored_edge.to.clone());
                            }
                        }
                    }

                    for next in next_nodes {
                        if visited_nodes.insert(next.clone()) {
                            queue.push_back((next, depth + 1));
                        }
                    }
                }
            }
        }

        Ok(results)
    }

    async fn neighbors(&self, node: &str, direction: QueryDirection) -> Result<Vec<GraphNeighbor>> {
        self.neighbors_internal(node, direction)
    }

    async fn export_snapshot(&self) -> Result<GraphSnapshot> {
        self.build_snapshot()
    }

    async fn flush(&self) -> Result<()> {
        self.db.flush().context("failed to flush RocksDB")?;
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::model::{EdgeOp, GraphEdge, QueryDirection};
    use std::collections::HashMap;
    use tempfile::tempdir;

    fn test_edge(from: &str, to: &str, relation: &str, weight: Option<f64>) -> GraphEdge {
        GraphEdge {
            id: None,
            from: from.to_string(),
            to: to.to_string(),
            relation: relation.to_string(),
            tags: vec!["sample".to_string()],
            weight,
            confidence: Some(0.85),
            properties: Value::Null,
            op: EdgeOp::Upsert,
        }
    }

    #[tokio::test]
    async fn neighbors_include_weight_and_direction() -> Result<()> {
        let dir = tempdir().unwrap();
        let graph = RocksGraph::open(dir.path())?;

        let id_out = graph
            .upsert_edge(test_edge("n1", "n2", "rel", Some(2.5)))
            .await?;
        let id_in = graph
            .upsert_edge(test_edge("n2", "n1", "rel", None))
            .await?;

        let outbound = graph.neighbors("n1", QueryDirection::Outbound).await?;
        assert_eq!(outbound.len(), 1);
        assert_eq!(outbound[0].target, "n2");
        assert_eq!(outbound[0].direction, NeighborDirection::Outbound);
        assert_eq!(outbound[0].relation, "rel");
        assert_eq!(outbound[0].edge_id, id_out);
        assert!((outbound[0].weight - 2.5).abs() < f64::EPSILON);

        let inbound = graph.neighbors("n1", QueryDirection::Inbound).await?;
        assert_eq!(inbound.len(), 1);
        assert_eq!(inbound[0].target, "n2");
        assert_eq!(inbound[0].direction, NeighborDirection::Inbound);
        assert_eq!(inbound[0].relation, "rel");
        assert_eq!(inbound[0].edge_id, id_in);
        assert!((inbound[0].weight - 1.0).abs() < f64::EPSILON);

        Ok(())
    }

    #[tokio::test]
    async fn snapshot_generates_weighted_adjacency() -> Result<()> {
        let dir = tempdir().unwrap();
        let graph = RocksGraph::open(dir.path())?;
        let id_ab = graph
            .upsert_edge(test_edge("a", "b", "rel", Some(1.2)))
            .await?;
        let id_bc = graph
            .upsert_edge(test_edge("b", "c", "rel", Some(0.4)))
            .await?;

        let snapshot = graph.export_snapshot().await?;
        let adjacency = snapshot.adjacency_map();
        assert!(adjacency.contains_key("a"));
        assert!(adjacency.contains_key("b"));
        assert!(adjacency.contains_key("c"));

        let edges_a = adjacency.get("a").unwrap();
        let edge_ab = edges_a
            .iter()
            .find(|edge| edge.edge_id == id_ab && edge.direction == NeighborDirection::Outbound)
            .expect("missing outbound edge a->b");
        assert_eq!(edge_ab.relation, "rel");
        assert_eq!(edge_ab.tags, vec!["sample"]);
        assert_eq!(edge_ab.confidence, Some(0.85));
        assert_eq!(edge_ab.properties, Value::Null);

        let edges_b = adjacency.get("b").unwrap();
        let inbound_ab = edges_b
            .iter()
            .find(|edge| edge.edge_id == id_ab && edge.direction == NeighborDirection::Inbound)
            .expect("missing inbound edge to b from a");
        assert_eq!(inbound_ab.weight, 1.2);

        let outbound_bc = edges_b
            .iter()
            .find(|edge| edge.edge_id == id_bc && edge.direction == NeighborDirection::Outbound)
            .expect("missing outbound edge b->c");
        assert_eq!(outbound_bc.weight, 0.4);

        let (nodes, matrix) = snapshot.to_weighted_adjacency(SnapshotOrientation::Outbound);
        let mut index_map = HashMap::new();
        for (idx, node) in nodes.iter().enumerate() {
            index_map.insert(node, idx);
        }

        // Edge a -> b with weight 1.2
        let a_idx = *index_map.get(&"a".to_string()).unwrap();
        assert!(matrix[a_idx]
            .iter()
            .any(|(dst, weight)| nodes[*dst] == "b" && (*weight - 1.2).abs() < f64::EPSILON));

        // Edge b -> c with weight 0.4
        let b_idx = *index_map.get(&"b".to_string()).unwrap();
        assert!(matrix[b_idx]
            .iter()
            .any(|(dst, weight)| nodes[*dst] == "c" && (*weight - 0.4).abs() < f64::EPSILON));

        let (_nodes_inbound, inbound_matrix) =
            snapshot.to_weighted_adjacency(SnapshotOrientation::Inbound);
        assert!(!inbound_matrix.is_empty());

        let (_nodes_undirected, undirected_matrix) =
            snapshot.to_weighted_adjacency(SnapshotOrientation::Undirected);
        assert!(!undirected_matrix.is_empty());

        Ok(())
    }
}
