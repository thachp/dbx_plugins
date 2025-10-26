use std::{
    collections::{HashSet, VecDeque},
    path::Path,
    sync::Arc,
};

use anyhow::{bail, Context, Result};
use async_trait::async_trait;
use parking_lot::Mutex;
use rocksdb::{ColumnFamilyDescriptor, DBWithThreadMode, MultiThreaded, Options, WriteBatch};
use serde::{Deserialize, Serialize};
use serde_json::Value;
use snowflake::ProcessUniqueId;

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
    properties: Value,
}

impl From<StoredEdge> for GraphEdge {
    fn from(edge: StoredEdge) -> Self {
        GraphEdge {
            id: Some(edge.id),
            from: edge.from,
            to: edge.to,
            relation: edge.relation,
            tags: edge.tags,
            weight: edge.weight,
            confidence: edge.confidence,
            properties: edge.properties,
            op: EdgeOp::Upsert,
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct AdjacencyEntry {
    id: String,
    relation: String,
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

    fn build_stored(edge_id: String, edge: GraphEdge) -> StoredEdge {
        StoredEdge {
            id: edge_id,
            from: edge.from,
            to: edge.to,
            relation: edge.relation,
            tags: edge.tags,
            weight: edge.weight,
            confidence: edge.confidence,
            properties: edge.properties,
        }
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
}

#[async_trait]
impl GraphStorage for RocksGraph {
    async fn upsert_edge(&self, mut edge: GraphEdge) -> Result<String> {
        if edge.from.is_empty() || edge.to.is_empty() || edge.relation.is_empty() {
            bail!("edge must contain non-empty from, to, and relation fields");
        }

        let edge_id = Self::ensure_edge_id(&mut edge);
        let stored = Self::build_stored(edge_id.clone(), edge);
        let entry_out = AdjacencyEntry {
            id: edge_id.clone(),
            relation: stored.relation.clone(),
        };
        let entry_in = entry_out.clone();

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

    async fn flush(&self) -> Result<()> {
        self.db.flush().context("failed to flush RocksDB")?;
        Ok(())
    }
}
