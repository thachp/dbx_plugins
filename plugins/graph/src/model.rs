use serde::{Deserialize, Serialize};
use serde_json::Value;

/// Action to apply for an edge mutation.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum EdgeOp {
    Upsert,
    Delete,
}

impl Default for EdgeOp {
    fn default() -> Self {
        EdgeOp::Upsert
    }
}

/// Representation of a directed relationship.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct GraphEdge {
    pub id: Option<String>,
    pub from: String,
    pub to: String,
    #[serde(rename = "type")]
    pub relation: String,
    #[serde(default)]
    pub tags: Vec<String>,
    #[serde(default)]
    pub weight: Option<f64>,
    #[serde(default)]
    pub confidence: Option<f64>,
    #[serde(default)]
    pub properties: Value,
    #[serde(default)]
    pub op: EdgeOp,
}

impl GraphEdge {
    pub fn key_components(&self) -> (&str, &str, &str) {
        (&self.from, self.relation.as_str(), &self.to)
    }
}

/// Query parameters for retrieving connections.
#[derive(Debug, Clone)]
pub struct EdgeQuery {
    pub node: String,
    pub direction: QueryDirection,
    pub relation: Option<String>,
    pub max_depth: u16,
    pub limit: Option<u32>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum QueryDirection {
    Outbound,
    Inbound,
    Both,
}

impl Default for EdgeQuery {
    fn default() -> Self {
        Self {
            node: String::new(),
            direction: QueryDirection::Outbound,
            relation: None,
            max_depth: 1,
            limit: None,
        }
    }
}
