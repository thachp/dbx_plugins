//! Core traits and data structures that EventDBX plugins implement, including
//! helpers for the Cap'n Proto socket protocol.

pub mod plugin_capnp {
    include!(concat!(env!("OUT_DIR"), "/plugin_capnp.rs"));
}

#[allow(unused_parens)] // generated Cap'n Proto bindings use extra parentheses
pub mod api_capnp {
    include!(concat!(env!("OUT_DIR"), "/api_capnp.rs"));
}

pub mod control_capnp {
    include!(concat!(env!("OUT_DIR"), "/control_capnp.rs"));
}

pub mod grpc {
    pub mod dbx {
        pub mod plugins {
            include!(concat!(env!("OUT_DIR"), "/dbx.plugins.rs"));
        }
    }
}

use async_trait::async_trait;
use capnp::message::{Builder, ReaderOptions};
use capnp::serialize::write_message_to_words;
use capnp_futures::serialize::read_message;
use chrono::{DateTime, SecondsFormat, Utc};
use futures::io::AsyncWriteExt;
use prost_types::{value::Kind, ListValue, Timestamp, Value as ProstValue};
use serde::{Deserialize, Serialize};
use serde_json::{self, Value as JsonValue};
use std::collections::BTreeMap;
use std::convert::TryFrom;
use thiserror::Error;
use tokio::net::TcpStream;
use tokio_util::compat::{Compat, TokioAsyncReadCompatExt};

pub mod event_record {
    pub use super::grpc::dbx::plugins::{EventRecord, StateEntry};
}

/// High-level representation of an event delivered to plugins.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct EventRecord {
    pub sequence: u64,
    pub aggregate_type: String,
    pub aggregate_id: String,
    pub event_type: String,
    pub event_version: u64,
    pub event_id: String,
    pub created_at: DateTime<Utc>,
    pub payload_json: serde_json::Value,
    pub metadata_json: serde_json::Value,
    pub hash: String,
    pub merkle_root: String,
    pub state_version: u64,
    pub state_archived: bool,
    pub state_merkle_root: String,
    pub state_entries: Vec<StateEntry>,
    pub schema_json: serde_json::Value,
    pub extensions_json: serde_json::Value,
}

impl EventRecord {
    /// Construct an `EventRecord` from the Cap'n Proto envelope reader.
    pub fn try_from_capnp(
        reader: plugin_capnp::plugin_event::Reader<'_>,
    ) -> Result<Self, PluginError> {
        let schema_json = parse_optional_json(reader.get_schema_json()?)?;
        let extensions_json = parse_optional_json(reader.get_extensions_json()?)?;
        Ok(Self {
            sequence: reader.get_sequence(),
            aggregate_type: reader.get_aggregate_type()?.to_string(),
            aggregate_id: reader.get_aggregate_id()?.to_string(),
            event_type: reader.get_event_type()?.to_string(),
            event_version: reader.get_event_version(),
            event_id: reader.get_event_id()?.to_string(),
            created_at: DateTime::<Utc>::from_timestamp_micros(
                reader.get_created_at_epoch_micros(),
            )
            .ok_or(PluginError::InvalidTimestamp)?,
            payload_json: serde_json::from_str(reader.get_payload_json()?)
                .map_err(PluginError::InvalidJson)?,
            metadata_json: serde_json::from_str(reader.get_metadata_json()?)
                .map_err(PluginError::InvalidJson)?,
            hash: reader.get_hash()?.to_string(),
            merkle_root: reader.get_merkle_root()?.to_string(),
            state_version: reader.get_state_version(),
            state_archived: reader.get_state_archived(),
            state_merkle_root: reader.get_state_merkle_root()?.to_string(),
            state_entries: reader
                .get_state_entries()?
                .iter()
                .map(StateEntry::try_from_capnp)
                .collect::<Result<_, _>>()?,
            schema_json,
            extensions_json,
        })
    }
}

/// State entry diff attached to an event.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct StateEntry {
    pub key: String,
    pub value: String,
}

impl StateEntry {
    fn try_from_capnp(entry: plugin_capnp::state_entry::Reader<'_>) -> Result<Self, PluginError> {
        Ok(Self {
            key: entry.get_key()?.to_string(),
            value: entry.get_value()?.to_string(),
        })
    }
}

/// Contextual information provided to plugins on initialization.
#[derive(Debug, Clone)]
pub struct PluginContext {
    pub plugin_name: String,
    pub version: String,
    pub target: String,
    pub transport: PluginTransport,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum PluginTransport {
    Http,
    Tcp,
    Capnp,
    Grpc,
    Unknown(String),
}

impl Default for PluginTransport {
    fn default() -> Self {
        PluginTransport::Capnp
    }
}

impl PluginTransport {
    fn from_str(value: &str) -> Self {
        match value {
            "" => PluginTransport::default(),
            "http" => PluginTransport::Http,
            "tcp" => PluginTransport::Tcp,
            "capnp" => PluginTransport::Capnp,
            "grpc" => PluginTransport::Grpc,
            other => PluginTransport::Unknown(other.to_string()),
        }
    }

    /// Return a lowercase identifier for transport. Unknown transports echo original value.
    pub fn as_str(&self) -> &str {
        match self {
            PluginTransport::Http => "http",
            PluginTransport::Tcp => "tcp",
            PluginTransport::Capnp => "capnp",
            PluginTransport::Grpc => "grpc",
            PluginTransport::Unknown(value) => value.as_str(),
        }
    }
}

impl PluginContext {
    /// Build a context from the Cap'n Proto `PluginInit` message.
    pub fn from_capnp(reader: plugin_capnp::plugin_init::Reader<'_>) -> Result<Self, PluginError> {
        let transport_raw = reader.get_transport().unwrap_or("");
        Ok(Self {
            plugin_name: reader.get_plugin_name()?.to_string(),
            version: reader.get_version()?.to_string(),
            target: reader.get_target()?.to_string(),
            transport: PluginTransport::from_str(transport_raw),
        })
    }
}

/// Error type emitted by plugins.
#[derive(Error, Debug)]
pub enum PluginError {
    #[error("invalid timestamp in event payload")]
    InvalidTimestamp,
    #[error("invalid JSON payload: {0}")]
    InvalidJson(serde_json::Error),
    #[error("protocol error: {0}")]
    Protocol(#[from] capnp::Error),
    #[error("plugin logic error: {0}")]
    Custom(String),
}

impl PluginError {
    /// Convenience helper to construct a custom error.
    pub fn custom<T: Into<String>>(message: T) -> Self {
        PluginError::Custom(message.into())
    }
}

impl From<&StateEntry> for event_record::StateEntry {
    fn from(entry: &StateEntry) -> Self {
        Self {
            key: entry.key.clone(),
            value: entry.value.clone(),
        }
    }
}

/// Convert a core `EventRecord` into the protobuf representation.
pub fn event_record_to_proto(
    record: &EventRecord,
) -> Result<event_record::EventRecord, PluginError> {
    Ok(event_record::EventRecord {
        sequence: record.sequence,
        aggregate_type: record.aggregate_type.clone(),
        aggregate_id: record.aggregate_id.clone(),
        event_type: record.event_type.clone(),
        event_version: record.event_version,
        event_id: record.event_id.clone(),
        created_at: Some(datetime_to_timestamp(&record.created_at)),
        payload_json: Some(json_to_prost(&record.payload_json)?),
        metadata_json: Some(json_to_prost(&record.metadata_json)?),
        hash: record.hash.clone(),
        merkle_root: record.merkle_root.clone(),
        state_version: record.state_version,
        state_archived: record.state_archived,
        state_merkle_root: record.state_merkle_root.clone(),
        state_entries: record
            .state_entries
            .iter()
            .map(event_record::StateEntry::from)
            .collect(),
        schema_json: Some(json_to_prost(&record.schema_json)?),
        extensions_json: Some(json_to_prost(&record.extensions_json)?),
    })
}

/// Convert a protobuf `EventRecord` into the core representation.
pub fn event_record_from_proto(
    proto: &event_record::EventRecord,
) -> Result<EventRecord, PluginError> {
    let timestamp = proto
        .created_at
        .as_ref()
        .ok_or(PluginError::InvalidTimestamp)?;
    Ok(EventRecord {
        sequence: proto.sequence,
        aggregate_type: proto.aggregate_type.clone(),
        aggregate_id: proto.aggregate_id.clone(),
        event_type: proto.event_type.clone(),
        event_version: proto.event_version,
        event_id: proto.event_id.clone(),
        created_at: timestamp_to_datetime(timestamp)?,
        payload_json: proto
            .payload_json
            .as_ref()
            .map_or(Ok(JsonValue::Null), prost_to_json)?,
        metadata_json: proto
            .metadata_json
            .as_ref()
            .map_or(Ok(JsonValue::Null), prost_to_json)?,
        hash: proto.hash.clone(),
        merkle_root: proto.merkle_root.clone(),
        state_version: proto.state_version,
        state_archived: proto.state_archived,
        state_merkle_root: proto.state_merkle_root.clone(),
        state_entries: proto
            .state_entries
            .iter()
            .map(core_state_entry_from_proto)
            .collect(),
        schema_json: proto
            .schema_json
            .as_ref()
            .map_or(Ok(JsonValue::Null), prost_to_json)?,
        extensions_json: proto
            .extensions_json
            .as_ref()
            .map_or(Ok(JsonValue::Null), prost_to_json)?,
    })
}

/// Fill an append-event request builder from an `EventRecord`.
pub fn write_append_event_request(
    record: &EventRecord,
    mut builder: api_capnp::append_event_request::Builder<'_>,
) -> Result<(), PluginError> {
    builder.set_aggregate_type(&record.aggregate_type);
    builder.set_aggregate_id(&record.aggregate_id);
    builder.set_event_type(&record.event_type);

    let payload = serde_json::to_string(&record.payload_json).map_err(PluginError::InvalidJson)?;
    builder.set_payload_json(&payload);

    if !record.metadata_json.is_null() {
        let metadata =
            serde_json::to_string(&record.metadata_json).map_err(PluginError::InvalidJson)?;
        builder.set_has_metadata(true);
        builder.set_metadata_json(&metadata);
    } else {
        builder.set_has_metadata(false);
        builder.set_metadata_json("");
    }

    if let Some(note) = metadata_note(&record.metadata_json) {
        builder.set_note(&note);
    } else {
        builder.set_note("");
    }

    if let Some(patch) = metadata_patch(&record.metadata_json) {
        builder.set_patch_json(&patch);
    } else {
        builder.set_patch_json("");
    }

    Ok(())
}

/// Convert an append-event response into an `EventRecord`.
pub fn read_append_event_response(
    response: api_capnp::append_event_response::Reader<'_>,
) -> Result<EventRecord, PluginError> {
    let event = response.get_event().map_err(PluginError::Protocol)?;
    event_record_from_api_event(event)
}

/// Populate a Cap'n Proto API event builder from an `EventRecord`.
pub fn write_api_event(
    record: &EventRecord,
    mut builder: api_capnp::event::Builder<'_>,
) -> Result<(), PluginError> {
    builder.set_aggregate_type(&record.aggregate_type);
    builder.set_aggregate_id(&record.aggregate_id);
    builder.set_event_type(&record.event_type);
    builder.set_version(record.event_version);

    let payload = serde_json::to_string(&record.payload_json).map_err(PluginError::InvalidJson)?;
    builder.set_payload_json(&payload);

    builder.set_hash(&record.hash);
    builder.set_merkle_root(&record.merkle_root);

    let mut metadata = builder.reborrow().init_metadata();
    metadata.set_event_id(&record.event_id);
    metadata.set_created_at(
        &record
            .created_at
            .to_rfc3339_opts(SecondsFormat::Micros, true),
    );

    if let Some(note) = metadata_note(&record.metadata_json) {
        metadata.set_note(&note);
    } else {
        metadata.set_note("");
    }

    let (group, user) = metadata_claims(&record.metadata_json);
    let mut issued = metadata.reborrow().init_issued_by();
    issued.set_group(&group);
    issued.set_user(&user);

    if !record.extensions_json.is_null() {
        let extensions =
            serde_json::to_string(&record.extensions_json).map_err(PluginError::InvalidJson)?;
        builder.set_extensions_json(&extensions);
    } else {
        builder.set_extensions_json("null");
    }

    Ok(())
}

/// Convert a Cap'n Proto API event reader into the unified `EventRecord`.
pub fn event_record_from_api_event(
    event: api_capnp::event::Reader<'_>,
) -> Result<EventRecord, PluginError> {
    let payload_json =
        serde_json::from_str(event.get_payload_json().map_err(PluginError::Protocol)?)
            .map_err(PluginError::InvalidJson)?;

    let metadata_reader = event.get_metadata().map_err(PluginError::Protocol)?;
    let created_at = parse_rfc3339(
        metadata_reader
            .get_created_at()
            .map_err(PluginError::Protocol)?,
    )?;
    let metadata_json = api_metadata_to_json(metadata_reader)?;
    let extensions_json =
        parse_optional_json(event.get_extensions_json().map_err(PluginError::Protocol)?)?;

    Ok(EventRecord {
        sequence: event.get_version(),
        aggregate_type: event
            .get_aggregate_type()
            .map_err(PluginError::Protocol)?
            .to_string(),
        aggregate_id: event
            .get_aggregate_id()
            .map_err(PluginError::Protocol)?
            .to_string(),
        event_type: event
            .get_event_type()
            .map_err(PluginError::Protocol)?
            .to_string(),
        event_version: event.get_version(),
        event_id: metadata_reader
            .get_event_id()
            .map_err(PluginError::Protocol)?
            .to_string(),
        created_at,
        payload_json,
        metadata_json,
        hash: event.get_hash().map_err(PluginError::Protocol)?.to_string(),
        merkle_root: event
            .get_merkle_root()
            .map_err(PluginError::Protocol)?
            .to_string(),
        state_version: event.get_version(),
        state_archived: false,
        state_merkle_root: event
            .get_merkle_root()
            .map_err(PluginError::Protocol)?
            .to_string(),
        state_entries: Vec::new(),
        schema_json: JsonValue::Null,
        extensions_json,
    })
}

fn datetime_to_timestamp(dt: &DateTime<Utc>) -> Timestamp {
    Timestamp {
        seconds: dt.timestamp(),
        nanos: dt.timestamp_subsec_nanos() as i32,
    }
}

fn timestamp_to_datetime(ts: &Timestamp) -> Result<DateTime<Utc>, PluginError> {
    DateTime::<Utc>::from_timestamp(ts.seconds, ts.nanos as u32)
        .ok_or(PluginError::InvalidTimestamp)
}

fn core_state_entry_from_proto(entry: &event_record::StateEntry) -> StateEntry {
    StateEntry {
        key: entry.key.clone(),
        value: entry.value.clone(),
    }
}

fn json_to_prost(value: &JsonValue) -> Result<ProstValue, PluginError> {
    let kind = match value {
        JsonValue::Null => Kind::NullValue(0),
        JsonValue::Bool(flag) => Kind::BoolValue(*flag),
        JsonValue::Number(num) => {
            let float = num
                .as_f64()
                .ok_or_else(|| PluginError::custom("number out of f64 range"))?;
            Kind::NumberValue(float)
        }
        JsonValue::String(text) => Kind::StringValue(text.clone()),
        JsonValue::Array(items) => {
            let mut list = Vec::with_capacity(items.len());
            for item in items {
                list.push(json_to_prost(item)?);
            }
            Kind::ListValue(ListValue { values: list })
        }
        JsonValue::Object(map) => {
            let mut fields = BTreeMap::new();
            for (key, value) in map {
                fields.insert(key.clone(), json_to_prost(value)?);
            }
            Kind::StructValue(prost_types::Struct { fields })
        }
    };

    Ok(ProstValue { kind: Some(kind) })
}

fn prost_to_json(value: &ProstValue) -> Result<JsonValue, PluginError> {
    match value
        .kind
        .as_ref()
        .ok_or_else(|| PluginError::custom("missing protobuf value kind"))?
    {
        Kind::NullValue(_) => Ok(JsonValue::Null),
        Kind::BoolValue(flag) => Ok(JsonValue::Bool(*flag)),
        Kind::NumberValue(num) => serde_json::Number::from_f64(*num)
            .map(JsonValue::Number)
            .ok_or_else(|| PluginError::custom("invalid f64 for JSON number")),
        Kind::StringValue(text) => Ok(JsonValue::String(text.clone())),
        Kind::StructValue(struct_value) => {
            let mut map = serde_json::Map::with_capacity(struct_value.fields.len());
            for (key, value) in &struct_value.fields {
                map.insert(key.clone(), prost_to_json(value)?);
            }
            Ok(JsonValue::Object(map))
        }
        Kind::ListValue(list) => {
            let mut items = Vec::with_capacity(list.values.len());
            for item in &list.values {
                items.push(prost_to_json(item)?);
            }
            Ok(JsonValue::Array(items))
        }
    }
}

fn parse_optional_json(raw: &str) -> Result<JsonValue, PluginError> {
    let trimmed = raw.trim();
    if trimmed.is_empty() {
        Ok(JsonValue::Null)
    } else {
        serde_json::from_str(trimmed).map_err(PluginError::InvalidJson)
    }
}

fn parse_rfc3339(value: &str) -> Result<DateTime<Utc>, PluginError> {
    chrono::DateTime::parse_from_rfc3339(value)
        .map(|dt| dt.with_timezone(&Utc))
        .map_err(|err| PluginError::custom(format!("invalid RFC3339 timestamp: {err}")))
}

fn metadata_note(metadata: &JsonValue) -> Option<String> {
    metadata
        .as_object()
        .and_then(|map| map.get("note"))
        .and_then(|value| value.as_str())
        .map(|value| value.to_string())
}

fn metadata_patch(metadata: &JsonValue) -> Option<String> {
    let map = metadata.as_object()?;
    for key in ["patchJson", "patch_json", "patch"] {
        if let Some(value) = map.get(key) {
            if let Some(text) = value.as_str() {
                return Some(text.to_owned());
            }
        }
    }
    None
}

fn metadata_claims(metadata: &JsonValue) -> (String, String) {
    let map = metadata.as_object();
    let claims = map
        .and_then(|map| map.get("issuedBy").or_else(|| map.get("issued_by")))
        .and_then(|value| value.as_object());

    let group = claims
        .and_then(|claims| claims.get("group"))
        .and_then(|value| value.as_str())
        .unwrap_or_default()
        .to_owned();
    let user = claims
        .and_then(|claims| claims.get("user"))
        .and_then(|value| value.as_str())
        .unwrap_or_default()
        .to_owned();

    (group, user)
}

fn api_metadata_to_json(
    metadata: api_capnp::event_metadata::Reader<'_>,
) -> Result<JsonValue, PluginError> {
    let mut map = serde_json::Map::new();

    let event_id = metadata.get_event_id().map_err(PluginError::Protocol)?;
    if !event_id.is_empty() {
        map.insert("eventId".into(), JsonValue::String(event_id.to_string()));
    }

    let created_at = metadata.get_created_at().map_err(PluginError::Protocol)?;
    if !created_at.is_empty() {
        map.insert(
            "createdAt".into(),
            JsonValue::String(created_at.to_string()),
        );
    }

    let note = metadata.get_note().map_err(PluginError::Protocol)?;
    if !note.is_empty() {
        map.insert("note".into(), JsonValue::String(note.to_string()));
    }

    if metadata.has_issued_by() {
        let issued = metadata.get_issued_by().map_err(PluginError::Protocol)?;
        let mut issued_map = serde_json::Map::new();
        let group = issued.get_group().map_err(PluginError::Protocol)?;
        if !group.is_empty() {
            issued_map.insert("group".into(), JsonValue::String(group.to_string()));
        }
        let user = issued.get_user().map_err(PluginError::Protocol)?;
        if !user.is_empty() {
            issued_map.insert("user".into(), JsonValue::String(user.to_string()));
        }
        if !issued_map.is_empty() {
            map.insert("issuedBy".into(), JsonValue::Object(issued_map));
        }
    }

    if map.is_empty() {
        Ok(JsonValue::Null)
    } else {
        Ok(JsonValue::Object(map))
    }
}

/// Core contract all EventDBX plugins implement.
#[async_trait]
pub trait Plugin: Send + Sync {
    /// Called once when the plugin starts. Plugins can allocate resources or start background tasks.
    async fn init(&self, ctx: PluginContext) -> Result<(), PluginError> {
        let _ = ctx;
        Ok(())
    }

    /// Handle an incoming event message.
    async fn on_event(&self, event: EventRecord) -> Result<(), PluginError>;

    /// Called when the runtime is about to shut down.
    async fn shutdown(&self) -> Result<(), PluginError> {
        Ok(())
    }
}

/// Specialized contract for search-capable plugins.
#[async_trait]
pub trait SearchPlugin: Plugin {
    async fn index_event(&self, event: EventRecord) -> Result<(), PluginError>;
    async fn search(
        &self,
        query: &str,
        filters: Option<SearchFilter>,
    ) -> Result<Vec<SearchResult>, PluginError>;
}

/// Filters attached to search queries.
#[derive(Debug, Clone, Serialize, Deserialize, Default)]
#[serde(rename_all = "camelCase", default)]
pub struct SearchFilter {
    #[serde(skip_serializing_if = "Option::is_none")]
    pub aggregate_type: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub aggregate_id: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub state_version_min: Option<u64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub state_version_max: Option<u64>,
}

/// Search result row returned by search plugins.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct SearchResult {
    pub aggregate_id: String,
    pub event_id: String,
    pub score: f32,
    pub payload_json: serde_json::Value,
    pub metadata_json: serde_json::Value,
}

/// Aggregate snapshot returned by the control socket.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub struct AggregateStateView {
    pub aggregate_type: String,
    pub aggregate_id: String,
    pub version: u64,
    pub state: BTreeMap<String, String>,
    pub merkle_root: String,
    pub archived: bool,
}

/// Actor identity associated with an event.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct ActorClaimsView {
    pub group: String,
    pub user: String,
}

/// Event metadata as returned by the control socket.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct EventMetadataView {
    #[serde(alias = "event_id")]
    pub event_id: String,
    #[serde(
        serialize_with = "serde_helpers::serialize_rfc3339",
        deserialize_with = "serde_helpers::deserialize_rfc3339"
    )]
    #[serde(alias = "created_at")]
    pub created_at: DateTime<Utc>,
    #[serde(skip_serializing_if = "Option::is_none")]
    #[serde(alias = "issued_by")]
    pub issued_by: Option<ActorClaimsView>,
    #[serde(skip_serializing_if = "Option::is_none")]
    #[serde(alias = "note")]
    pub note: Option<String>,
}

/// Event record returned by the control socket when listing or appending events.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct StoredEventRecord {
    #[serde(alias = "aggregate_type")]
    pub aggregate_type: String,
    #[serde(alias = "aggregate_id")]
    pub aggregate_id: String,
    #[serde(alias = "event_type")]
    pub event_type: String,
    #[serde(alias = "version")]
    pub version: u64,
    #[serde(alias = "payload")]
    pub payload: JsonValue,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    #[serde(alias = "extensions")]
    pub extensions: Option<JsonValue>,
    #[serde(alias = "metadata")]
    pub metadata: EventMetadataView,
    #[serde(alias = "hash")]
    pub hash: String,
    #[serde(alias = "merkle_root")]
    pub merkle_root: String,
}

/// Request payload for appending an event through the control socket.
#[derive(Debug, Clone, Serialize, Deserialize, Default)]
pub struct AppendEventRequest {
    pub token: String,
    pub aggregate_type: String,
    pub aggregate_id: String,
    pub event_type: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub payload: Option<JsonValue>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub patch: Option<JsonValue>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub metadata: Option<JsonValue>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub note: Option<String>,
}

mod serde_helpers {
    use chrono::{DateTime, Utc};
    use serde::{self, Deserialize, Deserializer, Serializer};

    pub fn serialize_rfc3339<S>(value: &DateTime<Utc>, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        serializer.serialize_str(&value.to_rfc3339())
    }

    pub fn deserialize_rfc3339<'de, D>(deserializer: D) -> Result<DateTime<Utc>, D::Error>
    where
        D: Deserializer<'de>,
    {
        let raw = String::deserialize(deserializer)?;
        chrono::DateTime::parse_from_rfc3339(&raw)
            .map(|dt| dt.with_timezone(&Utc))
            .map_err(serde::de::Error::custom)
    }
}

/// Errors produced by the control-socket client.
#[derive(Debug, Error)]
pub enum ControlClientError {
    #[error("io error: {0}")]
    Io(#[from] std::io::Error),
    #[error("capnp error: {0}")]
    Capnp(#[from] capnp::Error),
    #[error("server error ({code}): {message}")]
    Server { code: String, message: String },
    #[error("protocol error: {0}")]
    Protocol(String),
    #[error("json error: {0}")]
    Json(#[from] serde_json::Error),
}

pub type ControlResult<T> = std::result::Result<T, ControlClientError>;

/// Minimal client for the control socket exposed on port 6363.
pub struct ControlClient {
    stream: Compat<TcpStream>,
    next_id: u64,
}

impl ControlClient {
    /// Connect to the control socket at `addr` (e.g. `"127.0.0.1:6363"`).
    pub async fn connect(addr: &str) -> ControlResult<Self> {
        let stream = TcpStream::connect(addr).await?;
        Ok(Self {
            stream: stream.compat(),
            next_id: 1,
        })
    }

    /// Retrieve a sanitized list of aggregates.
    pub async fn list_aggregates(
        &mut self,
        skip: usize,
        take: Option<usize>,
    ) -> ControlResult<Vec<AggregateStateView>> {
        let skip_u64 = u64::try_from(skip)
            .map_err(|_| ControlClientError::Protocol("skip exceeds u64 range".into()))?;
        let take_u64 = match take {
            Some(value) => Some(
                u64::try_from(value)
                    .map_err(|_| ControlClientError::Protocol("take exceeds u64 range".into()))?,
            ),
            None => None,
        };

        let request_id = self.next_request_id();
        let mut message = Builder::new_default();
        {
            let mut request = message.init_root::<control_capnp::control_request::Builder>();
            request.set_id(request_id);
            let payload = request.reborrow().init_payload();
            let mut body = payload.init_list_aggregates();
            body.set_skip(skip_u64);
            if let Some(take) = take_u64 {
                body.set_has_take(true);
                body.set_take(take);
            } else {
                body.set_has_take(false);
                body.set_take(0);
            }
        }

        self.send_and_parse(message, request_id, |response| {
            match response.get_payload().which().map_err(|_| {
                ControlClientError::Protocol(
                    "unexpected response payload for listAggregates".into(),
                )
            })? {
                control_capnp::control_response::payload::ListAggregates(resp) => {
                    let resp = resp.map_err(ControlClientError::Capnp)?;
                    let json = read_text_field(resp.get_aggregates_json(), "aggregates_json")?;
                    let aggregates = serde_json::from_str::<Vec<AggregateStateView>>(&json)?;
                    Ok(aggregates)
                }
                control_capnp::control_response::payload::Error(err) => {
                    let err = err.map_err(ControlClientError::Capnp)?;
                    Err(server_error(err))
                }
                _ => Err(ControlClientError::Protocol(
                    "unexpected response to listAggregates".into(),
                )),
            }
        })
        .await
    }

    /// Fetch a single aggregate if it exists.
    pub async fn get_aggregate(
        &mut self,
        aggregate_type: &str,
        aggregate_id: &str,
    ) -> ControlResult<Option<AggregateStateView>> {
        let request_id = self.next_request_id();
        let mut message = Builder::new_default();
        {
            let mut request = message.init_root::<control_capnp::control_request::Builder>();
            request.set_id(request_id);
            let payload = request.reborrow().init_payload();
            let mut body = payload.init_get_aggregate();
            body.set_aggregate_type(aggregate_type);
            body.set_aggregate_id(aggregate_id);
        }

        self.send_and_parse(message, request_id, |response| {
            match response.get_payload().which().map_err(|_| {
                ControlClientError::Protocol("unexpected response payload for getAggregate".into())
            })? {
                control_capnp::control_response::payload::GetAggregate(resp) => {
                    let resp = resp.map_err(ControlClientError::Capnp)?;
                    if resp.get_found() {
                        let json = read_text_field(resp.get_aggregate_json(), "aggregate_json")?;
                        let aggregate = serde_json::from_str::<AggregateStateView>(&json)?;
                        Ok(Some(aggregate))
                    } else {
                        Ok(None)
                    }
                }
                control_capnp::control_response::payload::Error(err) => {
                    let err = err.map_err(ControlClientError::Capnp)?;
                    Err(server_error(err))
                }
                _ => Err(ControlClientError::Protocol(
                    "unexpected response to getAggregate".into(),
                )),
            }
        })
        .await
    }

    /// List events for a given aggregate.
    pub async fn list_events(
        &mut self,
        aggregate_type: &str,
        aggregate_id: &str,
        skip: usize,
        take: Option<usize>,
    ) -> ControlResult<Vec<StoredEventRecord>> {
        let skip_u64 = u64::try_from(skip)
            .map_err(|_| ControlClientError::Protocol("skip exceeds u64 range".into()))?;
        let take_u64 = match take {
            Some(value) => Some(
                u64::try_from(value)
                    .map_err(|_| ControlClientError::Protocol("take exceeds u64 range".into()))?,
            ),
            None => None,
        };

        let request_id = self.next_request_id();
        let mut message = Builder::new_default();
        {
            let mut request = message.init_root::<control_capnp::control_request::Builder>();
            request.set_id(request_id);
            let payload = request.reborrow().init_payload();
            let mut body = payload.init_list_events();
            body.set_aggregate_type(aggregate_type);
            body.set_aggregate_id(aggregate_id);
            body.set_skip(skip_u64);
            if let Some(take) = take_u64 {
                body.set_has_take(true);
                body.set_take(take);
            } else {
                body.set_has_take(false);
                body.set_take(0);
            }
        }

        self.send_and_parse(message, request_id, |response| {
            match response.get_payload().which().map_err(|_| {
                ControlClientError::Protocol("unexpected response payload for listEvents".into())
            })? {
                control_capnp::control_response::payload::ListEvents(resp) => {
                    let resp = resp.map_err(ControlClientError::Capnp)?;
                    let json = read_text_field(resp.get_events_json(), "events_json")?;
                    let events = serde_json::from_str::<Vec<StoredEventRecord>>(&json)?;
                    Ok(events)
                }
                control_capnp::control_response::payload::Error(err) => {
                    let err = err.map_err(ControlClientError::Capnp)?;
                    Err(server_error(err))
                }
                _ => Err(ControlClientError::Protocol(
                    "unexpected response to listEvents".into(),
                )),
            }
        })
        .await
    }

    /// Append an event to the store.
    pub async fn append_event(
        &mut self,
        request: AppendEventRequest,
    ) -> ControlResult<StoredEventRecord> {
        if request.payload.is_some() && request.patch.is_some() {
            return Err(ControlClientError::Protocol(
                "payload and patch cannot both be provided".into(),
            ));
        }
        if request.payload.is_none() && request.patch.is_none() {
            return Err(ControlClientError::Protocol(
                "either payload or patch must be provided".into(),
            ));
        }

        let payload_json = request
            .payload
            .as_ref()
            .map(serde_json::to_string)
            .transpose()?
            .unwrap_or_else(String::new);
        let patch_json = request
            .patch
            .as_ref()
            .map(serde_json::to_string)
            .transpose()?
            .unwrap_or_else(String::new);
        let metadata_json = request
            .metadata
            .as_ref()
            .map(serde_json::to_string)
            .transpose()?;

        let request_id = self.next_request_id();
        let mut message = Builder::new_default();
        {
            let mut cap_request = message.init_root::<control_capnp::control_request::Builder>();
            cap_request.set_id(request_id);
            let payload = cap_request.reborrow().init_payload();
            let mut body = payload.init_append_event();
            body.set_token(&request.token);
            body.set_aggregate_type(&request.aggregate_type);
            body.set_aggregate_id(&request.aggregate_id);
            body.set_event_type(&request.event_type);
            body.set_payload_json(&payload_json);
            body.set_patch_json(&patch_json);
            if let Some(metadata) = metadata_json.as_ref() {
                body.set_has_metadata(true);
                body.set_metadata_json(metadata);
            } else {
                body.set_has_metadata(false);
                body.set_metadata_json("");
            }
            if let Some(note) = request.note.as_ref() {
                body.set_has_note(true);
                body.set_note(note);
            } else {
                body.set_has_note(false);
                body.set_note("");
            }
        }

        self.send_and_parse(message, request_id, |response| {
            match response.get_payload().which().map_err(|_| {
                ControlClientError::Protocol("unexpected response payload for appendEvent".into())
            })? {
                control_capnp::control_response::payload::AppendEvent(resp) => {
                    let resp = resp.map_err(ControlClientError::Capnp)?;
                    let json = read_text_field(resp.get_event_json(), "event_json")?;
                    let record = serde_json::from_str::<StoredEventRecord>(&json)?;
                    Ok(record)
                }
                control_capnp::control_response::payload::Error(err) => {
                    let err = err.map_err(ControlClientError::Capnp)?;
                    Err(server_error(err))
                }
                _ => Err(ControlClientError::Protocol(
                    "unexpected response to appendEvent".into(),
                )),
            }
        })
        .await
    }

    /// Verify an aggregate's merkle root.
    pub async fn verify_aggregate(
        &mut self,
        aggregate_type: &str,
        aggregate_id: &str,
    ) -> ControlResult<String> {
        let request_id = self.next_request_id();
        let mut message = Builder::new_default();
        {
            let mut request = message.init_root::<control_capnp::control_request::Builder>();
            request.set_id(request_id);
            let payload = request.reborrow().init_payload();
            let mut body = payload.init_verify_aggregate();
            body.set_aggregate_type(aggregate_type);
            body.set_aggregate_id(aggregate_id);
        }

        self.send_and_parse(message, request_id, |response| {
            match response.get_payload().which().map_err(|_| {
                ControlClientError::Protocol(
                    "unexpected response payload for verifyAggregate".into(),
                )
            })? {
                control_capnp::control_response::payload::VerifyAggregate(resp) => {
                    let resp = resp.map_err(ControlClientError::Capnp)?;
                    let merkle = read_text_field(resp.get_merkle_root(), "merkle_root")?;
                    Ok(merkle)
                }
                control_capnp::control_response::payload::Error(err) => {
                    let err = err.map_err(ControlClientError::Capnp)?;
                    Err(server_error(err))
                }
                _ => Err(ControlClientError::Protocol(
                    "unexpected response to verifyAggregate".into(),
                )),
            }
        })
        .await
    }

    fn next_request_id(&mut self) -> u64 {
        let id = self.next_id;
        self.next_id = self.next_id.wrapping_add(1);
        id
    }

    async fn send_and_parse<F, T>(
        &mut self,
        message: Builder<capnp::message::HeapAllocator>,
        request_id: u64,
        parser: F,
    ) -> ControlResult<T>
    where
        F: FnOnce(control_capnp::control_response::Reader<'_>) -> ControlResult<T>,
    {
        let bytes = write_message_to_words(&message);
        self.stream.write_all(&bytes).await?;

        let response_message = read_message(&mut self.stream, ReaderOptions::new()).await?;
        let response = response_message
            .get_root::<control_capnp::control_response::Reader>()
            .map_err(ControlClientError::Capnp)?;

        if response.get_id() != request_id {
            return Err(ControlClientError::Protocol(format!(
                "mismatched response id (expected {}, got {})",
                request_id,
                response.get_id()
            )));
        }

        parser(response)
    }
}

fn read_text_field(
    field: capnp::Result<capnp::text::Reader<'_>>,
    _label: &str,
) -> ControlResult<String> {
    let reader = field.map_err(ControlClientError::Capnp)?;
    Ok(reader.to_string())
}

fn server_error(reader: control_capnp::control_error::Reader<'_>) -> ControlClientError {
    let code =
        read_text_field(reader.get_code(), "error code").unwrap_or_else(|_| "unknown".to_string());
    let message = read_text_field(reader.get_message(), "error message")
        .unwrap_or_else(|_| "unknown".to_string());
    ControlClientError::Server { code, message }
}
