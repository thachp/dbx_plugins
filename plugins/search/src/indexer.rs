use std::{
    collections::{BTreeMap, HashMap},
    fs,
};

use anyhow::{anyhow, Context, Result};
use chrono::{DateTime, Utc};
use dbx_plugin_api::EventRecord;
use serde_json::{self, Value as JsonValue};
use tantivy::{
    schema::{Cardinality, Field, NumericOptions, Schema, STRING, TEXT},
    Document, Index, IndexWriter, Term,
};
use tracing::{debug, info, warn};

use crate::config::Config;

const FIELD_AGGREGATE_KEY: &str = "aggregate_key";
const FIELD_AGGREGATE_TYPE: &str = "aggregate_type";
const FIELD_AGGREGATE_ID: &str = "aggregate_id";
const FIELD_STATE_VERSION: &str = "state_version";
const FIELD_EVENT_COUNT: &str = "event_count";
const FIELD_LAST_SEQUENCE: &str = "last_sequence";
const FIELD_LAST_EVENT_TYPE: &str = "last_event_type";
const FIELD_LAST_EVENT_VERSION: &str = "last_event_version";
const FIELD_LAST_EVENT_ID: &str = "last_event_id";
const FIELD_LAST_EVENT_EPOCH: &str = "last_event_epoch_micros";
const FIELD_STATE_JSON: &str = "state_json";
const FIELD_STATE_ARCHIVED: &str = "state_archived";
const FIELD_STATE_MERKLE: &str = "state_merkle_root";
const STATE_FIELD_PREFIX: &str = "state_";

/// Supported Tantivy field types for aggregate state columns.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum StateFieldType {
    Text,
    Bool,
    I64,
    F64,
}

#[derive(Clone, Debug)]
struct StateFieldDef {
    key: String,
    field_name: String,
    ty: StateFieldType,
}

struct StateFieldBinding {
    field: Field,
    ty: StateFieldType,
}

struct IndexFields {
    aggregate_key: Field,
    aggregate_type: Field,
    aggregate_id: Field,
    state_version: Field,
    event_count: Field,
    last_sequence: Field,
    last_event_type: Field,
    last_event_version: Field,
    last_event_id: Field,
    last_event_epoch_micros: Field,
    state_json: Field,
    state_archived: Field,
    state_merkle_root: Field,
    state_fields: HashMap<String, StateFieldBinding>,
}

/// Tantivy-backed indexer that maintains a single document per aggregate.
pub struct TantivyIndexer {
    index: Index,
    writer: IndexWriter,
    fields: IndexFields,
    aggregates: HashMap<AggregateKey, AggregateDoc>,
    commit_after_events: usize,
    pending_since_commit: usize,
    config: Config,
    state_field_defs: Vec<StateFieldDef>,
}

impl TantivyIndexer {
    pub fn open(config: Config) -> Result<Self> {
        if config.index_path.exists() {
            fs::remove_dir_all(&config.index_path).with_context(|| {
                format!(
                    "failed to clear existing index at {}",
                    config.index_path.display()
                )
            })?;
        }
        fs::create_dir_all(&config.index_path)?;

        let state_field_defs = Vec::new();
        let (index, writer, fields) = create_index_components(&config, &state_field_defs)?;

        info!(
            "index_path" = %config.index_path.display(),
            "commit_after_events" = config.commit_after_events,
            "writer_heap_size_bytes" = config.writer_heap_size_bytes,
            "msg" = "search index ready"
        );

        Ok(Self {
            index,
            writer,
            fields,
            aggregates: HashMap::new(),
            commit_after_events: config.commit_after_events,
            pending_since_commit: 0,
            config,
            state_field_defs,
        })
    }

    pub fn index_event(&mut self, record: &EventRecord) -> Result<()> {
        let key = AggregateKey::new(&record.aggregate_type, &record.aggregate_id);
        let state_entries = parse_state_entries(record);
        self.ensure_state_fields(&state_entries)?;

        match self.aggregates.get_mut(&key) {
            Some(doc) => {
                if !doc.apply_event(record, &state_entries) {
                    debug!(
                        "aggregate_type" = %record.aggregate_type,
                        "aggregate_id" = %record.aggregate_id,
                        "sequence" = record.sequence,
                        "msg" = "skipping stale event for aggregate"
                    );
                    return Ok(());
                }
            }
            None => {
                let doc = AggregateDoc::from_event(record, &state_entries);
                self.aggregates.insert(key.clone(), doc);
            }
        }

        self.upsert_document(&key)?;
        Ok(())
    }

    pub fn commit(&mut self) -> Result<()> {
        if self.pending_since_commit == 0 {
            return Ok(());
        }

        self.writer
            .commit()
            .with_context(|| "failed to commit Tantivy index".to_string())?;

        self.pending_since_commit = 0;
        Ok(())
    }

    fn upsert_document(&mut self, key: &AggregateKey) -> Result<()> {
        let term_value = key.term_value();
        let doc = self
            .aggregates
            .get(key)
            .ok_or_else(|| anyhow!("missing aggregate document for {}", term_value))?;

        self.writer.delete_term(Term::from_field_text(
            self.fields.aggregate_key,
            &term_value,
        ));
        self.writer
            .add_document(doc.to_document(&self.fields, &term_value))
            .with_context(|| {
                format!(
                    "failed to add document for aggregate {}:{}",
                    key.aggregate_type, key.aggregate_id
                )
            })?;

        self.pending_since_commit += 1;
        if self.pending_since_commit >= self.commit_after_events {
            self.commit()?;
        }
        Ok(())
    }

    fn ensure_state_fields(&mut self, state_entries: &BTreeMap<String, JsonValue>) -> Result<()> {
        let mut added = false;
        for (key, value) in state_entries {
            if self.state_field_defs.iter().any(|def| def.key == *key) {
                continue;
            }
            let ty = detect_state_type(value);
            let field_name = self.unique_field_name_for(key);
            self.state_field_defs.push(StateFieldDef {
                key: key.clone(),
                field_name,
                ty,
            });
            added = true;
        }

        if added {
            self.rebuild_index()?;
        }

        Ok(())
    }

    fn unique_field_name_for(&self, key: &str) -> String {
        let base = sanitize_field_name(key);
        if self
            .state_field_defs
            .iter()
            .all(|def| def.field_name != base)
        {
            return base;
        }

        let mut suffix = 1;
        loop {
            let candidate = format!("{base}_{suffix}");
            if self
                .state_field_defs
                .iter()
                .all(|def| def.field_name != candidate)
            {
                return candidate;
            }
            suffix += 1;
        }
    }

    fn rebuild_index(&mut self) -> Result<()> {
        if self.config.index_path.exists() {
            fs::remove_dir_all(&self.config.index_path).with_context(|| {
                format!(
                    "failed to recreate index directory {}",
                    self.config.index_path.display()
                )
            })?;
        }
        fs::create_dir_all(&self.config.index_path)?;

        let (index, mut writer, fields) =
            create_index_components(&self.config, &self.state_field_defs)?;

        for (key, doc) in &self.aggregates {
            let term_value = key.term_value();
            writer
                .add_document(doc.to_document(&fields, &term_value))
                .with_context(|| {
                    format!(
                        "failed to reindex aggregate {}:{}",
                        key.aggregate_type, key.aggregate_id
                    )
                })?;
        }
        writer
            .commit()
            .with_context(|| "failed to commit rebuilt Tantivy index".to_string())?;

        self.index = index;
        self.writer = writer;
        self.fields = fields;
        self.pending_since_commit = 0;

        info!(
            "msg" = "search index rebuilt",
            "state_columns" = self.state_field_defs.len()
        );
        Ok(())
    }

    #[cfg(test)]
    pub(crate) fn state_field_names(&self) -> Vec<String> {
        self.state_field_defs
            .iter()
            .map(|def| def.field_name.clone())
            .collect()
    }
}

fn create_index_components(
    config: &Config,
    state_defs: &[StateFieldDef],
) -> Result<(Index, IndexWriter, IndexFields)> {
    let schema = build_schema(state_defs);
    let index = Index::create_in_dir(&config.index_path, schema.clone()).with_context(|| {
        format!(
            "failed to create Tantivy index at {}",
            config.index_path.display()
        )
    })?;
    let fields = index_fields_from_schema(&index.schema(), state_defs)?;

    let num_threads = num_cpus::get().max(1);
    let heap_per_thread = config.writer_heap_size_bytes.max(4 * 1024 * 1024);
    let total_heap = heap_per_thread
        .checked_mul(num_threads)
        .unwrap_or(heap_per_thread);
    let writer = index
        .writer_with_num_threads(num_threads, total_heap)
        .with_context(|| "failed to initialize Tantivy index writer".to_string())?;
    Ok((index, writer, fields))
}

fn build_schema(state_defs: &[StateFieldDef]) -> Schema {
    let mut builder = Schema::builder();

    builder.add_text_field(FIELD_AGGREGATE_KEY, STRING.clone().set_stored());
    builder.add_text_field(FIELD_AGGREGATE_TYPE, STRING.clone().set_stored());
    builder.add_text_field(FIELD_AGGREGATE_ID, STRING.clone().set_stored());

    let numeric = |stored: bool, fast: bool| -> NumericOptions {
        let mut options = NumericOptions::default();
        if stored {
            options = options.set_stored();
        }
        if fast {
            options = options.set_fast(Cardinality::SingleValue);
        }
        options
    };

    builder.add_u64_field(FIELD_STATE_VERSION, numeric(true, true));
    builder.add_u64_field(FIELD_EVENT_COUNT, numeric(true, true));
    builder.add_u64_field(FIELD_LAST_SEQUENCE, numeric(true, true));
    builder.add_u64_field(FIELD_LAST_EVENT_VERSION, numeric(true, true));
    builder.add_i64_field(FIELD_LAST_EVENT_EPOCH, numeric(true, true));
    builder.add_text_field(FIELD_LAST_EVENT_TYPE, STRING.clone().set_stored());
    builder.add_text_field(FIELD_LAST_EVENT_ID, STRING.clone().set_stored());
    builder.add_text_field(FIELD_STATE_JSON, TEXT.clone().set_stored());
    builder.add_u64_field(FIELD_STATE_ARCHIVED, numeric(true, true));
    builder.add_text_field(FIELD_STATE_MERKLE, STRING.clone().set_stored());

    for def in state_defs {
        match def.ty {
            StateFieldType::Text => {
                builder.add_text_field(&def.field_name, TEXT.clone().set_stored());
            }
            StateFieldType::Bool => {
                builder.add_bool_field(&def.field_name, numeric(true, true));
            }
            StateFieldType::I64 => {
                builder.add_i64_field(&def.field_name, numeric(true, true));
            }
            StateFieldType::F64 => {
                builder.add_f64_field(&def.field_name, numeric(true, true));
            }
        };
    }

    builder.build()
}

fn index_fields_from_schema(schema: &Schema, defs: &[StateFieldDef]) -> Result<IndexFields> {
    let mut state_fields = HashMap::new();
    for def in defs {
        let field = schema
            .get_field(&def.field_name)
            .ok_or_else(|| anyhow!("missing field {} in schema", def.field_name))?;
        state_fields.insert(def.key.clone(), StateFieldBinding { field, ty: def.ty });
    }

    Ok(IndexFields {
        aggregate_key: get_field(schema, FIELD_AGGREGATE_KEY)?,
        aggregate_type: get_field(schema, FIELD_AGGREGATE_TYPE)?,
        aggregate_id: get_field(schema, FIELD_AGGREGATE_ID)?,
        state_version: get_field(schema, FIELD_STATE_VERSION)?,
        event_count: get_field(schema, FIELD_EVENT_COUNT)?,
        last_sequence: get_field(schema, FIELD_LAST_SEQUENCE)?,
        last_event_type: get_field(schema, FIELD_LAST_EVENT_TYPE)?,
        last_event_version: get_field(schema, FIELD_LAST_EVENT_VERSION)?,
        last_event_id: get_field(schema, FIELD_LAST_EVENT_ID)?,
        last_event_epoch_micros: get_field(schema, FIELD_LAST_EVENT_EPOCH)?,
        state_json: get_field(schema, FIELD_STATE_JSON)?,
        state_archived: get_field(schema, FIELD_STATE_ARCHIVED)?,
        state_merkle_root: get_field(schema, FIELD_STATE_MERKLE)?,
        state_fields,
    })
}

fn get_field(schema: &Schema, name: &str) -> Result<Field> {
    schema
        .get_field(name)
        .ok_or_else(|| anyhow!("field {name} missing from schema"))
}

fn detect_state_type(value: &JsonValue) -> StateFieldType {
    match value {
        JsonValue::Bool(_) => StateFieldType::Bool,
        JsonValue::Number(number) => {
            if number.is_i64() {
                StateFieldType::I64
            } else {
                StateFieldType::F64
            }
        }
        _ => StateFieldType::Text,
    }
}

fn sanitize_field_name(key: &str) -> String {
    let mut result = String::from(STATE_FIELD_PREFIX);
    for ch in key.chars() {
        match ch {
            'a'..='z' | 'A'..='Z' | '0'..='9' => result.push(ch.to_ascii_lowercase()),
            _ => result.push('_'),
        }
    }
    if result == STATE_FIELD_PREFIX {
        result.push_str("field");
    }
    result
}

fn parse_state_entries(record: &EventRecord) -> BTreeMap<String, JsonValue> {
    record
        .state_entries
        .iter()
        .map(|entry| {
            let value = parse_state_value(&entry.value);
            (entry.key.clone(), value)
        })
        .collect()
}

fn parse_state_value(raw: &str) -> JsonValue {
    match serde_json::from_str(raw) {
        Ok(value) => value,
        Err(_) => JsonValue::String(raw.to_string()),
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
struct AggregateKey {
    aggregate_type: String,
    aggregate_id: String,
}

impl AggregateKey {
    fn new(aggregate_type: &str, aggregate_id: &str) -> Self {
        Self {
            aggregate_type: aggregate_type.to_string(),
            aggregate_id: aggregate_id.to_string(),
        }
    }

    fn term_value(&self) -> String {
        serde_json::to_string(&[self.aggregate_type.as_str(), self.aggregate_id.as_str()])
            .expect("aggregate key serialization should succeed")
    }
}

#[derive(Debug, Clone)]
struct AggregateDoc {
    aggregate_type: String,
    aggregate_id: String,
    state_version: u64,
    event_count: u64,
    last_sequence: u64,
    last_event_type: String,
    last_event_version: u64,
    last_event_id: String,
    last_event_at: DateTime<Utc>,
    state_entries: BTreeMap<String, JsonValue>,
    state_archived: bool,
    state_merkle_root: String,
}

impl AggregateDoc {
    fn from_event(record: &EventRecord, state_entries: &BTreeMap<String, JsonValue>) -> Self {
        let mut doc = Self {
            aggregate_type: record.aggregate_type.clone(),
            aggregate_id: record.aggregate_id.clone(),
            state_version: 0,
            event_count: 0,
            last_sequence: 0,
            last_event_type: String::new(),
            last_event_version: 0,
            last_event_id: String::new(),
            last_event_at: record.created_at,
            state_entries: BTreeMap::new(),
            state_archived: record.state_archived,
            state_merkle_root: record.state_merkle_root.clone(),
        };
        let _ = doc.apply_event(record, state_entries);
        doc
    }

    fn apply_event(
        &mut self,
        record: &EventRecord,
        state_entries: &BTreeMap<String, JsonValue>,
    ) -> bool {
        if record.sequence <= self.last_sequence {
            return false;
        }

        self.event_count = self.event_count.saturating_add(1);
        self.state_version = record.state_version;
        self.last_sequence = record.sequence;
        self.last_event_type = record.event_type.clone();
        self.last_event_version = record.event_version;
        self.last_event_id = record.event_id.clone();
        self.last_event_at = record.created_at;
        self.state_entries = state_entries.clone();
        self.state_archived = record.state_archived;
        self.state_merkle_root = record.state_merkle_root.clone();
        true
    }

    fn to_document(&self, fields: &IndexFields, key_value: &str) -> Document {
        let mut doc = Document::new();
        doc.add_text(fields.aggregate_key, key_value);
        doc.add_text(fields.aggregate_type, &self.aggregate_type);
        doc.add_text(fields.aggregate_id, &self.aggregate_id);
        doc.add_u64(fields.state_version, self.state_version);
        doc.add_u64(fields.event_count, self.event_count);
        doc.add_u64(fields.last_sequence, self.last_sequence);
        doc.add_text(fields.last_event_type, &self.last_event_type);
        doc.add_u64(fields.last_event_version, self.last_event_version);
        doc.add_text(fields.last_event_id, &self.last_event_id);
        doc.add_i64(
            fields.last_event_epoch_micros,
            self.last_event_at.timestamp_micros(),
        );
        doc.add_text(fields.state_json, &self.state_json_string());
        doc.add_u64(
            fields.state_archived,
            if self.state_archived { 1 } else { 0 },
        );
        doc.add_text(fields.state_merkle_root, &self.state_merkle_root);
        fields.add_state_values(&mut doc, &self.state_entries);
        doc
    }

    fn state_json_string(&self) -> String {
        self.state_json_value().to_string()
    }

    fn state_json_value(&self) -> JsonValue {
        let mut map = serde_json::Map::new();
        for (key, value) in &self.state_entries {
            map.insert(key.clone(), value.clone());
        }
        JsonValue::Object(map)
    }
}

impl IndexFields {
    fn add_state_values(&self, document: &mut Document, entries: &BTreeMap<String, JsonValue>) {
        for (key, value) in entries {
            let Some(binding) = self.state_fields.get(key) else {
                continue;
            };
            match binding.ty {
                StateFieldType::Text => {
                    document.add_text(binding.field, json_value_to_string(value));
                }
                StateFieldType::Bool => {
                    if let Some(b) = value.as_bool() {
                        document.add_bool(binding.field, b);
                    } else {
                        warn!(
                            "msg" = "state value type mismatch; expected bool",
                            "state_key" = key,
                            "value" = ?value
                        );
                    }
                }
                StateFieldType::I64 => {
                    if let Some(i) = value.as_i64() {
                        document.add_i64(binding.field, i);
                    } else {
                        warn!(
                            "msg" = "state value type mismatch; expected i64",
                            "state_key" = key,
                            "value" = ?value
                        );
                    }
                }
                StateFieldType::F64 => {
                    if let Some(f) = value.as_f64() {
                        document.add_f64(binding.field, f);
                    } else {
                        warn!(
                            "msg" = "state value type mismatch; expected f64",
                            "state_key" = key,
                            "value" = ?value
                        );
                    }
                }
            }
        }
    }
}

fn json_value_to_string(value: &JsonValue) -> String {
    match value {
        JsonValue::String(text) => text.clone(),
        JsonValue::Null => "null".to_string(),
        other => other.to_string(),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use anyhow::Result;
    use chrono::TimeZone;
    use dbx_plugin_api::{EventRecord, StateEntry};
    use serde_json::json;
    use tempfile::tempdir;

    fn sample_event(
        sequence: u64,
        version: u64,
        state_entries: &[(&str, JsonValue)],
    ) -> EventRecord {
        EventRecord {
            sequence,
            aggregate_type: "order".to_string(),
            aggregate_id: "order-123".to_string(),
            event_type: "OrderUpdated".to_string(),
            event_version: version,
            event_id: format!("evt-{sequence}"),
            created_at: Utc
                .timestamp_millis_opt(1_732_540_000_000 + sequence as i64)
                .unwrap(),
            payload_json: json!({"status": "confirmed", "notes": ["rush"]}),
            metadata_json: json!({"source": "search-test"}),
            hash: format!("hash-{sequence}"),
            merkle_root: format!("merkle-{sequence}"),
            state_version: version,
            state_archived: false,
            state_merkle_root: format!("state-merkle-{version}"),
            state_entries: state_entries
                .iter()
                .map(|(k, value)| StateEntry {
                    key: k.to_string(),
                    value: value.to_string(),
                })
                .collect(),
            schema_json: JsonValue::Null,
            extensions_json: json!({"tags": ["priority", "express"]}),
        }
    }

    #[test]
    fn aggregate_doc_tracks_latest_state() {
        let first_state = parse_state_entries(&sample_event(
            1,
            1,
            &[("status", json!("created")), ("total", json!(15))],
        ));
        let mut doc = AggregateDoc::from_event(
            &sample_event(1, 1, &[("status", json!("created")), ("total", json!(15))]),
            &first_state,
        );

        assert_eq!(
            doc.state_entries.get("status").and_then(JsonValue::as_str),
            Some("created")
        );
        assert_eq!(doc.event_count, 1);

        let next_record = sample_event(
            2,
            2,
            &[
                ("status", json!("confirmed")),
                ("notes", json!("customer called")),
                ("priority", json!(true)),
            ],
        );
        let next_state = parse_state_entries(&next_record);
        let updated = doc.apply_event(&next_record, &next_state);
        assert!(updated);
        assert_eq!(
            doc.state_entries.get("status").and_then(JsonValue::as_str),
            Some("confirmed")
        );
        assert_eq!(
            doc.state_entries.get("notes").and_then(JsonValue::as_str),
            Some("customer called")
        );
        assert_eq!(
            doc.state_entries
                .get("priority")
                .and_then(JsonValue::as_bool),
            Some(true)
        );
        assert_eq!(doc.event_count, 2);

        let stale = doc.apply_event(
            &sample_event(1, 1, &[("status", json!("archived"))]),
            &parse_state_entries(&sample_event(1, 1, &[("status", json!("archived"))])),
        );
        assert!(!stale, "stale event should be ignored");
        assert_eq!(
            doc.state_entries.get("status").and_then(JsonValue::as_str),
            Some("confirmed")
        );
    }

    #[test]
    fn indexer_tracks_aggregates_and_commits() -> Result<()> {
        let temp = tempdir().unwrap();
        let config = Config {
            index_path: temp.path().to_path_buf(),
            commit_after_events: 1,
            writer_heap_size_bytes: 8 * 1024 * 1024,
        };

        let mut indexer = TantivyIndexer::open(config)?;
        indexer.index_event(&sample_event(
            1,
            1,
            &[
                ("status", json!("created")),
                ("total", json!(15)),
                ("priority", json!(true)),
            ],
        ))?;
        indexer.commit()?;

        assert!(
            indexer
                .state_field_names()
                .iter()
                .any(|name| name.starts_with("state_status")),
            "expected dedicated state field for 'status'"
        );
        assert!(
            indexer
                .state_field_names()
                .iter()
                .any(|name| name.starts_with("state_total")),
            "expected dedicated state field for 'total'"
        );
        assert!(
            indexer
                .state_field_names()
                .iter()
                .any(|name| name.starts_with("state_priority")),
            "expected dedicated state field for 'priority'"
        );

        assert!(
            indexer.config.index_path.join("meta.json").exists(),
            "commit should create meta.json"
        );
        Ok(())
    }
}
