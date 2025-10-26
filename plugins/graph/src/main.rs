mod config;
mod model;
mod net;
mod proto_capnp;
mod storage;

use std::{io::BufReader, sync::Arc};

use anyhow::{Context, Result};
use capnp::{message::ReaderOptions, serialize};
use dbx_plugin_api::{plugin_capnp, EventRecord, PluginContext};
use tokio::{signal, sync::mpsc};
use tracing::{error, info, warn};

use crate::{
    config::Config,
    model::{EdgeOp, GraphEdge},
    net::server,
    storage::{GraphStorage, RocksGraph},
};

const GRAPH_CHANNEL_DEPTH: usize = 1024;

#[derive(Debug)]
enum GraphCommand {
    Edge(GraphEdge),
}

#[tokio::main]
async fn main() -> Result<()> {
    let config = Config::load()?;
    dbx_plugin_logging::init_default("graphdb", "info,dbx_graphdb=info")?;

    std::fs::create_dir_all(&config.rocksdb_path)?;

    let storage: Arc<dyn GraphStorage> = RocksGraph::open(&config.rocksdb_path)?;
    info!(
        "listen" = %config.listen_addr,
        "rocksdb" = %config.rocksdb_path.display(),
        "msg" = "graph plugin starting"
    );

    let (cmd_tx, cmd_rx) = mpsc::channel::<GraphCommand>(GRAPH_CHANNEL_DEPTH);

    let ingest_handle = tokio::task::spawn_blocking({
        let tx = cmd_tx.clone();
        move || run_capnp_ingest(tx)
    });

    let processor_handle = tokio::spawn(process_commands(cmd_rx, storage.clone()));

    let server_fut = server::run_server(&config, storage.clone());
    let shutdown = async {
        signal::ctrl_c()
            .await
            .expect("failed to install Ctrl+C handler");
        info!("msg" = "received termination signal");
    };

    tokio::select! {
        result = server_fut => result?,
        result = processor_handle => result??,
        result = ingest_handle => result??,
        _ = shutdown => {}
    }

    storage.flush().await?;
    info!("msg" = "graph plugin exiting");
    Ok(())
}

async fn process_commands(
    mut rx: mpsc::Receiver<GraphCommand>,
    storage: Arc<dyn GraphStorage>,
) -> Result<()> {
    while let Some(command) = rx.recv().await {
        match command {
            GraphCommand::Edge(edge) => match edge.op {
                EdgeOp::Upsert => {
                    if let Err(err) = storage.upsert_edge(edge).await {
                        error!(error = ?err, "failed to upsert edge");
                    }
                }
                EdgeOp::Delete => {
                    let fallback = edge.key_components();
                    let id = edge.id.as_deref().unwrap_or("");
                    if let Err(err) = storage.delete_edge(id, fallback).await {
                        error!(error = ?err, "failed to delete edge");
                    }
                }
            },
        }
    }
    Ok(())
}

fn run_capnp_ingest(tx: mpsc::Sender<GraphCommand>) -> Result<()> {
    let stdin = std::io::stdin();
    let mut reader = BufReader::new(stdin.lock());
    let options = ReaderOptions::new();

    loop {
        match serialize::read_message(&mut reader, options) {
            Ok(message) => {
                let envelope: plugin_capnp::plugin_envelope::Reader<'_> = message
                    .get_root()
                    .context("failed to read PluginEnvelope root")?;
                if let Err(err) = handle_envelope(envelope, &tx) {
                    warn!(error = ?err, "failed to process envelope");
                }
            }
            Err(err) => {
                if err.kind == capnp::ErrorKind::Failed {
                    info!("msg" = "graph ingest reached EOF");
                    break;
                } else {
                    return Err(err.into());
                }
            }
        }
    }

    Ok(())
}

fn handle_envelope(
    envelope: plugin_capnp::plugin_envelope::Reader<'_>,
    tx: &mpsc::Sender<GraphCommand>,
) -> Result<()> {
    match envelope.get_message().which()? {
        plugin_capnp::plugin_envelope::message::Init(init) => {
            let init = init.context("missing init payload")?;
            let ctx = PluginContext::from_capnp(init)?;
            info!(
                "plugin_name" = ctx.plugin_name,
                "version" = ctx.version,
                "target" = ctx.target,
                "transport" = ctx.transport.as_str(),
                "msg" = "received init"
            );
        }
        plugin_capnp::plugin_envelope::message::Event(event) => {
            let event = event.context("missing event payload")?;
            let record = EventRecord::try_from_capnp(event)?;
            let edges = extract_edges(&record);
            for edge in edges {
                if tx.blocking_send(GraphCommand::Edge(edge)).is_err() {
                    warn!("msg" = "graph command channel closed; stopping ingest");
                    return Ok(());
                }
            }
        }
        plugin_capnp::plugin_envelope::message::Ack(_) => {}
        plugin_capnp::plugin_envelope::message::Error(err) => {
            let err = err.context("missing error payload")?;
            warn!(
                "sequence" = err.get_sequence(),
                "message" = err.get_message().unwrap_or("<unknown>"),
                "msg" = "controller reported error"
            );
        }
    }
    Ok(())
}

fn extract_edges(record: &EventRecord) -> Vec<GraphEdge> {
    let mut edges = Vec::new();
    let graph = record
        .extensions_json
        .get("@graph")
        .and_then(|value| value.as_object());
    let Some(graph_obj) = graph else {
        return edges;
    };

    let raw_edges = graph_obj
        .get("edges")
        .and_then(|value| value.as_array())
        .cloned()
        .unwrap_or_default();

    for raw_edge in raw_edges {
        if let Some(edge) = parse_edge_value(raw_edge) {
            edges.push(edge);
        }
    }
    edges
}

fn parse_edge_value(value: serde_json::Value) -> Option<GraphEdge> {
    let obj = value.as_object()?;
    let from = obj.get("from").and_then(|v| v.as_str())?.trim();
    let to = obj.get("to").and_then(|v| v.as_str())?.trim();
    let relation = obj
        .get("type")
        .or_else(|| obj.get("relation"))
        .and_then(|v| v.as_str())?
        .trim();

    if from.is_empty() || to.is_empty() || relation.is_empty() {
        warn!(?obj, "ignoring graph edge with missing fields");
        return None;
    }

    let id = obj
        .get("id")
        .and_then(|v| v.as_str())
        .map(|s| s.trim().to_string())
        .filter(|s| !s.is_empty());

    let tags = obj
        .get("tags")
        .and_then(|v| v.as_array())
        .map(|array| {
            array
                .iter()
                .filter_map(|item| item.as_str().map(|s| s.trim().to_string()))
                .filter(|s| !s.is_empty())
                .collect::<Vec<_>>()
        })
        .unwrap_or_default();

    let weight = obj.get("weight").and_then(|v| v.as_f64());
    let confidence = obj.get("confidence").and_then(|v| v.as_f64());
    let properties = obj
        .get("properties")
        .cloned()
        .unwrap_or(serde_json::Value::Null);

    let op = obj
        .get("op")
        .and_then(|v| v.as_str())
        .map(|s| match s.to_ascii_lowercase().as_str() {
            "delete" | "remove" => EdgeOp::Delete,
            _ => EdgeOp::Upsert,
        })
        .unwrap_or(EdgeOp::Upsert);

    Some(GraphEdge {
        id,
        from: from.to_string(),
        to: to.to_string(),
        relation: relation.to_string(),
        tags,
        weight,
        confidence,
        properties,
        op,
    })
}
