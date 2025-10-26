use std::{net::SocketAddr, sync::Arc};

use anyhow::{bail, Context, Result};
use capnp::message::{Builder, HeapAllocator};
use capnp::serialize;
use capnp_futures::serialize::try_read_message;
use futures::AsyncWriteExt;
use tokio::io::{BufReader, BufWriter};
use tokio::net::{TcpListener, TcpStream};
use tokio_util::compat::{TokioAsyncReadCompatExt, TokioAsyncWriteCompatExt};
use tracing::{debug, info, warn};

use crate::config::Config;
use crate::model::{EdgeOp, EdgeQuery, GraphEdge, QueryDirection};
use crate::proto_capnp::{edge, edge_query, graph_request, graph_response};
use crate::storage::GraphStorage;

pub async fn run_server(config: &Config, storage: Arc<dyn GraphStorage>) -> Result<()> {
    let listener = TcpListener::bind(config.listen_addr)
        .await
        .with_context(|| format!("failed to bind graph server on {}", config.listen_addr))?;
    info!("listen" = %config.listen_addr, "msg" = "graph server ready");

    loop {
        let (stream, addr) = listener.accept().await?;
        let storage = storage.clone();
        tokio::spawn(async move {
            if let Err(err) = handle_client(stream, addr, storage).await {
                warn!(%addr, error = ?err, "graph client disconnected with error");
            }
        });
    }
}

async fn handle_client(
    stream: TcpStream,
    addr: SocketAddr,
    storage: Arc<dyn GraphStorage>,
) -> Result<()> {
    debug!(%addr, "graph client connected");

    let (read_half, write_half) = stream.into_split();
    let mut reader = BufReader::new(read_half).compat();
    let mut writer = BufWriter::new(write_half).compat_write();
    let options = capnp::message::ReaderOptions::new();

    while let Some(message) = try_read_message(&mut reader, options).await? {
        let request = message.get_root::<graph_request::Reader>()?;
        let request_payload = decode_request(request)?;
        let response_payload = handle_payload(request_payload, storage.clone()).await?;
        let response_message = build_response(response_payload)?;
        let bytes = serialize::write_message_to_words(&response_message);
        drop(response_message);
        writer.write_all(&bytes).await?;
        writer.flush().await?;
    }

    debug!(%addr, "graph client disconnected");
    Ok(())
}

#[derive(Debug)]
enum ResponsePayload {
    Ack,
    Edges(Vec<GraphEdge>),
}

#[derive(Debug)]
enum RequestPayload {
    Mutations(Vec<GraphEdge>),
    DeleteIds(Vec<String>),
    Query(EdgeQuery),
}

fn decode_request(reader: graph_request::Reader<'_>) -> Result<RequestPayload> {
    match reader.which()? {
        graph_request::Which::UpsertEdges(list) => {
            let mut edges = Vec::new();
            for edge_reader in list?.iter() {
                edges.push(decode_edge(edge_reader)?);
            }
            Ok(RequestPayload::Mutations(edges))
        }
        graph_request::Which::DeleteEdges(ids) => {
            let mut to_delete = Vec::new();
            for edge_id in ids?.iter() {
                let id = edge_id?;
                if !id.is_empty() {
                    to_delete.push(id.to_string());
                }
            }
            Ok(RequestPayload::DeleteIds(to_delete))
        }
        graph_request::Which::QueryEdges(query_reader) => {
            let query = decode_query(query_reader?)?;
            Ok(RequestPayload::Query(query))
        }
    }
}

async fn handle_payload(
    payload: RequestPayload,
    storage: Arc<dyn GraphStorage>,
) -> Result<ResponsePayload> {
    match payload {
        RequestPayload::Mutations(edges) => {
            let mut created = Vec::new();
            for edge in edges {
                match edge.op {
                    EdgeOp::Upsert => {
                        let mut stored = edge.clone();
                        let id = storage.upsert_edge(edge).await?;
                        stored.id = Some(id);
                        created.push(stored);
                    }
                    EdgeOp::Delete => {
                        let id_owned = edge.id.clone().unwrap_or_default();
                        let fallback =
                            (edge.from.as_str(), edge.relation.as_str(), edge.to.as_str());
                        storage.delete_edge(id_owned.as_str(), fallback).await?;
                    }
                }
            }
            Ok(ResponsePayload::Edges(created))
        }
        RequestPayload::DeleteIds(ids) => {
            for id in ids {
                storage.delete_edge(id.as_str(), ("", "", "")).await?;
            }
            Ok(ResponsePayload::Ack)
        }
        RequestPayload::Query(query) => {
            let edges = storage.query_edges(query).await?;
            Ok(ResponsePayload::Edges(edges))
        }
    }
}

fn build_response(payload: ResponsePayload) -> Result<Builder<HeapAllocator>> {
    let mut message = Builder::new_default();
    {
        let mut response = message.init_root::<graph_response::Builder>();
        match payload {
            ResponsePayload::Ack => response.set_ack(()),
            ResponsePayload::Edges(edges) => {
                let mut edges_builder = response.reborrow().init_edges(edges.len() as u32);
                for (idx, edge) in edges.into_iter().enumerate() {
                    encode_edge(edges_builder.reborrow().get(idx as u32), &edge)?;
                }
            }
        }
    }
    Ok(message)
}

fn decode_edge(reader: edge::Reader<'_>) -> Result<GraphEdge> {
    let id = reader.get_id()?.trim();
    let from = reader.get_from()?.trim();
    let to = reader.get_to()?.trim();
    let relation = reader.get_relation()?.trim();

    if from.is_empty() || to.is_empty() || relation.is_empty() {
        bail!("edge must contain non-empty from, to, and relation");
    }

    let tags = reader
        .get_tags()?
        .iter()
        .filter_map(|tag| tag.ok().map(|t| t.trim().to_string()))
        .filter(|tag| !tag.is_empty())
        .collect::<Vec<_>>();

    let weight = reader.get_weight();
    let weight = if weight.is_nan() { None } else { Some(weight) };
    let confidence = reader.get_confidence();
    let confidence = if confidence.is_nan() {
        None
    } else {
        Some(confidence)
    };

    let properties = reader.get_props_json()?;
    let properties = if properties.trim().is_empty() {
        serde_json::Value::Null
    } else {
        serde_json::from_str(properties).context("failed to parse edge properties JSON")?
    };

    let op = match reader.get_op() {
        1 => EdgeOp::Delete,
        _ => EdgeOp::Upsert,
    };

    Ok(GraphEdge {
        id: if id.is_empty() {
            None
        } else {
            Some(id.to_string())
        },
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

fn decode_query(reader: edge_query::Reader<'_>) -> Result<EdgeQuery> {
    let node = reader.get_node()?.trim();
    if node.is_empty() {
        bail!("query must specify node id");
    }

    let direction = match reader.get_direction() {
        1 => QueryDirection::Inbound,
        2 => QueryDirection::Both,
        _ => QueryDirection::Outbound,
    };

    let relation = reader.get_relation()?.trim();
    let relation = if relation.is_empty() {
        None
    } else {
        Some(relation.to_string())
    };

    let max_depth = reader.get_max_depth();
    let max_depth = if max_depth == 0 { 1 } else { max_depth };
    let limit = reader.get_limit();
    let limit = if limit == 0 { None } else { Some(limit) };

    Ok(EdgeQuery {
        node: node.to_string(),
        direction,
        relation,
        max_depth,
        limit,
    })
}

fn encode_edge(mut builder: edge::Builder<'_>, edge: &GraphEdge) -> Result<()> {
    if let Some(id) = &edge.id {
        builder.set_id(id);
    } else {
        builder.set_id("");
    }
    builder.set_from(&edge.from);
    builder.set_to(&edge.to);
    builder.set_relation(&edge.relation);

    {
        let mut tags_builder = builder.reborrow().init_tags(edge.tags.len() as u32);
        for (i, tag) in edge.tags.iter().enumerate() {
            tags_builder.set(i as u32, tag);
        }
    }

    builder.set_weight(edge.weight.unwrap_or(f64::NAN));
    builder.set_confidence(edge.confidence.unwrap_or(f64::NAN));
    builder.set_props_json(&edge.properties.to_string());
    builder.set_op(match edge.op {
        EdgeOp::Upsert => 0,
        EdgeOp::Delete => 1,
    });

    Ok(())
}
