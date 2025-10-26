mod config;
mod indexer;

use std::io::{self, BufReader};

use anyhow::{Context, Result};
use capnp::{message::ReaderOptions, serialize};
use config::Config;
use dbx_plugin_api::{plugin_capnp, EventRecord, PluginContext};
use indexer::TantivyIndexer;
use tracing::{debug, error, info, warn};

fn main() -> Result<()> {
    let config = Config::load()?;
    dbx_plugin_logging::init_default("search", "info,dbx_search=info")?;

    let mut indexer = TantivyIndexer::open(config)?;
    run_capnp_ingest(&mut indexer)?;
    indexer.commit()?;
    info!("msg" = "search plugin exiting");

    Ok(())
}

fn run_capnp_ingest(indexer: &mut TantivyIndexer) -> Result<()> {
    let stdin = io::stdin();
    let mut reader = BufReader::new(stdin.lock());
    let options = ReaderOptions::new();

    loop {
        match serialize::read_message(&mut reader, options) {
            Ok(message) => {
                let envelope: plugin_capnp::plugin_envelope::Reader<'_> = message
                    .get_root()
                    .context("failed to read PluginEnvelope root")?;
                if let Err(err) = handle_envelope(envelope, indexer) {
                    warn!(error = ?err, "failed to process plugin envelope");
                }
            }
            Err(err) => {
                if err.kind == capnp::ErrorKind::Failed {
                    info!("msg" = "search ingest reached EOF");
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
    indexer: &mut TantivyIndexer,
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
                "msg" = "search plugin initialized"
            );
        }
        plugin_capnp::plugin_envelope::message::Event(event) => {
            let event = event.context("missing event payload")?;
            let record = EventRecord::try_from_capnp(event)?;
            debug!(
                "aggregate_type" = %record.aggregate_type,
                "aggregate_id" = %record.aggregate_id,
                "sequence" = record.sequence,
                "msg" = "indexing aggregate update"
            );
            indexer
                .index_event(&record)
                .with_context(|| "failed to index aggregate event".to_string())?;
        }
        plugin_capnp::plugin_envelope::message::Ack(_) => {}
        plugin_capnp::plugin_envelope::message::Error(err) => {
            let err = err.context("missing error payload")?;
            error!(
                "sequence" = err.get_sequence(),
                "message" = err.get_message().unwrap_or("<unknown>"),
                "msg" = "controller reported error"
            );
        }
    }

    Ok(())
}
