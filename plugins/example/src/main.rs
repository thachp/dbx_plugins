use anyhow::{anyhow, Context, Result};
use capnp::message::ReaderOptions;
use capnp::serialize;
use dbx_plugin_api::{event_record_to_proto, plugin_capnp, EventRecord, PluginContext};
use std::fs::{self, File, OpenOptions};
use std::io::Cursor;
use std::io::{self, BufReader, Read, Write};
use std::net::TcpStream;
use std::path::PathBuf;
use std::sync::{Mutex, OnceLock};

use dirs::home_dir;

const DEMO_FLAG: &str = "--demo";
const SOCKET_FLAG: &str = "--socket";
const LOG_FILE_NAME: &str = "dbx_example.log";

static LOGGER: OnceLock<Mutex<File>> = OnceLock::new();

fn init_logger() -> Result<()> {
    if LOGGER.get().is_some() {
        return Ok(());
    }

    let home = home_dir().context("failed to resolve home directory for logger")?;
    let dir: PathBuf = home.join(".eventdbx").join("logs");
    fs::create_dir_all(&dir)
        .with_context(|| format!("failed to create log directory {}", dir.display()))?;
    let path = dir.join(LOG_FILE_NAME);
    let file = OpenOptions::new()
        .create(true)
        .append(true)
        .open(&path)
        .with_context(|| format!("failed to open log file {}", path.display()))?;
    LOGGER
        .set(Mutex::new(file))
        .map_err(|_| anyhow!("logger already initialized"))?;
    Ok(())
}

fn log_line(line: &str) -> Result<()> {
    let mutex = LOGGER.get().expect("logger not initialized");
    let mut file = mutex.lock().expect("dbx_example log mutex poisoned");
    writeln!(file, "{}", line)
        .with_context(|| "failed to write to dbx_example log file".to_string())?;
    println!("{}", line);
    Ok(())
}

fn main() -> Result<()> {
    let mut args = std::env::args().skip(1);
    let mut mode = Mode::Stdin;

    while let Some(arg) = args.next() {
        match arg.as_str() {
            DEMO_FLAG => mode = Mode::Demo,
            SOCKET_FLAG => {
                let addr = args
                    .next()
                    .ok_or_else(|| anyhow!("--socket requires host:port argument"))?;
                mode = Mode::Socket(addr);
            }
            other => return Err(anyhow!("unrecognized argument: {}", other)),
        }
    }

    init_logger()?;

    match mode {
        Mode::Demo => {
            log_line("Running in demo mode; emitting a sample event.")?;
            let bytes = sample_envelope()?;
            listen_and_print(Cursor::new(bytes))
        }
        Mode::Socket(addr) => {
            log_line(&format!("Connecting to EventDBX socket at {}", addr))?;
            let stream = TcpStream::connect(&addr)
                .with_context(|| format!("failed to connect to {}", addr))?;
            listen_and_print(stream)
        }
        Mode::Stdin => {
            log_line("Reading Cap'n Proto envelopes from stdin…")?;
            listen_and_print(io::stdin())
        }
    }
}

enum Mode {
    Demo,
    Socket(String),
    Stdin,
}

fn listen_and_print<R: Read>(input: R) -> Result<()> {
    let mut reader = BufReader::new(input);

    loop {
        match serialize::read_message(&mut reader, ReaderOptions::new()) {
            Ok(message) => {
                let envelope: plugin_capnp::plugin_envelope::Reader<'_> = message
                    .get_root()
                    .context("failed to read PluginEnvelope root")?;
                handle_envelope(envelope)?;
            }
            Err(err) => {
                if err.kind == capnp::ErrorKind::Failed {
                    break;
                }
                return Err(err.into());
            }
        }
    }

    Ok(())
}

fn handle_envelope(envelope: plugin_capnp::plugin_envelope::Reader<'_>) -> Result<()> {
    match envelope
        .get_message()
        .which()
        .context("failed to resolve message union")?
    {
        plugin_capnp::plugin_envelope::message::Init(init) => {
            let init = init.context("missing init payload")?;
            let ctx = PluginContext::from_capnp(init)?;
            log_line(&format!(
                "[INIT] plugin={} version={} target={} transport={}",
                ctx.plugin_name,
                ctx.version,
                ctx.target,
                ctx.transport.as_str()
            ))?;
        }
        plugin_capnp::plugin_envelope::message::Event(event) => {
            let event = event.context("missing event payload")?;
            let record = EventRecord::try_from_capnp(event)?;
            print_event(&record)?;
        }
        plugin_capnp::plugin_envelope::message::Ack(ack) => {
            let ack = ack.context("missing ack payload")?;
            log_line(&format!("[ACK] sequence={}", ack.get_sequence()))?;
        }
        plugin_capnp::plugin_envelope::message::Error(err) => {
            let err = err.context("missing error payload")?;
            log_line(&format!(
                "[ERROR] sequence={} message={}",
                err.get_sequence(),
                err.get_message().unwrap_or("<unknown>")
            ))?;
        }
    }

    Ok(())
}

fn print_event(record: &EventRecord) -> Result<()> {
    let pretty = serde_json::to_string_pretty(record).context("failed to format event as JSON")?;
    log_line(&format!("[EVENT] sequence={}", record.sequence))?;
    for line in pretty.lines() {
        log_line(line)?;
    }

    // Demonstrate conversion to gRPC representation for parity with other transports.
    let proto = event_record_to_proto(record)?;
    log_line(&format!(
        "[EVENT→gRPC] aggregate_id={} event_id={} payload_kind={:?}",
        proto.aggregate_id,
        proto.event_id,
        proto
            .payload_json
            .as_ref()
            .and_then(|value| value.kind.clone())
    ))?;

    Ok(())
}

#[allow(dead_code)]
fn sample_envelope() -> Result<Vec<u8>> {
    use capnp::message::Builder;
    use capnp::serialize::write_message;
    use std::io::Cursor;

    let mut message = Builder::new_default();
    {
        let mut envelope = message.init_root::<plugin_capnp::plugin_envelope::Builder<'_>>();
        let mut event = envelope.reborrow().init_message().init_event();
        event.set_sequence(1);
        event.set_aggregate_type("order");
        event.set_aggregate_id("order-123");
        event.set_event_type("OrderCreated");
        event.set_event_version(1);
        event.set_event_id("evt-1");
        event.set_created_at_epoch_micros(1_723_836_800_000_000);
        event.set_payload_json(r#"{"status":"created"}"#);
        event.set_metadata_json(r#"{"source":"example"}"#);
        event.set_hash("hash");
        event.set_merkle_root("merkle");
        event.set_state_version(1);
        event.set_state_archived(false);
        event.set_state_merkle_root("state-merkle");
        {
            let mut entries = event.reborrow().init_state_entries(1);
            let mut entry = entries.reborrow().get(0);
            entry.set_key("status");
            entry.set_value("created");
        }
        event.set_schema_json(r#"{"name":"OrderCreated"}"#);
        event.set_extensions_json("null");
    }

    let mut buffer = Cursor::new(Vec::new());
    write_message(&mut buffer, &message).context("failed to serialize sample message")?;
    Ok(buffer.into_inner())
}
