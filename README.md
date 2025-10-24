# DBX Plugins Workspace

This repository collects EventDBX plugins in a single Cargo workspace so each plugin can evolve independently while sharing common tooling.

## Layout

- `Cargo.toml`: workspace definition that lists every plugin crate.
- `common/plugin_api`: shared transport bindings (Cap'n Proto envelopes and socket API helpers, plus optional gRPC event bindings) and trait contracts used by all plugins.
- `plugins/<name>/`: individual plugin crates (for example `queue`, `postgres`, `rest`, `grpc`, `graphql`, `search`).
- `plugins/example`: sample binary that consumes Cap'n Proto envelopes and prints the derived `EventRecord`.

## Control socket client

`common/plugin_api` now ships a lightweight `ControlClient` for talking to the daemon’s socket on port `6363`. The client hides the Cap'n Proto framing and returns typed responses so HTTP/gRPC surface plugins can live entirely outside the core repository.

```rust
use dbx_plugin_api::{AppendEventRequest, ControlClient};

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    let mut client = ControlClient::connect("127.0.0.1:6363").await?;
    let aggregates = client.list_aggregates(0, Some(25)).await?;

    let request = AppendEventRequest {
        token: std::env::var("EVENTDBX_TOKEN")?,
        aggregate_type: "order".into(),
        aggregate_id: "order-123".into(),
        event_type: "OrderCreated".into(),
        payload: Some(serde_json::json!({ "status": "pending" })),
        ..Default::default()
    };

    let record = client.append_event(request).await?;
    println!("appended event {}", record.metadata.event_id);
    Ok(())
}
```

## Plugin Targets

- `plugins/rest_api` – Axum REST surface that proxies to the daemon through `ControlClient`.
- `plugins/graphql_api` – async-graphql server backed by the control socket.
- `plugins/grpc_api` – tonic gRPC server implementing the EventDBX `EventService`.

Each plugin exposes CLI flags for the bind address, control endpoint, and paging defaults. Run `cargo run -p <plugin> -- --help` for usage details.

## Getting Started

1. Scaffold plugin crates:
   ```sh
   cargo new --lib plugins/queue
   cargo new --lib plugins/postgres
   cargo new --lib plugins/rest
   cargo new --lib plugins/grpc
   cargo new --lib plugins/graphql
   cargo new --lib plugins/search
   ```
2. Wiring is already in place for `common/plugin_api`; point new plugins at that crate with `dbx_plugin_api = { path = "../../common/plugin_api" }`.
3. Drop any new plugin under `plugins/` and Cargo will auto-discover it via the workspace glob.

## Development Notes

- Decide how EventDBX loads plugins (static libs, `cdylib`, or dynamic discovery) and set each crate type accordingly.
- Keep per-plugin documentation inside `plugins/<name>/README.md` to capture configuration and integration details.
- Use integration tests or example binaries to verify plugins against EventDBX APIs before deployment.

## Example

The `plugins/example` binary demonstrates consuming Cap'n Proto messages from standard input and printing the unified `EventRecord`:

```sh
cargo run -p example -- --demo
```

Running with `--demo` emits a canned envelope so you can see the structured output without wiring EventDBX yet. Omit the flag to have the binary block on stdin and process live envelopes.

To stream real traffic from an EventDBX socket (replace the address with your endpoint):

```sh
cargo run -p example -- --socket 127.0.0.1:9000
```
