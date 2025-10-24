# GraphQL Plugin Plan

## Goals
- Rehost the current async-graphql schema from `eventdbx` inside the `dbx_plugins` workspace as a standalone binary plugin.
- Use the new `ControlClient` to call core operations (list aggregates, fetch aggregates/events, append events, verify merkle roots) instead of shelling out through the CLI.
- Preserve authorization semantics by forwarding the caller's `Authorization` header (Bearer token) to the append operation.

## Architecture
1. **Crate layout**
   - Create `plugins/graphql_api` (binary target).
   - Dependencies: `async-graphql`, `async-graphql-axum`, `axum`, `tokio`, `tower-http`, `clap`, `serde`, `serde_json`, `dbx_plugin_api`.
   - CLI flags (via Clap):
     - `--bind <ADDR>`: HTTP bind address (default `0.0.0.0:7071`).
     - `--control <ADDR>`: Control socket address (default `127.0.0.1:6363`).
     - `--page-size <usize>` / `--page-limit <usize>`: client-side defaults mirroring server config.

2. **Control client usage**
   - Instantiate on demand (`ControlClient::connect(&addr).await?`). Each resolver/mutation opens a short-lived connection; this keeps the client `Send`/`Sync` story simple.
   - Map GraphQL resolvers to control calls:

     | GraphQL field | Control call |
     | ------------- | ------------- |
     | `Query.aggregates` | `list_aggregates(skip, take)` |
     | `Query.aggregate` | `get_aggregate(ty, id)` |
     | `Query.aggregateEvents` | `list_events(ty, id, skip, take)` |
     | `Query.verifyAggregate` | `verify_aggregate(ty, id)` |
     | `Mutation.appendEvent` | `append_event(AppendEventRequest)` |

   - The returned JSON payloads already exclude hidden fields and enforce restrict-mode semantics; the plugin only needs to deserialize into the helper structs (`AggregateStateView`, `StoredEventRecord`).

3. **GraphQL schema**
   - Recreate the existing types (`Aggregate`, `Event`, `EventMetadata`) by wrapping the control models (e.g., `AggregateStateView`) or by mapping the JSON payloads into structs that mirror the old schema.
   - Inject `HeaderMap` into the GraphQL context (via Axum) so the mutation can extract the Bearer token.
   - Preserve pagination defaults by pulling the page size/limit from plugin CLI options.

4. **Server bootstrap**
   - Build an Axum router identical to the in-repo version: `/graphql` (POST) + GraphQL Playground.
   - Add `TraceLayer` for logging and reuse the `tower_http` tracing setup.
   - Shutdown handling: respond to Ctrl+C similar to EventDBX for consistency.

5. **Testing & validation**
   - Add a smoke test (Tokio) that spins up the plugin with a mock control socket (or uses the real daemon behind a feature flag) and exercises the `health` query.
   - Update workspace CI by adding `cargo check -p graphql_api` and `cargo fmt -- --check`.

## Migration steps
1. Scaffold the crate with the starter `main.rs` wiring Clap, logging, and the Axum server skeleton.
2. Port the GraphQL schema code, swapping direct `AppState`/CLI interactions for `ControlClient` calls.
3. Add integration with the plugin manager (document how to launch alongside the daemon, or provide a stub entry in `README.md`).
4. Mark the old in-repo GraphQL module behind a `legacy-graphql` Cargo feature once the plugin reaches feature parity.
