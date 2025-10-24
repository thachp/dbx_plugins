# REST Plugin Plan

## Goals
- Reproduce the `eventdbx` REST endpoints (`/health`, `/v1/aggregates…`, `/v1/events`, `verify`) as a standalone plugin.
- Replace the in-process `AppState`/CLI calls with the control-socket client so the plugin works against a running daemon.
- Preserve request semantics (query params, JSON bodies, bearer auth) to maintain API compatibility.

## Architecture
1. **Crate layout**
   - Create `plugins/rest_api` (binary target) alongside the GraphQL plugin.
   - Dependencies: `axum`, `serde`, `serde_json`, `tokio`, `tower-http`, `tracing`, `clap`, `dbx_plugin_api`.
   - CLI flags: `--bind`, `--control`, `--page-size`, `--page-limit` (matching GraphQL plugin) plus optional `--read-timeout`/`--write-timeout` if future throttling is needed.

2. **Routing**
   - Mirror current routes:
     - `GET /health` → static “ok”.
     - `GET /v1/aggregates` → `ControlClient::list_aggregates`.
     - `GET /v1/aggregates/{type}/{id}` → `get_aggregate`.
     - `GET /v1/aggregates/{type}/{id}/events` → `list_events`.
     - `POST /v1/events` → global append.
     - `POST /v1/aggregates/{type}/{id}/events` (optional alias) → same append.
     - `GET /v1/aggregates/{type}/{id}/verify` → `verify_aggregate`.
   - Convert control-model responses into the legacy JSON shapes (`AggregateState`, `EventRecord`, `VerifyResponse`) to avoid breaking clients.

3. **Append flow**
   - Extract bearer token from `Authorization` header, ensure payload vs patch exclusivity, and pass through `ControlClient::append_event`.
   - The plugin should mini-batch the control request and forward the resulting `StoredEventRecord` as JSON.

4. **Shared utilities**
   - Reuse helpers from GraphQL plugin (pagination clamp, bearer extraction). Consider moving these into `common/plugin_api` if they become shared.

5. **Testing**
   - Unit-test request handlers with a mocked `ControlClient` trait (or by injecting closures).
   - Add integration smoke test that exercises `/health` and `/v1/aggregates` against a stub control server.

## Migration steps
1. Scaffold `plugins/rest_api` with Clap + Axum skeleton.
2. Port REST handlers, replacing `run_cli_json` calls with `ControlClient`.
3. Document plugin usage (example command, systemd/script snippet) and add to workspace README.
4. Gate the in-repo REST handlers with a `legacy-rest` feature once the plugin is validated.
