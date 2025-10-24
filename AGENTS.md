# EventDBX Agents

This workspace uses the term **agent** for every plugin crate that consumes
EventDBX envelopes and fans them into an external system. Each agent is a
stand‑alone Cargo package that shares transport and data model helpers via the
`common/plugin_api` crate.

## Agent Index

| Agent        | Crate Path          | Status    | Role |
|--------------|---------------------|-----------|------|
| Queue        | `plugins/queue`     | Scaffold  | Template for streaming events into a message queue (Kafka, NATS, etc.). |
| Postgres     | `plugins/postgres`  | Scaffold  | Captures EventDBX records and writes them into Postgres tables. |
| REST         | `plugins/rest`      | Scaffold  | Exposes selected events over a RESTful HTTP surface. |
| gRPC         | `plugins/grpc`      | Scaffold  | Serves EventDBX records via gRPC endpoints. |
| GraphQL      | `plugins/graphql`   | Scaffold  | Projects the event store into a GraphQL schema for queries/subscriptions. |
| Search       | `plugins/search`    | Scaffold  | Demonstrates indexing events into a search backend (OpenSearch, Meilisearch, etc.). |
| Example      | `plugins/example`   | Working   | CLI demo that prints decoded events; useful for validating transport wiring. |

**Scaffold** agents only include a `fn main()` stub today. Flesh them out when
you begin integrating with the corresponding downstream system.

## Shared Infrastructure

- `common/plugin_api` exposes:
  - Cap'n Proto bindings for socket transport (`schemas/*.capnp`).
  - Protobuf types and helpers for gRPC (`protos/*.proto`).
  - High-level Rust types such as `EventRecord`, `StateEntry`, and `PluginContext`.
- Build scripts expect both `capnpc` and `protoc` to be available. If you hit a
  build error about `protoc` not being found, install it (e.g. `brew install protobuf`)
  or point the `PROTOC` env var to your binary.
- Every agent should depend on `dbx_plugin_api` via the path dependency already
  wired up in the workspace `Cargo.toml`.

## Common Dev Flow

1. Add real logic inside the agent's `src/` directory. For most agents you'll
   replace the generated `main.rs` with a library crate plus a thin binary.
2. Implement a `run()` function that accepts a `PluginContext` and streams
   `EventRecord` instances from the socket or gRPC source.
3. Add integration tests or an `examples/` binary that exercises the agent end-to-end.
4. Update this document with the agent's status once it moves beyond scaffolding.

## Creating a New Agent

1. Scaffold the crate: `cargo new --lib plugins/<name>`.
2. Add the new crate to the workspace if the glob does not pick it up later.
3. Declare `dbx_plugin_api = { path = "../../common/plugin_api" }` in `Cargo.toml`.
4. Document the agent in this file so others know what it does and how mature it is.
5. Add any additional dependencies or tooling notes to the agent's own README once
   it exists.

Keeping this checklist current makes it easier for the EventDBX team to track
which integrations are production-ready and which are still being prototyped.

