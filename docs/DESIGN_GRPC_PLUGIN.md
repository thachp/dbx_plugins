# gRPC Plugin Plan

## Goals
- Move the tonic-based `EventService` out of `eventdbx` and into the plugin workspace.
- Serve the same protobuf contract (`proto/api.proto`) while sourcing all data from the control socket.
- Maintain streaming semantics and error mapping identical to the in-repo implementation.

## Architecture
1. **Crate layout**
   - Create `plugins/grpc_api` (binary target).
   - Dependencies: `tonic`, `prost`, `tokio`, `tower`, `tracing`, `clap`, `dbx_plugin_api`.
   - Build script reuses `common/plugin_api`’s generated protobufs via the shared crate; the plugin can depend directly on the generated module (`dbx_plugin_api::grpc::dbx::plugins::event_record`) or include `proto/api.proto` locally to generate the gRPC service stub.
   - CLI flags: `--bind`, `--control`, `--page-size`, `--page-limit`, optional `--tls` parameters for future transport security.

2. **Service implementation**
   - Implement tonic `EventService` trait:
     | RPC | Control call |
     |-----|--------------|
     | `Health` | (static) |
     | `ListAggregates` | `list_aggregates` |
     | `GetAggregate` | `get_aggregate` |
     | `ListEvents` | `list_events` |
     | `AppendEvent` | `append_event` |
     | `VerifyAggregate` | `verify_aggregate` |
   - Map control errors to `Status` codes using the same logic as the current `GrpcApi::map_error`.
   - Convert between `StoredEventRecord`/`AggregateStateView` and the protobuf messages.

3. **Authorization & validation**
   - Extract bearer token from gRPC metadata, forward it in the `AppendEventRequest`, and enforce payload/patch exclusivity before invoking `ControlClient`.
   - Use `ControlClient` responses to preserve restrict-mode validation and snapshotting (handled by the daemon).

4. **Server bootstrap**
   - Clap CLI configures a tonic `Server::builder()` with optional TCP keepalive or concurrency settings.
   - Option to share HTTP and gRPC ports is no longer needed; plugin will run its own listener.

5. **Testing**
   - Add unit tests for error mapping and request translation.
   - Provide an integration test that launches the tonic server on an ephemeral port and uses a gRPC client to call `Health` and `ListAggregates` against a mock control backend.

## Migration steps
1. Scaffold `plugins/grpc_api` with Clap + tonic boilerplate (service builder + shutdown signal).
2. Port RPC handlers to use `ControlClient`.
3. Document deployment and add to workspace README.
4. Gate the in-repo gRPC server behind a `legacy-grpc` feature after validation.
