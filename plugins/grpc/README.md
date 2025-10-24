# EventDBX gRPC Agent

Tonic-based gRPC surface for EventDBX that implements the published protobuf services.
The agent bridges requests to the control socket via `dbx_plugin_api`, enabling remote
systems to fetch and stream EventDBX data over standard gRPC.
