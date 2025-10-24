# EventDBX GraphQL Agent

GraphQL server built with `async-graphql` and Axum that projects EventDBX aggregates
and events into a schema suitable for queries and subscriptions. It consumes the
shared `dbx_plugin_api` client to talk to the EventDBX control socket.
