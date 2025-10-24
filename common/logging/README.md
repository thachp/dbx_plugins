# dbx_plugin_logging

Shared tracing configuration for EventDBX plugins. The crate centralizes log setup,
including rotating file appenders and environment-driven filters, so each agent can
adopt consistent diagnostics with minimal boilerplate.
