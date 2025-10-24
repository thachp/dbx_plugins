# EventDBX Example Agent

Tiny CLI that connects to an EventDBX socket, decodes envelopes via `dbx_plugin_api`,
and prints the resulting `EventRecord`. Use it to validate transport wiring or to see
how a minimal plugin consumes the shared helpers.
