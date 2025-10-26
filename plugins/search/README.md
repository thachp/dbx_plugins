# EventDBX Search Agent

`dbx_search` consumes EventDBX envelopes and projects every aggregate into a
local [Tantivy](https://tantivy-search.github.io/) index. Each aggregate is
represented by a single document that is updated whenever new events are
delivered, making the index immediately searchable for aggregate identifiers,
the serialized aggregate state, and individual state columns.

## Runtime Behaviour

- Reads Cap'n Proto envelopes from `stdin`.
- Acknowledges plugin initialization messages and indexes every `EventRecord`
  into Tantivy.
- Maintains one document per `(aggregate_type, aggregate_id)` pair. Incoming
  events overwrite the existing document only if they advance the aggregate
  sequence.
- Captures aggregate metadata (type, id, sequence, versions) and materializes
  each entry in the aggregate state JSON as its own Tantivy column (text, bool,
  integer, or float) in addition to storing the flattened JSON blob.

## Configuration

Environment variables control the on-disk index and commit strategy:

| Variable | Description | Default |
| --- | --- | --- |
| `EVENTDBX_SEARCH_INDEX_PATH` | Directory where the Tantivy index is stored. | `~/.eventdbx/search-index` |
| `EVENTDBX_SEARCH_COMMIT` | Number of aggregate updates before forcing an index commit. | `100` |
| `EVENTDBX_SEARCH_WRITER_MB` | Heap budget (MiB) reserved for Tantivy's index writer. | `64` |

Logs are emitted via `dbx_plugin_logging` to both stdout and a rotating file
under the standard EventDBX log directory.

## Development Notes

- The index schema is defined in `indexer.rs` and can be extended with additional
  fields as downstream search requirements evolve.
- Unit tests cover aggregate projection logic and a basic smoke test that writes
  into a temporary Tantivy directory.
- When integrating with external search services, replace or augment
  `TantivyIndexer` with the appropriate backend-specific implementation while
  retaining the streaming envelope processor in `main.rs`.
