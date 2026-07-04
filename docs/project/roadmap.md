# Roadmap

por-que is an educational project; the roadmap is oriented around teaching
value and correctness rather than feature breadth.

## In progress

- **Performance work** — the parser core has been made synchronous so metadata
  parsing can run in worker processes while async I/O overlaps fetches; column
  projection and selective structure loading bound memory on large files.
- **Serialization contract** — documenting and stabilizing the JSON export
  format ver-por-que depends on. This should land with or before the v0.3.0
  release, since ver-por-que consumes it (see the
  [serialization guide](../guides/serialization.md)).

## Planned

- **Learn and guides content** — the teaching narrative that ties the API
  reference together (phase 2 of the docs build).
- **Bloom filters** — reading and explaining bloom-filter structures.
- **Encryption** — pointers and, eventually, support for encrypted files.
- **CLI** — the designed [command-line interface](cli.md), shipped as the
  `por-que[cli]` extra.

## Not planned

- **Write support** — por-que reads Parquet; it does not create it.
- **Production performance** — readability comes first; use a production
  library if you need throughput.
