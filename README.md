# Spire Take-home Assignment

Given the provided trait representing a key-value store, implement a struct KVLog that implements KVStore with the properties:
- reentrant (when shared across either threads or async tasks)
- backed by a log (filesystem is sufficient for this purpose)
- persistence is guaranteed before access, e.g. [Write Ahead Log](https://en.wikipedia.org/wiki/Write-ahead_logging) or equivalent guarantee
- will load the persisted state on startup

Please include the following:
- brief description of implementation decisions, including:
  - what is persisted (files, directories) and any significant tradeoffs
  - choices about contention and access control (e.g. Mutexes, Marker files, etc.)
  - assurances that recovery will always be in a good state, e.g. no partial writes
- basic tests for the above properties
- bonus: tests with multiple async tasks, single and multi-threaded executor
- extra bonus: thoughts on the interface (e.g. trait_variant, non-mut get and delete, return value on set, etc.)

