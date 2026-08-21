# Flint

**An embedded, single-binary key-value store: a persistent append-only log with an in-memory index.**
The SQLite of key-value stores. No server to administer, no JVM, no cluster, no Zookeeper.
Point it at a directory and you get a durable, crash-safe KV store that is instantly fast.
By Pavan Nallamothu.

## Why

Redis is wonderful and also a lot: a dedicated server process, persistence tuning, and cluster
management just to cache a few thousand keys. SQLite proved developers want a database that is
just a file in the project directory. Flint is that for key-value data. One static Rust binary,
two small dependencies, an append-only log on disk, and a hash index in memory.

## Use it as a library

```rust
use flint::Store;

let store = Store::open("mydata", 64 * 1024 * 1024)?; // dir, segment size
store.set(b"user:1", b"alice")?;
assert_eq!(store.get(b"user:1")?.as_deref(), Some(&b"alice"[..]));
store.delete(b"user:1")?;
store.compact()?; // reclaim space from stale versions and tombstones
```

Keys and values are arbitrary bytes. Writes append and return once the record is written.
Reads are a single seek. Reopening the directory replays the log to rebuild the index.

## Use it as a server

```
flint --dir ./data serve --port 6380          # add --fsync for power-loss durability
```

Then talk to it with anything, even netcat:

```
SET user alice        -> OK
GET user              -> $5 (then the raw value bytes)
DEL user              -> 1
LEN                   -> number of live keys
COMPACT               -> OK
PING                  -> PONG
```

A background thread compacts segments automatically as they accumulate.

## Use it from the shell

```
flint --dir ./data set greeting "hello world"
flint --dir ./data get greeting
flint --dir ./data del greeting
flint --dir ./data compact
```

## How it works

Flint is a Bitcask-style log-structured store.

- **Append-only segments.** Every write appends a CRC-checked record (key, value, or a tombstone
  for deletes) to the active segment file. When a segment passes its size cap, it rolls to a new one.
- **In-memory keydir.** A hash map from each key to the file and byte offset of its latest value.
  This is why a read is one seek and a write never has to move old data.
- **Crash safety.** Each record carries a CRC32. On open, Flint replays every segment and stops a
  segment at the first torn or corrupt record, so a half-written trailing write from a crash is
  discarded rather than trusted.
- **Compaction is crash-atomic.** The merged segment is fsync'd and its directory entry made durable
  *before* any old segment is unlinked, so a power loss mid-compaction can never destroy the only
  copy of the data. On reopen the merged segment wins.

## Durability

By default writes are crash-safe against process death: on reopen Flint replays the log and discards
any torn trailing record. For durability against **power loss**, fsync the writes:

- `Store::flush()` performs a real `fsync` of the active segment.
- `Store::set_sync_writes(true)` fsyncs every `set` and `delete` before it returns.
- The server takes `--fsync` for the same per-write guarantee. Segment rollover and compaction
  always fsync regardless, so only the most recent un-rolled writes depend on this flag.

The CLI and MCP server fsync after each write, so a `flint set` is durable when the command returns.

## Use it as agent memory (MCP)

```
flint --dir ./data mcp
claude mcp add flint -- /path/to/flint --dir ./data mcp
```

Exposes `flint_set`, `flint_get`, `flint_delete`, and `flint_len` over stdio, so an agent gets
durable scratch memory that survives across sessions.

## Stack

Rust. The engine depends only on `crc32fast` for record integrity; `clap` powers the CLI and
`serde_json` the MCP server. Release builds are LTO-optimized and stripped into a single static
binary. See [DESIGN.md](DESIGN.md).

## Status

v0.1: storage engine (append-only segments, in-memory index, CRC crash safety, segment rollover,
compaction), TCP line protocol with background compaction, a CLI, and an MCP server. Next: an HTTP
API and optional group-commit fsync durability modes.
