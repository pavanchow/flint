# Flint design

## The wedge

Most KV stores are servers first. Flint is a library first, a file-on-disk second, and a server
only when you ask. The mental model is SQLite, not Redis: the store is a directory, the engine is
linked into your process, and there is nothing to operate.

## Model: Bitcask

Flint uses the log-structured hash-table model (Bitcask). It is the right fit for the wedge because
it is simple enough to fit in one file and correct, while still giving O(1) reads and appends.

### On-disk record

Little-endian, one record per write:

```
crc32 : u32   over the following fields
klen  : u32
vlen  : u32   (0xFFFFFFFF marks a tombstone; no value bytes follow)
key   : klen bytes
value : vlen bytes
```

Segments are named `NNNNNN.flog` in the data directory. The highest-numbered segment is active.

### In-memory index (keydir)

`HashMap<key, { file_id, value_offset, value_len }>`. A write appends a record and updates the
keydir to point at the new value. A read looks up the keydir and does one seek plus one read. Old
versions stay on disk untouched until compaction, which is what keeps writes cheap.

### Durability and crash safety

Every record is CRC32-checked. On `open`, each segment is replayed in order and folded into the
keydir; replay stops a segment at the first record whose CRC fails or whose bytes are short, which
is exactly what a torn trailing write from a crash looks like. Committed records before the tear are
preserved. `flush` pushes the active segment's buffered bytes to the OS. A future durability mode
will add per-write or group-commit `fsync` for callers who need it.

### Compaction

Overwrites and deletes leave dead bytes behind. Compaction walks the live keydir, copies each
current value into a single new merged segment, deletes the old segments, and starts a fresh empty
active segment. The server runs this on a background thread once the segment count passes a
threshold; the library and CLI expose it explicitly.

## Concurrency

v0.1 guards the engine with a single mutex, so operations are serialized and always consistent,
including during compaction. This is the correctness-first baseline. A later version can move reads
to a read-write lock or per-segment immutable readers so reads run concurrently with writes, since
old segments are never mutated in place.

## Deliberate non-goals for v0.1

No replication, no clustering, no range scans or secondary indexes, no TTL. The point is the SQLite
lane: one binary, one directory, correct and fast for the single-node embedded case. Those features
belong to later versions and would dilute the wedge if rushed.
