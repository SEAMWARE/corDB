# corDB persistence - design

How it got here - what measuring found, and what changed: [the history](history/persistence.md).

*Draft, 2026-10-02 (moved here from coraine#219 with corDB). The concrete form of coraine's [`ToDo.md`](https://github.com/SEAMWARE/coraine/blob/main/ToDo.md) § 15 steps 1-2 - the record format, and log +
snapshot + recovery. History (the selector, retention, the temporal index) builds on it later and is
only constrained here, not designed.*

## 1. What it has to do

corDB keeps every tenant's entities, subscriptions and registrations in RAM, one `CorNode` tree per
tenant (`corDbStore.h`). A restart loses all of it. After this step:

- a broker **stopped cleanly** (SIGTERM, SIGINT) loses nothing: everything in RAM is on disk before
  it exits (§ 5a)
- a broker that **dies** - `kill -9`, a crash, an OOM kill - loses nothing it acknowledged: a write's
  record is in the kernel's page cache before the response (§ 5)
- a **machine** that dies - a power cut, a kernel panic - loses at most the last ~100 ms of writes
  (the default; MongoDB's journal makes the same promise) - nothing, with `--dbSync request`
- recovery time is bounded by a snapshot, not by the age of the broker
- **no new library**: `open`, `mmap`, `posix_fallocate`, `fdatasync`, `rename`, `ftruncate` - and the
  cor:// codec the broker already has. `--dbCompress` uses zstd, loaded (`dlopen`) when needed and
  never linked - libzstd is in every coraine that has OpenSSL (libcrypto depends on it)

Not in this step: history retention, the selector, the temporal index, a standalone corDB server.
Each needs the log; none changes its format (§ 7).

## 2. The shape

```
                    write path (under the tenant's write lock)
  request ─► corDB mutates the tree ─► appends the EFFECT as a record ─► unlock ─► response
                                                  │
                                                  ▼
                               the log segment, mapped ──► flusher thread: fdatasync
                               (a copy into the page cache)              every 100 ms
  recovery:  newest valid snapshot  ─►  replay the log after it  ─►  serve
```

**One directory per tenant** (`<--dbDir>/<tenant>/`, `_` for the default tenant): the lock, the store
and the files are all per tenant already, and a tenant drop becomes a directory delete. Beside them,
`<--dbDir>/_storageFormat`: the version of the files' format (§ 4a).

```
  snap-000041.cor     the tree as of log sequence 41 (complete, or not there at all)
  log-000041.cor      every record since snapshot 41
  log-000042.cor      ... after the next snapshot
```

## 3. What a record is: the EFFECT, not the request

A record says what the write **did to the tree**, at attribute granularity - never the request that
did it. Replaying a request would mean replaying NGSI-LD semantics (merge, `observedAt` handling, the
`@context`, `noOverwrite` ...) with the code of the day of the replay, not of the day of the write; two
versions of the broker would rebuild two different stores from one log. An effect replays with no
semantics at all: "these attributes now are exactly this".

| op | body | written by |
|---|---|---|
| `ENTITY_PUT` | the whole entity, as stored | create, replace |
| `ATTRS_PUT` | entity id + the members it touched, each **whole, after the change** (`attrs`), and those it removed (`deleted`) | PATCH of an attribute, merge, update and append of attributes, `PUT` of an attribute, purge - and each entity of a batch merge |
| `ATTRS_DELETE` | entity id + attribute names (+ datasetId) | attribute delete (not written yet: the entity, `ENTITY_PUT`) |
| `ENTITY_DELETE` | entity id | delete, batch delete |
| `SUB_PUT` / `SUB_DELETE` | the subscription / its id | subscription create, update, replace, delete |
| `REG_PUT` / `REG_DELETE` | the registration / its id | registration create, update, delete |
| `BATCH_BEGIN` / `BATCH_END` | count | a batch: replayed all or nothing |

`ATTRS_PUT` carries the attribute after the merge, not the merge's input, so a one-attribute PATCH of
a 20-attribute entity logs one attribute (~100 bytes), not the entity (~2 KB) - at 100 000 PATCH/s that
is the difference between 10 MB/s and 200 MB/s of log.

The 16 places that take `COR_DB_WRITE` are the places that append: each knows exactly what it
changed, and appends it before the lock goes. Lock order is log order.

A write that updates attributes logs `ATTRS_PUT`: `{ "id", "attrs": { ... }, "deleted": [ ... ] }` -
the members the request named (and `modifiedAt`, `type`, `scope`, which a change refreshes), as the
entity has them after the write, and of those the ones it no longer has. Encoded from the entity's own
member nodes under the write lock - nothing is cloned for it. Replay replaces a member the entity has
where it is and appends a new one at the end, where the write put it; then removes the deleted ones.
A write naming more than 61 members logs the entity (`ENTITY_PUT`).

Batch update and batch upsert replace whole entities the broker has already merged - the driver never
sees what changed - and log `ENTITY_PUT`, encoded before the lock. Create, replace and the attribute
delete log `ENTITY_PUT` too. A batch logs one record per entity, with no `BATCH_BEGIN`/`BATCH_END`:
an NGSI-LD batch is not atomic (each entity has its own outcome), so a replay that stops inside one
is a state the broker could have been in.

## 4. The record format

```
  offset  size  field
  0       4     magic + version   "cr" 0x01 <op>
  4       4     body length (bytes)
  8       8     sequence (per tenant, monotonic - the snapshot's name is one)
  16      8     system time, ns   (= createdAt/modifiedAt/deletedAt of what it wrote: § 4.10a of coraine's cor-protocol-details.md)
  24      4     CRC-32C of the body, continued over bytes 0-23
  28      n     body: the cor binary tree (corTree corTreeBin, coraine's doc/cor-protocol-details.md § 4)
```

- **The body is the cor:// codec** - one serializer for the wire, the log and the snapshot
  (coraine's `ToDo.md` § 15: "specifying it once is the difference between one serializer and two that
  diverge"). Core terms are 1-byte ids, as on the wire.
- **Every record decodes on its own**: the codec's preloaded namespace table only, no per-connection
  string table (cor:// builds one as a connection goes; a log record must not depend on the records
  before it - for a torn tail now, for the temporal index's random access later).
- **The system time is in the header**, as an integer - KZ 2026-10-01: system timestamps are corDB's,
  stamped under the write lock, kept beside the node and not as tree members. History's other axis
  (`observedAt`) is in the attributes themselves.
- **CRC-32C** (SSE4.2 / ARMv8 CRC instructions; a table where neither) - a torn or rotten record is
  detected, never applied. Body first, header second: a write whose entity exists before the write
  lock (create, replace, batch create and update) encodes the body and takes its CRC before the lock;
  under it only the header (sequence, time, the CRC finished over 24 bytes) and a copy of the body.

## 4a. The storage format of a store

`<--dbDir>/_storageFormat` holds one integer, as text: the storage format of the files under
`--dbDir`. `_` is escaped in a tenant's directory name (`%5F`), so no tenant's directory has that name.

| version | written by | what |
|---|---|---|
| 1 | coraine 0.5.0 | records of version 1 (§ 4), snapshots and history segments as 0.5.0 writes them |

At start, before anything under `--dbDir` is read or written (`corDbPersist.c`, `storageFormat`):

- **no file, nothing else in `--dbDir`**: a new store - this corDB's version is written;
- **no file, files in `--dbDir`**: a store written before the version was recorded - read as it is, and
  this corDB's version is written;
- **a version up to this corDB's**: read; a lower one is brought up to this corDB's (the upgrade steps
  of the formats after it, in order) and this corDB's version is written;
- **a version above this corDB's**: the broker does not start. The error names the version found and
  the newest this corDB knows: the store was written by a newer release, and is to be run by that
  release or a newer one. A downgrade is not supported once a newer release has written to a store.

A record of a version this corDB does not know would not be recognised as a record: in a log's last
segment, recovery would cut the log there (§ 6). The version file refuses the store before that.

The file is written as a `.tmp`, synced and renamed. A change to the files that an older corDB would
misread is a new version, a new line in `formatV` (`corDbPersist.c`) and, if a store in the older
format is not read as it is, its upgrade step.

## 5. Writing: a copy into the page cache, group commit on a timer

The open log segment is **mapped** (`mmap`, `MAP_SHARED`) over 1.25 GiB of address space, and the file
is allocated ahead of its records 4 MiB at a time (`posix_fallocate`). A write copies its record into
the mapping under the tenant's write lock (memcpy - no system call, but the allocation every 4 MiB).
The pages are the kernel's page cache: once the copy is done, the record outlives the process. One
**flusher thread** per broker, every `--dbSyncInterval` ms (default 100): for each tenant,
`fdatasync()` - the history log, then the log.

- the response does not wait for the disk. A broker that dies (`kill -9`, a crash, an OOM kill) has
  lost nothing it acknowledged; a machine that dies, at most the last interval, as with MongoDB's
  default journal
- `posix_fallocate`, not a sparse file: a page of a sparse file the file system cannot back when it is
  first written kills the process (SIGBUS) - a full disk would. Allocated ahead, a full disk is an
  error at the allocation, below
- while open, a segment is longer than its records: the rest is zeros, which recovery reads as the end
  (§ 6). A finished segment - rolled, or at a clean stop - is cut to its records (`ftruncate`)
- `--dbSync request`: the response waits for its record's `fdatasync` - group commit: every writer
  that arrived during one sync shares the next (the coroutines yield on it; a worker waits on a
  condition). Per-request durability, paid for by whoever asks
- `--dbSync none`: no fsync at all (tests, benchmarks, a RAM disk) - the log is still written
- a write error (disk full) is not silent: logged, a metric, and with `--dbSync request` the request
  fails 503 - a broker that acknowledges what it cannot keep is worse than one that refuses

## 5a. A clean stop loses nothing

On SIGTERM or SIGINT, in this order:

1. **no new requests** - the servers stop taking them; the requests in flight finish. A write appends
   its record under the tenant's write lock, so once the locks are free every acknowledged write is
   in a log segment
2. **every tenant's log segments `fdatasync`ed** and cut to their records
3. **a snapshot per tenant** (§ 6) - the next start loads it and has no log to replay

Only then does the broker exit. A stop that takes too long is still a clean stop: step 2 does not
depend on step 3, so a broker killed during its snapshots has lost nothing either - it replays the
log on the next start. Only the machine dying can lose the writes since the last sync (§ 5).

## 6. Snapshots and recovery

**Snapshot** - on a thread of its own (`corDbSnapshot`), after `--dbSnapshotEvery` MiB of log and at
least as much log as the last snapshot was big (so the snapshots of a growing store cost in proportion
to what is written), and at a clean shutdown:

1. **the start**, under the tenant's write lock and syncing nothing: the log switched to `log-N.cor`
   (N = M + 1, opened and mapped), the sequence taken, a cursor set on the first entity. After the
   lock, `log-M.cor` is synced and cut to its records
2. **the store**, in slices of 1000 entities, each under the **read lock** - readers run beside it,
   writers between the slices (a writer that takes or swaps the cursor's entity moves it). Every
   entity, subscription and registration as a PUT record, so a snapshot replays with the same loop as
   a log, into buffers of 64 MiB
3. **the file**: `snap-N.tmp`, `fdatasync`, `rename` to `snap-N.cor`, `fsync` the directory; then the
   files before N are deleted

The snapshot is not the store at one instant, and does not need to be: the records are effects (an
entity put, an id deleted), and recovery replays `log-N` - every write since the start - on top of it.
A write a slice saw replays to the same state, one it missed to its state, an entity deleted after a
slice took it is deleted again; the creation order holds, because a slice walks the list in its order.
The longest a writer waits on a snapshot: ~8 ms (measured).

**A log segment rolls at 1 GiB** on its own, without a snapshot: no file reaches the 2 GiB the reader's
offsets allow, whatever the snapshots do. Recovery replays every segment from the snapshot's on.
Older snapshots and logs are deleted once the new snapshot is durable (history, later, keeps them:
§ 7). Sizes, measured: 100 000 entities of four attributes are 38 MB, as a log or as a snapshot.

**Recovery**, at start, per tenant directory:

1. the newest `snap-N.cor` (a `.tmp` is an interrupted snapshot: deleted). A snapshot that does not
   decode in full stops the broker: the files before it are gone
2. `log-N.cor`, `log-N+1.cor` ... in order, record by record, until a record that is short or fails
   its CRC:
   - **zeros** from there to the end: the space allocated ahead of the records (§ 5) - the end of the
     segment, cut to its records
   - anything else: **the torn tail** - the file is truncated there (`truncate`) and writing continues
     after it. Only the newest segment can have one; a bad record anywhere else stops the broker
   - a record whose **sequence** does not follow the last one replayed is after a **gap**: pages the
     machine wrote out of order before it died, past the last sync. The log ends before it - replaying
     past a gap would build a store no write made. The rest of that segment is cut; a segment that
     starts after the gap, and every one after it, is renamed `<segment>.torn` - kept, not replayed.
     A write that cannot be logged (a full disk) uses no sequence number, so the log has no gap of
     its own
3. the indexes (`corDbIndex`) are rebuilt from the tree, as at any load

Measured target: recovery at memory-bandwidth speed - decoding cor binary, no JSON parse.

## 6a. `--dbCompress`

The snapshots and the finished log and history segments are compressed with zstd (level 3) - never
the open segment, which a write appends to. On the snapshot thread: a snapshot is compressed as it is
written, a finished segment (synced, cut, closed by the flusher) after the snapshots - the same thread
drops the segments a snapshot covers, so nothing is compressed while it is deleted or renamed back
into being after. A segment is compressed into `<segment>.ztmp`, synced, and renamed over itself; a
`.ztmp` found at a start is an interrupted compression, deleted.

- **One name, two forms**: the first four bytes - zstd's magic, or a record's `'c'` - tell a compressed
  file from a plain one. A start reads either, with the option or without (libzstd loaded on demand;
  absent, the broker does not start on a store it cannot read).
- **64 MiB frames**: a file is a sequence of zstd frames of at most 64 MiB of content - a segment is
  never one buffer, to compress or to read.
- **Segments roll at 64 MiB** with the option (1 GiB without): a history, which no snapshot drops, is
  then mostly finished, compressed segments.
- **A compressed file is whole** (it appeared by a rename): one that does not decode to its end is
  damage, and the broker does not start - not a torn tail to cut.
- The newest segment compressed (finished, and the next one never written) - the log goes on in a new one.
- A start with the option compresses the finished segments a run without it left.

Measured on perfRun's fixture (2026-10-05): five attributes, ~550 bytes of JSON an entity, created in
batches of 500; the history, every entity's `speed` then updated ten times. Bytes on disk - corDB its
files after a clean stop, MongoDB 8.2 `storageSize` + `indexSize`, PostgreSQL 16 + TimescaleDB (coraine's
`--troe timescale`) the database's growth:

| | corDB | corDB `--dbCompress` | MongoDB | PostgreSQL + TimescaleDB |
|---|---:|---:|---:|---:|
| current state, 1 000 entities | 740 172 | **12 282** | 196 608 | - |
| current state, 100 000 entities | 74 418 601 | **1 360 203** | 11 329 536 | - |
| history, 1 000 entities × 10 updates | 4 225 466 | 4 225 466 ¹ | - | 28 088 840 |
| history, 100 000 entities × 10 updates | 425 960 175 | **52 081 577** ² | - | 1 727 322 632 |

¹ all of it in the open segment (under 64 MiB), which is never compressed
² six finished segments compressed, the open one not

The fixture favours any compressor - every entity carries the same 200-character description - and
MongoDB's (WiredTiger, snappy) as much as corDB's. TimescaleDB's own compression (a policy per
hypertable) is off, as coraine creates the tables.

## 7. What history adds later, and why nothing here stops it

- **History on** = keep logs and snapshots past the newest snapshot, by the retention policy
  (duration / bytes - § 15 "still open"). Compaction - deleting old segments - is where the TRoE
  boolean, the selector and the retention policy meet; in this step it just deletes.
- **The selector** (what history records) marks the record - a flag in the op byte - and compaction
  honours it. It does not change what durability writes.
- **"The entity as at T" by system time** = the snapshot at or before T, plus its log up to T: the
  header's system time makes that a prefix, and keeping a chain of snapshots makes it cheap.
- **The temporal index** - `(entity, attribute, time) -> record offset` - needs records that decode
  alone, which § 4 gives them.
- **cor:// replication / standalone corDB** (haaux): the records are already cor frames.

## 8. Options

| option | default | |
|---|---|---|
| `--dbDir <path>` | none: no persistence, as today | the tenants' directories |
| `--dbSync interval\|request\|none` | `interval` | § 5 |
| `--dbSyncInterval <ms>` | 100 | |
| `--dbCompress` | off | the snapshots and the finished segments compressed (zstd), on the snapshot thread; segments roll at 64 MiB instead of 1 GiB; a start reads a compressed store with or without it - § 6a |
| `--dbSnapshotEvery <MiB of log>` | 64 | and at a clean stop |

No `--dbDir`, no change: a corDB without a directory is the in-RAM store of today, at today's speed.

## 9. Order of work, and how each step is proven

1. **The record writer and reader** in corDB, unit-tested: encode every op, decode it back; a torn
   tail and a flipped bit are found and stop the replay.
2. **The 16 write sites append**; the flusher; `--dbDir`, `--dbSync`.
3. **Recovery**: snapshot load + replay. Functests, two kinds that assert different things:
   - **a clean stop loses nothing** - write, stop (SIGTERM), restart, read it ALL back, the last write
     before the stop included, with `--dbSync interval` (the default - so a stop that skipped § 5a's
     flush fails): entities, attributes after PATCH/merge/delete, subscriptions, registrations, two
     tenants
   - **a death loses at most the unsynced** - write, `kill -9`, restart: everything written before the
     last sync is back (with `--dbSync request`: everything acknowledged); a truncated log; an
     interrupted snapshot (`.tmp`)
4. **Snapshots** + log rollover; recovery from snapshot + log.
5. **Measure**: write throughput with `--dbSync interval` against no `--dbDir` (the bar: within a few
   per cent), `request` against it, recovery time for 100 000 entities. Documented in
   coraine's [`doc/performance.md`](https://github.com/SEAMWARE/coraine/blob/main/doc/performance.md), the losers too.

## 10. Maintenance - corsh

KZ 2026-10-03: deleting, backing up, adding indexes - a tool for it, **corsh** (the corDB shell). A
**maintenance tool only**, for now; a general broker shell is for later. **corDB becomes its own
repository, and corsh is part of it.** Interactive, and scriptable (`corsh -c 'backup /mnt/b'`).

**Two ways in.** The data of a running broker is in that broker's RAM, under its locks: anything done
to live data goes **through the broker** - corsh speaks cor:// to it, the requests served by the
`admin` API plugin (`/admin/db/...`). Nothing reads or writes the files under a running broker. With
no broker running, corsh works on a tenant directory **directly**: what needs no live data.

| command | | |
|---|---|---|
| `stats` | online | per tenant: entities, subscriptions, registrations; RAM, log and snapshot bytes; the last sync |
| `backup <dir>` | online | a snapshot now, then `snap-N` and every `log-M` after it hard-linked (or copied, across file systems) into `<dir>`. Consistent without stopping anything: a snapshot is never written again once renamed, a log segment never once rolled. Restore = that directory as `--dbDir` |
| `snapshot` | online | one now (§ 6), and the log rolls |
| `compact` | online | log segments a snapshot covers removed (with history on: those past the retention) |
| `purge <tenant> [type / q]` | online | entities matching, deleted as one batch - logged like any write |
| `drop <tenant>` | online | the tenant emptied; its directory goes |
| `index add / drop / list` | online | secondary indexes (below) |
| `verify <dir>` | offline | every record's CRC, every snapshot decodes, sequences contiguous |
| `dump <dir>` | offline | the records, readable (op, sequence, time, entity id) - for a broken log |
| `export` / `import` | either | entities as NDJSON - between brokers, and to and from `mongoc` |

**Order.** Entities are kept in **creation order**, and that is the default order of a query - as
`mongoc`'s (`createdAt` ascending, `_id` breaking ties). A replace keeps the entity's place (and its
`createdAt`); a delete and a create append it. Recovery reproduces it exactly: a snapshot is written
in store order, and on replay an `ENTITY_PUT` for an entity that exists replaces it **in place** - it
does not move to the end. Otherwise every restart would reorder query results.

**Indexes.** corDB has two today: the **entity id** (a hash, to the entity's predecessor in the store
- lookup and unlink O(1)) and, in effect, **`createdAt`**: the store list itself, in creation order.
The id table grows 8x at 4 entities per slot; the old table's slots move into the new one 32 at a
time, at every link, unlink and replace - no write holds the lock for a whole rebuild (lookups try
the new table, then the old). A table built from a loaded store is sized to it.
A query by type walks every entity of the tenant; so do a `q` and a geo-query. Indexes worth having,
each measured before it stays:

- **type** -> its entities, **in creation order** (KZ 2026-10-03: yes, with persistence). `?type=X`
  then pages by `createdAt` without walking the other types; a deep `offset` still walks within the
  type (an array per type would make it O(1) and a delete O(n) - to be measured)
- **an attribute's value** - equality and range, for `q` on a property that is queried often
- **geo** - an R-tree over a GeoProperty, for `georel`

An index is a **declaration** kept in the tenant's directory (a record in the log, `INDEX_PUT`), built
when declared - under the write lock, a full pass - and at every load; the 16 write sites keep it
current. Not in the snapshot: rebuilt from the data, so it can never disagree with it.

## 11. Open

- Snapshot trigger: log bytes only, or also a time?
- With `--dbSync request`, a failed write or sync is logged, and the request still answers its
  success: the wait happens as the write lock is released, after the operation has returned. The
  503 of § 5 needs the outcome carried back to the operation.
- **Size.** Without `--dbCompress` nothing is compressed: on perfRun's fixture (2026-10-05) the current
  state takes 744 bytes an entity - MongoDB's documents 1 072, its files 113 (WiredTiger compresses).
  A record decodes on its own, so each carries its attributes' expanded IRIs again. Not done yet: a
  table of the IRIs per snapshot or segment, the records naming them by number (a format change).

Decided: the CRC is CRC-32C (corBase's `corCrc32c`, hardware on x86-64 and ARMv8). Hosted
`@context`s are not corDB's - the plugin implements none of the driver's context functions - so the
log holds the store tree and nothing else.
