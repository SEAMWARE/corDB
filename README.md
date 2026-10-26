# corDB

The in-process database of the [coraine](https://github.com/SEAMWARE/coraine) NGSI-LD context broker:
entities, subscriptions and registrations in the broker's own memory, one tree per tenant, no database
server. Its plugins:

| Plugin | Selected with | What it is |
|---|---|---|
| `corDB.so` | `--database corDB` | the current-state store - and with `--troe corDB` the temporal history (TRoE), in the same store |
| `ramDB.so` | `--database ramDB` | corDB in RAM only: no `--dbDir`, no `--troe corDB` |
| `troe/ramDB.so` | `--troe ramDB` | a ring of the broker's TRoE events in RAM (the most recent N, `/admin/troe/dump`) - dev/test today, the seed of a short-term history |

What it measures against MongoDB is in coraine's
[performance and footprint](https://github.com/SEAMWARE/coraine/blob/main/doc/performance.md).

- **License:** [Apache License 2.0](LICENSE) - Copyright 2026 Seamware

## Building

A Cor-Lib like the others: a checkout beside its siblings (`../corTree`, `../corNgsild`, ...), and beside
`../coraine`, whose plugin interface (`src/lib/db/DbDriver.h`, `src/lib/troe/TroeDriver.h`) it
implements. The [corLibs](https://github.com/SEAMWARE/corLibs) umbrella builds it with the rest.

```console
make                    # debug (traces in), as coraine's debug broker
make BUILD=release      # release
make install            # into /opt/seamware/plugins/db/currentState and .../troe/temporal
```

A plugin is built with the broker's feature switches: `COR_FEATURE_SUBSCRIPTIONS=0` and
`COR_FEATURE_REGISTRATIONS=0` for a broker built without them. `OBJDIR` and `OUT` build a variant
elsewhere. Needs `libgeos_c` (geo-queries).

### The DB plugin interface stamp

The plugins share structs with the broker by layout (`DbDriver`, `DbQueryFilter`, `Tenant`,
`TroeDriver`, `CorNode`, ...), so a plugin built against other headers than the broker that loads it
corrupts memory. Both sides carry a stamp of those headers and refuse a mismatch - coraine's
[plugin architecture](https://github.com/SEAMWARE/coraine/blob/main/doc/plugin-architecture.md),
"The DB plugin interface stamp":

- the makefile runs `../coraine/tools/dbAbiStamp.sh` on every `make` (into `obj/<BUILD>/dbAbiStamp.h`,
  rewritten only when the stamp changed), and every plugin here - `corDB.so`, `ramDB.so`,
  `troe/ramDB.so` - exports it as `dbPluginAbi`, compiled from coraine's `src/plugins/shared/dbPluginAbi.c`
- the broker refuses a plugin without `dbPluginAbi` or with another stamp than its own `coraineDbAbi`
- `dbRegister` / `troeRegister` first check the broker's `coraineDbAbi`, and exit 1 when it is missing
  (a broker older than the check) or different

Either way the message names both stamps: rebuild the plugin against the broker's source, or install
matching versions. A coraine checkout without `tools/dbAbiStamp.sh` is older than the stamp: `make`
stops with an error.

### System timestamps - one per created entity

corNgsild gives the entity, every attribute instance and every sub-attribute a `createdAt` and a
`modifiedAt`. An entity is created whole, with one time, so the store keeps that one time - the
entity's `createdAt` - and, below the entity, only the times that differ from it (`corDbSysTimes.h`):

| | stored |
|---|---|
| a created entity | the entity's `createdAt`; its `modifiedAt` once it differs |
| a modified attribute | its own `modifiedAt` - its `createdAt` is still the entity's |
| an added attribute | its own `createdAt` and `modifiedAt` |

- **A time that is not there is the entity's `createdAt`.** An object whose time was left out is marked
  (CorNode flag `0x04`: `createdAt` inherited, `0x08`: `modifiedAt`); the entity's `createdAt` is kept
  at its front, after the `id` the index puts first - a hop or two.
- **The conversion is at the store's edges.** In, the inherited times are left out and the object marked;
  out, the copy is corTree's own clone (`corTreeCloneMarked`) and only a marked object is looked at.
  Retrieve and query copy under the read lock as a plain clone and put the times back after it, on the
  request's own copy - the lock is held no longer than for a store with every time in place.
- **corNgsild keeps it so**: `ldEntityAttrsSet` keeps an inherited `createdAt` inherited when it replaces
  an instance, and its report's `preValue` has every time in place.
- **Everything outside corDB sees every time.** The log and the snapshot keep the short form; the marks
  are not written, `corDbTreeIn` sets them again on replay. A store written with every time in place is
  read as it is. A `q` on the entity's `modifiedAt`, or on an attribute's own `createdAt` / `modifiedAt`,
  matches each candidate on a copy with every time in place.

**The attribute type in the node.** On disk, the log and the snapshot already keep an attribute's
`type` as the object's kind (cor:// - coraine `doc/cor-protocol-details.md` § 4.2). In RAM it is kept the
same way: `corDbTreeIn` folds the `"type"` member of an attribute instance or sub-attribute (one of the 8
attribute types) into the node (`CorNode.kind`, in its padding byte - the node stays 40 bytes), and every
copy out gets the member back, first. One node less per attribute instance and sub-attribute (a 54-byte
allocation, 64 with malloc's overhead). Inside corDB, what reads a type reads the kind: corNgsild's
`ldAttrTypeDetect` (the matchers go through it), the codec, the deletion tombstone; what
`ldEntityAttrsSet` writes in place is folded after it (`corDbAttrFold`).

100 000 entities of 10 Property attributes, corDB in RAM (`ramDB`), broker RSS, release builds, against
the same code with the type as a member - two runs each, alternated (2026-10-07):

| | type as a member | type in the node | |
|---|---:|---:|---:|
| created | 380 596 kB | 317 932 kB | **-16.5 %** |
| every value then changed once (a merge per entity) | 494 414 kB | 429 876 kB | **-13.1 %** |

Throughput, `perfRun` on `--dbDir` (8 cores, release builds, four runs each, 2026-10-07): every
scenario within -0.7 % to +4.5 % of the type as a member, but concurrent creates **-7.7 %**. That one
is glibc's allocator, not the work: with snapshots off it stays (-6.5 %), the profile has it in malloc
and free (one long-lived node less per attribute changes how a request's short-lived allocations
coalesce among them), and under jemalloc the two forms are equal, under tcmalloc within 2.2 %. In RAM
they are within 1.5 %; with a writer-preferring lock the type in the node is **+8.6 %**. The tables:
coraine's [performance.md](https://github.com/SEAMWARE/coraine/blob/main/doc/performance.md), "Writes
that notify, and which writer the lock lets in first".

**Memory** - 100 000 entities of 10 Property attributes, `--database corDB` in RAM, broker RSS, against
the same code keeping every time (2026-10-06):

| | every time kept | one per entity | |
|---|---:|---:|---:|
| created | 612 472 kB | 481 268 kB | **-21.4 %** |
| every value then changed once (a merge per entity) | 831 620 kB | 638 288 kB | **-23.3 %** |
| with 2 constant sub-attributes + `observedAt` per attribute, created | 1 620 080 kB | 1 238 836 kB | **-23.5 %** |
| the same, every value then changed once | 2 350 892 kB | 1 657 848 kB | **-29.5 %** |

A changed attribute takes back one time, its `modifiedAt`; a constant sub-attribute none. The growth from
"created" to "changed" is glibc's: the merges run on 32 connections, and the memory the old nodes are
freed into stays with the broker's malloc arenas.

**Throughput** - test/perf/perfRun.sh, release builds, broker on 8 cores; requests/s against the same code
keeping every time (the mean of two runs, their difference in brackets), 2026-10-06:

| scenario | in RAM | durable (`--dbDir`) | durable + history (`--troe corDB`) |
|---|---|---|---|
| query_c50 | 70,159 / 70,782 / **+0.9 %** (+8.5 %) | 73,918 / 75,039 / **+1.5 %** (-0.5 %) | 73,364 / 74,589 / **+1.7 %** (+1.3 %) |
| query_c200 | 70,661 / 70,159 / **-0.7 %** (+4.6 %) | 70,950 / 72,283 / **+1.9 %** (-0.7 %) | 70,856 / 72,018 / **+1.6 %** (+0.7 %) |
| query_l1_c50 | 416,201 / 410,725 / **-1.3 %** (+3.7 %) | 415,046 / 427,785 / **+3.1 %** (-1.1 %) | 415,178 / 416,412 / **+0.3 %** (+0.9 %) |
| query_l100_c50 | 15,998 / 15,827 / **-1.1 %** (+2.7 %) | 16,614 / 16,236 / **-2.3 %** (-3.0 %) | 16,287 / 15,732 / **-3.4 %** (+1.6 %) |
| retrieve_c50 | 454,890 / 456,724 / **+0.4 %** (+4.2 %) | 456,256 / 468,133 / **+2.6 %** (+1.0 %) | 461,334 / 470,225 / **+1.9 %** (+0.6 %) |
| patch_c50 | 179,072 / 186,271 / **+4.0 %** (+3.5 %) | 136,268 / 147,922 / **+8.6 %** (-0.7 %) | 111,845 / 115,857 / **+3.6 %** (+0.1 %) |
| patch_c1 | 39,245 / 38,792 / **-1.2 %** (+0.5 %) | 37,644 / 37,686 / **+0.1 %** (-0.4 %) | 35,436 / 35,069 / **-1.0 %** (-0.7 %) |
| batch20_c50 | 39,999 / 40,207 / **+0.5 %** (+1.5 %) | 33,594 / 33,493 / **-0.3 %** (+1.2 %) | 21,823 / 21,044 / **-3.6 %** (-0.2 %) |
| create_c50 | 189,802 / 196,711 / **+3.6 %** (+0.8 %) | 112,943 / 125,750 / **+11.3 %** (+2.1 %) | 80,622 / 91,839 / **+13.9 %** (-1.4 %) |
| create_c1 | 30,368 / 31,039 / **+2.2 %** (+0.0 %) | 27,048 / 28,021 / **+3.6 %** (-0.4 %) | 21,640 / 22,464 / **+3.8 %** (-0.2 %) |
| batch20create_c50 | 49,623 / 51,534 / **+3.9 %** (+2.2 %) | 13,316 / 15,578 / **+17.0 %** (-2.2 %) | 7,254 / 7,712 / **+6.3 %** (-3.6 %) |
| merge_c50 | 126,632 / 130,141 / **+2.8 %** (-0.8 %) | 102,224 / 99,835 / **-2.3 %** (-4.4 %) | 75,924 / 73,889 / **-2.7 %** (-1.8 %) |
| delete_c50 | 226,585 / 241,294 / **+6.5 %** (-0.5 %) | 204,700 / 215,551 / **+5.3 %** (+3.2 %) | 184,706 / 197,403 / **+6.9 %** (-0.2 %) |
| batch20delete_c50 | 67,732 / 76,385 / **+12.8 %** (-0.2 %) | 65,040 / 70,548 / **+8.5 %** (+0.3 %) | 47,226 / 47,964 / **+1.6 %** (-2.8 %) |

**Forms measured and not taken** (perfRun as above, the change against every time kept; how each one
led to the next: [history](doc/history/sys-times.md)):

| form | memory | worst |
|---|---|---|
| `createdAt` and `modifiedAt` each inherited per object (from the parent / the object's own `createdAt`), looked up member by member | -22 % | patch_c50 -27.5 %, delete -19 %, merge -14 % (RAM) |
| one time per created entity, the out-copy checking every member | -21.7 % | patch_c50 -25 %, merge -15 %, delete -11 % |
| marked objects; append filling and dropping the times in place under the write lock | -21.5 % | patch_c50 -13 % (RAM), batch update -21 % (history) |

The NGSI-LD and Cor-Lib functions the plugins call are resolved from the broker at `dlopen` (it is
linked `-rdynamic`), so a plugin links none of them - and is the same for both of coraine's HTTP
servers.

## Queries by type - the type index

Every entity type has a list of its entities, in the store's order (`corDbTypeIndex.h`). A query by one
type, or by one AND group of types (`(A;B)`: the member with the shortest list), walks that list instead
of the store: a page of a type that is 0.1 % of the store no longer costs the store up to where the page
ends. A query with no type, or with types in OR (`A,B`), walks the store. Either way the answer is the
same, in the same order; a debug build walks the store too for every query by type, and aborts on a
difference.

- **Built by the first query that can use it**, under the read lock, from the store's list. A store that
  is only written pays nothing until then, and a store loaded from the disk is indexed by its first such
  query. The index is not on disk.
- **Kept** by the three functions that link, unlink and swap an entity in the store's list
  (`corDbIndex.c`), and where an entity's types change in place: a merge, a batch upsert, a replayed
  record.
- **Out of memory:** the index is dropped, and queries walk the store.
- **What it costs:** 48 bytes for each type of each entity, plus the entity map's buckets; about
  0.2 µs of write-lock time for each created entity. Measured in coraine's
  [performance](https://github.com/SEAMWARE/coraine/blob/main/doc/performance.md), "corDB's type index".
- **`make COR_FEATURE_TYPE_INDEX=0`** leaves it out of the build (8 KiB of `.text`).

## Queries by value - declared indexes

An attribute whose `q` value terms an index serves is **declared** - narrowed to an entity type, to a tenant,
if so declared: `--dbValueIndexes`, or the broker's admin API (`/admin/indexes`, coraine's
[installation](https://github.com/SEAMWARE/coraine/blob/main/doc/installation.md), "Declared indexes"). The
admin API's declarations are kept in the default tenant's store (documents of the collection `dbIndexes`) and
come back with it at a start.

A declaration is a value index in the store of every tenant it covers (`corDbValueIndex.h`): a skip list of
the attribute's values - every instance's, an array's elements; booleans, numbers, strings - each with its
entity. A query whose `q` has a term on the attribute with `==`, a range, a list, `<`, `<=`, `>` or `>=` takes
its candidates from the values the term can match, in the store's order, and matches each of them as before:
the answer is the walk's, in the same order, and a debug build walks the store too and aborts on a difference.
An AND takes its term with the fewest candidates; an OR is served when all its members are. A declaration
narrowed to a type serves a query of that type (or of an AND group of types with it). Not served: `!=`, `~=`,
`!~=`, existence, a sub-attribute, a `[member]` path - the query walks, or takes its type's list, as without.
A query by type whose type's list is shorter than the candidates walks that list. A page (offset + limit
matches) whose candidates exceed `sqrt(page * entities)`, at least 64, walks the store: the walk stops at the
page, the index gathers and sorts every candidate. A count, or a query that wants every match, takes the
index whatever its candidates.

- **Built by the first query that can use it**, under the read lock - and at once in every store when the
  admin API declares one. Not on disk.
- **Kept** by the functions that link, unlink and swap an entity in the store's list, and by
  `corDbIndexChanged` wherever an entity changes in place (a merge, a batch upsert, a replayed record): each
  declaration's values of the entity are compared with its entries, and only a declaration whose values
  differ changes its skip list.
- **Only the entities with a value of a declared attribute** are in the index. Another entity costs a create,
  a replace or a delete a check of its declared attributes, a merge of no declared attribute a comparison of
  names. An entity that gains a value where it is takes its place in the store's order: the index walks the
  store from it to the next entity it holds.
- **Counted**, per declaration and store, since it was built: `accesses`, the `q` terms it served, and
  `updates`, the writes that changed it - an entity with the attribute created or deleted, its values changed.
- **A change of the declarations** (the admin API) drops every store's index - one index a store holds every
  declaration - and builds it again: at once for a declaration made, by the next query that can use it for one
  taken away; the counters start from 0. About 0.5 µs an entity and declaration (1 000 000 entities, two
  declarations: 0.87 s), under the store's read lock - the store's writes wait. Declare the indexes at start
  (`--dbValueIndexes`); coraine's [performance](https://github.com/SEAMWARE/coraine/blob/main/doc/performance.md#building-a-declared-index).
- **Out of memory:** the index is dropped, and queries walk the store.
- **`make COR_FEATURE_DB_INDEXES=0`** leaves it out of the build: 23,616 bytes of `corDB.so` (`.text` +
  `.data`, release).

## Tested

Through the broker: coraine's functional suite run with `-db corDB` (`corTest`), and in coraine's CI.

## Persistence

In RAM by default. With `--dbDir <directory>` the store survives a restart: every write appends its
effect to a log (per tenant, memory-mapped: the record is in the kernel's page cache before the
response), synced every 100 ms, with a snapshot as the log grows - taken in slices, so a writer waits
for it a few milliseconds at most. A clean stop loses nothing, and neither does `kill -9`, a crash or
an OOM kill; a machine that dies, at most the last 100 ms (`--dbSync request`: nothing acknowledged).
100 000 entities are back in 0.2 s.

```console
coraine --database corDB --dbDir /var/lib/coraine
```

| Option | Default | |
|---|---|---|
| `--dbDir` | none: in RAM only | one subdirectory per tenant: `snap-N.cor`, `log-N.cor` |
| `--dbSync` | `interval` | `request`: a write answers once its record is synced; `none`: never synced |
| `--dbSyncInterval` | 100 | ms |
| `--dbSnapshotEvery` | 64 | MiB of log (and at least as much as the last snapshot) |
| `--dbCompress` | off | snapshots and finished segments compressed (zstd, loaded only then); read with or without it |
| `--dbLockPrefer` | `reads` | whom a tenant's store lock lets in first when readers and writers both wait: `writes` (also for `ramDB`). Which is faster is the workload's - coraine's performance.md, "Writes that notify, and which writer the lock lets in first" |
| `--dbValueIndexes` | none | declared indexes (also for `ramDB`): the attributes expanded (IRIs; a short name is an error), comma-separated, or a JSON array of `{ "attribute", "type"?, "tenant"? }` - [Queries by value](#queries-by-value---declared-indexes) |

`--dbSync`, `--dbSyncInterval`, `--dbSnapshotEvery` and `--dbCompress` act on the disk: given without
`--dbDir` (on the command line, in the configuration file or in the environment), the broker does not
start - each one is named. As `ramDB` refuses `--dbDir`, an option it does not have.

[The design](doc/persistence.md), [how it got here](doc/history/persistence.md); what it costs: coraine's
[performance](https://github.com/SEAMWARE/coraine/blob/main/doc/performance.md), "corDB on disk".

## History (TRoE)

`--database corDB --troe corDB`: every write that reaches an attribute appends an instance, captured
where current state changes; on disk with `--dbDir` (`hist-N.cor`). [The design](doc/troe.md),
[how it got here](doc/history/troe.md).

## Options

`corDB.so`'s options - added to `coraine -u` with `--database corDB`. The same entries are in coraine's
[Command-line options](https://github.com/SEAMWARE/coraine/blob/main/doc/options.md#plugin-cordb-loaded-by-database-cordb),
where they are changed: this copy is written from there by coraine's `tools/pluginOptions.sh`.

<!-- options of the corDB plugin: copied from doc/options.md by tools/pluginOptions.sh - edit them there -->

### `--dbDir`

`-dbDir` · `CORAINE_DBDIR` · String · no default - the store is in RAM only, and a restart starts empty
· values: a directory path · plugin: corDB

The directory the store is kept in, created (with its parents) if missing. Inside: `_storageFormat` (the
[storage format](https://github.com/SEAMWARE/coraine/blob/main/doc/installation.md#storage-format), a number as text) and one directory per tenant, `_` for
the default one, each with its log segments (`log-<N>.cor`) and its latest snapshot (`snap-<N>.cor`). At
start every tenant directory is loaded - the snapshot, then the log after it. A store written by a newer
release (a storage format above this build's) stops the start and is left as it is. A directory that cannot
be created stops the start: `corDB: --dbDir '<dir>': <reason>`. With `--troe corDB` the history is kept here
too. `--dbSync`, `--dbSyncInterval`, `--dbSnapshotEvery` and `--dbCompress` act only with it.

    coraine --database corDB --dbDir /var/lib/coraine
    # a POST, a stop, a start on the same directory: the Entity is back
    $ ls /var/lib/coraine /var/lib/coraine/_
    /var/lib/coraine:
    _  _storageFormat

    /var/lib/coraine/_:
    log-000001.cor  snap-000001.cor

### `--dbSync`

`-dbSync` · `CORAINE_DBSYNC` · String · default: `interval`
· values: `interval`, `request`, `none` · plugin: corDB

When a write reaches the disk. A write copies its record into the memory-mapped log - the kernel's page
cache - before its response; this option decides when that is `fdatasync`ed.

- `interval` - a flusher thread syncs every tenant's log every `--dbSyncInterval` ms. The response does not
  wait. A broker that dies loses nothing it acknowledged; a machine that dies, at most the last interval.
- `request` - the response waits for its record's sync. The writers that arrive during one sync share the
  next (group commit). A write that cannot be logged (a full disk) is answered 503.
- `none` - the log is written, never synced: for tests, benchmarks, a RAM disk.

Given without `--dbDir` it stops the start: `corDB: --dbSync without --dbDir - the store is in RAM only, there is no disk for it to act on`.

An unknown value stops the start - with `--dbDir` only, as without it the option is not read:
`corDB: --dbSync 'sometimes': interval, request or none`.

    coraine --database corDB --dbDir /var/lib/coraine --dbSync request

### `--dbSyncInterval`

`-dbSyncInterval` · `CORAINE_DBSYNCINTERVAL` · Int · default: `100`
· values: 1-60000 (milliseconds) · plugin: corDB

The time between two rounds of the flusher, which writes and syncs every tenant's log. With
`--dbSync interval` it bounds what a machine that dies can lose; with `--dbSync request` a waiting write
starts a round at once, and the interval is the longest the log goes unsynced without one. A value out of
range stops the start: `coraine: value 0 for option '--dbSyncInterval' is inferior to its lower limit.
Allowed range: 1-60000`. Given without `--dbDir` it stops the start: `corDB: --dbSyncInterval without --dbDir - the store is in RAM only, there is no disk for it to act on`.

### `--dbSnapshotEvery`

`-dbSnapshotEvery` · `CORAINE_DBSNAPSHOTEVERY` · Int · default: `64`
· values: 1-65536 (MiB) · plugin: corDB

How much log a tenant writes before it is snapshotted: once it has written this many MiB of log **and** at
least as much log as its last snapshot was big, so that the snapshots of a growing store cost in proportion
to what is written. A snapshot is written on a thread of its own, in slices under the read lock; the log
before it is then deleted, and a start replays only the log after it. Every tenant is also snapshotted at a
clean stop (SIGTERM, SIGINT). Smaller: faster starts, more snapshot work while running.
Given without `--dbDir` it stops the start: `corDB: --dbSnapshotEvery without --dbDir - the store is in RAM only, there is no disk for it to act on`.

### `--dbCompress`

`-dbCompress` · `CORAINE_DBCOMPRESS` · Bool · default: `false`
· values: a flag · plugin: corDB

Compresses the snapshots and the finished log and history segments with zstd (level 3), on the snapshot
thread; the open segment, which writes append to, never. With it a segment rolls at 64 MiB instead of
1 GiB, so a history - which no snapshot drops - is mostly compressed segments. A start reads a compressed
store with the option or without it, and a start with it compresses the finished segments a run without it
left. libzstd (`libzstd.so.1`) is loaded only then; missing, the start is refused: `corDB: --dbCompress needs
libzstd.so.1 (zstd) - not found`. Given without `--dbDir` it stops the start: `corDB: --dbCompress without --dbDir - the store is in RAM only, there is no disk for it to act on`. On disk, 100 000 Entities: 74 MB plain, 1.4 MB
compressed ([persistence § 6a](https://github.com/SEAMWARE/corDB/blob/main/doc/persistence.md#6a---dbcompress)).

    coraine --database corDB --dbDir /var/lib/coraine --dbCompress

### `--dbLockPrefer`

`-dbLockPrefer` · `CORAINE_DBLOCKPREFER` · String · default: `reads`
· values: `reads`, `writes` · plugin: corDB (and ramDB)

Whom a tenant's store lock lets in first when readers and writers both wait. `reads` is glibc's default
read-write lock; `writes` makes it writer-preferring. Measured: writers first, a bare `PATCH` +2-4 %, a
`PATCH` that notifies -3-4 % (the match and the notification read the Entity)
([Performance](https://github.com/SEAMWARE/coraine/blob/main/doc/performance.md#writes-that-notify-and-which-writer-the-lock-lets-in-first)). Another value
stops the start: `corDB: --dbLockPrefer 'both': reads or writes`.

    coraine --database corDB --dbLockPrefer writes

### `--dbValueIndexes`

`-dbValueIndexes` · `CORAINE_DBVALUEINDEXES` · String · no default - no declared indexes (the admin API can add some)
· values: attribute IRIs, comma-separated, or a JSON array of `{ "attribute", "type"?, "tenant"? }`; at most 64 · plugin: corDB (and ramDB)

The attributes whose `q` value terms an index serves, declared at start. Each declaration is a value index
in the store of every tenant it covers - the attribute's values, each with its Entity, in order - serving
`==`, a range, a list, `<`, `<=`, `>` and `>=`. Not on disk: built by the first query that can use it, again
after every start; a change of the declarations (the admin API) builds every index again. The format, the
expanded names, and the start refused for a short name (`corDB: --dbValueIndexes: 'speed' is not an IRI -
...`) are as for mongoc's option of the same name. Needs a build with `COR_FEATURE_DB_INDEXES` (on by
default). [Declared indexes](https://github.com/SEAMWARE/coraine/blob/main/doc/installation.md#declared-indexes),
[corDB's README](https://github.com/SEAMWARE/corDB#queries-by-value---declared-indexes).

    coraine --database corDB --dbValueIndexes https://example.org/speed

### `--troeInstanceCap`

`-troeCap` · `CORAINE_TROEINSTANCECAP` · Int · default: `1000000`
· values: 1-1000000 · plugin: corDB, with `--troe corDB`

The most instances per Attribute a temporal retrieve or query returns when it has no `lastN` or `firstN`
(§ 6.4.7.3), oldest first. More there: the response is a `200` with the first page and a
`Link: <...?offsetN=<cap>>; rel="intervalafter"` header for the next. The default is a safety bound, not a
page size. Present only with `--troe corDB` (timescale has an option of the same name).

    $ coraine --database corDB --troe corDB --troeInstanceCap 2
    # an Entity created with speed 1, then PATCHed to 2, 3, 4
    $ curl -i localhost:1026/ngsi-ld/v1/temporal/entities/urn:ngsi-ld:T:capx1
    HTTP/1.1 200 OK
    Link: </ngsi-ld/v1/temporal/entities/urn:ngsi-ld:T:capx1?offsetN=2>;rel="intervalafter";type="application/json"

    {"id":"urn:ngsi-ld:T:capx1","type":"T","speed":[{"type":"Property","value":1,...},{"type":"Property","value":2,...}]}

<!-- end of the options of the corDB plugin -->

## Next

corsh, the maintenance tool ([the design](doc/persistence.md) § 10), and a PATCH that logs only the
attributes it touched. Further ideas: coraine's [Ideas](https://github.com/SEAMWARE/coraine/blob/main/doc/ideas.md).
