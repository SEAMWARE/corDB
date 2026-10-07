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

[The design](doc/persistence.md), [how it got here](doc/history/persistence.md); what it costs: coraine's
[performance](https://github.com/SEAMWARE/coraine/blob/main/doc/performance.md), "corDB on disk".

## History (TRoE)

`--database corDB --troe corDB`: every write that reaches an attribute appends an instance, captured
where current state changes; on disk with `--dbDir` (`hist-N.cor`). [The design](doc/troe.md),
[how it got here](doc/history/troe.md).

## Next

corsh, the maintenance tool ([the design](doc/persistence.md) § 10), and a PATCH that logs only the
attributes it touched. Further ideas: coraine's [Ideas](https://github.com/SEAMWARE/coraine/blob/main/doc/ideas.md).
