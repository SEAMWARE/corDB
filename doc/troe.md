# corDB history - TRoE in the store (`--troe corDB`)

How it got here - what measuring found, and what changed: [the history](history/troe.md).

With `--database corDB --troe corDB`, corDB keeps the temporal history itself: no second plugin, no
second database. The broker takes `troeRegister` from corDB's current-state plugin and refuses
`--troe corDB` with any other `--database` (a separate temporal store is the timescale plugin).

## 1. What is recorded

History is captured where current state changes - corDB's write sites - not from the broker's TRoE
events (their hooks are NULL with `--troe corDB`). Current state overwrites; history appends.

- **Every write that reaches an attribute appends an instance of it**: create, replace, merge, PATCH,
  batch create / update / upsert / merge. A PATCH with an unchanged value still appends one - it
  changes `modifiedAt`.
- **A replace records only what it wrote.** The instances it carried over unchanged (the same
  `modifiedAt` as the stored ones) are not recorded again.
- **A deleted attribute instance** appends a tombstone: its kind, the value `urn:ngsi-ld:null`, and
  `deletedAt`.
- **The Scope** is recorded as a Property (§ 5.3.2.5) - in current state a plain member, no instance:
  when an entity is created with one, when a write changes it, and its tombstone when it goes.
- **A write whose instances are no value anybody supplied** - a bridge's placeholders,
  `"uninitialized"` - records the entity's event only (`corNgsild.troeEntityOnly`, set around the write).
- **Entity events** - created, replaced, deleted - are records of their own: `{ id, type, entityOp, at }`.
  A deleted entity's history stays.

- **History imported from another store** (coraine-import, the broker's doc/migration.md) comes in
  through `TroeDriver.historyImport`, not through the write sites: the entity events at their time,
  every instance with the instanceId, createdAt, modifiedAt and observedAt it had in the source - into
  the index and the history log as the temporal API's own writes are. A deletion is the tombstone a
  live deletion writes (the type the record gives, else that of the attribute's last instance). The
  import writes the current state with `corNgsild.troeSkip`, so nothing else is recorded for it. A batch
  is refused whole when the first instance of one of its attributes is there already (a re-import).

An instance record is `{ id, type?, attr, datasetId?, deletedAt?, instanceId, instance }`, cor binary.
The instanceId is the one the instance carries, or one generated; it sits beside the instance in the
record and is put back into it on a read.

Not recorded selectively yet: the selector (which writes history keeps) and "the entity at time T" are
not in the spec and are backlog. The records carry every timestamp and their own operation, so
neither needs a format change.

## 2. Where it lives

- **In RAM**, per tenant: entity id → attribute → instances in the order written. An instance is its
  encoded record plus the four timestamps a filter reads without decoding (`observedAt`, `createdAt`,
  `modifiedAt`, `deletedAt`). Attribute names and entity types are interned; the first two instances
  of an attribute are inline in it. The encoded records and named dataset ids are appended to a
  per-tenant arena of 1 MiB chunks - history only grows (until retention, which drops whole chunks).
- **On disk**, with `--dbDir`: `hist-N.cor` segments beside the current-state log, the same 28-byte
  record header and CRC-32C, a new segment at 1 GiB. Snapshots never remove them - they are the
  history. Without `--dbDir` history is RAM only.

## 3. The write path

```
  before the write lock:  the records a create / replace / batch will need, encoded
  under the write lock:   current state changed; its history QUEUED (one malloc per item, in lock order)
  after the write lock:   the queue drained under the tenant's history mutex - index + hist log
  answer
```

The queue is drained before the answer, so a temporal read right after a write sees it. A temporal
read drains first, then reads under the history mutex. With `--dbSync request` a write answers once
its current-state **and** its history records are synced.

## 4. Recovery

The `hist-N.cor` segments are replayed in order after the current state is back; an instance keeps
the instanceId it was given. A torn tail is cut in the newest segment only.

## 5. Reading it - the temporal API

`GET /temporal/entities/{id}`, `GET /temporal/entities` and `POST /temporal/entityOperations/query`
answer from the index, under the history mutex (what the writes have queued is applied first). The
answer is the one the timescale plugin gives - the same functional tests check both
(`corTest -db corDB -troeDb corDB`):

| parameter | |
|---|---|
| `attrs`, `datasetId` | which attributes, which instances (`@none`: the default one) |
| `timeproperty` | `observedAt` (default), `modifiedAt`, `createdAt` (the instances that created their attribute), `deletedAt` (the tombstones) |
| `timerel`, `timeAt`, `endTimeAt` | `before` exclusive, `after` inclusive, `between` = [timeAt, endTimeAt) - an instance without the time property is in no window |
| `lastN`, `firstN`, `offsetN`, `--troeInstanceCap` | the page, per attribute and datasetId: descending for lastN, ascending otherwise; the cap (default 1 000 000) when neither is given |
| `id`, `type`, `idPattern`, `limit`, `offset`, `count` | the query: entities with at least one instance in the window, by id |
| `q` | an entity whose history has, for each term, an instance that satisfies it - anywhere in the history, as timescale's EXISTS; `!attr`: no instance |
| `georel`, `geometry`, `coordinates`, `geoproperty` | an entity with a GeoProperty instance in the window that satisfies the georel - the current-state store's GEOS matcher |
| `aggrMethods`, `aggrPeriodDuration` | computed by the broker from the instances (corDB declines the push-down) |

- **Order**: per attribute the default instance first, then by datasetId; by the time property; an
  instance without it last; equal times in the order they were written.
- **The entity** carries `createdAt` and `modifiedAt` (sysAttrs - the broker strips them unless asked)
  and `deletedAt` when it was deleted: its history stays.
## 6. Writing it - the temporal API's own writes

The correction path: history otherwise comes in from current state (§ 1).

| request | |
|---|---|
| `POST /temporal/entities` | create - or, the entity there already, append (§ 5.6.11.4: 204); its types the list given |
| `POST /temporal/entities/{id}/attrs` | append; new type names added to the entity's (§ 11.2.3.4) |
| `DELETE /temporal/entities/{id}` | the entity's whole history |
| `DELETE /temporal/entities/{id}/attrs/{attr}` | the default instances, one `datasetId`'s, or all (`deleteAll`) |
| `PATCH /temporal/entities/{id}/attrs/{attr}/{instanceId}` | the value replaced, `observedAt` if given, `modifiedAt` now; type, createdAt, instanceId kept |
| `DELETE /temporal/entities/{id}/attrs/{attr}/{instanceId}` | one instance |

- Appended instances are a millisecond apart (`createdAt` = `modifiedAt` = the request's time + n ms),
  so the order given is the order written - as timescale does. A supplied instanceId is ignored: it is
  the system's to give.
- Every write is a record of the history log: an added instance like any other, a removal or a
  modification `{ id, histOp, ... }` - applied by one function, live after logging it and at recovery.
- A removed instance's record stays in the arena, unreferenced, until retention exists to reclaim it.
- An entity's types: a list, in the order given; one renders as a string, several as an array
  (§ 5.2.6.4.2); the query's `type` matches any of them.

## 7. What it costs

Broker CPU per history instance, `--troe corDB` against `--troe none` on the same workload (PGO
release, broker pinned to 2 cores, AMD Ryzen 9 8940HX, 2026-10-04):

| write | instances per request | µs per instance, in RAM | with `--dbDir` |
|---|---:|---:|---:|
| batch create (20 entities × 5 attributes) | 100 | 1.4 | 1.7 |
| create (5 attributes) | 5 | 1.4 | 1.5 |
| batch update (20 entities, 1 attribute) | 20 | 2.0 | 2.2 |
| PATCH (1 attribute) | 1 | 3.0 | 2.9 |

A PATCH pays a fixed part per write (the history entity's lookup, the queue item, the mutexes) that a
batch spreads over many instances.

Throughput, coraine's `test/perf/perfRun.sh corDB`, same machine and pinning, requests/s:

| scenario | `--troe none` | `--troe corDB` | change | `--dbDir`, none | `--dbDir`, corDB | change |
|---|---:|---:|---:|---:|---:|---:|
| query, `limit=20`, c50 | 22 721 | 24 664 | +9 % | 24 670 | 24 455 | −1 % |
| `GET /entities/{id}`, c50 | 160 516 | 168 580 | +5 % | 167 942 | 171 759 | +2 % |
| `PATCH`, c50 | 169 132 | 134 111 | −21 % | 133 511 | 105 568 | −21 % |
| `PATCH`, c1 | 40 174 | 38 212 | −5 % | 38 702 | 35 824 | −7 % |
| merge, c50 | 137 425 | 89 594 | −35 % | 105 429 | 78 619 | −25 % |
| `DELETE`, c50 | 164 379 | 158 592 | −4 % | 162 191 | 147 963 | −9 % |
| batch update (20), c50 | 14 112 | 11 202 | −21 % | 11 605 | 8 784 | −24 % |
| batch delete (20), c50 | 34 812 | 32 524 | −7 % | 36 874 | 27 063 | −27 % |
| create, c50 | 102 516 | 73 158 | −29 % | 79 230 | 56 286 | −29 % |
| create, c1 | 31 456 | 25 670 | −18 % | 28 276 | 24 946 | −12 % |
| batch create (20), c50 | 15 994 | 7 456 | −53 % | 11 026 | 4 964 | −55 % |

Reads do not touch history (their spread, ±9 %, is the run-to-run noise of this machine). A write
loses what its instances cost: batch create, a hundred instances per request, the most.

Reading it, against timescale (`--troe timescale`, PostgreSQL 17 + TimescaleDB on the same machine,
not pinned - so if anything favoured), the same broker and the same history (100 vehicles, 51 instances
of speed each), broker pinned to 2 cores, 50 connections, median of 3 × 10 s, requests/s:

| read | corDB | timescale | |
|---|---:|---:|---:|
| `GET /temporal/entities/{id}?lastN=10` | 56 190 | 9 388 | 6× |
| `GET /temporal/entities/{id}` (all of it) | 32 144 | 5 398 | 6× |
| `GET /temporal/entities?type=…&timerel=after&lastN=1` (100 entities) | 7 239 | 170 | 43× |

