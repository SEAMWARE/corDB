# corDB history (TRoE) - how it got here

The design is [troe.md](../troe.md). This is what measuring found on the way. Newest first.

## 2026-10-04 - phase 2: reading history

The temporal retrieve and query answer from the index with every filter but q, geoQ and aggregation.
How they were checked, and what that found:

- **The timescale tests as the oracle.** `corTest -db corDB -troeDb corDB` runs every `--troe
  timescale` test on `--troe corDB` with the same expectations; seven that read timescale's tables
  (psql) are timescale's only (`REQUIRE_TROEDB: timescale`). Before phase 2: 13 of 70 passed; after it,
  38 of 63 - every one left needs phase 3 (q, geoQ, aggregation) or phase 5 (the temporal writes).
- **Two expectations had captured a coin flip.** Instances with no observedAt (the default time
  property) all tie, and PostgreSQL returned ties in whatever order the plan gave: two tests expected
  3, 2, 1 and 10, 5 where the spec says ascending. timescale now breaks ties by modified_at (the
  order written), corDB the same, and the tests say 1, 2, 3 and 5, 10.
- **The instance cap** was 100 in corDB, a constant copied from timescale's source - whose real
  default is its `--troeInstanceCap` option's, 1 000 000. corDB has the option now, with that default.
- **Missing from corDB's history**: the Scope (§ 5.3.2.5 - a plain member in current state, so the
  write sites skipped it), now recorded; and recorded when it should not be: a bridge's placeholders
  (`"uninitialized"`), which timescale's event path keeps out by design - now kept out by
  `corNgsild.troeEntityOnly` around that write.
- **instanceIds** differ in form (timescale a UUID, corDB its hex:counter, as every id coraine
  generates); the tests take both.
- **Measured** against timescale on the same history (doc/troe.md § 6): a retrieve 6×, a query of 100
  entities 43× - timescale answers a query with three SQL round trips per entity (the counts for the
  page, the instances, the entity's timestamps).

## 2026-10-04 - CPU per instance, and three costs removed

### Measuring the right build

A local `make pgo` in coraine ended with `make di` on every lib it had built with the profile - and
corDB is a plugin, not an archive linked into the PGO broker, so the **debug** corDB.so (no `-O`,
traces compiled in) stayed installed beside a profile-guided broker. Every local perf number with
corDB from 2026-10-03 12:40 (corDB's move to its own repository) to this fix measured debug corDB
code on both sides of the comparison. coraine's `make install_pgo` now installs corDB's PGO release
plugins; the numbers below were measured with it, the ones before it were measured again.

### The method

Broker CPU (utime + stime of every thread, from `/proc/<pid>/stat`) per request, over 3 × 10 s of a
fixed workload with `--troe none` and with `--troe corDB`, broker pinned to 2 cores. The difference,
divided by the instances a request writes, is the CPU an instance costs. Then a profile of each, per
request, symbol by symbol: the difference is where it goes.

### What it found, and what changed

| µs per instance, in RAM | before | the replace fix | the arena and the lookups |
|---|---:|---:|---:|
| batch create (100 per request) | 1.4 | 1.4 | 1.4 |
| create (5) | 1.8 | 1.8 | 1.4 |
| PATCH (1) | 3.9 | 3.6 | 3.0 |
| batch update (20) | 7.8 | 5.6 | 2.0 |

1. **A replace encoded every instance and kept one in five.** A batch update is a replace in corDB:
   the whole merged entity is stored, and its history was prepared from all of it - five attributes
   encoded, four thrown away because their `modifiedAt` had not changed. Now only the instances stamped
   with the write's own time are prepared; any other one that differs from the stored one is encoded
   under the lock (the same set of instances is recorded as before).
2. **malloc and linear lookups were most of what remained.** The batch-update profile, per request,
   with history against without: `corTreeLookup` +24 µs, malloc/free +27 µs (a heap that only grows,
   one long-lived malloc per instance), the encoding itself ~+7 µs.
   - **The arena**: encoded records go into the request's arena, are copied into their queue item
     (one short-lived malloc, struct + strings + body), and from there into a per-tenant arena of
     1 MiB chunks - no malloc per instance kept.
   - **The lookups**: an instance's instanceId and four timestamps are read in one pass over its
     members (five walks before); a replace walks its old and new entity with a cursor (both keep
     their attributes in the same order); an instance the write stamped is recorded without comparing.

What did not change: batch create, 1.4 µs, the encoding itself - the next step there is not to
encode at all what the current-state log has already encoded (the "one log" idea).

Throughput (perfRun, 2 cores), what history costs, before (debug corDB.so, 2026-10-04 night) → now:
batch update −55 % → −21 %, PATCH c50 −26 % → −21 %, create c50 −35 % → −29 %, merge −34 % → −35 %,
batch create −58 % → −53 %, delete −13 % → −4 %.

## 2026-10-04 (night) - two things that did not help

Measured on the debug corDB.so (above), against each other, so the comparisons hold:

- **Less malloc in the encoding** (a reusable per-thread encode buffer, interned attribute names and
  entity types, the first two instances inline in their attribute): no measurable change in
  throughput.
- **History after the write lock** (a per-tenant queue, drained by whoever releases the lock): no
  measurable change either. With the broker CPU-bound on 2 cores, moving work out of the lock does not
  make it cheaper - it is CPU per instance, which is what the measurement above then looked for.

Both stay: the queue keeps the write lock short, which matters as soon as the broker is not CPU-bound.

## 2026-10-03 - phase 1

History recorded at the write sites, the RAM index, `hist-N.cor` segments, recovery, the temporal
retrieve. The first numbers were measured on a debug build by mistake and are not quoted.
