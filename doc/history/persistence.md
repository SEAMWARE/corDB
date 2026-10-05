# corDB persistence - history

How the persistence of [the design](../persistence.md) got to what it is: what measuring found, and
what changed because of it. Newest first.

## 2026-10-05 - an attribute update logs the attributes (`ATTRS_PUT`)

Every write logged the whole entity (`ENTITY_PUT`), a PATCH of one attribute too: encoded under the
write lock and copied into the log, the whole entity each time. Now PATCH of an attribute, merge,
update and append of attributes, `PUT` of an attribute, purge and batch merge log `ATTRS_PUT`: the
members the write named, each whole after it, and those it removed. Built from the entity's own member
nodes, nothing cloned. Measured with coraine's perfRun, PGO, against the build before it:

- `PATCH`: +11-13 % on disk (8 cores 127 762 → 144 465; with `--troe corDB` 107 860 → 119 962;
  nothing pinned 99 571 → 111 438); merge +4-9 %; in RAM, untouched, within ±3 %
- the log: twenty PATCHes of a small attribute of an entity with a 2 KB one - 47 194 bytes before,
  under 20 KB now

Batch update and upsert still log the entity: the broker hands the driver whole merged entities, and
the driver never sees which attributes changed. So does the attribute delete (a retrieve and a
replace in the broker) - `ATTRS_DELETE` is written by nobody yet.

## 2026-10-05 - the log segments memory-mapped: a dead broker loses nothing

A write appended its record to a buffer of the process; the flusher `write()`s and `fdatasync`s it
every 100 ms. A broker that died between two flushes - `kill -9`, a crash, an OOM kill - lost what the
buffer held: up to 100 ms of writes it had acknowledged. `cordb_persist_kill_default_sync` (coraine)
shows it: three writes and their history, then `kill -9` with `--dbSyncInterval 60000` - the store came
back empty. MongoDB's default loses nothing on a process death, only on a machine death.

Now the open segment is mapped and a write copies its record into the page cache under the lock: the
same test finds all of it. What it costs - perfRun, corDB on disk with `--troe corDB`,
8 cores, PGO, one run each, the buffered log (2026-10-04 22:57) against the mapped one (23:35):

| | buffered | mapped | |
|---|---:|---:|---:|
| create | 86 027 | 81 754 | −5.0 % |
| create, 1 connection | 23 004 | 22 418 | −2.5 % |
| `PATCH` | 106 000 | 103 004 | −2.8 % |
| `PATCH`, 1 connection | 35 110 | 35 271 | +0.5 % |
| merge | 78 048 | 75 357 | −3.4 % |
| batch update (20) | 19 931 | 18 237 | −8.5 % |
| batch create (20) | 7 413 | 7 051 | −4.9 % |
| `DELETE` | 180 645 | 179 772 | −0.5 % |
| batch delete (20) | 45 250 | 47 745 | +5.5 % |
| *reads, which do not touch the log* | | | −0.4 % to +3.0 % |

The reads say what one run's noise is: ±3 %. The writes that append the most per request lose 5-8 %.

**Tried and dropped: faulting the next pages in ahead of the writers.** The flusher, after its sync,
`madvise`d the 16 MiB past the records (`MADV_POPULATE_READ` or `_WRITE`, Linux 5.14), so a write
would copy into a page already mapped. `wrk`, 8 cores, plain `-O2`, two runs each, requests/s:

| | none | `POPULATE_READ` | `POPULATE_WRITE` |
|---|---:|---:|---:|
| `PATCH` | 135 741 / 133 758 | 123 500 / 124 182 | 130 441 / 129 319 |
| batch update (20) | 27 258 / 27 671 | 18 556 / 26 585 | 27 796 / 27 008 |
| create | 152 597 / 153 960 | 116 651 / 130 233 | 161 272 / 155 395 |
| batch create (20) | 21 461 / 22 745 | 20 043 / 22 723 | 20 728 / 17 815 |
| bytes to the disk, `PATCH` run | 487 / 481 MB | 444 / 446 MB | 765 / 762 MB |

`READ` slower everywhere; `WRITE` within the noise, and 58 % more to the disk - the zero pages made
dirty ahead are synced, then written again with the records. So it is not the first touch of a page
that the mapping costs; the likelier cost is the sync itself, which write-protects the pages it writes
out - the next copy into a page the records only half filled faults again, and the protection change
reaches every core the broker runs on. Not tried yet: one `write()` per record instead of the mapping
(in the kernel before the response as well, without those faults, at a system call per write).

## 2026-10-04 - the id table grows in steps

The entity-id table grew by rebuilding it, eight times larger, from the whole store, under the
tenant's write lock. Timed with a probe around the rebuild (release build, batch create, 20 per request):

| entities | rebuild |
|---|---|
| 4 097 | 0.3 ms |
| 32 769 | 4.5 ms |
| 262 145 | 40-44 ms |
| 2 097 153 | 313 ms |

Every request of the tenant waited for it. `wrk`, batch create of 20, 50 connections, 8 cores, 2 s
warmup + 5 s, PGO release broker, plain `-O2` plugins, two runs each:

| ramDB | p50 | p90 | p99 | max | requests/s |
|---|---|---|---|---|---|
| main | 0.95-0.96 ms | 1.42-1.52 ms | 152-217 ms | 208 ms | 47 500-47 581 |
| slots moved 32 per write | 0.83-0.85 ms | 1.26-1.31 ms | 2.53-2.70 ms | 6.8 ms | 52 542-52 874 |

corDB on disk with `--troe corDB`, same runs: p99 11.8-13.4 ms and 18 800-19 300 requests/s on both -
at ~19 000 requests/s the store stays under 2.1 million entities in 7 s, so the 313 ms rebuild never
came, and the 40 ms one fell in the warmup.

The rebuild also left the entry count at what it was before, so a table built from a loaded store
counted 0 and grew late; it is sized to the store and counted now.

A debug build (whose `indexCheck` aborts on a wrong lookup) took 50 000 creates in batches of 20 with
deletes, replaces and retrieves of earlier entities mixed in across the 4 096 and 32 768 growths, on
ramDB, on corDB with `--dbDir`, and after its restart: every live entity found with its value, every
deleted one 404.

## 2026-10-03 - measured, and what it found

Measured with coraine's `test/perf/perfRun.sh` (PGO release, one tenant, 50 connections unless said)
and with `wrk` on single scenarios, against the same broker without `--dbDir`.

### The snapshots

1. **A snapshot of a million entities hung the broker's stop.** The encode buffer grew by doubling an
   `int` size; past 1 GiB it overflowed and the loop never ended. The stop joined the thread that was
   in it. Snapshots are now encoded into a list of 64 MiB buffers, and the codec's buffer grows in 64
   bits and refuses past an `int` (the same overflow was in corTree's encoder - fixed there too).
2. **A log that reached 2 GiB could not be replayed.** With snapshots off, one segment grew to 3.27 GB;
   the reader's offsets are `int`s and recovery refused the file. A segment now rolls at 1 GiB on its
   own, snapshot or not.
3. **A growing store was snapshotted with the square of its size.** The trigger was "every 64 MiB of
   log": 8 snapshots in 10 s of creates, each of the whole store. It became "64 MiB AND as much log as
   the last snapshot was big".
4. **Writers waited for whole snapshots.** A snapshot held the tenant's write lock while it encoded the
   store: p99 of a create 241 ms, of a one-connection create 85 ms. Now only its start takes the write
   lock; the store is encoded in slices of 1000 entities under the read lock, writers running in
   between - correct without one consistent instant, because the records are effects and recovery
   replays the log from the snapshot's start. Stress-tested: creates and 78 230 PATCHes during 6
   snapshots, `kill -9`, restart - the count, the patched values, the first and last 1000 entities in
   order identical. The longest a writer waited on a snapshot: 8 ms.
5. **The start still synced under the write lock** (the buffered records into the old segment): a
   create's p99 123 ms. The start now only swaps the buffer out under it; the old segment is written
   and synced after.
6. **Writers barely got in between slices.** glibc's rwlock prefers readers - the slice's read lock was
   taken straight back before a woken writer ran. 50 µs between slices.
7. **Snapshots ran back to back.** The "log since the last snapshot" restarted when a snapshot
   finished, and the flusher re-armed the next one while it ran. It restarts at the start now, and no
   snapshot is armed while one runs. 10 s of creates: 75 164 req/s (61 012 before), 4 snapshots (7).

The snapshots run on a thread of their own (`corDbSnapshot`): a snapshot of a big store takes seconds,
and on the flusher's thread every sync - and every `--dbSync request` writer - waited behind it.

### The write path

- **A record's body is encoded before the write lock** where the entity exists before it (create,
  replace, batch create and update) - and so the CRC covers the body first, then the header. Batch
  create, snapshots off: 19 088 to 30 692 req/s (no `--dbDir`: 44 854).
- **What a single create still pays** (~25 %, snapshots off): ~3 µs of CPU a request for the encode,
  on HTTP workers that are CPU-bound, and ~10 % more lock waits. Cleared on the way, measured: page
  faults (the same, 0.74 against 0.76 a request), PGO treating the log as cold code (a plain `-O2`
  build loses the same), the append under the lock (built to do nothing: noise).
