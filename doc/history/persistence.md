# corDB persistence - history

How the persistence of [the design](../persistence.md) got to what it is: what measuring found, and
what changed because of it. Newest first.

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
