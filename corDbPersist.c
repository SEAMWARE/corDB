//
// FILE            corDbPersist.c
//
// AUTHOR          Ken Zangelin
//
// Copyright 2026 Seamware
// SPDX-License-Identifier: Apache-2.0
//
// Persistence - doc/persistence.md § 2, § 5, § 5a, § 6.
//
//   <--dbDir>/<tenant>/snap-000041.cor   the store as of a sequence number: every entity, subscription
//                                       and registration, as PUT records - recovery is one replay loop
//   <--dbDir>/<tenant>/log-000041.cor    every write after that snapshot
//
// A snapshot is taken under the tenant's write lock, at a segment switch: the records up to it go to
// log-N, the snapshot is snap-N+1, the writes after it go to log-N+1. Written as .tmp, synced,
// renamed: a snapshot is complete or not there. Only then are snap-N and log-N deleted.
//
// A tenant's directory is its name with every byte outside [A-Za-z0-9-] written %XX - a tenant name is
// whatever the NGSILD-Tenant header said, '/' and '..' included - and the default tenant's is '_',
// which no escaped name can be.
//
#define _GNU_SOURCE                                    // pthread_setname_np
#include <errno.h>                                     // errno
#include <fcntl.h>                                     // open, O_*
#include <pthread.h>                                   // pthread_*
#include <stdbool.h>                                   // bool
#include <stdint.h>                                    // uint64_t
#include <stdio.h>                                     // snprintf
#include <stdlib.h>                                    // free
#include <string.h>                                    // strcmp, strerror, strlen
#include <dirent.h>                                    // opendir, readdir
#include <sys/stat.h>                                  // mkdir
#include <time.h>                                      // clock_gettime
#include <unistd.h>                                    // write, fdatasync, close

#include "corLog/corLog.h"                             // COR_E, COR_I, COR_W
#include "corAlloc/CorAlloc.h"                         // CorAlloc
#include "corAlloc/corAllocBufferInit.h"               // corAllocBufferInit
#include "corAlloc/corAllocBufferReset.h"              // corAllocBufferReset
#include "corBase/corCrc32c.h"                         // corCrc32c
#include "corBase/corCoLoop.h"                         // corCoBlocking
#include "corTree/CorNode.h"                           // CorNode
#include "corTree/corTreeLookup.h"                     // corTreeLookup
#include "corRest/CorRestState.h"                      // corRestP

#include "db/Tenant.h"                                 // Tenant, tenantGetOrCreate

#include "corDB/corDbGlobals.h"                        // corDbDir, corDbSync, corDbSyncInterval
#include "corDB/corDbLog.h"                            // corDbLogEncode
#include "corDB/corDbHistory.h"                        // corDbHistoryEntity, corDbHistoryInstanceAdd
#include "corDB/corDbReplay.h"                         // corDbReplay
#include "corDB/corDbStore.h"                          // CorDbStore
#include "corDB/corDbPersist.h"                        // Own interface



// -----------------------------------------------------------------------------
//
// SyncMode - --dbSync
//
typedef enum SyncMode
{
  SyncInterval,                                        // the flusher syncs every --dbSyncInterval ms
  SyncRequest,                                         // ... and a write waits for its record's sync
  SyncNone                                             // written, never synced
} SyncMode;



// -----------------------------------------------------------------------------
//
// The flusher's state. 'flushMutex' guards the list head, 'kicked' and 'stopping'; the list only
// grows (at its head) until corDbPersistClose, so the flusher walks it without the mutex.
//
static SyncMode         syncMode     = SyncInterval;
static CorDbPersist*    persistList  = NULL;
static pthread_mutex_t  flushMutex   = PTHREAD_MUTEX_INITIALIZER;
static pthread_cond_t   kickCond;                     // the flusher waits on it: the interval, or a kick
static pthread_cond_t   syncedCond   = PTHREAD_COND_INITIALIZER;   // --dbSync request waits on it
static bool             kicked       = false;
static bool             stopping     = false;
static bool             flusherUp    = false;
static pthread_t        flusherTid;
static pthread_cond_t   snapCond     = PTHREAD_COND_INITIALIZER;   // the snapshotter waits on it
static bool             snapKicked   = false;
static pthread_t        snapTid;



// -----------------------------------------------------------------------------
//
// What this thread appended under the write lock it holds - for corDbPersistSyncWait, called by the
// lock's release. Set and read with no yield between: a coroutine cannot yield holding a pthread lock.
//
static __thread CorDbPersist* pendingP   = NULL;
static __thread uint64_t      pendingSeq = 0;



// -----------------------------------------------------------------------------
//
// nowNs - the record's system time
//
static uint64_t nowNs(void)
{
  struct timespec ts;

  clock_gettime(CLOCK_REALTIME, &ts);
  return (uint64_t) ts.tv_sec * 1000000000ULL + (uint64_t) ts.tv_nsec;
}



// -----------------------------------------------------------------------------
//
// tenantDir - the tenant's directory name, escaped (see the top of the file)
//
static void tenantDir(const char* name, char* out, int outSize)
{
  if (name[0] == 0)
  {
    snprintf(out, outSize, "_");
    return;
  }

  static const char hex[] = "0123456789ABCDEF";
  int               o     = 0;

  for (const unsigned char* p = (const unsigned char*) name; *p != 0 && o < outSize - 4; p++)
  {
    if (((*p >= 'a') && (*p <= 'z')) || ((*p >= 'A') && (*p <= 'Z')) || ((*p >= '0') && (*p <= '9')) || (*p == '-'))
      out[o++] = (char) *p;
    else
    {
      out[o++] = '%';
      out[o++] = hex[*p >> 4];
      out[o++] = hex[*p & 0xF];
    }
  }

  out[o] = 0;
}



// -----------------------------------------------------------------------------
//
// writeAll - n bytes to fd, through short writes and EINTR
//
static bool writeAll(int fd, const char* buf, int n)
{
  while (n > 0)
  {
    ssize_t w = write(fd, buf, n);

    if (w < 0)
    {
      if (errno == EINTR)
        continue;
      return false;
    }

    buf += w;
    n   -= (int) w;
  }

  return true;
}



// -----------------------------------------------------------------------------
//
// filePath - <dir>/<kind>-<n>.<suffix>
//
static void filePath(CorDbPersist* pP, const char* kind, unsigned int n, const char* suffix, char* out, int outSize)
{
  snprintf(out, outSize, "%s/%s-%06u.%s", pP->dir, kind, n, suffix);
}



// -----------------------------------------------------------------------------
//
// segmentNext - the log continues in log-<segment + 1>; false if that file cannot be opened
//
// Under ioMutex. The old segment is complete: everything in it is written (and synced, by the
// flush that called this or the snapshot that did).
//
static bool segmentNext(CorDbPersist* pP)
{
  char path[600];

  filePath(pP, "log", pP->segment + 1, "cor", path, sizeof(path));

  int fd = open(path, O_WRONLY | O_CREAT | O_APPEND | O_CLOEXEC, 0600);

  if (fd < 0)
  {
    COR_E("corDB: the next log segment '%s': %s - '%s' goes on", path, strerror(errno), pP->path);
    return false;
  }

  close(pP->fd);
  pP->fd       = fd;
  pP->segment += 1;
  pP->segBytes = 0;
  strcpy(pP->path, path);

  return true;
}



// -----------------------------------------------------------------------------
//
// histSegmentNext - the history log continues in hist-<histSegment + 1> (under ioMutex)
//
static bool histSegmentNext(CorDbPersist* pP)
{
  char path[600];

  filePath(pP, "hist", pP->histSegment + 1, "cor", path, sizeof(path));

  int fd = open(path, O_WRONLY | O_CREAT | O_APPEND | O_CLOEXEC, 0600);

  if (fd < 0)
  {
    COR_E("corDB: the next history segment '%s': %s - '%s' goes on", path, strerror(errno), pP->histPath);
    return false;
  }

  close(pP->histFd);
  pP->histFd       = fd;
  pP->histSegment += 1;
  pP->histSegBytes = 0;
  strcpy(pP->histPath, path);

  return true;
}



// -----------------------------------------------------------------------------
//
// flushLocked - the buffer swapped out under 'mutex', written and synced with only 'ioMutex' held
//
static void flushLocked(CorDbPersist* pP)
{
  pthread_mutex_lock(&pP->mutex);

  CorBinBuffer out  = pP->buf;
  uint64_t     last = pP->seq;
  CorBinBuffer hist = pP->histBuf;

  pP->buf          = pP->spare;
  pP->buf.len      = 0;
  pP->histBuf      = pP->histSpare;
  pP->histBuf.len  = 0;
  pthread_mutex_unlock(&pP->mutex);

  bool ok = true;

  //
  // History first: a write's history and its current state reach the disk in the same round, before
  // syncedSeq says the write is there - so --dbSync request covers both
  //
  if (hist.len > 0)
  {
    if ((writeAll(pP->histFd, hist.buf, hist.len) == false) || ((syncMode != SyncNone) && (fdatasync(pP->histFd) != 0)))
    {
      COR_E("corDB: writing the history log '%s': %s - what was buffered may NOT be on the disk", pP->histPath, strerror(errno));
      ok = false;
    }

    pP->histSegBytes += (unsigned long long) hist.len;
    if (pP->histSegBytes >= 1024ULL * 1024 * 1024)
      histSegmentNext(pP);
  }

  hist.len      = 0;
  pP->histSpare = hist;

  if (out.len > 0)
  {
    if (writeAll(pP->fd, out.buf, out.len) == false)
    {
      COR_E("corDB: writing the log '%s': %s - what was buffered is NOT on the disk", pP->path, strerror(errno));
      ok = false;
    }
    else if ((syncMode != SyncNone) && (fdatasync(pP->fd) != 0))
    {
      COR_E("corDB: fdatasync of the log '%s': %s - what was buffered may NOT be on the disk", pP->path, strerror(errno));
      ok = false;
    }

    pP->segBytes       += (unsigned long long) out.len;
    pP->sinceSnapBytes += (unsigned long long) out.len;

    //
    // A snapshot is due after --dbSnapshotEvery MiB of log AND at least as much log as the last
    // snapshot was big. Every 64 MiB alone, a growing store was snapshotted whole, under its write
    // lock, again and again - 8 times in 10 s of creates, the work growing with the square of the
    // store. Tied to the snapshot's size, the snapshots cost in proportion to what is written.
    //
    unsigned long long every = (unsigned long long) corDbSnapshotEvery * 1024 * 1024;

    if ((pP->snapshotting == false) && (pP->sinceSnapBytes >= every) && (pP->sinceSnapBytes >= pP->lastSnapBytes))
      pP->snapshotDue = true;

    //
    // And the segment rolls at 1 GiB whatever the snapshots do: the reader's offsets are ints, so no
    // file may reach 2 GiB - a log that did (snapshots rare, or failing) could not be replayed
    //
    if (pP->segBytes >= 1024ULL * 1024 * 1024)
      segmentNext(pP);
  }

  out.len   = 0;
  pP->spare = out;                                     // only under ioMutex: one flush at a time

  if (ok)
    __atomic_store_n(&pP->syncedSeq, last, __ATOMIC_RELEASE);
  else
    __atomic_store_n(&pP->failed, true, __ATOMIC_RELEASE);
}



// -----------------------------------------------------------------------------
//
// flushOne - one tenant's buffer, written and synced
//
static void flushOne(CorDbPersist* pP)
{
  pthread_mutex_lock(&pP->ioMutex);
  flushLocked(pP);
  pthread_mutex_unlock(&pP->ioMutex);
}



// -----------------------------------------------------------------------------
//
// syncDir - a rename made durable
//
static void syncDir(const char* dir)
{
  int fd = open(dir, O_RDONLY | O_DIRECTORY | O_CLOEXEC);

  if (fd >= 0)
  {
    fsync(fd);
    close(fd);
  }
}



// -----------------------------------------------------------------------------
//
// nameNumber - the n of "<kind>-<n>.<suffix>"; false if the name is not one
//
static bool nameNumber(const char* name, const char* kind, const char* suffix, unsigned int* nP)
{
  int kindLen = strlen(kind);

  if ((strncmp(name, kind, kindLen) != 0) || (name[kindLen] != '-'))
    return false;

  char*         endP;
  unsigned long n = strtoul(&name[kindLen + 1], &endP, 10);

  if ((endP == &name[kindLen + 1]) || (*endP != '.') || (strcmp(&endP[1], suffix) != 0))
    return false;

  *nP = (unsigned int) n;
  return true;
}



// -----------------------------------------------------------------------------
//
// dropBefore - the snapshots and log segments older than segment n deleted: n's snapshot holds them
//
static void dropBefore(CorDbPersist* pP, unsigned int n)
{
  DIR* dirP = opendir(pP->dir);

  if (dirP == NULL)
    return;

  struct dirent* entryP;

  while ((entryP = readdir(dirP)) != NULL)
  {
    unsigned int k;

    if ((nameNumber(entryP->d_name, "snap", "cor", &k) || nameNumber(entryP->d_name, "log", "cor", &k)) && (k < n))
    {
      char path[1024];

      snprintf(path, sizeof(path), "%s/%s", pP->dir, entryP->d_name);
      if (unlink(path) != 0)
        COR_W("corDB: deleting '%s': %s", path, strerror(errno));
    }
  }

  closedir(dirP);
}



// -----------------------------------------------------------------------------
//
// Chunks - an encoded snapshot, in buffers of about 64 MiB: no one buffer for the whole store, which
// past 2 GiB is more than a CorBinBuffer can hold
//
typedef struct Chunks
{
  CorBinBuffer*  v;
  int            n;
  int            size;
} Chunks;



// -----------------------------------------------------------------------------
//
// chunkCurrent - the buffer the next record goes into; a new one once the current is 64 MiB
//
static CorBinBuffer* chunkCurrent(Chunks* cP)
{
  if ((cP->n > 0) && (cP->v[cP->n - 1].len < 64 * 1024 * 1024))
    return &cP->v[cP->n - 1];

  if (cP->n == cP->size)
  {
    int           size = (cP->size == 0) ? 8 : cP->size * 2;
    CorBinBuffer* v    = (CorBinBuffer*) realloc(cP->v, size * sizeof(CorBinBuffer));

    if (v == NULL)
      return NULL;

    cP->v    = v;
    cP->size = size;
  }

  CorBinBuffer* bP = &cP->v[cP->n++];

  bP->buf  = NULL;
  bP->len  = 0;
  bP->size = 0;

  return bP;
}



// -----------------------------------------------------------------------------
//
// chunksFree -
//
static void chunksFree(Chunks* cP)
{
  for (int i = 0; i < cP->n; i++)
    free(cP->v[i].buf);

  free(cP->v);
}



// -----------------------------------------------------------------------------
//
// encodeArray - every member of a store array as a record of 'op'
//
static bool encodeArray(CorNode* arrayP, CorDbLogOp op, Chunks* cP, uint64_t seq, uint64_t t)
{
  for (CorNode* nodeP = arrayP->value.head; nodeP != NULL; nodeP = nodeP->next)
  {
    CorBinBuffer* bP = chunkCurrent(cP);

    if ((bP == NULL) || (corDbLogEncode(bP, op, seq, t, nodeP) == false))
      return false;
  }

  return true;
}



// -----------------------------------------------------------------------------
//
// SNAPSHOT_SLICE - entities encoded per hold of the read lock: about a millisecond
//
enum { SNAPSHOT_SLICE = 1000 };



// -----------------------------------------------------------------------------
//
// snapshot - the tenant's store as snap-N+1, the log continued in log-N+1, the older files deleted
//
// Under the write lock only the start: what is buffered goes to log-N, the segment switches, the
// cursor is set on the first entity. Then the store is encoded in slices of SNAPSHOT_SLICE entities,
// each under the READ lock - readers run beside it, writers between the slices.
//
// So the snapshot is not the store at one instant, and does not need to be: the records are effects
// (a whole entity put, an id deleted), and recovery replays log-N+1 - every write since the start -
// on top of it. A write that a slice saw is replayed to the same state; one it missed is replayed to
// its state; an entity deleted after a slice took it is deleted again. The creation order holds: a
// slice walks the list in its order, and an entity created during the snapshot is at its end in both.
//
// Held under the write lock, the whole store was encoded at once and every writer waited: p99 of a
// create 241 ms while a growing store was snapshotted.
//
// A snapshot that fails leaves snap-N and every log after it - recovery replays the longer way,
// nothing is lost.
//
static void snapshot(CorDbPersist* pP)
{
  CorDbStore*     storeP = pP->storeP;
  Chunks          snap   = { NULL, 0, 0 };
  bool            ok     = false;
  unsigned int    n;
  struct timespec t0;
  struct timespec t1;

  uint64_t        seq    = 0;
  long            maxUs  = 0;                          // the longest any writer could have waited on it

  //
  // The start. ioMutex first - the flusher waits, so whatever is appended from here on stays in the
  // buffer for the NEW segment - then, under the tenant's write lock, only what touches no disk: the
  // buffered records (the old segment's) swapped out, the sequence, the cursor. They are written and
  // synced into the old segment after the lock: a writer never waits for the disk here (syncing under
  // the lock was a create's p99 of 123 ms).
  //
  // Lock order: ioMutex, then the tenant's lock, then 'mutex' - as everywhere: a writer takes the
  // tenant's lock and 'mutex', the flusher ioMutex and 'mutex'.
  //
  pthread_mutex_lock(&pP->ioMutex);

  clock_gettime(CLOCK_MONOTONIC, &t0);
  pthread_rwlock_wrlock(&storeP->lock);
  pthread_mutex_lock(&pP->mutex);

  CorBinBuffer old     = pP->buf;
  uint64_t     oldLast = pP->seq;

  pP->buf             = pP->spare;
  pP->buf.len         = 0;
  pP->spare           = (CorBinBuffer) { NULL, 0, 0 };
  seq                 = pP->seq;
  storeP->snapCursor  = storeP->entities->value.head;
  pP->snapshotDue     = false;
  pP->snapshotting    = true;
  pP->sinceSnapBytes  = 0;                             // the new segment is what this snapshot's log will be

  pthread_mutex_unlock(&pP->mutex);
  pthread_rwlock_unlock(&storeP->lock);
  clock_gettime(CLOCK_MONOTONIC, &t1);
  maxUs = (t1.tv_sec - t0.tv_sec) * 1000000 + (t1.tv_nsec - t0.tv_nsec) / 1000;

  //
  // The old segment completed, the log switched - still under ioMutex, outside the tenant's lock
  //
  ok = true;
  if (old.len > 0)
  {
    if ((writeAll(pP->fd, old.buf, old.len) == false) || ((syncMode != SyncNone) && (fdatasync(pP->fd) != 0)))
    {
      COR_E("corDB: writing the log '%s': %s - what was buffered may NOT be on the disk", pP->path, strerror(errno));
      __atomic_store_n(&pP->failed, true, __ATOMIC_RELEASE);
      ok = false;
    }
    else
      __atomic_store_n(&pP->syncedSeq, oldLast, __ATOMIC_RELEASE);

    pP->segBytes += (unsigned long long) old.len;
  }
  else
    __atomic_store_n(&pP->syncedSeq, oldLast, __ATOMIC_RELEASE);

  old.len   = 0;
  pP->spare = old;                                     // the buffer goes on as the flusher's spare

  if (ok && (segmentNext(pP) == false))
  {
    COR_E("corDB: snapshot of '%s': no new log segment - no snapshot", pP->tenant);
    ok = false;
  }

  n = pP->segment;
  pthread_mutex_unlock(&pP->ioMutex);

  if (ok == false)                                     // the cursor goes: no slices to walk it
  {
    pthread_rwlock_wrlock(&storeP->lock);
    storeP->snapCursor = NULL;
    pthread_rwlock_unlock(&storeP->lock);
  }

  pthread_mutex_lock(&flushMutex);                     // its flush may have synced what a --dbSync request writer waits for
  pthread_cond_broadcast(&syncedCond);
  pthread_mutex_unlock(&flushMutex);

  //
  // The slices, under the read lock - the cursor is moved by writers in between (corDbIndex.c)
  //
  uint64_t t    = nowNs();
  bool     done = (ok == false);

  while (done == false)
  {
    clock_gettime(CLOCK_MONOTONIC, &t0);
    pthread_rwlock_rdlock(&storeP->lock);

    CorNode* eP = storeP->snapCursor;

    for (int k = 0; (eP != NULL) && (k < SNAPSHOT_SLICE) && ok; k++, eP = eP->next)
    {
      CorBinBuffer* bP = chunkCurrent(&snap);

      ok = (bP != NULL) && corDbLogEncode(bP, CorDbLogEntityPut, seq, t, eP);
    }

    storeP->snapCursor = eP;

    if ((eP == NULL) || (ok == false))
    {
      //
      // The last slice: the subscriptions and registrations, few, whole
      //
      ok = ok &&
           encodeArray(corTreeLookup(storeP->tree, "subscriptions"), CorDbLogSubPut, &snap, seq, t) &&
           encodeArray(corTreeLookup(storeP->tree, "registrations"), CorDbLogRegPut, &snap, seq, t);
      storeP->snapCursor = NULL;
      done               = true;
    }

    pthread_rwlock_unlock(&storeP->lock);
    clock_gettime(CLOCK_MONOTONIC, &t1);

    long us = (t1.tv_sec - t0.tv_sec) * 1000000 + (t1.tv_nsec - t0.tv_nsec) / 1000;
    if (us > maxUs)
      maxUs = us;

    //
    // A breath between two slices. glibc's rwlock prefers readers: released and taken straight back,
    // the read lock went to this thread again before a woken writer ran, and the writers of a growing
    // store got a fraction of the lock while a snapshot lasted
    //
    if (done == false)
    {
      struct timespec pause = { 0, 50 * 1000 };
      nanosleep(&pause, NULL);
    }
  }

  long lockedMs = maxUs / 1000;

  if (ok == false)
  {
    COR_E("corDB: snapshot of '%s': out of memory - no snapshot", pP->tenant);
    chunksFree(&snap);
    pthread_mutex_lock(&pP->ioMutex);
    pP->snapshotting = false;
    pthread_mutex_unlock(&pP->ioMutex);
    return;
  }

  char               tmpPath[600];
  char               snapPath[600];
  unsigned long long bytes = 0;

  filePath(pP, "snap", n, "tmp", tmpPath, sizeof(tmpPath));
  filePath(pP, "snap", n, "cor", snapPath, sizeof(snapPath));

  int fd = open(tmpPath, O_WRONLY | O_CREAT | O_TRUNC | O_CLOEXEC, 0600);

  ok = (fd >= 0);
  for (int i = 0; ok && (i < snap.n); i++)
  {
    ok     = writeAll(fd, snap.v[i].buf, snap.v[i].len);
    bytes += (unsigned long long) snap.v[i].len;
  }
  ok = ok && (fdatasync(fd) == 0);

  if (fd >= 0)
    close(fd);

  chunksFree(&snap);

  if (ok && (rename(tmpPath, snapPath) == 0))
  {
    syncDir(pP->dir);
    dropBefore(pP, n);

    pthread_mutex_lock(&pP->ioMutex);                  // the flusher's counters
    pP->lastSnapBytes = bytes;
    pP->snapshotting  = false;
    pthread_mutex_unlock(&pP->ioMutex);

    COR_I("corDB: snapshot of '%s': %llu bytes as '%s' (writers held at most %ld ms at a time)", pP->tenant, bytes, snapPath, lockedMs);
  }
  else
  {
    COR_E("corDB: snapshot of '%s' to '%s': %s - the log since the last snapshot is kept", pP->tenant, tmpPath, strerror(errno));
    unlink(tmpPath);

    pthread_mutex_lock(&pP->ioMutex);
    pP->snapshotting = false;
    pthread_mutex_unlock(&pP->ioMutex);
  }
}



// -----------------------------------------------------------------------------
//
// flushAll - every tenant's buffer, the waiters of --dbSync request woken, then the snapshots due
//
static void flushAll(void)
{
  pthread_mutex_lock(&flushMutex);
  CorDbPersist* headP = persistList;
  pthread_mutex_unlock(&flushMutex);

  for (CorDbPersist* pP = headP; pP != NULL; pP = pP->next)
    flushOne(pP);

  bool due = false;

  for (CorDbPersist* pP = headP; pP != NULL; pP = pP->next)
    due = due || pP->snapshotDue;

  pthread_mutex_lock(&flushMutex);
  pthread_cond_broadcast(&syncedCond);
  if (due)
  {
    snapKicked = true;
    pthread_cond_signal(&snapCond);                    // the snapshots are the snapshotter's - a sync never waits behind one
  }
  pthread_mutex_unlock(&flushMutex);
}



// -----------------------------------------------------------------------------
//
// snapshotter - takes the snapshots the flusher found due, on a thread of its own
//
// A snapshot of a big store takes seconds; on the flusher's thread, every sync of every tenant - and
// every --dbSync request writer - waited for it.
//
static void* snapshotter(void* unused)
{
  (void) unused;

  pthread_mutex_lock(&flushMutex);

  while (stopping == false)
  {
    while ((snapKicked == false) && (stopping == false))
      pthread_cond_wait(&snapCond, &flushMutex);

    snapKicked = false;
    if (stopping)
      break;

    CorDbPersist* headP = persistList;
    pthread_mutex_unlock(&flushMutex);

    for (CorDbPersist* pP = headP; pP != NULL; pP = pP->next)
    {
      if (pP->snapshotDue)
        snapshot(pP);
    }

    pthread_mutex_lock(&flushMutex);
  }

  pthread_mutex_unlock(&flushMutex);
  return NULL;
}



// -----------------------------------------------------------------------------
//
// flusher - every --dbSyncInterval ms, or at once when a --dbSync request writer waits
//
// A kick that comes while a round runs is taken by the next round: the writers that arrive during
// one fdatasync share the next one - group commit.
//
static void* flusher(void* unused)
{
  (void) unused;

  pthread_mutex_lock(&flushMutex);

  while (stopping == false)
  {
    if (kicked == false)
    {
      struct timespec deadline;

      clock_gettime(CLOCK_MONOTONIC, &deadline);
      deadline.tv_sec  += corDbSyncInterval / 1000;
      deadline.tv_nsec += (long) (corDbSyncInterval % 1000) * 1000000L;
      if (deadline.tv_nsec >= 1000000000L)
      {
        deadline.tv_sec  += 1;
        deadline.tv_nsec -= 1000000000L;
      }

      pthread_cond_timedwait(&kickCond, &flushMutex, &deadline);
    }

    kicked = false;
    pthread_mutex_unlock(&flushMutex);

    flushAll();

    pthread_mutex_lock(&flushMutex);
  }

  pthread_mutex_unlock(&flushMutex);
  return NULL;
}



// -----------------------------------------------------------------------------
//
// mkdirs - the directory and every missing parent (mkdir -p)
//
static bool mkdirs(const char* path)
{
  char p[512];

  if (snprintf(p, sizeof(p), "%s", path) >= (int) sizeof(p))
  {
    errno = ENAMETOOLONG;
    return false;
  }

  for (char* sP = &p[1]; *sP != 0; sP++)
  {
    if (*sP != '/')
      continue;

    *sP = 0;
    if ((mkdir(p, 0700) != 0) && (errno != EEXIST))
      return false;
    *sP = '/';
  }

  return (mkdir(p, 0700) == 0) || (errno == EEXIST);
}



// -----------------------------------------------------------------------------
//
// corDbPersistInit -
//
bool corDbPersistInit(void)
{
  if ((corDbDir == NULL) || (corDbDir[0] == 0))
    return true;

  if      (strcmp(corDbSync, "interval") == 0) syncMode = SyncInterval;
  else if (strcmp(corDbSync, "request")  == 0) syncMode = SyncRequest;
  else if (strcmp(corDbSync, "none")     == 0) syncMode = SyncNone;
  else
  {
    COR_E("corDB: --dbSync '%s': interval, request or none", corDbSync);
    return false;
  }

  if (mkdirs(corDbDir) == false)
  {
    COR_E("corDB: --dbDir '%s': %s", corDbDir, strerror(errno));
    return false;
  }

  pthread_condattr_t attr;

  pthread_condattr_init(&attr);
  pthread_condattr_setclock(&attr, CLOCK_MONOTONIC);
  pthread_cond_init(&kickCond, &attr);
  pthread_condattr_destroy(&attr);

  if (pthread_create(&flusherTid, NULL, flusher, NULL) != 0)
  {
    COR_E("corDB: no flusher thread: %s", strerror(errno));
    return false;
  }

  pthread_setname_np(flusherTid, "corDbFlusher");     // not one of the broker's own threads, to whoever looks (top, gdb, /proc)

  if (pthread_create(&snapTid, NULL, snapshotter, NULL) != 0)
  {
    COR_E("corDB: no snapshot thread: %s", strerror(errno));
    return false;
  }

  pthread_setname_np(snapTid, "corDbSnapshot");
  flusherUp = true;
  COR_I("corDB: persistent in '%s' (--dbSync %s, every %d ms)", corDbDir, corDbSync, corDbSyncInterval);
  return true;
}



// -----------------------------------------------------------------------------
//
// histReplay - one history record into the tenant's history (corDbHistory.h)
//
static bool histReplay(CorDbStore* storeP, CorDbLogRecord* recP, CorAlloc* kaP)
{
  if ((recP->op != CorDbLogHistInstance) || (recP->bodyP == NULL))
  {
    COR_E("corDB: history record %llu: op %d is not a history record", (unsigned long long) recP->seq, recP->op);
    return false;
  }

  CorNode* idP    = corTreeLookup(recP->bodyP, "id");
  CorNode* typeP  = corTreeLookup(recP->bodyP, "type");
  CorNode* attrP  = corTreeLookup(recP->bodyP, "attr");
  CorNode* dsP    = corTreeLookup(recP->bodyP, "datasetId");
  CorNode* delP   = corTreeLookup(recP->bodyP, "deletedAt");
  CorNode* instP  = corTreeLookup(recP->bodyP, "instance");

  CorNode* opP = corTreeLookup(recP->bodyP, "entityOp");

  if ((idP != NULL) && (idP->type == CorString) && (opP != NULL) && (opP->type == CorString))
  {
    //
    // An entity-level record: created, replaced, deleted
    //
    CorNode*         atP = corTreeLookup(recP->bodyP, "at");
    CorDbHistEntity* eP  = corDbHistoryEntity(storeP->historyP, idP->value.s, ((typeP != NULL) && (typeP->type == CorString)) ? typeP->value.s : NULL, true);
    CorBinBuffer     scratch;

    if (eP == NULL)
      return false;

    bool ok = corDbHistoryEntityEvent(eP, opP->value.s, ((atP != NULL) && (atP->type == CorInt)) ? (uint64_t) atP->value.i : 0, kaP, &scratch);
    free(scratch.buf);                                 // replayed: it is on the disk already
    return ok;
  }

  if ((idP == NULL) || (idP->type != CorString) || (attrP == NULL) || (attrP->type != CorString) || (instP == NULL))
  {
    COR_E("corDB: history record %llu is incomplete", (unsigned long long) recP->seq);
    return false;
  }

  CorDbHistEntity* eP = corDbHistoryEntity(storeP->historyP, idP->value.s, ((typeP != NULL) && (typeP->type == CorString)) ? typeP->value.s : NULL, true);

  if (eP == NULL)
    return false;

  uint64_t deletedAtNs = ((delP != NULL) && (delP->type == CorInt)) ? (uint64_t) delP->value.i : 0;

  return corDbHistoryInstanceAdd(eP, attrP->value.s, ((dsP != NULL) && (dsP->type == CorString)) ? dsP->value.s : NULL,
                                 instP, deletedAtNs, kaP) != NULL;
}



// -----------------------------------------------------------------------------
//
// loadFile - one snapshot or log segment replayed into the store (§ 6); false if it cannot be trusted
//
// Record by record until the end or the first record that is short or fails its CRC - what a death
// in mid-write leaves. In the LAST log segment that is a torn tail: the file is truncated there, so
// the records appended after the restart follow the last good one and a later replay reaches them.
// Anywhere else - a snapshot, an older segment - it is damage, and the broker does not start on it.
//
// Each record decodes into a scratch arena and is cloned into the store, as a request's tree is.
// The arena is emptied every 1000 records: memory stays bounded whatever the file's length.
//
static bool loadFile(CorDbPersist* pP, CorDbStore* storeP, const char* path, bool tornIsTail, int* recordsP, bool history)
{
  int fd = open(path, O_RDONLY | O_CLOEXEC);

  if (fd < 0)
  {
    COR_E("corDB: '%s': %s", path, strerror(errno));
    return false;
  }

  struct stat st;

  if (fstat(fd, &st) != 0)
  {
    COR_E("corDB: '%s': %s", path, strerror(errno));
    close(fd);
    return false;
  }

  if (st.st_size == 0)
  {
    close(fd);
    return true;
  }

  if (st.st_size > 0x7FFFFFFF)                         // the reader's offsets are ints
  {
    COR_E("corDB: '%s' is %lld bytes - more than one file can be", path, (long long) st.st_size);
    close(fd);
    return false;
  }

  int   len = (int) st.st_size;
  char* buf = (char*) malloc(len);
  int   got = 0;

  while ((buf != NULL) && (got < len))
  {
    ssize_t n = read(fd, &buf[got], len - got);

    if ((n < 0) && (errno == EINTR))
      continue;
    if (n <= 0)
      break;
    got += (int) n;
  }

  close(fd);

  if ((buf == NULL) || (got < len))
  {
    COR_E("corDB: reading '%s': %s", path, (buf == NULL) ? "out of memory" : strerror(errno));
    free(buf);
    return false;
  }

  enum { ARENA_INIT = 64 * 1024 };
  char*          arenaBuf = (char*) malloc(ARENA_INIT);
  CorAlloc       arena;
  int            off      = 0;
  int            failed   = 0;
  CorDbLogStatus status   = CorDbLogEnd;
  CorDbLogRecord rec;

  if (arenaBuf == NULL)
  {
    free(buf);
    return false;
  }

  corAllocBufferInit(&arena, arenaBuf, ARENA_INIT, 1024 * 1024, NULL, "corDB replay");

  while ((status = corDbLogNext(buf, len, &off, &arena, &rec)) == CorDbLogOk)
  {
    if (history == true)
    {
      if (histReplay(storeP, &rec, &arena) == false)
        ++failed;
      pP->histSeq = rec.seq;
    }
    else
    {
      if (corDbReplay(storeP, &rec) == false)
        ++failed;                                      // said why; the next records still apply
      pP->seq = rec.seq;
    }

    if ((++*recordsP % 1000) == 0)
      corAllocBufferReset(&arena, true);
  }

  corAllocBufferReset(&arena, false);
  free(arenaBuf);
  free(buf);

  if (failed != 0)
    COR_W("corDB: '%s': %d records did not apply - see above", path, failed);

  if (status != CorDbLogTorn)
    return true;

  if (tornIsTail == false)
  {
    COR_E("corDB: '%s': a damaged record at byte %d of %d", path, off, len);
    return false;
  }

  COR_W("corDB: '%s': a torn record at byte %d of %d - the %d bytes after the last good record are cut", path, off, len, len - off);

  if (truncate(path, off) != 0)
  {
    COR_E("corDB: cutting the torn tail of '%s': %s", path, strerror(errno));
    return false;
  }

  return true;
}



// -----------------------------------------------------------------------------
//
// uintCompare - for qsort
//
static int uintCompare(const void* aP, const void* bP)
{
  unsigned int a = *(const unsigned int*) aP;
  unsigned int b = *(const unsigned int*) bP;

  return (a < b) ? -1 : (a > b);
}



// -----------------------------------------------------------------------------
//
// recover - the newest snapshot, then every log segment from it on, in order (§ 6)
//
// Also what a death between two steps of a snapshot leaves: a .tmp (never renamed - deleted), and
// files older than the newest snapshot (its rename made them redundant - deleted). The log continues
// in the newest segment.
//
static bool recover(CorDbPersist* pP, CorDbStore* storeP)
{
  DIR* dirP = opendir(pP->dir);

  if (dirP == NULL)
  {
    COR_E("corDB: '%s': %s", pP->dir, strerror(errno));
    return false;
  }

  unsigned int   snapN    = 0;
  bool           haveSnap = false;
  unsigned int*  logV     = NULL;
  int            logs     = 0;
  int            logSize  = 0;
  unsigned int*  histV    = NULL;
  int            hists    = 0;
  int            histSize = 0;
  struct dirent* entryP;

  while ((entryP = readdir(dirP)) != NULL)
  {
    unsigned int k;

    if (nameNumber(entryP->d_name, "snap", "tmp", &k))
    {
      char path[1024];

      snprintf(path, sizeof(path), "%s/%s", pP->dir, entryP->d_name);
      unlink(path);                                    // an interrupted snapshot
    }
    else if (nameNumber(entryP->d_name, "snap", "cor", &k))
    {
      if ((haveSnap == false) || (k > snapN))
        snapN = k;
      haveSnap = true;
    }
    else if (nameNumber(entryP->d_name, "hist", "cor", &k))
    {
      if (hists == histSize)
      {
        histSize = (histSize == 0) ? 16 : histSize * 2;
        histV    = (unsigned int*) realloc(histV, histSize * sizeof(unsigned int));
        if (histV == NULL)
        {
          closedir(dirP);
          free(logV);
          return false;
        }
      }

      histV[hists++] = k;
    }
    else if (nameNumber(entryP->d_name, "log", "cor", &k))
    {
      if (logs == logSize)
      {
        logSize = (logSize == 0) ? 16 : logSize * 2;
        logV    = (unsigned int*) realloc(logV, logSize * sizeof(unsigned int));
        if (logV == NULL)
        {
          closedir(dirP);
          return false;
        }
      }

      logV[logs++] = k;
    }
  }

  closedir(dirP);
  qsort(logV, logs, sizeof(unsigned int), uintCompare);

  struct timespec t0;
  struct timespec t1;
  int             records = 0;
  bool            ok      = true;
  char            path[600];

  clock_gettime(CLOCK_MONOTONIC, &t0);

  pP->segment = haveSnap ? snapN : 0;

  struct stat st;

  if (haveSnap)
  {
    filePath(pP, "snap", snapN, "cor", path, sizeof(path));
    ok = loadFile(pP, storeP, path, false, &records, false);

    if (stat(path, &st) == 0)
      pP->lastSnapBytes = (unsigned long long) st.st_size;
  }

  for (int i = 0; (ok == true) && (i < logs); i++)
  {
    if (haveSnap && (logV[i] < snapN))
      continue;

    filePath(pP, "log", logV[i], "cor", path, sizeof(path));
    ok          = loadFile(pP, storeP, path, (i == logs - 1), &records, false);
    pP->segment = logV[i];

    if (stat(path, &st) == 0)                          // after a torn tail's cut
      pP->sinceSnapBytes += (unsigned long long) st.st_size;
  }

  free(logV);

  //
  // The history: every segment, oldest first - no snapshot bounds it (retention will). Without
  // --troe corDB a history found here is left alone: it is not this run's to read, nor to drop.
  //
  qsort(histV, hists, sizeof(unsigned int), uintCompare);

  if ((ok == true) && (hists > 0) && (storeP->historyP == NULL))
    COR_W("corDB: tenant '%s' has a history (%d segments) and this broker runs without --troe corDB - left as it is", pP->tenant, hists);

  for (int i = 0; (ok == true) && (storeP->historyP != NULL) && (i < hists); i++)
  {
    filePath(pP, "hist", histV[i], "cor", path, sizeof(path));
    ok              = loadFile(pP, storeP, path, (i == hists - 1), &records, true);
    pP->histSegment = histV[i];

    if ((i == hists - 1) && (stat(path, &st) == 0))
      pP->histSegBytes = (unsigned long long) st.st_size;
  }

  free(histV);

  if (ok == false)
    return false;

  if (haveSnap)
    dropBefore(pP, snapN);

  pP->syncedSeq = pP->seq;

  clock_gettime(CLOCK_MONOTONIC, &t1);
  long ms = (t1.tv_sec - t0.tv_sec) * 1000 + (t1.tv_nsec - t0.tv_nsec) / 1000000;

  if (records > 0)
    COR_I("corDB: tenant '%s': %d records replayed in %ld ms (sequence %llu)", pP->tenant, records, ms, (unsigned long long) pP->seq);

  return true;
}



// -----------------------------------------------------------------------------
//
// corDbPersistOpen -
//
CorDbPersist* corDbPersistOpen(Tenant* tenantP, CorDbStore* storeP)
{
  if (flusherUp == false)
    return NULL;

  CorDbPersist* pP = (CorDbPersist*) calloc(1, sizeof(CorDbPersist));

  if (pP == NULL)
    return NULL;

  char dir[256];

  tenantDir(tenantP->name, dir, sizeof(dir));
  if (snprintf(pP->dir, sizeof(pP->dir), "%s/%s", corDbDir, dir) >= (int) sizeof(pP->dir))
  {
    COR_E("corDB: --dbDir '%s' too long - the tenant '%s' is NOT persistent", corDbDir, tenantP->name);
    free(pP);
    return NULL;
  }

  if ((mkdir(pP->dir, 0700) != 0) && (errno != EEXIST))
  {
    COR_E("corDB: tenant directory '%s': %s - the tenant is NOT persistent", pP->dir, strerror(errno));
    free(pP);
    return NULL;
  }

  snprintf(pP->tenant, sizeof(pP->tenant), "%s", tenantP->name);
  pP->storeP = storeP;

  if (recover(pP, storeP) == false)
  {
    COR_E("corDB: tenant '%s': its files in '%s' could not be read - the broker does not start on a store it cannot trust", tenantP->name, pP->dir);
    exit(1);
  }

  filePath(pP, "log", pP->segment, "cor", pP->path, sizeof(pP->path));

  pP->fd = open(pP->path, O_WRONLY | O_CREAT | O_APPEND | O_CLOEXEC, 0600);
  if (pP->fd < 0)
  {
    COR_E("corDB: log '%s': %s - the tenant is NOT persistent", pP->path, strerror(errno));
    free(pP);
    return NULL;
  }

  struct stat st;
  if (fstat(pP->fd, &st) == 0)
    pP->segBytes = (unsigned long long) st.st_size;

  pP->histFd = -1;
  if (storeP->historyP != NULL)
  {
    filePath(pP, "hist", pP->histSegment, "cor", pP->histPath, sizeof(pP->histPath));

    pP->histFd = open(pP->histPath, O_WRONLY | O_CREAT | O_APPEND | O_CLOEXEC, 0600);
    if (pP->histFd < 0)
      COR_E("corDB: history log '%s': %s - the tenant's history is NOT persistent", pP->histPath, strerror(errno));
  }

  pthread_mutex_init(&pP->mutex, NULL);
  pthread_mutex_init(&pP->ioMutex, NULL);

  pthread_mutex_lock(&flushMutex);
  pP->next    = persistList;
  persistList = pP;
  pthread_mutex_unlock(&flushMutex);

  return pP;
}



// -----------------------------------------------------------------------------
//
// corDbPersistAppend -
//
void corDbPersistAppend(CorDbPersist* pP, CorDbLogOp op, CorNode* bodyP)
{
  if (pP == NULL)
    return;

  uint64_t t = nowNs();

  pthread_mutex_lock(&pP->mutex);

  uint64_t seq = pP->seq + 1;

  if (corDbLogEncode(&pP->buf, op, seq, t, bodyP) == true)
    pP->seq = seq;
  else
  {
    COR_E("corDB: out of memory for the log of '%s' - a write is NOT persistent", pP->path);
    __atomic_store_n(&pP->failed, true, __ATOMIC_RELEASE);
  }

  pthread_mutex_unlock(&pP->mutex);

  pendingP   = pP;
  pendingSeq = seq;
}



// -----------------------------------------------------------------------------
//
// corDbPersistAppendId -
//
void corDbPersistAppendId(CorDbPersist* pP, CorDbLogOp op, const char* id)
{
  if (pP == NULL)
    return;

  CorNode idNode = { 0 };

  idNode.type    = CorString;
  idNode.value.s = (char*) id;

  corDbPersistAppend(pP, op, &idNode);
}



// -----------------------------------------------------------------------------
//
// corDbPersistHistAppend -
//
void corDbPersistHistAppend(CorDbPersist* pP, const char* body, int bodyLen)
{
  if ((pP == NULL) || (pP->histFd < 0))
    return;

  uint32_t crc = corCrc32c(0, body, bodyLen);
  uint64_t t   = nowNs();

  pthread_mutex_lock(&pP->mutex);

  if (corDbLogAppendEncoded(&pP->histBuf, CorDbLogHistInstance, pP->histSeq + 1, t, body, bodyLen, crc) == true)
    ++pP->histSeq;
  else
  {
    COR_E("corDB: out of memory for the history log of '%s' - a history record is NOT persistent", pP->tenant);
    __atomic_store_n(&pP->failed, true, __ATOMIC_RELEASE);
  }

  pthread_mutex_unlock(&pP->mutex);
}



// -----------------------------------------------------------------------------
//
// corDbPersistOn - is there a log to write to (--dbDir)
//
bool corDbPersistOn(void)
{
  return flusherUp;
}



// -----------------------------------------------------------------------------
//
// corDbPersistPreAdd - one more body, encoded now; its index
//
int corDbPersistPreAdd(CorDbPre* preP, CorNode* bodyP)
{
  if (preP->n == preP->size)
  {
    int size = (preP->size == 0) ? 4 : preP->size * 2;
    int*      offV = (int*)      realloc(preP->offV, size * sizeof(int));
    int*      lenV = (offV != NULL) ? (int*) realloc(preP->lenV, size * sizeof(int)) : NULL;
    uint32_t* crcV = (lenV != NULL) ? (uint32_t*) realloc(preP->crcV, size * sizeof(uint32_t)) : NULL;

    if (offV != NULL) preP->offV = offV;
    if (lenV != NULL) preP->lenV = lenV;
    if (crcV != NULL) preP->crcV = crcV;
    if (crcV == NULL)
      return -1;

    preP->size = size;
  }

  int ix = preP->n++;

  preP->offV[ix] = preP->buf.len;
  preP->lenV[ix] = -1;

  if ((bodyP != NULL) && (corDbLogBodyEncode(&preP->buf, bodyP, &preP->lenV[ix], &preP->crcV[ix]) == false))
    preP->lenV[ix] = -1;

  return ix;
}



// -----------------------------------------------------------------------------
//
// corDbPersistAppendPre - the record of a pre-encoded body; under the write lock
//
void corDbPersistAppendPre(CorDbPersist* pP, CorDbLogOp op, CorDbPre* preP, int ix, CorNode* bodyP)
{
  if (pP == NULL)
    return;

  if ((ix < 0) || (ix >= preP->n) || (preP->lenV[ix] < 0))
  {
    corDbPersistAppend(pP, op, bodyP);                 // not pre-encoded: here, as before
    return;
  }

  uint64_t t = nowNs();

  pthread_mutex_lock(&pP->mutex);

  uint64_t seq = pP->seq + 1;

  if (corDbLogAppendEncoded(&pP->buf, op, seq, t, &preP->buf.buf[preP->offV[ix]], preP->lenV[ix], preP->crcV[ix]) == true)
    pP->seq = seq;
  else
  {
    COR_E("corDB: out of memory for the log of '%s' - a write is NOT persistent", pP->path);
    __atomic_store_n(&pP->failed, true, __ATOMIC_RELEASE);
  }

  pthread_mutex_unlock(&pP->mutex);

  pendingP   = pP;
  pendingSeq = seq;
}



// -----------------------------------------------------------------------------
//
// corDbPersistPreFree - the cleanup behind COR_DB_PRE
//
void corDbPersistPreFree(CorDbPre* preP)
{
  free(preP->buf.buf);
  free(preP->offV);
  free(preP->lenV);
  free(preP->crcV);
}



// -----------------------------------------------------------------------------
//
// SyncWait - the record a request waits for
//
typedef struct SyncWait
{
  CorDbPersist*  pP;
  uint64_t       seq;
} SyncWait;



// -----------------------------------------------------------------------------
//
// syncWait - kick the flusher, wait for its round to have synced 'seq' (or failed)
//
static void syncWait(void* arg)
{
  SyncWait* wP = (SyncWait*) arg;

  pthread_mutex_lock(&flushMutex);

  while ((__atomic_load_n(&wP->pP->syncedSeq, __ATOMIC_ACQUIRE) < wP->seq) &&
         (__atomic_load_n(&wP->pP->failed, __ATOMIC_ACQUIRE) == false) &&
         (stopping == false))
  {
    kicked = true;
    pthread_cond_signal(&kickCond);
    pthread_cond_wait(&syncedCond, &flushMutex);
  }

  pthread_mutex_unlock(&flushMutex);
}



// -----------------------------------------------------------------------------
//
// corDbPersistSyncWait -
//
// In a coroutine the wait runs on a thread of its own and the loop serves the rest (corCoBlocking);
// on a worker thread it is a plain wait. Either way, after the tenant's lock is released.
//
void corDbPersistSyncWait(void)
{
  CorDbPersist* pP  = pendingP;
  SyncWait      w   = { pP, pendingSeq };

  pendingP = NULL;

  if ((pP == NULL) || (syncMode != SyncRequest))
    return;

  CorRestState* savedP = corRestP;                     // corCoBlocking: the thread may be rebound meanwhile

  corCoBlocking(syncWait, &w);
  corRestP = savedP;
}



// -----------------------------------------------------------------------------
//
// unescape - a tenant directory's name back to the tenant's (tenantDir's inverse); false if it is not one
//
static bool unescape(const char* dir, char* out, int outSize)
{
  int o = 0;

  for (const char* p = dir; *p != 0; p++)
  {
    if (o >= outSize - 1)
      return false;

    if (*p != '%')
    {
      out[o++] = *p;
      continue;
    }

    unsigned int c;

    if ((p[1] == 0) || (p[2] == 0) || (sscanf(&p[1], "%2X", &c) != 1))
      return false;

    out[o++] = (char) c;
    p += 2;
  }

  out[o] = 0;
  return (o > 0);
}



// -----------------------------------------------------------------------------
//
// corDbPersistTenants - every tenant with a directory in --dbDir, created and loaded
//
// At start, before the broker loads its subscription and registration caches - which walk the
// tenants it knows. The default tenant's store is built by corDbInit itself.
//
void corDbPersistTenants(void)
{
  if (flusherUp == false)
    return;

  DIR* dirP = opendir(corDbDir);

  if (dirP == NULL)
  {
    COR_E("corDB: --dbDir '%s': %s", corDbDir, strerror(errno));
    return;
  }

  struct dirent* entryP;

  while ((entryP = readdir(dirP)) != NULL)
  {
    if ((entryP->d_name[0] == '.') || (strcmp(entryP->d_name, "_") == 0))
      continue;

    char name[256];

    if (unescape(entryP->d_name, name, sizeof(name)) == false)
    {
      COR_W("corDB: '%s/%s' is not a tenant directory - left alone", corDbDir, entryP->d_name);
      continue;
    }

    Tenant* tP = tenantGetOrCreate(name);

    if (tP == NULL)
    {
      COR_E("corDB: '%s/%s': the tenant '%s' could not be created", corDbDir, entryP->d_name, name);
      continue;
    }

    corDbStoreOf(tP);                                  // built - and so loaded - now, not on the first request
  }

  closedir(dirP);
}



// -----------------------------------------------------------------------------
//
// corDbPersistClose -
//
void corDbPersistClose(void)
{
  if (flusherUp == false)
    return;

  pthread_mutex_lock(&flushMutex);
  stopping = true;
  pthread_cond_signal(&kickCond);
  pthread_cond_signal(&snapCond);
  pthread_mutex_unlock(&flushMutex);

  pthread_join(flusherTid, NULL);
  pthread_join(snapTid, NULL);                         // a snapshot it is taking is finished first
  flusherUp = false;

  //
  // The last round: no request runs any more, so what is in a buffer now is all there will be.
  // Then a snapshot per tenant (§ 5a step 3) - the next start loads it and has no log to replay. A
  // stop killed during the snapshots has lost nothing: the logs are written and synced already.
  //
  flushAll();

  for (CorDbPersist* pP = persistList; pP != NULL; pP = pP->next)
  {
    if (pP->sinceSnapBytes != 0)                       // written since the last snapshot
      snapshot(pP);
  }

  CorDbPersist* nextP;

  for (CorDbPersist* pP = persistList; pP != NULL; pP = nextP)
  {
    nextP = pP->next;

    if (fsync(pP->fd) != 0)
      COR_E("corDB: fsync of the log '%s': %s", pP->path, strerror(errno));
    close(pP->fd);

    if (pP->histFd >= 0)
    {
      if (fsync(pP->histFd) != 0)
        COR_E("corDB: fsync of the history log '%s': %s", pP->histPath, strerror(errno));
      close(pP->histFd);
    }
    free(pP->histBuf.buf);
    free(pP->histSpare.buf);

    if (__atomic_load_n(&pP->failed, __ATOMIC_ACQUIRE) == true)
      COR_E("corDB: the log '%s' had a write error - what it lost is in the errors above", pP->path);

    free(pP->buf.buf);
    free(pP->spare.buf);
    pthread_mutex_destroy(&pP->mutex);
    pthread_mutex_destroy(&pP->ioMutex);
    free(pP);
  }

  persistList = NULL;
  COR_I("corDB: logs written and synced");
}
