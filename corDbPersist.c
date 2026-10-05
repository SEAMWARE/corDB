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
#include <sys/mman.h>                                  // mmap, munmap
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
#include "corTree/corTreeBuilder.h"                    // corTreeChildRemove, corTreeChildAdd
#include "corTree/corTreeLookup.h"                     // corTreeLookup
#include "corRest/CorRestState.h"                      // corRestP

#include "db/Tenant.h"                                 // Tenant, tenantGetOrCreate

#include "corDB/corDbIndex.h"                         // corDbEntityId
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
// The segments - mapped, appended to by a copy
//
// A segment is mapped once, MAP_SHARED, over more address space than it will ever hold (it rolls at
// 1 GiB), and the file is allocated ahead of its records, SEG_STEP at a time. An append copies the
// record in, under the tenant's write lock: the page is the kernel's page cache, so a broker that dies
// after the copy has lost nothing - the kernel writes it out. The flusher's fdatasync (every
// --dbSyncInterval ms, or at once for a --dbSync request writer) is what survives the MACHINE dying.
// Before, records waited in a buffer of the process for the flusher's write(): a crash, kill -9 or
// an OOM kill lost up to --dbSyncInterval ms of acknowledged writes.
//
// posix_fallocate, not ftruncate: a page of a sparse file that the file system cannot back when it is
// first written kills the process (SIGBUS) - a full disk would. Allocated ahead, a full disk is an
// error at the allocation: the write is reported as not persistent, and the broker goes on.
//
// While open, the file is longer than its records and the rest is zeros; recovery reads a header of
// zeros as the end (loadFile). A finished segment is cut to its length.
//
#define SEG_ROLL      (1024ULL * 1024 * 1024)          // the reader's offsets are ints: no file may reach 2 GiB
#define SEG_HEADROOM  (256ULL * 1024 * 1024)           // address space past the roll, for the record that crosses it
#define SEG_STEP      (4ULL * 1024 * 1024)



// -----------------------------------------------------------------------------
//
// segOpen - <kind>-<n>.cor opened (created if need be) and mapped, its records' length its size;
// 'need': the record it is opened for
//
static bool segOpen(CorDbPersist* pP, const char* kind, unsigned int n, CorDbSeg* sP, unsigned long long need)
{
  CorDbSeg    seg = { -1, NULL, 0, 0, 0, n, "" };
  struct stat st;

  filePath(pP, kind, n, "cor", seg.path, sizeof(seg.path));

  seg.fd = open(seg.path, O_RDWR | O_CREAT | O_CLOEXEC, 0600);
  if (seg.fd < 0)
  {
    COR_E("corDB: log segment '%s': %s", seg.path, strerror(errno));
    return false;
  }

  if (fstat(seg.fd, &st) != 0)
  {
    COR_E("corDB: log segment '%s': %s", seg.path, strerror(errno));
    close(seg.fd);
    return false;
  }

  seg.len    = (unsigned long long) st.st_size;
  seg.alloc  = seg.len;
  seg.mapLen = SEG_ROLL + SEG_HEADROOM;

  if (seg.len + need + SEG_STEP > seg.mapLen)
    seg.mapLen = seg.len + need + SEG_STEP;

  seg.map = (char*) mmap(NULL, seg.mapLen, PROT_READ | PROT_WRITE, MAP_SHARED, seg.fd, 0);
  if (seg.map == MAP_FAILED)
  {
    COR_E("corDB: mapping the log segment '%s': %s", seg.path, strerror(errno));
    close(seg.fd);
    return false;
  }

  *sP = seg;
  return true;
}



// -----------------------------------------------------------------------------
//
// segFinish - unmapped, cut to its records, synced (unless --dbSync none) and closed
//
static bool segFinish(CorDbSeg* sP, bool sync)
{
  bool ok = true;

  if (sP->fd < 0)
    return true;

  if (sP->map != NULL)
    munmap(sP->map, sP->mapLen);

  if ((sP->alloc != sP->len) && (ftruncate(sP->fd, (off_t) sP->len) != 0))
  {
    COR_E("corDB: cutting the log segment '%s' to its %llu bytes: %s", sP->path, sP->len, strerror(errno));
    ok = false;
  }

  if (sync && (fdatasync(sP->fd) != 0))
  {
    COR_E("corDB: fdatasync of the log segment '%s': %s - its last records may NOT be on the disk", sP->path, strerror(errno));
    ok = false;
  }

  close(sP->fd);
  sP->fd  = -1;
  sP->map = NULL;

  return ok;
}



// -----------------------------------------------------------------------------
//
// segRoll - the next segment takes over; the open one goes to 'retired', for the flusher. Under
// 'mutex'. False: no next segment, the open one goes on.
//
static bool segRoll(CorDbPersist* pP, CorDbSeg* sP, const char* kind, unsigned long long need)
{
  CorDbSeg next;

  if (pP->retiredN == (int) (sizeof(pP->retired) / sizeof(pP->retired[0])))
  {
    COR_E("corDB: '%s': %d segments wait for the flusher - '%s' goes on", pP->tenant, pP->retiredN, sP->path);
    return false;
  }

  if (segOpen(pP, kind, sP->n + 1, &next, need) == false)
  {
    COR_E("corDB: no next log segment - '%s' goes on", sP->path);
    return false;
  }

  pP->retired[pP->retiredN++] = *sP;
  *sP = next;

  return true;
}



// -----------------------------------------------------------------------------
//
// segAppend - one record copied into the open segment, which rolls at 1 GiB. Under 'mutex'.
//
static bool segAppend(CorDbPersist* pP, CorDbSeg* sP, const char* kind, const char* rec, int n)
{
  if ((sP->len >= SEG_ROLL) || (sP->len + (unsigned long long) n > sP->mapLen))
    segRoll(pP, sP, kind, (unsigned long long) n);

  if (sP->len + (unsigned long long) n > sP->mapLen)
  {
    COR_E("corDB: a record of %d bytes does not fit in '%s'", n, sP->path);
    return false;
  }

  if (sP->len + (unsigned long long) n > sP->alloc)
  {
    unsigned long long want = sP->len + (unsigned long long) n + SEG_STEP;

    if (want > sP->mapLen)
      want = sP->mapLen;

    int e = posix_fallocate(sP->fd, (off_t) sP->alloc, (off_t) (want - sP->alloc));

    if (e != 0)
    {
      COR_E("corDB: allocating log segment '%s' to %llu bytes: %s", sP->path, want, strerror(e));
      return false;
    }

    sP->alloc = want;
  }

  memcpy(&sP->map[sP->len], rec, n);
  sP->len += (unsigned long long) n;

  return true;
}



// -----------------------------------------------------------------------------
//
// flushLocked - the retired segments finished, the open ones synced; under 'ioMutex'
//
// The records are in the segments already (segAppend): what a flush adds is the sync. 'mutex' only to
// read how far the logs go - the fds stay valid without it: only this function and a snapshot (both
// under ioMutex) close a segment.
//
static void flushLocked(CorDbPersist* pP)
{
  CorDbSeg retired[sizeof(pP->retired) / sizeof(pP->retired[0])];

  pthread_mutex_lock(&pP->mutex);

  uint64_t           last     = pP->seq;
  uint64_t           histLast = pP->histSeq;
  int                logFd    = pP->log.fd;
  int                histFd   = pP->hist.fd;
  unsigned long long appended = pP->appended;
  int                retiredN = pP->retiredN;

  memcpy(retired, pP->retired, retiredN * sizeof(CorDbSeg));
  pP->retiredN = 0;
  pthread_mutex_unlock(&pP->mutex);

  bool sync   = (syncMode != SyncNone);
  bool ok     = true;
  bool histOk = true;

  //
  // The retired segments first - they hold the older records - then the history, then the log: a
  // write's history and its current state reach the disk in the same round, before syncedSeq says the
  // write is there, so --dbSync request covers both
  //
  for (int i = 0; i < retiredN; i++)
    ok = segFinish(&retired[i], sync) && ok;

  if (sync && (histFd >= 0) && (fdatasync(histFd) != 0))
  {
    COR_E("corDB: fdatasync of the history log '%s': %s - what was appended may NOT be on the disk", pP->hist.path, strerror(errno));
    histOk = false;
  }

  if (ok && histOk)
    __atomic_store_n(&pP->syncedHistSeq, histLast, __ATOMIC_RELEASE);

  if (sync && (logFd >= 0) && (fdatasync(logFd) != 0))
  {
    COR_E("corDB: fdatasync of the log '%s': %s - what was appended may NOT be on the disk", pP->log.path, strerror(errno));
    ok = false;
  }

  pP->sinceSnapBytes  += appended - pP->flushedAppended;
  pP->flushedAppended  = appended;

  //
  // A snapshot is due after --dbSnapshotEvery MiB of log AND at least as much log as the last
  // snapshot was big. Every 64 MiB alone, a growing store was snapshotted whole, under its write
  // lock, again and again - 8 times in 10 s of creates, the work growing with the square of the
  // store. Tied to the snapshot's size, the snapshots cost in proportion to what is written.
  //
  unsigned long long every = (unsigned long long) corDbSnapshotEvery * 1024 * 1024;

  if ((pP->snapshotting == false) && (pP->sinceSnapBytes >= every) && (pP->sinceSnapBytes >= pP->lastSnapBytes))
    pP->snapshotDue = true;

  if (ok && histOk)
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
  // The start. ioMutex first - the flusher waits - then, under the tenant's write lock, only what
  // touches no disk but an open: the sequence, the cursor, and the log switched to a new segment, the
  // one this snapshot's log will be. The old segment is synced and cut after the lock (flushLocked):
  // a writer never waits for the disk here (syncing under the lock was a create's p99 of 123 ms).
  //
  // Lock order: ioMutex, then the tenant's lock, then 'mutex' - as everywhere: a writer takes the
  // tenant's lock and 'mutex', the flusher ioMutex and 'mutex'.
  //
  pthread_mutex_lock(&pP->ioMutex);

  clock_gettime(CLOCK_MONOTONIC, &t0);
  pthread_rwlock_wrlock(&storeP->lock);
  pthread_mutex_lock(&pP->mutex);

  ok                  = segRoll(pP, &pP->log, "log", 0);
  n                   = pP->log.n;
  seq                 = pP->seq;
  pP->snapshotDue     = false;

  if (ok)
  {
    storeP->snapCursor  = storeP->entities->value.head;
    pP->snapshotting    = true;
    pP->sinceSnapBytes  = 0;                           // the new segment is what this snapshot's log will be
    pP->flushedAppended = pP->appended;                // and what went before it is not
  }

  pthread_mutex_unlock(&pP->mutex);
  pthread_rwlock_unlock(&storeP->lock);
  clock_gettime(CLOCK_MONOTONIC, &t1);
  maxUs = (t1.tv_sec - t0.tv_sec) * 1000000 + (t1.tv_nsec - t0.tv_nsec) / 1000;

  bool rolled = ok;

  if (ok == false)
    COR_E("corDB: snapshot of '%s': no new log segment - no snapshot", pP->tenant);

  flushLocked(pP);                                     // the old segment synced and cut
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
    if (rolled)                                        // else said already
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

  //
  // A temporal-API write (corDbTroeWrite.c): applied by the same code that applied it live
  //
  if (corTreeLookup(recP->bodyP, "histOp") != NULL)
    return corDbHistoryOpApply(storeP->historyP, recP->bodyP, kaP) != -1;

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

    if ((typeP != NULL) && (typeP->type == CorString) && (strcmp(opP->value.s, "deleted") != 0))
      corDbHistoryEntityTypes(storeP->historyP, eP, typeP->value.s, true);   // created / replaced: exactly these

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

  //
  // The record keeps the instanceId beside the instance: handed back to it, so the replay keeps the id
  // the instance was given, not a new one
  //
  CorNode* instanceIdP = corTreeLookup(recP->bodyP, "instanceId");

  if ((instanceIdP != NULL) && (corTreeLookup(instP, "instanceId") == NULL))
  {
    corTreeChildRemove(recP->bodyP, instanceIdP);
    corTreeChildAdd(instP, instanceIdP);
  }

  return corDbHistoryInstanceAdd(storeP->historyP, eP, attrP->value.s, ((dsP != NULL) && (dsP->type == CorString)) ? dsP->value.s : NULL,
                                 instP, deletedAtNs, kaP) != NULL;
}



// -----------------------------------------------------------------------------
//
// setAside - a segment after a gap: renamed <name>.torn, out of every later replay but kept
//
static void setAside(const char* path)
{
  char torn[700];

  snprintf(torn, sizeof(torn), "%s.torn", path);

  if (rename(path, torn) == 0)
    COR_W("corDB: '%s' comes after a gap in the log - set aside as '%s', not replayed", path, torn);
  else
    COR_E("corDB: setting '%s' aside: %s", path, strerror(errno));
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
typedef enum LoadResult
{
  LoadOk,                                              // read to its end (a tail of zeros or a torn record cut)
  LoadGap,                                             // the sequence jumped: the log ends there, and so do the segments after it
  LoadFail
} LoadResult;

static LoadResult loadFile(CorDbPersist* pP, CorDbStore* storeP, const char* path, bool tornIsTail, int* recordsP, bool history, uint64_t* expectP)
{
  int fd = open(path, O_RDONLY | O_CLOEXEC);

  if (fd < 0)
  {
    COR_E("corDB: '%s': %s", path, strerror(errno));
    return LoadFail;
  }

  struct stat st;

  if (fstat(fd, &st) != 0)
  {
    COR_E("corDB: '%s': %s", path, strerror(errno));
    close(fd);
    return LoadFail;
  }

  if (st.st_size == 0)
  {
    close(fd);
    return LoadOk;
  }

  if (st.st_size > 0x7FFFFFFF)                         // the reader's offsets are ints
  {
    COR_E("corDB: '%s' is %lld bytes - more than one file can be", path, (long long) st.st_size);
    close(fd);
    return LoadFail;
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
    return LoadFail;
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
    return LoadFail;
  }

  corAllocBufferInit(&arena, arenaBuf, ARENA_INIT, 1024 * 1024, NULL, "corDB replay");

  int  recOff = 0;
  bool gap    = false;

  //
  // The sequence: a log segment's records follow each other, +1 each (the first one of a replay
  // follows whatever came before - a snapshot, possibly empty). A record that does not is after a
  // gap - pages the machine wrote out of order before it died, past the last sync - and the log ends
  // before it: replaying past a gap would build a store no write ever made.
  //
  while ((recOff = off, status = corDbLogNext(buf, len, &off, &arena, &rec)) == CorDbLogOk)
  {
    if (expectP != NULL)
    {
      if ((*expectP != 0) && (rec.seq != *expectP))
      {
        gap = true;
        off = recOff;
        break;
      }

      *expectP = rec.seq + 1;
    }

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

  //
  // Zeros from a torn record to the end: the space allocated ahead of a segment's records
  // (segAppend) - not damage, the end
  //
  bool zeros = (status == CorDbLogTorn) && (gap == false);

  for (int i = off; zeros && (i < len); i++)
    zeros = (buf[i] == 0);

  corAllocBufferReset(&arena, false);
  free(arenaBuf);
  free(buf);

  if (failed != 0)
    COR_W("corDB: '%s': %d records did not apply - see above", path, failed);

  if (gap && (off == 0))
  {
    setAside(path);                                    // all of it after the gap: kept, not replayed
    return LoadGap;
  }

  if (gap)
  {
    COR_W("corDB: '%s': record %llu at byte %d, after %llu - a gap; the log ends before it, and the %d bytes from there are cut",
          path, (unsigned long long) rec.seq, off, (unsigned long long) (*expectP - 1), len - off);
    if (truncate(path, off) != 0)
    {
      COR_E("corDB: cutting '%s' at its gap: %s", path, strerror(errno));
      return LoadFail;
    }
    return LoadGap;
  }

  if (status != CorDbLogTorn)
    return LoadOk;

  if (zeros)                                           // cut, so the file is its records
  {
    if (truncate(path, off) != 0)
    {
      COR_E("corDB: cutting the allocated tail of '%s': %s", path, strerror(errno));
      return LoadFail;
    }
    return LoadOk;
  }

  if (tornIsTail == false)
  {
    COR_E("corDB: '%s': a damaged record at byte %d of %d", path, off, len);
    return LoadFail;
  }

  COR_W("corDB: '%s': a torn record at byte %d of %d - the %d bytes after the last good record are cut", path, off, len, len - off);

  if (truncate(path, off) != 0)
  {
    COR_E("corDB: cutting the torn tail of '%s': %s", path, strerror(errno));
    return LoadFail;
  }

  return LoadOk;
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

  pP->log.n = haveSnap ? snapN : 0;

  struct stat st;

  if (haveSnap)
  {
    filePath(pP, "snap", snapN, "cor", path, sizeof(path));
    ok = (loadFile(pP, storeP, path, false, &records, false, NULL) == LoadOk);

    if (stat(path, &st) == 0)
      pP->lastSnapBytes = (unsigned long long) st.st_size;
  }

  uint64_t expect = 0;
  bool     gap    = false;

  for (int i = 0; (ok == true) && (i < logs); i++)
  {
    if (haveSnap && (logV[i] < snapN))
      continue;

    filePath(pP, "log", logV[i], "cor", path, sizeof(path));

    if (gap)
    {
      setAside(path);
      continue;
    }

    LoadResult r = loadFile(pP, storeP, path, (i == logs - 1), &records, false, &expect);

    ok        = (r != LoadFail);
    gap       = (r == LoadGap);
    pP->log.n = logV[i];

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

  expect = 0;
  gap    = false;

  for (int i = 0; (ok == true) && (storeP->historyP != NULL) && (i < hists); i++)
  {
    filePath(pP, "hist", histV[i], "cor", path, sizeof(path));

    if (gap)
    {
      setAside(path);
      continue;
    }

    LoadResult r = loadFile(pP, storeP, path, (i == hists - 1), &records, true, &expect);

    ok         = (r != LoadFail);
    gap        = (r == LoadGap);
    pP->hist.n = histV[i];
  }

  free(histV);

  if (ok == false)
    return false;

  if (haveSnap)
    dropBefore(pP, snapN);

  pP->syncedSeq     = pP->seq;
  pP->syncedHistSeq = pP->histSeq;

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

  if (segOpen(pP, "log", pP->log.n, &pP->log, 0) == false)
  {
    COR_E("corDB: tenant '%s' is NOT persistent", tenantP->name);
    free(pP);
    return NULL;
  }

  pP->hist.fd = -1;
  if ((storeP->historyP != NULL) && (segOpen(pP, "hist", pP->hist.n, &pP->hist, 0) == false))
  {
    COR_E("corDB: tenant '%s': its history is NOT persistent", tenantP->name);
    pP->hist.fd = -1;
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
// logPut - the record in pP->buf into the log's segment; under 'mutex'. False: not persistent (said
// why), and the sequence number is not used - a log's numbers have no gap (loadFile reads one as the
// end of the log)
//
static bool logPut(CorDbPersist* pP)
{
  if (segAppend(pP, &pP->log, "log", pP->buf.buf, pP->buf.len) == false)
  {
    __atomic_store_n(&pP->failed, true, __ATOMIC_RELEASE);
    return false;
  }

  pP->appended += (unsigned long long) pP->buf.len;
  return true;
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

  pP->buf.len = 0;
  if (corDbLogEncode(&pP->buf, op, seq, t, bodyP) == false)
  {
    COR_E("corDB: out of memory for the log of '%s' - a write is NOT persistent", pP->tenant);
    __atomic_store_n(&pP->failed, true, __ATOMIC_RELEASE);
    seq = pP->seq;
  }
  else if (logPut(pP) == false)
    seq = pP->seq;
  else
    pP->seq = seq;

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
// corDbPersistAppendAttrs -
//
// The body is built of COPIES of the entity's member nodes - the node struct only: its children are
// the entity's own, read by the encoder and touched by nobody while the write lock is held. Nothing
// is cloned for the record.
//
#define ATTRS_MAX  64

static bool named(const char* name, const char** names, int n)
{
  for (int i = 0; i < n; i++)
  {
    if ((names[i] != NULL) && (strcmp(names[i], name) == 0))
      return true;
  }

  return false;
}

void corDbPersistAppendAttrs(CorDbPersist* pP, CorNode* entityP, const char** names, int n)
{
  if (pP == NULL)
    return;

  const char* id = corDbEntityId(entityP);

  if ((n > ATTRS_MAX) || (id == NULL))
  {
    corDbPersistAppend(pP, CorDbLogEntityPut, entityP);
    return;
  }

  CorNode setV[ATTRS_MAX];
  CorNode delV[ATTRS_MAX];
  int     nSet = 0;
  int     nDel = 0;
  CorNode idN      = { 0 };
  CorNode attrsN   = { 0 };
  CorNode deletedN = { 0 };
  CorNode bodyN    = { 0 };

  //
  // The members, in the entity's order: one the write added is at its end, and replay appends it
  // there too
  //
  for (CorNode* mP = entityP->value.head; mP != NULL; mP = mP->next)
  {
    if ((mP->name == NULL) || (strcmp(mP->name, "id") == 0) || (named(mP->name, names, n) == false))
      continue;

    setV[nSet]      = *mP;
    setV[nSet].next = NULL;
    if (nSet > 0)
      setV[nSet - 1].next = &setV[nSet];
    ++nSet;
  }

  //
  // The names the entity no longer has: deleted by the write (an attribute set to null in a merge)
  //
  for (int i = 0; i < n; i++)
  {
    if ((names[i] == NULL) || (strcmp(names[i], "id") == 0) || (corTreeLookup(entityP, names[i]) != NULL) || named(names[i], names, i))
      continue;

    delV[nDel].name    = (char*) "";
    delV[nDel].type    = CorString;
    delV[nDel].value.s = (char*) names[i];
    delV[nDel].next    = NULL;
    if (nDel > 0)
      delV[nDel - 1].next = &delV[nDel];
    ++nDel;
  }

  idN.name    = (char*) "id";
  idN.type    = CorString;
  idN.value.s = (char*) id;

  attrsN.name       = (char*) "attrs";
  attrsN.type       = CorObject;
  attrsN.value.head = (nSet > 0) ? &setV[0] : NULL;
  attrsN.value.tail = (nSet > 0) ? &setV[nSet - 1] : NULL;
  idN.next          = &attrsN;

  if (nDel > 0)
  {
    deletedN.name       = (char*) "deleted";
    deletedN.type       = CorArray;
    deletedN.value.head = &delV[0];
    deletedN.value.tail = &delV[nDel - 1];
    attrsN.next         = &deletedN;
  }

  bodyN.type       = CorObject;
  bodyN.value.head = &idN;
  bodyN.value.tail = (nDel > 0) ? &deletedN : &attrsN;

  corDbPersistAppend(pP, CorDbLogAttrsPut, &bodyN);
}



// -----------------------------------------------------------------------------
//
// corDbPersistHistAppend -
//
void corDbPersistHistAppend(CorDbPersist* pP, const char* body, int bodyLen)
{
  if ((pP == NULL) || (pP->hist.fd < 0))
    return;

  uint32_t crc = corCrc32c(0, body, bodyLen);
  uint64_t t   = nowNs();

  pthread_mutex_lock(&pP->mutex);

  pP->histBuf.len = 0;
  if (corDbLogAppendEncoded(&pP->histBuf, CorDbLogHistInstance, pP->histSeq + 1, t, body, bodyLen, crc) == false)
  {
    COR_E("corDB: out of memory for the history log of '%s' - a history record is NOT persistent", pP->tenant);
    __atomic_store_n(&pP->failed, true, __ATOMIC_RELEASE);
  }
  else if (segAppend(pP, &pP->hist, "hist", pP->histBuf.buf, pP->histBuf.len) == true)
    ++pP->histSeq;
  else
    __atomic_store_n(&pP->failed, true, __ATOMIC_RELEASE);

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

  pP->buf.len = 0;
  if (corDbLogAppendEncoded(&pP->buf, op, seq, t, &preP->buf.buf[preP->offV[ix]], preP->lenV[ix], preP->crcV[ix]) == false)
  {
    COR_E("corDB: out of memory for the log of '%s' - a write is NOT persistent", pP->tenant);
    __atomic_store_n(&pP->failed, true, __ATOMIC_RELEASE);
    seq = pP->seq;
  }
  else if (logPut(pP) == false)
    seq = pP->seq;
  else
    pP->seq = seq;

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
  uint64_t       histSeq;                              // the history appended by then: a write's history is drained after its lock,
} SyncWait;                                            // maybe after the flush that synced its own record



// -----------------------------------------------------------------------------
//
// syncWait - kick the flusher, wait for its round to have synced 'seq' (or failed)
//
static void syncWait(void* arg)
{
  SyncWait* wP = (SyncWait*) arg;

  pthread_mutex_lock(&flushMutex);

  while (((__atomic_load_n(&wP->pP->syncedSeq, __ATOMIC_ACQUIRE) < wP->seq) || (__atomic_load_n(&wP->pP->syncedHistSeq, __ATOMIC_ACQUIRE) < wP->histSeq)) &&
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
  SyncWait      w   = { pP, pendingSeq, 0 };

  pendingP = NULL;

  if ((pP == NULL) || (syncMode != SyncRequest))
    return;

  pthread_mutex_lock(&pP->mutex);
  w.histSeq = pP->histSeq;                             // drained before this (corDbStoreUnlock): it covers this write's history
  pthread_mutex_unlock(&pP->mutex);

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

    segFinish(&pP->log, true);                         // cut to its records, synced whatever --dbSync says
    segFinish(&pP->hist, true);
    free(pP->histBuf.buf);

    if (__atomic_load_n(&pP->failed, __ATOMIC_ACQUIRE) == true)
      COR_E("corDB: the log of '%s' had a write error - what it lost is in the errors above", pP->tenant);

    free(pP->buf.buf);
    pthread_mutex_destroy(&pP->mutex);
    pthread_mutex_destroy(&pP->ioMutex);
    free(pP);
  }

  persistList = NULL;
  COR_I("corDB: logs written and synced");
}
