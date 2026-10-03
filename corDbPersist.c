//
// FILE            corDbPersist.c
//
// AUTHOR          Ken Zangelin
//
// Copyright 2026 Seamware
// SPDX-License-Identifier: Apache-2.0
//
// The write side of persistence - doc/persistence.md § 2, § 5, § 5a.
//
//   <--dbDir>/<tenant>/log-000000.cor
//
// A tenant's directory is its name with every byte outside [A-Za-z0-9-] written %XX - a tenant name is
// whatever the NGSILD-Tenant header said, '/' and '..' included - and the default tenant's is '_',
// which no escaped name can be.
//
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
#include "corBase/corCoLoop.h"                         // corCoBlocking
#include "corTree/CorNode.h"                           // CorNode
#include "corRest/CorRestState.h"                      // corRestP

#include "db/Tenant.h"                                 // Tenant, tenantGetOrCreate

#include "corDB/corDbGlobals.h"                        // corDbDir, corDbSync, corDbSyncInterval
#include "corDB/corDbLog.h"                            // corDbLogEncode
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
// flushOne - one tenant's buffer: swapped out under its mutex, written and synced with no lock held
//
static void flushOne(CorDbPersist* pP)
{
  pthread_mutex_lock(&pP->mutex);

  CorBinBuffer out  = pP->buf;
  uint64_t     last = pP->seq;

  pP->buf       = pP->spare;
  pP->buf.len   = 0;
  pthread_mutex_unlock(&pP->mutex);

  bool ok = true;

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
  }

  out.len   = 0;
  pP->spare = out;                                     // only the flusher (or the close, after it) touches spare

  if (ok)
    __atomic_store_n(&pP->syncedSeq, last, __ATOMIC_RELEASE);
  else
    __atomic_store_n(&pP->failed, true, __ATOMIC_RELEASE);
}



// -----------------------------------------------------------------------------
//
// flushAll - every tenant's buffer, then the waiters of --dbSync request woken
//
static void flushAll(void)
{
  pthread_mutex_lock(&flushMutex);
  CorDbPersist* headP = persistList;
  pthread_mutex_unlock(&flushMutex);

  for (CorDbPersist* pP = headP; pP != NULL; pP = pP->next)
    flushOne(pP);

  pthread_mutex_lock(&flushMutex);
  pthread_cond_broadcast(&syncedCond);
  pthread_mutex_unlock(&flushMutex);
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

  flusherUp = true;
  COR_I("corDB: persistent in '%s' (--dbSync %s, every %d ms)", corDbDir, corDbSync, corDbSyncInterval);
  return true;
}



// -----------------------------------------------------------------------------
//
// load - the tenant's log replayed into its store (§ 6); a torn tail cut off; false if unreadable
//
// Record by record until the end or the first record that is short or fails its CRC - what a death
// in mid-write leaves. The file is truncated there, so the records appended after the restart follow
// the last good one and a later replay reaches them.
//
// Each record decodes into a scratch arena and is cloned into the store, as a request's tree is.
// The arena is emptied every 1000 records: memory stays bounded whatever the log's length.
//
static bool load(CorDbPersist* pP, CorDbStore* storeP, const char* tenantName)
{
  int fd = open(pP->path, O_RDONLY | O_CLOEXEC);

  if (fd < 0)
  {
    if (errno == ENOENT)
      return true;                                     // a new tenant

    COR_E("corDB: log '%s': %s", pP->path, strerror(errno));
    return false;
  }

  struct stat st;

  if (fstat(fd, &st) != 0)
  {
    COR_E("corDB: log '%s': %s", pP->path, strerror(errno));
    close(fd);
    return false;
  }

  if (st.st_size == 0)
  {
    close(fd);
    return true;
  }

  if (st.st_size > 0x7FFFFFFF)                         // the reader's offsets are ints - rollover (§ 6) keeps a log far below
  {
    COR_E("corDB: log '%s' is %lld bytes - more than one log file can be", pP->path, (long long) st.st_size);
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
    COR_E("corDB: reading the log '%s': %s", pP->path, (buf == NULL) ? "out of memory" : strerror(errno));
    free(buf);
    return false;
  }

  struct timespec t0;
  struct timespec t1;
  clock_gettime(CLOCK_MONOTONIC, &t0);

  enum { ARENA_INIT = 64 * 1024 };
  char*          arenaBuf = (char*) malloc(ARENA_INIT);
  CorAlloc       arena;
  int            off      = 0;
  int            records  = 0;
  int            failed   = 0;
  uint64_t       lastSeq  = 0;
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
    if (corDbReplay(storeP, &rec) == false)
      ++failed;                                        // said why; the next records still apply

    lastSeq = rec.seq;

    if ((++records % 1000) == 0)
      corAllocBufferReset(&arena, true);
  }

  corAllocBufferReset(&arena, false);
  free(arenaBuf);
  free(buf);

  if (status == CorDbLogTorn)
  {
    COR_W("corDB: log '%s': a torn record at byte %d of %d - the %d bytes after the last good record are cut", pP->path, off, len, len - off);

    if (truncate(pP->path, off) != 0)
    {
      COR_E("corDB: cutting the torn tail of '%s': %s", pP->path, strerror(errno));
      return false;
    }
  }

  pP->seq       = lastSeq;
  pP->syncedSeq = lastSeq;

  clock_gettime(CLOCK_MONOTONIC, &t1);
  long ms = (t1.tv_sec - t0.tv_sec) * 1000 + (t1.tv_nsec - t0.tv_nsec) / 1000000;

  COR_I("corDB: tenant '%s': %d records replayed in %ld ms (last sequence %llu)%s", tenantName, records, ms, (unsigned long long) lastSeq, (failed != 0) ? " - some did not apply, see above" : "");
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

  char dir[256];
  char dirPath[512];

  tenantDir(tenantP->name, dir, sizeof(dir));
  if (snprintf(dirPath, sizeof(dirPath), "%s/%s", corDbDir, dir) >= (int) sizeof(dirPath) - 16)
  {
    COR_E("corDB: --dbDir '%s' too long - the tenant '%s' is NOT persistent", corDbDir, tenantP->name);
    return NULL;
  }

  if ((mkdir(dirPath, 0700) != 0) && (errno != EEXIST))
  {
    COR_E("corDB: tenant directory '%s': %s - the tenant is NOT persistent", dirPath, strerror(errno));
    return NULL;
  }

  CorDbPersist* pP = (CorDbPersist*) calloc(1, sizeof(CorDbPersist));

  if (pP == NULL)
    return NULL;

  if (snprintf(pP->path, sizeof(pP->path), "%s/log-000000.cor", dirPath) >= (int) sizeof(pP->path))
  {
    free(pP);                                          // cannot be: dirPath left room above
    return NULL;
  }

  if (load(pP, storeP, tenantP->name) == false)
  {
    COR_E("corDB: tenant '%s': its log could not be read - the broker does not start on a store it cannot trust", tenantP->name);
    exit(1);
  }

  pP->fd = open(pP->path, O_WRONLY | O_CREAT | O_APPEND | O_CLOEXEC, 0600);
  if (pP->fd < 0)
  {
    COR_E("corDB: log '%s': %s - the tenant is NOT persistent", pP->path, strerror(errno));
    free(pP);
    return NULL;
  }

  pthread_mutex_init(&pP->mutex, NULL);

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
  pthread_mutex_unlock(&flushMutex);

  pthread_join(flusherTid, NULL);
  flusherUp = false;

  //
  // The last round: no request runs any more, so what is in a buffer now is all there will be
  //
  flushAll();

  CorDbPersist* nextP;

  for (CorDbPersist* pP = persistList; pP != NULL; pP = nextP)
  {
    nextP = pP->next;

    if (fsync(pP->fd) != 0)
      COR_E("corDB: fsync of the log '%s': %s", pP->path, strerror(errno));
    close(pP->fd);

    if (__atomic_load_n(&pP->failed, __ATOMIC_ACQUIRE) == true)
      COR_E("corDB: the log '%s' had a write error - what it lost is in the errors above", pP->path);

    free(pP->buf.buf);
    free(pP->spare.buf);
    pthread_mutex_destroy(&pP->mutex);
    free(pP);
  }

  persistList = NULL;
  COR_I("corDB: logs written and synced");
}
