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
#include <sys/stat.h>                                  // mkdir
#include <time.h>                                      // clock_gettime
#include <unistd.h>                                    // write, fdatasync, close

#include "corLog/corLog.h"                             // COR_E, COR_I, COR_W
#include "corBase/corCoLoop.h"                         // corCoBlocking
#include "corTree/CorNode.h"                           // CorNode
#include "corRest/CorRestState.h"                      // corRestP

#include "db/Tenant.h"                                 // Tenant

#include "corDB/corDbGlobals.h"                        // corDbDir, corDbSync, corDbSyncInterval
#include "corDB/corDbLog.h"                            // corDbLogEncode
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

  if ((mkdir(corDbDir, 0700) != 0) && (errno != EEXIST))
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
// corDbPersistOpen -
//
CorDbPersist* corDbPersistOpen(Tenant* tenantP)
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
