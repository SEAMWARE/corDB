#ifndef CORDB_CORDBPERSIST_H_
#define CORDB_CORDBPERSIST_H_

//
// FILE            corDbPersist.h
//
// AUTHOR          Ken Zangelin
//
// Copyright 2026 Seamware
// SPDX-License-Identifier: Apache-2.0
//
// The write side of persistence (doc/persistence.md § 5): every write appends its effect to its
// tenant's log buffer under the tenant's write lock; one flusher thread writes the buffers out and
// fdatasyncs them. Without --dbDir none of it exists: a store's persistP is NULL and an append is a
// test of it.
//
#include <pthread.h>                                   // pthread_mutex_t
#include <stdbool.h>                                   // bool
#include <stdint.h>                                    // uint64_t

#include "corTree/CorNode.h"                           // CorNode
#include "corTree/corTreeBin.h"                        // CorBinBuffer

#include "db/Tenant.h"                                 // Tenant

#include "corDB/corDbLog.h"                            // CorDbLogOp



// -----------------------------------------------------------------------------
//
// CorDbPersist - one tenant's log: what is buffered, and the file it goes to
//
// Lock order: the tenant's write lock, then 'mutex'. The flusher takes only 'mutex', and only to
// swap the buffer out - the write and the fdatasync run with no lock held.
//
typedef struct CorDbPersist
{
  pthread_mutex_t       mutex;                         // guards buf and seq
  CorBinBuffer          buf;                           // the records not yet written
  CorBinBuffer          spare;                         // the flusher's: written out, emptied, swapped back in
  uint64_t              seq;                           // the last sequence number given out
  uint64_t              syncedSeq;                     // the last one on the disk (atomic)
  bool                  failed;                        // a write or a sync failed: what is buffered is not on the disk
  int                   fd;                            // the open log file
  char                  path[512];                     // ... and its path, for the errors
  struct CorDbPersist*  next;                          // every tenant's, for the flusher
} CorDbPersist;



// -----------------------------------------------------------------------------
//
// corDbPersistInit - start the flusher; false if --dbDir is unusable or an option is wrong
//
// No --dbDir: nothing to start, true.
//
extern bool corDbPersistInit(void);



// -----------------------------------------------------------------------------
//
// corDbPersistOpen - the tenant's log, opened (its directory created) - NULL without --dbDir
//
// Called once per store, while it is built and before anyone else sees it.
//
extern CorDbPersist* corDbPersistOpen(Tenant* tenantP);



// -----------------------------------------------------------------------------
//
// corDbPersistAppend - a write's effect, appended to its tenant's log buffer
//
// Under the tenant's write lock, after the tree changed - lock order is log order. bodyP is the
// record's body (corDbLog.h); corDbPersistAppendId's is the id alone, for the deletes.
//
extern void corDbPersistAppend(CorDbPersist* persistP, CorDbLogOp op, CorNode* bodyP);
extern void corDbPersistAppendId(CorDbPersist* persistP, CorDbLogOp op, const char* id);



// -----------------------------------------------------------------------------
//
// corDbPersistSyncWait - with --dbSync request: wait for this thread's last record to be on the disk
//
// For corDbStoreUnlock: with --dbSync request, the write lock released, the request waits for its
// last record's fdatasync (corDbPersistSyncWait) - with the lock held, every other writer of the
// tenant would wait for the disk too.
//
extern void corDbPersistSyncWait(void);



// -----------------------------------------------------------------------------
//
// corDbPersistClose - the flusher stopped, every buffer written and synced, the files closed
//
// After the last request: what is acknowledged is on the disk when this returns (§ 5a).
//
extern void corDbPersistClose(void);

#endif  // CORDB_CORDBPERSIST_H_
