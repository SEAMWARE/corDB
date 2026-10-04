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



struct CorDbStore;



// -----------------------------------------------------------------------------
//
// CorDbPersist - one tenant's log: what is buffered, and the file it goes to
//
// Lock order: the tenant's write lock, then 'ioMutex', then 'mutex'. A write takes the tenant's lock
// and 'mutex'; the flusher 'ioMutex' for a flush and 'mutex' only to swap the buffer out, so a write
// never waits for the disk; a snapshot all three.
//
typedef struct CorDbPersist
{
  pthread_mutex_t       mutex;                         // guards buf and seq
  CorBinBuffer          buf;                           // the records not yet written
  CorBinBuffer          spare;                         // the flusher's: written out, emptied, swapped back in
  uint64_t              seq;                           // the last sequence number given out
  uint64_t              syncedSeq;                     // the last one on the disk (atomic)
  bool                  failed;                        // a write or a sync failed: what is buffered is not on the disk

  pthread_mutex_t       ioMutex;                       // the file: a flush's write + sync, a snapshot's switch of segment
  int                   fd;                            // the open log segment, log-<segment>.cor
  unsigned int          segment;
  unsigned long long    segBytes;                      // written to it - the segment rolls at 1 GiB
  unsigned long long    sinceSnapBytes;                // log written since the last snapshot
  unsigned long long    lastSnapBytes;                 // the size of that snapshot
  bool                  snapshotDue;
  bool                  snapshotting;                  // one is being taken: the flusher arms no other
  char                  path[600];                     // the segment's path, for the errors
  char                  dir[512];                      // the tenant's directory
  char                  tenant[64];                    // the tenant's name, for the log lines
  struct CorDbStore*    storeP;                        // the store the snapshots are taken of

  //
  // The history log (`--troe corDB`): its own segments, hist-<n>.cor, appended to and flushed with the
  // current-state log but never dropped by a snapshot - history is not a store's state at one instant.
  // Guarded as the log is: 'mutex' for histBuf/histSeq, 'ioMutex' for the file.
  //
  CorBinBuffer          histBuf;
  CorBinBuffer          histSpare;
  uint64_t              histSeq;
  uint64_t              syncedHistSeq;                 // the last history record on the disk (atomic)
  int                   histFd;                        // -1: no history
  unsigned int          histSegment;
  unsigned long long    histSegBytes;
  char                  histPath[600];

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
// corDbPersistOpen - the tenant's log replayed into its store, then opened - NULL without --dbDir
//
// Called once per store, while it is built and before anyone else sees it: recovery (§ 6) is the
// store's construction. A log that exists and cannot be read ends the broker - serving the store
// without it would answer, and then overwrite, a past that is not the real one.
//
extern CorDbPersist* corDbPersistOpen(Tenant* tenantP, struct CorDbStore* storeP);



// -----------------------------------------------------------------------------
//
// corDbPersistHistAppend - a history record (its body encoded by corDbHistoryInstanceAdd) to the
// tenant's history log. Under the tenant's write lock, as an append to the log is.
//
extern void corDbPersistHistAppend(CorDbPersist* persistP, const char* body, int bodyLen);



// -----------------------------------------------------------------------------
//
// corDbPersistTenants - the tenants --dbDir has directories for, created and their stores loaded
//
extern void corDbPersistTenants(void);



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
// CorDbPre - record bodies encoded BEFORE the write lock
//
// Where the entity a write stores exists before the lock - the clone of a create, a replace, a batch
// create or update - its body is encoded and its CRC taken first, and under the lock only the header
// is written and the body copied (corDbLogBodyEncode / corDbLogAppendEncoded). Encoded under the lock,
// the record cost a batch create of twenty entities 61 % of its throughput. Index i is the i-th
// corDbPersistPreAdd; a body that could not be encoded (or a NULL node) has length -1, and its
// record is then encoded under the lock as before.
//
typedef struct CorDbPre
{
  CorBinBuffer  buf;
  int           n;
  int           size;
  int*          offV;
  int*          lenV;
  uint32_t*     crcV;
} CorDbPre;

#define COR_DB_PRE(name)  CorDbPre name __attribute__((cleanup(corDbPersistPreFree))) = { { NULL, 0, 0 }, 0, 0, NULL, NULL, NULL }

extern bool corDbPersistOn(void);
extern int  corDbPersistPreAdd(CorDbPre* preP, CorNode* bodyP);
extern void corDbPersistAppendPre(CorDbPersist* persistP, CorDbLogOp op, CorDbPre* preP, int ix, CorNode* bodyP);
extern void corDbPersistPreFree(CorDbPre* preP);



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
// corDbPersistClose - the flusher stopped, every buffer written and synced, a snapshot per tenant
//
// After the last request, with the stores still there: what is acknowledged is on the disk when
// this returns, and the next start loads the snapshots with no log to replay (§ 5a).
//
extern void corDbPersistClose(void);

#endif  // CORDB_CORDBPERSIST_H_
