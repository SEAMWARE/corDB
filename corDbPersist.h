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
// CorDbSeg - one open log segment, mapped (corDbPersist.c, "The segments")
//
// An append is a copy into 'map': the bytes are the kernel's the moment the copy is done, so a broker
// that dies loses none of them - only the machine dying loses what the last sync had not reached.
//
typedef struct CorDbSeg
{
  int                   fd;                            // -1: none
  char*                 map;                           // MAP_SHARED over the file, 'mapLen' bytes of address space
  unsigned long long    mapLen;
  unsigned long long    len;                           // the records in it
  unsigned long long    alloc;                         // the file's size: allocated ahead of 'len' (posix_fallocate)
  unsigned int          n;                             // its number, <kind>-<n>.cor
  char                  path[600];                     // for the errors
} CorDbSeg;



// -----------------------------------------------------------------------------
//
// CorDbPersist - one tenant's logs
//
// Lock order: 'ioMutex', then the tenant's write lock, then 'mutex'. A write takes the tenant's lock
// and 'mutex' - its record goes into the segment there, and it never waits for the disk; the flusher
// takes 'ioMutex' for a flush and 'mutex' only to read where the segments are; a snapshot all three.
//
typedef struct CorDbPersist
{
  pthread_mutex_t       mutex;                         // guards buf, seq, the segments' appends, 'retired'
  CorBinBuffer          buf;                           // a record being encoded - copied into the segment and emptied
  uint64_t              seq;                           // the last sequence number given out
  uint64_t              syncedSeq;                     // the last one on the disk (atomic)
  bool                  failed;                        // a write or a sync failed: what is buffered is not on the disk

  pthread_mutex_t       ioMutex;                       // the syncs: a flush, a snapshot's switch of segment
  CorDbSeg              log;                           // the open log segment, log-<n>.cor
  unsigned long long    appended;                      // bytes appended to the log, ever (under 'mutex')
  unsigned long long    flushedAppended;               // 'appended' at the last flush (ioMutex)
  unsigned long long    sinceSnapBytes;                // log written since the last snapshot
  unsigned long long    lastSnapBytes;                 // the size of that snapshot
  bool                  snapshotDue;
  bool                  snapshotting;                  // one is being taken: the flusher arms no other
  char                  dir[512];                      // the tenant's directory
  char                  tenant[64];                    // the tenant's name, for the log lines
  struct CorDbStore*    storeP;                        // the store the snapshots are taken of

  //
  // Segments an append or a snapshot has moved on from: the flusher syncs them, cuts them to their
  // length and closes them, before it syncs the open ones (under 'mutex' to add, ioMutex to finish)
  //
  CorDbSeg              retired[8];
  int                   retiredN;

  //
  // The history log (`--troe corDB`): its own segments, hist-<n>.cor, appended to and flushed with the
  // current-state log but never dropped by a snapshot - history is not a store's state at one instant.
  // Guarded as the log is.
  //
  CorBinBuffer          histBuf;
  uint64_t              histSeq;
  uint64_t              syncedHistSeq;                 // the last history record on the disk (atomic)
  CorDbSeg              hist;                          // hist.fd -1: no history

  //
  // The tenant was dropped (corDbPersistDrop): its files are gone, the flusher, the snapshotter and the
  // close at a stop pass it by. It stays in the list - the flusher and the snapshotter walk the list
  // without the lock, so nothing is ever taken out of it.
  //
  bool                  dropped;

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
// corDbPersistAppendAttrs - the record of a write that changed some members of an entity (an
// attribute update): CorDbLogAttrsPut, the members 'names' names as the entity has them now, each
// whole, and those of them it no longer has. Under the write lock, after the change. A PATCH of one
// attribute logs that attribute, not the entity. Too many names: the whole entity (CorDbLogEntityPut).
//
extern void corDbPersistAppendAttrs(CorDbPersist* persistP, CorNode* entityP, const char** names, int n);



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

//
// corDbPersistPreAddAttrs - an ATTRS_PUT body (corDbPersistAppendAttrs) encoded before the lock; its index,
// lenV[ix] -1 when it could not be (too many names - the caller logs the entity)
//
extern int corDbPersistPreAddAttrs(CorDbPre* preP, CorNode* entityP, const char** names, int n);

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



// -----------------------------------------------------------------------------
//
// corDbPersistDrop - a tenant's log dropped: marked, a snapshot in progress waited for (its store is empty
// by now - corDbTenantDrop), the segments closed, every file and the directory deleted
//
extern void corDbPersistDrop(CorDbPersist* pP);

#endif  // CORDB_CORDBPERSIST_H_
