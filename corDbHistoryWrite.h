#ifndef CORDB_CORDBHISTORYWRITE_H_
#define CORDB_CORDBHISTORYWRITE_H_

//
// FILE            corDbHistoryWrite.h
//
// AUTHOR          Ken Zangelin
//
// Copyright 2026 Seamware
// SPDX-License-Identifier: Apache-2.0
//
// What the write sites call to record history (`--troe corDB`): current state overwrites, history
// appends - under the same write lock, into the index and the history log. Every write that reaches an
// attribute appends an instance of it (a PATCH with the same value still changes its modifiedAt).
//
// Which instances a write reached is the broker's own answer (ldInstanceWritten, the merge report) -
// what its TRoE events say for timescale.
//
// All of them: a no-op without `--troe corDB` (the store's historyP is NULL).
//
#include "corAlloc/CorAlloc.h"                         // CorAlloc
#include "corTree/CorNode.h"                           // CorNode
#include "corNgsild/ldEntityMerge.h"                   // LdMergeReport

#include "corDB/corDbHistory.h"                        // CorDbHistRecord
#include "corDB/corDbStore.h"                          // CorDbStore



// -----------------------------------------------------------------------------
//
// corDbHistoryDrain - the history the writes queued, applied in order (index + history log)
//
// A write queues its history under the tenant's write lock and drains after releasing it
// (corDbStoreUnlock) - before its answer, so a temporal read after it sees it. A temporal read drains
// too, then reads under the store's histMutex.
//
extern void corDbHistoryDrain(CorDbStore* storeP);



// -----------------------------------------------------------------------------
//
// CorDbHistPre - an entity's history records, encoded BEFORE the write lock
//
// Create and replace have their entity (the clone) before the lock: its records are encoded then, and
// under the lock only added (corDbHistoryCreatedPre, corDbHistoryReplacedPre). Encoded under the lock,
// history cost a create at 50 connections 45 % of its throughput.
//
typedef struct CorDbHistPre
{
  CorDbHistRecord*  recV;
  int               recs;
  int               size;
  CorBinBuffer      event;                           // the entity-level record (created / replaced), encoded
  const char*       entityOp;
  uint64_t          atNs;
} CorDbHistPre;

#define COR_DB_HIST_PRE(name)  CorDbHistPre name __attribute__((cleanup(corDbHistoryPreFree))) = { NULL, 0, 0, { NULL, 0, 0 }, NULL, 0 }

extern void corDbHistoryPrepare(CorDbHistPre* preP, CorNode* entityP, const char* entityOp, CorAlloc* kaP);
extern void corDbHistoryPreFree(CorDbHistPre* preP);
extern void corDbHistoryCreatedPre(CorDbStore* storeP, CorDbHistPre* preP, CorNode* entityP, CorAlloc* kaP);
extern void corDbHistoryReplacedPre(CorDbStore* storeP, CorDbHistPre* preP, CorNode* newEntityP, CorNode* oldEntityP, CorAlloc* kaP);

//
// CorDbHistPreV - a batch's: one CorDbHistPre per entity, freed (with what no index took) when the scope
// ends - after the write lock, declared before it
//
typedef struct CorDbHistPreV
{
  CorDbHistPre*  v;
  int            n;
} CorDbHistPreV;

#define COR_DB_HIST_PREV(name)  CorDbHistPreV name __attribute__((cleanup(corDbHistoryPreVFree))) = { NULL, 0 }

extern void corDbHistoryPrepareV(CorDbHistPreV* preVP, CorNode** entityV, int n, const char* entityOp, CorAlloc* kaP);
extern void corDbHistoryPreVFree(CorDbHistPreV* preVP);



// -----------------------------------------------------------------------------
//
// corDbHistoryDeletePrepare / corDbHistoryDeletedPre - an entity delete's record, encoded before the lock
// (the id and the time are all it holds - the type is in the entity's earlier records)
//
typedef struct CorDbHistDel
{
  CorBinBuffer  event;
  uint64_t      atNs;
} CorDbHistDel;

extern void corDbHistoryDeletePrepare(CorDbHistDel* delP, const char* entityId, CorAlloc* kaP);
extern void corDbHistoryDeletedPre(CorDbStore* storeP, CorDbHistDel* delP, const char* entityId);



// -----------------------------------------------------------------------------
//
// corDbHistoryCreated - an entity created: the event, and every instance of every attribute
//
extern void corDbHistoryCreated(CorDbStore* storeP, CorNode* entityP, CorAlloc* kaP);



// -----------------------------------------------------------------------------
//
// corDbHistoryReplaced - an entity replaced: the event, every instance of the new one, and a deletion
// for every instance of the old one the new one lacks
//
extern void corDbHistoryReplaced(CorDbStore* storeP, CorNode* newEntityP, CorNode* oldEntityP, CorAlloc* kaP);



// -----------------------------------------------------------------------------
//
// corDbHistoryMerged - attributes written (merge, PATCH, append): the instances the report says the
// write reached, from the live entity after it; a deletion for each it removed
//
extern void corDbHistoryMerged(CorDbStore* storeP, CorNode* liveEntityP, LdMergeReport* reportP, CorAlloc* kaP);



// -----------------------------------------------------------------------------
//
// corDbHistoryDeleted - an entity deleted from current state: the event (its history stays)
//
extern void corDbHistoryDeleted(CorDbStore* storeP, CorNode* goneEntityP, CorAlloc* kaP);

#endif  // CORDB_CORDBHISTORYWRITE_H_
