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

#include "corDB/corDbStore.h"                          // CorDbStore



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
