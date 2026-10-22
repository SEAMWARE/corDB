#ifndef CORDB_CORDBVALUEINDEX_H_
#define CORDB_CORDBVALUEINDEX_H_

//
// FILE            corDbValueIndex.h
//
// AUTHOR          Ken Zangelin
//
// Copyright 2026 Seamware
// SPDX-License-Identifier: Apache-2.0
//
// The value indexes of a store (COR_FEATURE_DB_INDEXES): for every declared index that covers the store's
// tenant (corDbIndexes.h), the values of the attribute - every instance, an array's elements - in order, each
// with its entity. A q value term on the attribute takes its candidates from the values it can match instead
// of from the whole store, and every candidate is matched as before: the index only has to hold every value a
// term can match on.
//
// The candidates come back in the store's order - the order of a page: every entity has a sequence number
// in the index, its place in the store's list.
//
// Kept by the three functions that link, unlink and swap an entity in the store's list (corDbIndex.c), and by
// corDbIndexChanged where an entity changes in place. Built from the store's list by the first query that
// can use it, and again when the declarations change (corDbValueIndexDrop).
//
#include <stdint.h>                                  // int64_t

#include "corTree/CorNode.h"                         // CorNode

#include "db/DbQueryFilter.h"                        // DbQueryFilter
#include "db/Tenant.h"                               // Tenant

#include "corDB/corDbStore.h"                        // CorDbStore



// -----------------------------------------------------------------------------
//
// CorDbValueCand - an entity that a query served by a value index can match, and its place in the store
//
typedef struct CorDbValueCand
{
  CorNode*  entityP;
  int64_t   seq;
} CorDbValueCand;



// -----------------------------------------------------------------------------
//
// corDbValueIndexOf - the store's value indexes, built if they are not yet; NULL: no declaration covers the
// tenant, or out of memory (the query walks as it did)
//
// Under the store's lock, read or write: a build under the read lock is serialised by the store's
// valueIndexMutex, and readers see the index only once it is complete.
//
extern struct CorDbValueIndex* corDbValueIndexOf(CorDbStore* storeP, Tenant* tenantP);



// -----------------------------------------------------------------------------
//
// corDbValueIndexCandidates - the candidates of a query's q, in the store's order: the entities whose
// values a term of the q's AND can match, for a term on an attribute an index serves. -1: no index serves
// the q, or - needed > 0, the matches a page needs - too many candidates for the index to beat the walk to the
// page. The array is the request's (corRest.kallocP).
//
extern int corDbValueIndexCandidates(struct CorDbValueIndex* vxP, DbQueryFilter* filterP, int needed, CorDbValueCand** candVP);



// -----------------------------------------------------------------------------
//
// corDbValueIndexSeq - an entity's place in the store, as the candidates have it; -1: not in the index
//
extern int64_t corDbValueIndexSeq(struct CorDbValueIndex* vxP, CorNode* entityP);



// -----------------------------------------------------------------------------
//
// The store's list changes - called by corDbIndexLink, corDbIndexUnlink, corDbIndexReplace and
// corDbIndexChanged, the write lock held. Nothing to do while the index is not built.
//
// corDbValueIndexLink    - the entity, appended to the store's list
// corDbValueIndexUnlink  - the entity, about to be taken out of it
// corDbValueIndexReplace - newP, in oldP's place
// corDbValueIndexChanged - the entity, changed in place (a merge, a replayed attribute record): namesV, the
//                          members the write changed (namesN -1: not known), retyped: its types changed (or
//                          not known) - nothing to do when it wrote no declared attribute and no declaration
//                          narrowed to a type can have gained or lost it
//
extern void corDbValueIndexLink(CorDbStore* storeP, CorNode* entityP);
extern void corDbValueIndexUnlink(CorDbStore* storeP, CorNode* entityP);
extern void corDbValueIndexReplace(CorDbStore* storeP, CorNode* oldP, CorNode* newP);
extern void corDbValueIndexChanged(CorDbStore* storeP, CorNode* entityP, const char** namesV, int namesN, bool retyped);



// -----------------------------------------------------------------------------
//
// corDbValueIndexStats - what the store's index of a declaration holds, the q terms it served and the writes
// that changed it (an entity with the attribute created or deleted, its values changed) since it was built:
// false: none (not built, or the declaration does not cover the tenant). Under the store's lock, read or write.
//
extern bool corDbValueIndexStats(CorDbStore* storeP, const char* declId, int64_t* entriesP, int64_t* bytesP, int64_t* accessesP, int64_t* updatesP);



// -----------------------------------------------------------------------------
//
// corDbValueIndexDrop - the store's value indexes freed (the declarations changed, the store emptied or
// freed); the write lock held, or nobody can reach the store. The next query that can use one builds them.
//
extern void corDbValueIndexDrop(CorDbStore* storeP);

#endif  // CORDB_CORDBVALUEINDEX_H_
