#ifndef CORDB_CORDBTYPEINDEX_H_
#define CORDB_CORDBTYPEINDEX_H_

//
// FILE            corDbTypeIndex.h
//
// AUTHOR          Ken Zangelin
//
// Copyright 2026 Seamware
// SPDX-License-Identifier: Apache-2.0
//
// The type index (COR_FEATURE_TYPE_INDEX): for every entity type, the store's entities of that type, in the
// store's order - creation order, the order of a page. A query by type walks its type's list instead of the
// whole store: a page of a rare type no longer costs the store up to where the page ends.
//
// Kept by the three functions that link, unlink and swap an entity in the store's list (corDbIndex.c), and by
// corDbTypeIndexRetype where an entity's types change in place. Built from the store's list on the first
// query that can use it: a store that is only written pays nothing, and a store loaded from the disk is
// indexed by its first query.
//
#include <stdbool.h>                                 // bool

#include "corTree/CorNode.h"                         // CorNode

#include "corDB/corDbStore.h"                        // CorDbStore



// -----------------------------------------------------------------------------
//
// CorDbTypeRef - an entity in the list of one of its types
//
typedef struct CorDbTypeRef
{
  CorNode*               entityP;
  struct CorDbTypeRef*   prev;                       // in the type's list, in the store's order
  struct CorDbTypeRef*   next;
  struct CorDbTypeRef*   sibling;                    // the entity's ref in its next type's list
  struct CorDbTypeList*  listP;                      // the type's list
  struct CorDbTypeRef*   mapNext;                    // the entity map's chain - on an entity's first ref only
} CorDbTypeRef;



// -----------------------------------------------------------------------------
//
// corDbTypeIndexOf - the store's type index, built if it is not yet; NULL: out of memory (the query walks
// the store)
//
// Under the store's lock, read or write: a build under the read lock is serialised by the store's
// typeIndexMutex, and readers see the index only once it is complete.
//
extern struct CorDbTypeIndex* corDbTypeIndexOf(CorDbStore* storeP);



// -----------------------------------------------------------------------------
//
// corDbTypeIndexHead - the first entity of the type, in the store's order; NULL: none. *countP: how many
//
extern CorDbTypeRef* corDbTypeIndexHead(struct CorDbTypeIndex* ixP, const char* type, int* countP);



// -----------------------------------------------------------------------------
//
// corDbTypeIndexRef - the entity's ref in the type's list; NULL: the entity is not of that type
//
extern CorDbTypeRef* corDbTypeIndexRef(struct CorDbTypeIndex* ixP, CorNode* entityP, const char* type);



// -----------------------------------------------------------------------------
//
// The store's list changes - called by corDbIndexLink, corDbIndexUnlink and corDbIndexReplace, the write
// lock held. Nothing to do while the index is not built.
//
// corDbTypeIndexLink    - the entity, appended to the store's list
// corDbTypeIndexUnlink  - the entity, about to be taken out of it
// corDbTypeIndexReplace - newP, in oldP's place (its types may differ)
//
extern void corDbTypeIndexLink(CorDbStore* storeP, CorNode* entityP);
extern void corDbTypeIndexUnlink(CorDbStore* storeP, CorNode* entityP);
extern void corDbTypeIndexReplace(CorDbStore* storeP, CorNode* oldP, CorNode* newP);



// -----------------------------------------------------------------------------
//
// corDbTypeIndexRetype - the entity's "type" changed in place (a merge, a replayed attribute record); the
// write lock held
//
extern void corDbTypeIndexRetype(CorDbStore* storeP, CorNode* entityP);



// -----------------------------------------------------------------------------
//
// corDbTypeIndexFree - the store's type index freed (the store emptied or freed); the write lock held, or
// nobody can reach the store
//
extern void corDbTypeIndexFree(CorDbStore* storeP);

#endif  // CORDB_CORDBTYPEINDEX_H_
