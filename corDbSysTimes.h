#ifndef CORDB_CORDBSYSTIMES_H_
#define CORDB_CORDBSYSTIMES_H_

//
// FILE            corDbSysTimes.h
//
// AUTHOR          Ken Zangelin
//
// Copyright 2026 Seamware
// SPDX-License-Identifier: Apache-2.0
//
// System timestamps by inheritance (coraine doc/cor-protocol-details.md § 4.10a) - a build choice,
// COR_DB_SYS_TIMES (0 by default).
//
// corNgsild gives the entity, every attribute instance and every sub-attribute a createdAt and a
// modifiedAt member. Nearly all of them say the same thing: an entity is created whole, so its objects'
// createdAt is the entity's, and an object nobody changed has modifiedAt == createdAt. With
// COR_DB_SYS_TIMES=1 the store keeps only what an object cannot inherit:
//
//   createdAt   kept only where it differs from the parent's (the entity's for an instance, the
//               instance's for a sub-attribute) - the entity always keeps its own
//   modifiedAt  kept only where it differs from the object's own createdAt
//
// A tree is converted at the store's edges only - corDbTreeIn drops what is inherited, corDbTreeOut puts
// it back (last, where corNgsild puts it). Everything outside corDB sees the members as ever, and the
// store is plain CorNodes, freed as ever. A reader of a STORE object asks corDbCreatedAt / corDbModifiedAt
// with the object's parent chain. Never inside a value: a Property's JSON value is the user's.
//
// With COR_DB_SYS_TIMES=0 corDbTreeIn / corDbTreeOut are corTreeClone, the accessors member lookups.
//
#include <stdbool.h>                                  // bool
#include <stdint.h>                                   // int64_t

#include "corAlloc/CorAlloc.h"                        // CorAlloc
#include "corTree/CorNode.h"                          // CorNode



#ifndef COR_DB_SYS_TIMES
#define COR_DB_SYS_TIMES 0
#endif



// -----------------------------------------------------------------------------
//
// corDbTreeIn - an ENTITY (or an attribute of one: 'parentCreatedAt' its entity's createdAt) into the
// store, malloc, what is inherited left out
//
extern CorNode* corDbTreeIn(CorNode* srcP, int64_t parentCreatedAt);



// -----------------------------------------------------------------------------
//
// corDbTreeOut - a store ENTITY (or attribute: 'parentCreatedAt' as above) out of it, its times complete
//
extern CorNode* corDbTreeOut(CorAlloc* kaP, CorNode* storeP, int64_t parentCreatedAt);



// -----------------------------------------------------------------------------
//
// corDbCreatedAt / corDbModifiedAt - an object's times as they are: its member, else inherited
//
// parentCreatedAt: the parent's createdAt (0 for the entity itself)
//
extern int64_t corDbCreatedAt(CorNode* nodeP, int64_t parentCreatedAt);
extern int64_t corDbModifiedAt(CorNode* nodeP, int64_t parentCreatedAt);

// -----------------------------------------------------------------------------
//
// corDbFullView - a store entity for history (--troe corDB), which encodes instances whole: itself with
// COR_DB_SYS_TIMES=0 or no history, else a copy (kaP) with every time in place
//
extern bool corDbHistoryOn;

static inline CorNode* corDbFullView(CorNode* storeP, CorAlloc* kaP)
{
  return ((COR_DB_SYS_TIMES == 0) || (corDbHistoryOn == false) || (storeP == NULL)) ? storeP : corDbTreeOut(kaP, storeP, 0);
}

#endif  // CORDB_CORDBSYSTIMES_H_
