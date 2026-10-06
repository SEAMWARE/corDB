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
// modifiedAt member. An entity is created whole, with one time. With COR_DB_SYS_TIMES=1 the store keeps
// that one time - the entity's createdAt - and, below the entity, only the times that differ from it:
//
//   created entity       the entity's createdAt; its modifiedAt only once it differs
//   modified attribute   its own modifiedAt (its createdAt still the entity's)
//   added attribute      its own createdAt and modifiedAt
//
// A tree is converted at the store's edges only, in one pass - corDbTreeIn leaves out every createdAt /
// modifiedAt equal to the entity's createdAt, corDbTreeOut puts them back (a createdAt right before its
// object's modifiedAt, both last when neither is there - where corNgsild puts them). Everything outside
// corDB sees the members as ever; the store, the log and the snapshot keep the short form, plain
// CorNodes. Never inside a value: a Property's JSON value is the user's.
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
// corDbTreeIn - an ENTITY (entityCreatedAt 0), or one of its attributes (entityCreatedAt its entity's),
// into the store, malloc, what it inherits left out
//
extern CorNode* corDbTreeIn(CorNode* srcP, int64_t entityCreatedAt);



// -----------------------------------------------------------------------------
//
// corDbTreeOut - a store ENTITY (entityCreatedAt 0), or one of its attributes (entityCreatedAt its
// entity's), out of it, every time in place
//
extern CorNode* corDbTreeOut(CorAlloc* kaP, CorNode* storeP, int64_t entityCreatedAt);



// -----------------------------------------------------------------------------
//
// corDbAttrTimesFill / corDbAttrTimesDrop - an attribute of a store entity, IN PLACE: its instances'
// missing times put in (the entity's createdAt) / the times equal to the entity's createdAt taken out -
// around corNgsild's in-place update (corDbEntityAttrsSet)
//
extern void corDbAttrTimesFill(CorNode* attrP, int64_t entityCreatedAt);
extern void corDbAttrTimesDrop(CorNode* attrP, int64_t entityCreatedAt);



// -----------------------------------------------------------------------------
//
// corDbCreatedAt / corDbModifiedAt - an object's times as they are: its member, else the entity's
// createdAt - parentCreatedAt (0 for the entity itself, whose modifiedAt is then its createdAt)
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
