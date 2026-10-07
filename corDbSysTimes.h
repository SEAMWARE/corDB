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
// System timestamps - one per created entity (coraine doc/cor-protocol-details.md § 4.10a)
//
// corNgsild gives the entity, every attribute instance and every sub-attribute a createdAt and a
// modifiedAt member. An entity is created whole, with one time. The store keeps that one time - the
// entity's createdAt - and, below the entity, only the times that differ from it:
//
//   created entity       the entity's createdAt; its modifiedAt only once it differs
//   modified attribute   its own modifiedAt (its createdAt still the entity's)
//   added attribute      its own createdAt and modifiedAt
//
// A time that is not there is the entity's createdAt. An object whose time was left out is MARKED (CorNode
// flag 0x04: createdAt inherited, 0x08: modifiedAt inherited); the entity's createdAt is kept at its front,
// after the id the index puts first. In, corDbTreeIn leaves the inherited times out and marks; out, a copy
// is corTree's clone and the marked objects get their times back - a createdAt right before its object's
// modifiedAt, both last when neither is there (where corNgsild puts them). corNgsild's ldEntityAttrsSet
// keeps an inherited createdAt inherited. Everything outside corDB sees every time; the store, the log and
// the snapshot keep the short form (the marks are not written - corDbTreeIn sets them again on replay). A
// store written with every time in place is read as it is. Never inside a value: a Property's JSON value is
// the user's.
//
#include <stdbool.h>                                  // bool
#include <stdint.h>                                   // int64_t

#include "corAlloc/CorAlloc.h"                        // CorAlloc
#include "corTree/CorNode.h"                          // CorNode



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
// corDbEntityCopy / corDbEntityCopyFinish - corDbTreeOut of a store ENTITY in two steps: the copy, under
// the store's lock (as fast as a plain clone - the lock is held no longer than without inherited times),
// then the times put back, after the lock, on the request's own copy. *createdAtP: what Finish needs.
//
extern CorNode* corDbEntityCopy(CorAlloc* kaP, CorNode* storeP, int64_t* createdAtP);
extern void     corDbEntityCopyFinish(CorAlloc* kaP, CorNode* copyP, int64_t createdAt);



// -----------------------------------------------------------------------------
//
// corDbCreatedAt / corDbModifiedAt - an object's times as they are: its member, else the entity's
// createdAt - parentCreatedAt (0 for the entity itself, whose modifiedAt is then its createdAt)
//
extern int64_t corDbCreatedAt(CorNode* nodeP, int64_t parentCreatedAt);
extern int64_t corDbModifiedAt(CorNode* nodeP, int64_t parentCreatedAt);

// -----------------------------------------------------------------------------
//
// corDbAttrFold - an attribute of a store entity written in place (ldEntityAttrsSet): the "type" members it
// brought kept in the nodes (CorNode.kind), as corDbTreeIn keeps them
//
extern void corDbAttrFold(CorNode* attrP);



// -----------------------------------------------------------------------------
//
// corDbFullView - a store entity for history (--troe corDB), which encodes instances whole: itself with no
// history, else a copy (kaP) with every time in place
//
extern bool corDbHistoryOn;

static inline CorNode* corDbFullView(CorNode* storeP, CorAlloc* kaP)
{
  return ((corDbHistoryOn == false) || (storeP == NULL)) ? storeP : corDbTreeOut(kaP, storeP, 0);
}

#endif  // CORDB_CORDBSYSTIMES_H_
