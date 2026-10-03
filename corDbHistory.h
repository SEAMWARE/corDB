#ifndef CORDB_CORDBHISTORY_H_
#define CORDB_CORDBHISTORY_H_

//
// FILE            corDbHistory.h
//
// AUTHOR          Ken Zangelin
//
// Copyright 2026 Seamware
// SPDX-License-Identifier: Apache-2.0
//
// corDB's temporal history (TRoE, `--troe corDB`): every write that reaches an attribute appends an
// instance of it - current state overwrites, history appends, under the same lock. Per tenant, kept
// beside the store: entity -> attribute -> instances in the order they were written, each instance
// ENCODED (cor binary - a fifth of the tree) with the timestamps queries filter on beside it.
//
// It is the history log's materialised view, as the store is the current-state log's: rebuilt from
// the hist-N.cor segments at recovery (corDbPersist.c).
//
#include <stdbool.h>                                   // bool
#include <stdint.h>                                    // uint64_t

#include "corAlloc/CorAlloc.h"                         // CorAlloc
#include "corTree/CorNode.h"                           // CorNode
#include "corTree/corTreeBin.h"                        // CorBinBuffer



// -----------------------------------------------------------------------------
//
// CorDbInstance - one attribute instance
//
// The timestamps are the instance's own, in ns since the epoch, 0 when it has none: what a
// timeproperty filter and an ordering read without decoding the body.
//
typedef struct CorDbInstance
{
  uint64_t   observedAtNs;
  uint64_t   createdAtNs;
  uint64_t   modifiedAtNs;
  uint64_t   deletedAtNs;                              // != 0: the attribute was deleted - the instance is its tombstone
  char*      instanceId;
  char*      datasetId;                                // NULL: the default instance
  char*      body;                                     // the history RECORD, cor binary: { id, type, attr, datasetId?, deletedAt?, instance }
  int        bodyLen;                                  //   - the same bytes as the history log's record body
} CorDbInstance;



// -----------------------------------------------------------------------------
//
// CorDbHistAttr / CorDbHistEntity
//
typedef struct CorDbHistAttr
{
  char*                  name;                         // expanded
  CorDbInstance*         instanceV;                    // in the order written
  int                    instances;
  int                    size;
  struct CorDbHistAttr*  next;
} CorDbHistAttr;

typedef struct CorDbHistEntity
{
  char*                    id;
  char*                    type;                       // expanded; the newest type the entity was written with
  uint64_t                 createdAtNs;
  uint64_t                 deletedAtNs;                // != 0: deleted from current state (its history stays)
  CorDbHistAttr*           attrs;
  CorDbHistAttr*           lastAttr;
  struct CorDbHistEntity*  next;                       // creation order
} CorDbHistEntity;



// -----------------------------------------------------------------------------
//
// CorDbHistory - one tenant's history
//
typedef struct CorDbHistory
{
  struct CorHashTable*  byId;                          // entity id -> CorDbHistEntity
  int                   slots;
  int                   count;
  CorDbHistEntity*      first;                         // creation order
  CorDbHistEntity*      last;
} CorDbHistory;



// -----------------------------------------------------------------------------
//
// corDbHistoryOn - `--troe corDB` given: the write sites record history
//
extern bool corDbHistoryOn;



// -----------------------------------------------------------------------------
//
// corDbHistoryEntity - the entity's history, created when 'create' and it has none; NULL otherwise
//
extern CorDbHistEntity* corDbHistoryEntity(CorDbHistory* hP, const char* entityId, const char* entityType, bool create);



// -----------------------------------------------------------------------------
//
// corDbHistoryInstanceAdd - one instance appended to an attribute's history
//
// instanceP is the instance as the store has it (an object: type, value, observedAt, sub-attributes
// ...) - read, never changed: it may be the live store's. What is kept is its encoding, with its
// instanceId in it: the one it carries, or one generated. kaP is scratch for that (a request's arena);
// nothing in it is kept. Returns the instance, or NULL out of memory.
//
extern CorDbInstance* corDbHistoryInstanceAdd(CorDbHistEntity* eP, const char* attrName, const char* datasetId,
                                              CorNode* instanceP, uint64_t deletedAtNs, CorAlloc* kaP);



// -----------------------------------------------------------------------------
//
// corDbHistoryEntityEvent - an entity created, replaced or deleted, at atNs: the index updated and the
// record { id, type, entityOp, at } encoded into *outP (the caller logs it, then frees outP->buf)
//
// entityOp: "created", "replaced", "deleted". Returns false out of memory.
//
extern bool corDbHistoryEntityEvent(CorDbHistEntity* eP, const char* entityOp, uint64_t atNs, CorAlloc* kaP, CorBinBuffer* outP);



// -----------------------------------------------------------------------------
//
// corDbHistoryInstanceDecode - an instance back as a tree, in kaP (a request's arena)
//
extern CorNode* corDbHistoryInstanceDecode(CorDbInstance* iP, CorAlloc* kaP);



// -----------------------------------------------------------------------------
//
// corDbHistoryFree - every entity, attribute and instance of a tenant's history
//
extern void corDbHistoryFree(CorDbHistory* hP);

#endif  // CORDB_CORDBHISTORY_H_
