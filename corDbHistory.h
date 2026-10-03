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
  char*      datasetId;                                // NULL: the default instance (malloc only for a named one)
  char*      body;                                     // the history RECORD, cor binary: { id, type, attr, datasetId?, deletedAt?, instanceId, instance }
  int        bodyLen;                                  //   - the same bytes as the history log's record body
} CorDbInstance;



// -----------------------------------------------------------------------------
//
// CorDbHistAttr / CorDbHistEntity
//
typedef struct CorDbHistAttr
{
  const char*            name;                         // expanded - interned (CorDbHistory.names): not this attribute's to free
  CorDbInstance*         instanceV;                    // in the order written - inlineV until it outgrows it
  int                    instances;
  int                    size;
  struct CorDbHistAttr*  next;
  CorDbInstance          inlineV[2];                   // the first two instances: no array to allocate for most attributes
} CorDbHistAttr;

typedef struct CorDbHistEntity
{
  char*                    id;
  const char*              type;                       // expanded, interned; the newest type the entity was written with
  uint64_t                 createdAtNs;
  uint64_t                 deletedAtNs;                // != 0: deleted from current state (its history stays)
  CorDbHistAttr*           attrs;
  CorDbHistAttr*           lastAttr;
  struct CorDbHistEntity*  next;                       // creation order
} CorDbHistEntity;



struct CorDbHistory;



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

  //
  // Attribute names and entity types, interned: the same few strings, repeated for every entity -
  // a strdup each was a malloc per attribute per entity (measured: malloc a quarter of a batch create)
  //
  struct CorHashTable*  names;
  char**                nameV;                         // every interned string, to free them
  int                   namesN;
  int                   namesSize;
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
// CorDbHistRecord - one history record, encoded and not yet in the index
//
// What a write prepares BEFORE the write lock where it can (corDbHistoryWrite.c): the encoding is the
// expensive half; adding it to the index, under the lock, is a few stores.
//
typedef struct CorDbHistRecord
{
  char*        body;                                 // malloc, exactly its size - taken over by corDbHistoryRecordAdd
  int          bodyLen;
  const char*  attrName;                             // these four point into the caller's data / arena
  const char*  instanceId;
  const char*  datasetId;
  CorNode*     instanceP;
  uint64_t     observedAtNs;
  uint64_t     createdAtNs;
  uint64_t     modifiedAtNs;
  uint64_t     deletedAtNs;
} CorDbHistRecord;



// -----------------------------------------------------------------------------
//
// corDbHistoryRecordEncode - an instance's record, encoded; no lock, nothing shared touched
//
// instanceP is read, never changed. kaP: scratch (a generated instanceId lives there). False: out of
// memory.
//
extern bool corDbHistoryRecordEncode(const char* entityId, const char* entityType, const char* attrName, const char* datasetId,
                                     CorNode* instanceP, uint64_t deletedAtNs, CorAlloc* kaP, CorDbHistRecord* recP);



// -----------------------------------------------------------------------------
//
// corDbHistoryRecordAdd - an encoded record into the entity's history (its body taken over); under
// the write lock. NULL out of memory (the body then still the caller's).
//
extern CorDbInstance* corDbHistoryRecordAdd(CorDbHistory* hP, CorDbHistEntity* eP, CorDbHistRecord* recP);



// -----------------------------------------------------------------------------
//
// corDbHistoryInstanceAdd - one instance appended to an attribute's history
//
// instanceP is the instance as the store has it (an object: type, value, observedAt, sub-attributes
// ...) - read, never changed: it may be the live store's. What is kept is its encoding, with its
// instanceId in it: the one it carries, or one generated. kaP is scratch for that (a request's arena);
// nothing in it is kept. Returns the instance, or NULL out of memory.
//
extern CorDbInstance* corDbHistoryInstanceAdd(CorDbHistory* hP, CorDbHistEntity* eP, const char* attrName, const char* datasetId,
                                              CorNode* instanceP, uint64_t deletedAtNs, CorAlloc* kaP);



// -----------------------------------------------------------------------------
//
// corDbHistoryEntityEvent - an entity created, replaced or deleted, at atNs: the index updated and the
// record { id, type, entityOp, at } encoded into *outP (the caller logs it, then frees outP->buf)
//
// entityOp: "created", "replaced", "deleted". Returns false out of memory.
//
extern bool corDbHistoryEntityEvent(CorDbHistEntity* eP, const char* entityOp, uint64_t atNs, CorAlloc* kaP, CorBinBuffer* outP);

//
// ... in its two halves: the record encoded (no lock - before it), the index updated (under it)
//
extern bool corDbHistoryEntityEventEncode(const char* entityId, const char* entityType, const char* entityOp, uint64_t atNs, CorAlloc* kaP, CorBinBuffer* outP);
extern void corDbHistoryEntityEventApply(CorDbHistEntity* eP, const char* entityOp, uint64_t atNs);



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
