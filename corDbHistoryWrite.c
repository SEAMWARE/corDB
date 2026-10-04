//
// FILE            corDbHistoryWrite.c
//
// AUTHOR          Ken Zangelin
//
// Copyright 2026 Seamware
// SPDX-License-Identifier: Apache-2.0
//
#include <stdbool.h>                                   // bool
#include <stdint.h>                                    // uint64_t
#include <stdlib.h>                                    // free, realloc
#include <string.h>                                    // strcmp
#include <time.h>                                      // clock_gettime
#include <pthread.h>                                   // pthread_mutex_*

#include "corLog/corLog.h"                             // COR_E
#include "corAlloc/CorAlloc.h"                         // CorAlloc
#include "corTree/CorNode.h"                           // CorNode
#include "corTree/corTreeBuilder.h"                    // corTreeObject, corTreeString, corTreeInteger, corTreeChildAdd
#include "corTree/corTreeLookup.h"                     // corTreeLookup

#include "corNgsild/ldEntityMerge.h"                   // LdMergeReport
#include "corNgsild/ldInstanceWritten.h"               // ldInstanceWritten
#include "corNgsild/ldTermId.h"                        // ldTermId
#include "corNgsild/CorTerm.h"                         // CorTerm*

#include "corDB/corDbHistory.h"                        // CorDbHistory, corDbHistoryEntity, corDbHistoryInstanceAdd, corDbHistoryEntityEvent
#include "corDB/corDbPersist.h"                        // corDbPersistHistAppend
#include "corDB/corDbStore.h"                          // CorDbStore
#include "corDB/corDbHistoryWrite.h"                   // Own interface



// -----------------------------------------------------------------------------
//
// isAttribute - an entity member that is an Attribute (not id, type, scope, a system timestamp, @...)
//
static bool isAttribute(CorNode* memberP)
{
  if ((memberP->name == NULL) || (memberP->name[0] == '@') || (memberP->type != CorObject))
    return false;

  if (strcmp(memberP->name, "_id") == 0)
    return false;

  switch (ldTermId(memberP))
  {
  case CorTermId:
  case CorTermType:
  case CorTermScope:
  case CorTermCreatedAt:
  case CorTermModifiedAt:
    return false;
  default:
    return true;
  }
}



// -----------------------------------------------------------------------------
//
// typeOf - the entity's type, as the history keeps it (the first one of several)
//
static const char* typeOf(CorNode* entityP)
{
  CorNode* typeP = corTreeLookup(entityP, "type");

  if (typeP == NULL)
    return NULL;
  if (typeP->type == CorString)
    return typeP->value.s;
  if ((typeP->type == CorArray) && (typeP->value.head != NULL) && (typeP->value.head->type == CorString))
    return typeP->value.head->value.s;
  return NULL;
}



// -----------------------------------------------------------------------------
//
// timeOf - when the write happened: the entity's modifiedAt (the store's ns), now without one
//
static uint64_t timeOf(CorNode* entityP)
{
  CorNode* tP = (entityP != NULL) ? corTreeLookup(entityP, "modifiedAt") : NULL;

  if ((tP != NULL) && (tP->type == CorInt) && (tP->value.i > 0))
    return (uint64_t) tP->value.i;

  struct timespec ts;
  clock_gettime(CLOCK_REALTIME, &ts);
  return (uint64_t) ts.tv_sec * 1000000000ULL + (uint64_t) ts.tv_nsec;
}



// -----------------------------------------------------------------------------
//
// HistCtx - the entity a write records history for: what an enqueued item names
//
typedef struct HistCtx
{
  CorDbStore*  storeP;
  const char*  id;
  const char*  type;
} HistCtx;



// -----------------------------------------------------------------------------
//
// CorDbHistItem - one queued piece of history: a record (an instance) or an entity event, owning its
// body and its strings - one malloc, the strings after the struct. Drained by whoever releases the
// write lock next, maybe another request's thread: nothing in it may point into a request's arena.
//
typedef struct CorDbHistItem
{
  struct CorDbHistItem*  next;
  bool                   isEvent;
  char*                  body;                       // a record's: in the item (strings[]); an event's: malloc
  int                    bodyLen;
  uint64_t               observedAtNs;
  uint64_t               createdAtNs;
  uint64_t               modifiedAtNs;
  uint64_t               deletedAtNs;
  uint64_t               atNs;                       // an event's
  const char*            entityId;
  const char*            entityType;                 // NULL: unknown here (a delete)
  const char*            attrName;
  const char*            datasetId;
  const char*            entityOp;
  char                   strings[];
} CorDbHistItem;



// -----------------------------------------------------------------------------
//
// itemNew - an item with copies of its strings and of a record's body (an event's body is its malloc)
//
static CorDbHistItem* itemNew(const char* id, const char* type, const char* attrName, const char* datasetId, const char* entityOp,
                              const char* body, int bodyLen)
{
  const char* sV[5] = { id, type, attrName, datasetId, entityOp };
  int         lenV[5];
  int         total = 0;

  for (int i = 0; i < 5; i++)
  {
    lenV[i] = (sV[i] != NULL) ? (int) strlen(sV[i]) + 1 : 0;
    total  += lenV[i];
  }

  CorDbHistItem* itemP = (CorDbHistItem*) malloc(sizeof(CorDbHistItem) + total + ((body != NULL) ? bodyLen : 0));

  if (itemP == NULL)
    return NULL;

  memset(itemP, 0, sizeof(CorDbHistItem));

  const char** dstV[5] = { &itemP->entityId, &itemP->entityType, &itemP->attrName, &itemP->datasetId, &itemP->entityOp };
  char*        p       = itemP->strings;

  for (int i = 0; i < 5; i++)
  {
    if (sV[i] == NULL)
      continue;

    memcpy(p, sV[i], lenV[i]);
    *dstV[i] = p;
    p       += lenV[i];
  }

  if (body != NULL)
  {
    memcpy(p, body, bodyLen);
    itemP->body    = p;
    itemP->bodyLen = bodyLen;
  }

  return itemP;
}



// -----------------------------------------------------------------------------
//
// enqueue - under the tenant's write lock: lock order is history order
//
static void enqueue(CorDbStore* storeP, CorDbHistItem* itemP)
{
  pthread_mutex_lock(&storeP->histQMutex);

  if (storeP->histQTail != NULL)
    storeP->histQTail->next = itemP;
  else
    __atomic_store_n(&storeP->histQHead, itemP, __ATOMIC_RELEASE);
  storeP->histQTail = itemP;

  pthread_mutex_unlock(&storeP->histQMutex);
}



// -----------------------------------------------------------------------------
//
// corDbHistoryDrain - every queued item into the index and the history log, in order
//
// Under histMutex, which also guards the index: the list is taken whole, and a later drainer waits for
// this one - so the items are applied in the order they were queued, whoever drains them.
//
void corDbHistoryDrain(CorDbStore* storeP)
{
  if ((storeP == NULL) || (storeP->historyP == NULL))
    return;

  pthread_mutex_lock(&storeP->histMutex);

  pthread_mutex_lock(&storeP->histQMutex);
  CorDbHistItem* itemP = storeP->histQHead;
  __atomic_store_n(&storeP->histQHead, NULL, __ATOMIC_RELEASE);
  storeP->histQTail = NULL;
  pthread_mutex_unlock(&storeP->histQMutex);

  CorDbHistItem* nextP;

  for (; itemP != NULL; itemP = nextP)
  {
    nextP = itemP->next;

    CorDbHistEntity* eP = corDbHistoryEntity(storeP->historyP, itemP->entityId, itemP->entityType, true);

    if (eP == NULL)
      COR_E("corDB: out of memory for the history of '%s'", itemP->entityId);
    else if (itemP->isEvent)
    {
      corDbHistoryEntityEventApply(eP, itemP->entityOp, itemP->atNs);
      corDbPersistHistAppend(storeP->persistP, itemP->body, itemP->bodyLen);
    }
    else
    {
      CorDbHistRecord rec;

      memset(&rec, 0, sizeof(rec));
      rec.body         = itemP->body;
      rec.bodyLen      = itemP->bodyLen;
      rec.attrName     = itemP->attrName;
      rec.datasetId    = itemP->datasetId;
      rec.observedAtNs = itemP->observedAtNs;
      rec.createdAtNs  = itemP->createdAtNs;
      rec.modifiedAtNs = itemP->modifiedAtNs;
      rec.deletedAtNs  = itemP->deletedAtNs;

      CorDbInstance* iP = corDbHistoryRecordAdd(storeP->historyP, eP, &rec);

      if (iP == NULL)
        COR_E("corDB: out of memory recording an instance of '%s' of '%s'", itemP->attrName, itemP->entityId);
      else
        corDbPersistHistAppend(storeP->persistP, iP->body, iP->bodyLen);
    }

    if (itemP->isEvent)
      free(itemP->body);
    free(itemP);
  }

  pthread_mutex_unlock(&storeP->histMutex);
}



// -----------------------------------------------------------------------------
//
// enqueueRecord / enqueueEvent - an encoded record or event, queued (its body handed over)
//
static void enqueueRecord(HistCtx* cP, CorDbHistRecord* recP)
{
  CorDbHistItem* itemP = itemNew(cP->id, cP->type, recP->attrName, recP->datasetId, NULL, recP->body, recP->bodyLen);

  if (itemP == NULL)
  {
    COR_E("corDB: out of memory queueing history of '%s'", cP->id);
    return;
  }

  itemP->observedAtNs = recP->observedAtNs;
  itemP->createdAtNs  = recP->createdAtNs;
  itemP->modifiedAtNs = recP->modifiedAtNs;
  itemP->deletedAtNs  = recP->deletedAtNs;

  enqueue(cP->storeP, itemP);
}

static void enqueueEvent(CorDbStore* storeP, const char* id, const char* type, const char* entityOp, uint64_t atNs, char* body, int bodyLen)
{
  CorDbHistItem* itemP = itemNew(id, type, NULL, NULL, entityOp, NULL, 0);

  if (itemP == NULL)
  {
    COR_E("corDB: out of memory queueing history of '%s'", id);
    free(body);
    return;
  }

  itemP->isEvent = true;
  itemP->body    = body;
  itemP->bodyLen = bodyLen;
  itemP->atNs    = atNs;

  enqueue(storeP, itemP);
}



// -----------------------------------------------------------------------------
//
// entityOf - the context of a write: its store, the entity's id and type
//
static HistCtx* entityOf(CorDbStore* storeP, HistCtx* cP, CorNode* entityP)
{
  CorNode* idP = corTreeLookup(entityP, "id");

  if ((idP == NULL) || (idP->type != CorString))
    return NULL;

  cP->storeP = storeP;
  cP->id     = idP->value.s;
  cP->type   = typeOf(entityP);

  return cP;
}



// -----------------------------------------------------------------------------
//
// entityEvent - an entity-level record: the index and the history log
//
static void entityEvent(CorDbStore* storeP, HistCtx* eP, const char* entityOp, uint64_t atNs, CorAlloc* kaP)
{
  CorBinBuffer body;

  if (corDbHistoryEntityEventEncode(eP->id, eP->type, entityOp, atNs, kaP, &body) == false)
  {
    COR_E("corDB: out of memory recording '%s' %s", eP->id, entityOp);
    free(body.buf);
    return;
  }

  enqueueEvent(storeP, eP->id, eP->type, entityOp, atNs, body.buf, body.len);
}



// -----------------------------------------------------------------------------
//
// instanceAppend - one instance: the index and the history log
//
static void instanceAppend(CorDbStore* storeP, HistCtx* eP, const char* attrName, const char* datasetId,
                           CorNode* instanceP, uint64_t deletedAtNs, CorAlloc* kaP)
{
  CorDbHistRecord rec;

  (void) storeP;

  if (corDbHistoryRecordEncode(eP->id, eP->type, attrName, datasetId, instanceP, deletedAtNs, kaP, &rec) == false)
  {
    COR_E("corDB: out of memory recording an instance of '%s' of '%s'", attrName, eP->id);
    return;
  }

  enqueueRecord(eP, &rec);
}



// -----------------------------------------------------------------------------
//
// tombstone - the instance that records a deletion: the deleted instance's kind, the NGSI-LD Null as
// its value, and deletedAt (TS 104-175 clause 5.7 - "an instance of the Property is recorded with its
// value set to urn:ngsi-ld:null and the deletedAt Temporal Property set")
//
static CorNode* tombstone(CorNode* deletedInstanceP, uint64_t atNs, CorAlloc* kaP)
{
  CorNode* tP    = corTreeObject(kaP, NULL);
  CorNode* typeP = (deletedInstanceP != NULL) ? corTreeLookup(deletedInstanceP, "type") : NULL;

  if (tP == NULL)
    return NULL;

  if ((typeP != NULL) && (typeP->type == CorString))
    corTreeChildAdd(tP, corTreeString(kaP, "type", typeP->value.s));

  corTreeChildAdd(tP, corTreeString(kaP, "value", "urn:ngsi-ld:null"));
  corTreeChildAdd(tP, corTreeInteger(kaP, "deletedAt", (long long) atNs));
  corTreeChildAdd(tP, corTreeInteger(kaP, "modifiedAt", (long long) atNs));

  return tP;
}



// -----------------------------------------------------------------------------
//
// datasetOf - an instance's datasetId as the store keys it (the member name), NULL for the default
//
static const char* datasetOf(CorNode* instanceP)
{
  if ((instanceP->name == NULL) || (strcmp(instanceP->name, "@none") == 0))
    return NULL;

  return instanceP->name;
}



// -----------------------------------------------------------------------------
//
// allInstances - every instance of an Attribute (the store keys them by datasetId)
//
static void allInstances(CorDbStore* storeP, HistCtx* eP, CorNode* attrP, CorAlloc* kaP)
{
  for (CorNode* instP = attrP->value.head; instP != NULL; instP = instP->next)
  {
    if (instP->type == CorObject)
      instanceAppend(storeP, eP, attrP->name, datasetOf(instP), instP, 0, kaP);
  }
}



// -----------------------------------------------------------------------------
//
// removedInstances - a deletion for each instance of preP that postP lacks (postP NULL: all of them)
//
static void removedInstances(CorDbStore* storeP, HistCtx* eP, const char* attrName, CorNode* preP, CorNode* postP,
                             uint64_t atNs, CorAlloc* kaP)
{
  if ((preP == NULL) || (preP->type != CorObject))
    return;

  for (CorNode* instP = preP->value.head; instP != NULL; instP = instP->next)
  {
    if ((instP->name == NULL) || (instP->type != CorObject))
      continue;

    if ((postP == NULL) || (corTreeLookup(postP, instP->name) == NULL))
    {
      CorNode* tP = tombstone(instP, atNs, kaP);

      if (tP != NULL)
        instanceAppend(storeP, eP, attrName, datasetOf(instP), tP, atNs, kaP);
    }
  }
}



// -----------------------------------------------------------------------------
//
// lookupFrom - a member by name, searched from *cursorPP on and then from the start; *cursorPP left
// after it. Two entities walked with their attributes in the same order (a replace's new and old
// entity, a clone and its original) cost a compare or two per attribute instead of a walk each.
//
static CorNode* lookupFrom(CorNode* containerP, CorNode** cursorPP, const char* name)
{
  if ((containerP == NULL) || (containerP->type != CorObject))
    return NULL;

  CorNode* startP = (*cursorPP != NULL) ? *cursorPP : containerP->value.head;

  for (CorNode* mP = startP; mP != NULL; mP = mP->next)
  {
    if ((mP->name != NULL) && (strcmp(mP->name, name) == 0))
    {
      *cursorPP = mP->next;
      return mP;
    }
  }

  for (CorNode* mP = containerP->value.head; mP != startP; mP = mP->next)
  {
    if ((mP->name != NULL) && (strcmp(mP->name, name) == 0))
    {
      *cursorPP = mP->next;
      return mP;
    }
  }

  return NULL;
}



// -----------------------------------------------------------------------------
//
// modifiedAtOf - an instance's modifiedAt (0: none)
//
static uint64_t modifiedAtOf(CorNode* instanceP)
{
  CorNode* tP = corTreeLookup(instanceP, "modifiedAt");

  return ((tP != NULL) && (tP->type == CorInt) && (tP->value.i > 0)) ? (uint64_t) tP->value.i : 0;
}



// -----------------------------------------------------------------------------
//
// sameModifiedAt - do two instances carry the same modifiedAt
//
static bool sameModifiedAt(CorNode* aP, CorNode* bP)
{
  CorNode* aTP = corTreeLookup(aP, "modifiedAt");
  CorNode* bTP = corTreeLookup(bP, "modifiedAt");

  return (aTP != NULL) && (bTP != NULL) && (aTP->type == CorInt) && (bTP->type == CorInt) && (aTP->value.i == bTP->value.i);
}



// -----------------------------------------------------------------------------
//
// corDbHistoryPrepare - every instance of every attribute of the entity, encoded (no lock)
//
void corDbHistoryPrepare(CorDbHistPre* preP, CorNode* entityP, const char* entityOp, CorAlloc* kaP)
{
  CorNode* idP = (entityP != NULL) ? corTreeLookup(entityP, "id") : NULL;

  if ((idP == NULL) || (idP->type != CorString))
    return;

  const char* type = typeOf(entityP);

  //
  // A replace stores the whole entity, but the instances it did not write are carried over with their
  // modifiedAt and recorded by nobody: encoding them here was 4/5 of a batch update's history cost.
  // Only the instances stamped with the write's own time are prepared; corDbHistoryReplacedPre encodes
  // any other instance that differs from the stored one under the lock.
  //
  bool onlyWritten = (strcmp(entityOp, "replaced") == 0);

  preP->entityOp = entityOp;
  preP->atNs     = timeOf(entityP);
  if (corDbHistoryEntityEventEncode(idP->value.s, type, entityOp, preP->atNs, kaP, &preP->event) == false)
  {
    free(preP->event.buf);
    preP->event.buf = NULL;                          // encoded under the lock instead
    preP->event.len = 0;
  }

  for (CorNode* attrP = entityP->value.head; attrP != NULL; attrP = attrP->next)
  {
    if (isAttribute(attrP) == false)
      continue;

    for (CorNode* instP = attrP->value.head; instP != NULL; instP = instP->next)
    {
      if (instP->type != CorObject)
        continue;

      if ((onlyWritten == true) && (modifiedAtOf(instP) != preP->atNs))
        continue;

      if (preP->recs == preP->size)
      {
        int              size = (preP->size == 0) ? 8 : preP->size * 2;
        CorDbHistRecord* v    = (CorDbHistRecord*) realloc(preP->recV, size * sizeof(CorDbHistRecord));

        if (v == NULL)
          return;                                    // what is not prepared is encoded under the lock: see ...Pre

        preP->recV = v;
        preP->size = size;
      }

      if (corDbHistoryRecordEncode(idP->value.s, type, attrP->name, datasetOf(instP), instP, 0, kaP, &preP->recV[preP->recs]) == true)
        ++preP->recs;
    }
  }
}



// -----------------------------------------------------------------------------
//
// corDbHistoryPrepareV / corDbHistoryPreVFree - a batch's
//
void corDbHistoryPrepareV(CorDbHistPreV* preVP, CorNode** entityV, int n, const char* entityOp, CorAlloc* kaP)
{
  preVP->v = (CorDbHistPre*) calloc((n > 0) ? n : 1, sizeof(CorDbHistPre));
  preVP->n = (preVP->v != NULL) ? n : 0;

  for (int i = 0; i < preVP->n; i++)
  {
    if (entityV[i] != NULL)
      corDbHistoryPrepare(&preVP->v[i], entityV[i], entityOp, kaP);
  }
}

void corDbHistoryPreVFree(CorDbHistPreV* preVP)
{
  for (int i = 0; i < preVP->n; i++)
    corDbHistoryPreFree(&preVP->v[i]);

  free(preVP->v);
  preVP->v = NULL;
  preVP->n = 0;
}



// -----------------------------------------------------------------------------
//
// corDbHistoryPreFree - the bodies no index took over
//
void corDbHistoryPreFree(CorDbHistPre* preP)
{
  free(preP->recV);                                  // the bodies are in the request's arena
  free(preP->event.buf);
  memset(preP, 0, sizeof(*preP));
}



// -----------------------------------------------------------------------------
//
// recordAppend - a prepared record: into the index, then its bytes to the history log
//
static void recordAppend(CorDbStore* storeP, HistCtx* eP, CorDbHistRecord* recP)
{
  (void) storeP;
  enqueueRecord(eP, recP);
}



// -----------------------------------------------------------------------------
//
// preparedEvent - the entity-level record a Pre carries: applied to the index, its bytes logged (encoded
// under the lock when the Pre has none)
//
static void preparedEvent(CorDbStore* storeP, HistCtx* eP, CorDbHistPre* preP, CorAlloc* kaP)
{
  if (preP->event.buf == NULL)
  {
    entityEvent(storeP, eP, (preP->entityOp != NULL) ? preP->entityOp : "created", preP->atNs, kaP);
    return;
  }

  enqueueEvent(storeP, eP->id, eP->type, preP->entityOp, preP->atNs, preP->event.buf, preP->event.len);
  preP->event.buf = NULL;                            // the queue's now
  preP->event.len = 0;
}



// -----------------------------------------------------------------------------
//
// corDbHistoryDeletePrepare / corDbHistoryDeletedPre -
//
void corDbHistoryDeletePrepare(CorDbHistDel* delP, const char* entityId, CorAlloc* kaP)
{
  delP->atNs = timeOf(NULL);

  if (corDbHistoryEntityEventEncode(entityId, NULL, "deleted", delP->atNs, kaP, &delP->event) == false)
  {
    free(delP->event.buf);
    delP->event.buf = NULL;
    delP->event.len = 0;
  }
}

void corDbHistoryDeletedPre(CorDbStore* storeP, CorDbHistDel* delP, const char* entityId)
{
  if ((storeP == NULL) || (storeP->historyP == NULL) || (delP->event.buf == NULL))
    return;

  enqueueEvent(storeP, entityId, NULL, "deleted", delP->atNs, delP->event.buf, delP->event.len);
  delP->event.buf = NULL;                            // the queue's now
  delP->event.len = 0;
}



// -----------------------------------------------------------------------------
//
// corDbHistoryCreatedPre -
//
void corDbHistoryCreatedPre(CorDbStore* storeP, CorDbHistPre* preP, CorNode* entityP, CorAlloc* kaP)
{
  if ((storeP == NULL) || (storeP->historyP == NULL) || (entityP == NULL))
    return;

  HistCtx ctx; HistCtx* eP = entityOf(storeP, &ctx, entityP);

  if (eP == NULL)
    return;

  preparedEvent(storeP, eP, preP, kaP);

  for (int i = 0; i < preP->recs; i++)
    recordAppend(storeP, eP, &preP->recV[i]);
}



// -----------------------------------------------------------------------------
//
// corDbHistoryReplacedPre - as corDbHistoryReplaced, from the prepared records
//
void corDbHistoryReplacedPre(CorDbStore* storeP, CorDbHistPre* preP, CorNode* newEntityP, CorNode* oldEntityP, CorAlloc* kaP)
{
  if ((storeP == NULL) || (storeP->historyP == NULL) || (newEntityP == NULL))
    return;

  HistCtx ctx; HistCtx* eP = entityOf(storeP, &ctx, newEntityP);

  if (eP == NULL)
    return;

  uint64_t atNs = (preP->atNs != 0) ? preP->atNs : timeOf(newEntityP);

  preparedEvent(storeP, eP, preP, kaP);

  //
  // The entity walked in the order corDbHistoryPrepare walked it: an instance it prepared is the next
  // record. One stamped with this write's time was written by it - recorded, no comparison needed.
  // Any other (carried over, or not prepared: out of memory) is recorded if it differs from the stored
  // one, encoded here.
  //
  int      k       = 0;
  CorNode* oldCurP = NULL;

  for (CorNode* attrP = newEntityP->value.head; attrP != NULL; attrP = attrP->next)
  {
    if (isAttribute(attrP) == false)
      continue;

    CorNode* oldAttrP = (oldEntityP != NULL) ? lookupFrom(oldEntityP, &oldCurP, attrP->name) : NULL;

    for (CorNode* instP = attrP->value.head; instP != NULL; instP = instP->next)
    {
      if (instP->type != CorObject)
        continue;

      CorDbHistRecord* recP = ((k < preP->recs) && (preP->recV[k].instanceP == instP)) ? &preP->recV[k++] : NULL;

      if ((recP != NULL) && (recP->modifiedAtNs == atNs))
      {
        recordAppend(storeP, eP, recP);
        continue;
      }

      CorNode* oldInstP = ((oldAttrP != NULL) && (instP->name != NULL)) ? corTreeLookup(oldAttrP, instP->name) : NULL;

      if ((oldInstP != NULL) && (sameModifiedAt(instP, oldInstP) == true))
        continue;                                    // carried over, not written

      if (recP != NULL)
        recordAppend(storeP, eP, recP);
      else
        instanceAppend(storeP, eP, attrP->name, datasetOf(instP), instP, 0, kaP);
    }
  }

  if (oldEntityP == NULL)
    return;

  CorNode* newCurP = NULL;

  for (CorNode* oldAttrP = oldEntityP->value.head; oldAttrP != NULL; oldAttrP = oldAttrP->next)
  {
    if (isAttribute(oldAttrP))
      removedInstances(storeP, eP, oldAttrP->name, oldAttrP, lookupFrom(newEntityP, &newCurP, oldAttrP->name), atNs, kaP);
  }
}



// -----------------------------------------------------------------------------
//
// corDbHistoryCreated -
//
void corDbHistoryCreated(CorDbStore* storeP, CorNode* entityP, CorAlloc* kaP)
{
  if ((storeP == NULL) || (storeP->historyP == NULL) || (entityP == NULL))
    return;

  HistCtx ctx; HistCtx* eP = entityOf(storeP, &ctx, entityP);

  if (eP == NULL)
    return;

  entityEvent(storeP, eP, "created", timeOf(entityP), kaP);

  for (CorNode* attrP = entityP->value.head; attrP != NULL; attrP = attrP->next)
  {
    if (isAttribute(attrP))
      allInstances(storeP, eP, attrP, kaP);
  }
}



// -----------------------------------------------------------------------------
//
// corDbHistoryReplaced -
//
void corDbHistoryReplaced(CorDbStore* storeP, CorNode* newEntityP, CorNode* oldEntityP, CorAlloc* kaP)
{
  if ((storeP == NULL) || (storeP->historyP == NULL) || (newEntityP == NULL))
    return;

  HistCtx ctx; HistCtx* eP = entityOf(storeP, &ctx, newEntityP);

  if (eP == NULL)
    return;

  uint64_t atNs = timeOf(newEntityP);

  entityEvent(storeP, eP, "replaced", atNs, kaP);

  //
  // Every instance the replace WROTE - and a write gives what it writes a new modifiedAt. An instance
  // that kept its modifiedAt was carried over, not written: the store implements an Attribute delete as
  // a replace of the entity without it, and recording the others then doubled their history.
  //
  for (CorNode* attrP = newEntityP->value.head; attrP != NULL; attrP = attrP->next)
  {
    if (isAttribute(attrP) == false)
      continue;

    CorNode* oldAttrP = (oldEntityP != NULL) ? corTreeLookup(oldEntityP, attrP->name) : NULL;

    for (CorNode* instP = attrP->value.head; instP != NULL; instP = instP->next)
    {
      if (instP->type != CorObject)
        continue;

      CorNode* oldInstP = ((oldAttrP != NULL) && (instP->name != NULL)) ? corTreeLookup(oldAttrP, instP->name) : NULL;

      if ((oldInstP != NULL) && (sameModifiedAt(instP, oldInstP) == true))
        continue;

      instanceAppend(storeP, eP, attrP->name, datasetOf(instP), instP, 0, kaP);
    }
  }

  if (oldEntityP == NULL)
    return;

  for (CorNode* oldAttrP = oldEntityP->value.head; oldAttrP != NULL; oldAttrP = oldAttrP->next)
  {
    if (isAttribute(oldAttrP))
      removedInstances(storeP, eP, oldAttrP->name, oldAttrP, corTreeLookup(newEntityP, oldAttrP->name), atNs, kaP);
  }
}



// -----------------------------------------------------------------------------
//
// corDbHistoryMerged -
//
void corDbHistoryMerged(CorDbStore* storeP, CorNode* liveEntityP, LdMergeReport* reportP, CorAlloc* kaP)
{
  if ((storeP == NULL) || (storeP->historyP == NULL) || (liveEntityP == NULL) || (reportP == NULL) || (reportP->changes == NULL))
    return;

  HistCtx ctx; HistCtx* eP = entityOf(storeP, &ctx, liveEntityP);

  if (eP == NULL)
    return;

  uint64_t atNs = timeOf(liveEntityP);

  for (CorNode* changeP = reportP->changes->value.head; changeP != NULL; changeP = changeP->next)
  {
    CorNode* attrNameP = corTreeLookup(changeP, "attr");

    if ((attrNameP == NULL) || (attrNameP->type != CorString))
      continue;

    const char* attrName = attrNameP->value.s;
    CorNode*    preP     = corTreeLookup(changeP, "preValue");
    CorNode*    postP    = corTreeLookup(liveEntityP, attrName);

    //
    // The instances the write reached - ldInstanceWritten, the broker's one answer to "touched" (the one
    // its TRoE events and a subscription watching attr@datasetId get too)
    //
    if ((postP != NULL) && (postP->type == CorObject))
    {
      for (CorNode* instP = postP->value.head; instP != NULL; instP = instP->next)
      {
        if ((instP->name == NULL) || (instP->type != CorObject) || (ldInstanceWritten(preP, postP, instP->name) == false))
          continue;

        instanceAppend(storeP, eP, attrName, datasetOf(instP), instP, 0, kaP);
      }
    }

    removedInstances(storeP, eP, attrName, preP, postP, atNs, kaP);
  }
}



// -----------------------------------------------------------------------------
//
// corDbHistoryDeleted -
//
void corDbHistoryDeleted(CorDbStore* storeP, CorNode* goneEntityP, CorAlloc* kaP)
{
  if ((storeP == NULL) || (storeP->historyP == NULL) || (goneEntityP == NULL))
    return;

  HistCtx ctx; HistCtx* eP = entityOf(storeP, &ctx, goneEntityP);

  if (eP != NULL)
    entityEvent(storeP, eP, "deleted", timeOf(NULL), kaP);
}
