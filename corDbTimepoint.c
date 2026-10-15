//
// FILE            corDbTimepoint.c
//
// AUTHOR          Ken Zangelin
//
// Copyright 2026 Seamware
// SPDX-License-Identifier: Apache-2.0
//
// The entity AS IT WAS at a point in time, from the history (corDbHistory.h) - the timepoint resources'
// reads (TroeDriver.entityAtRetrieve / entityAtQuery): Core API retrieve and query, evaluated at T.
//
// An entity at T is rebuilt in the store's own model - { id, type, scope?, createdAt, modifiedAt, attr:
// { "@none": instance, "<datasetId>": instance } } - from the instances the history keeps, which are the
// store's own: what the broker does with a store entity it does with this one (projection, rendering, q,
// geo, paging). A query rebuilds each entity at T and runs the store's own query over them
// (corDbEntityQueryList).
//
// What T means:
//   - the entity existed at T: its last lifecycle event at or before T (CorDbHistEntity.lifeV) is a create
//     or a replace - system time, whatever the axis
//   - its life at T started with the last create at or before T: instances older than that belong to an
//     earlier life (an entity delete leaves no tombstones on its attributes)
//   - per attribute and datasetId, the instance of that life with the latest time at or before T on the axis
//     (TROE_AT_MODIFIED: modifiedAt; TROE_AT_OBSERVED: observedAt, modifiedAt for one without it); a
//     tombstone (deletedAt) counts at its deletedAt and leaves the attribute out
//   - its types are its current ones: the history does not keep a change of types
//
#include <stdbool.h>                                   // bool
#include <stdint.h>                                    // uint64_t
#include <string.h>                                    // strcmp

#include "corAlloc/CorAlloc.h"                         // CorAlloc
#include "corAlloc/corAlloc.h"                         // corAlloc
#include "corTree/CorNode.h"                           // CorNode
#include "corTree/corTreeBuilder.h"                    // corTreeObject, corTreeArray, corTreeString, corTreeInteger, corTreeChildAdd
#include "corTree/corTreeLookup.h"                     // corTreeLookup
#include "corRest/CorRestState.h"                      // corRest
#include "corNgsild/LdVocab.h"                         // LD_VOCAB_SCOPE

#include "db/DbDriver.h"                               // DB_OK
#include "db/DbQueryFilter.h"                          // DbQueryFilter
#include "db/Tenant.h"                                 // Tenant
#include "troe/TroeDriver.h"                           // TROE_*, TROE_DRIVER_ENTITY_AT

#include "corDB/corDbHistory.h"                        // CorDbHistory, CorDbHistEntity, corDbHistoryEntity, corDbHistoryInstanceDecode
#include "corDB/corDbHistoryWrite.h"                   // corDbHistoryDrain
#include "corDB/corDbStore.h"                          // corDbStoreOf
#include "corDB/corDbEntityQuery.h"                    // corDbEntityQueryList
#include "corDB/corDbTimepoint.h"                      // Own interface

#ifdef TROE_DRIVER_ENTITY_AT



// -----------------------------------------------------------------------------
//
// lifeStart - when the life of the entity that T is in started; 0: it did not exist at T
//
// An entity with no lifecycle events (its history imported, or written before the list was kept) answers
// from the index's createdAtNs and deletedAtNs - its only life.
//
static uint64_t lifeStart(CorDbHistEntity* eP, uint64_t atNs)
{
  if (eP->lifeN == 0)
  {
    if ((eP->createdAtNs == 0) || (eP->createdAtNs > atNs))
      return 0;
    if ((eP->deletedAtNs != 0) && (eP->deletedAtNs <= atNs))
      return 0;
    return eP->createdAtNs;
  }

  uint64_t start = 0;

  for (int i = 0; i < eP->lifeN; i++)
  {
    CorDbHistLife* lP = &eP->lifeV[i];

    if (lP->atNs > atNs)
      break;

    if (lP->op == 'd')
      start = 0;
    else if ((lP->op == 'c') || (start == 0))         // a replace of an entity whose create is not in the list: its life from there
      start = lP->atNs;
  }

  return start;
}



// -----------------------------------------------------------------------------
//
// instanceTime - an instance's time on the axis; a tombstone's is its deletedAt
//
static uint64_t instanceTime(CorDbInstance* iP, int axis)
{
  if (iP->deletedAtNs != 0)
    return iP->deletedAtNs;

  if ((axis == TROE_AT_OBSERVED) && (iP->observedAtNs != 0))
    return iP->observedAtNs;

  return iP->modifiedAtNs;
}



// -----------------------------------------------------------------------------
//
// sameDataset -
//
static bool sameDataset(const char* a, const char* b)
{
  if ((a == NULL) || (b == NULL))
    return (a == b);

  return strcmp(a, b) == 0;
}



// -----------------------------------------------------------------------------
//
// entityAt - the entity eP as it was at atNs, in kaP; NULL: it did not exist then
//
// Under the history mutex: the instances decoded here are copied out of the arena by the decoder.
//
static CorNode* entityAt(CorDbHistEntity* eP, uint64_t atNs, int axis, CorAlloc* kaP)
{
  uint64_t start = lifeStart(eP, atNs);

  if (start == 0)
    return NULL;

  CorNode* entityP = corTreeObject(kaP, NULL);

  if (entityP == NULL)
    return NULL;

  corTreeChildAdd(entityP, corTreeString(kaP, "id", eP->id));

  if (eP->typeN == 1)
    corTreeChildAdd(entityP, corTreeString(kaP, "type", eP->typeV[0]));
  else if (eP->typeN > 1)                            // several: an array, in the order given (§ 5.2.6.4.2)
  {
    CorNode* typeP = corTreeArray(kaP, "type");

    for (int i = 0; i < eP->typeN; i++)
      corTreeChildAdd(typeP, corTreeString(kaP, NULL, eP->typeV[i]));
    corTreeChildAdd(entityP, typeP);
  }

  uint64_t modifiedNs = start;                         // the entity's modifiedAt at T: its last write in this life, at or before T

  for (int i = 0; i < eP->lifeN; i++)
  {
    if ((eP->lifeV[i].atNs >= start) && (eP->lifeV[i].atNs <= atNs) && (eP->lifeV[i].atNs > modifiedNs))
      modifiedNs = eP->lifeV[i].atNs;
  }

  CorNode* firstAttrP = NULL;                          // the attributes - after id, type, scope, createdAt, modifiedAt
  CorNode* lastAttrP  = NULL;
  CorNode* scopeP     = NULL;

  for (CorDbHistAttr* aP = eP->attrs; aP != NULL; aP = aP->next)
  {
    //
    // Per datasetId: the instance of this life with the latest time at or before atNs - the last written
    // of those with that time
    //
    int* bestV = (int*) corAlloc(kaP, (aP->instances + 1) * sizeof(int));     // one slot per datasetId seen
    int  bestN = 0;

    if (bestV == NULL)
      return NULL;

    for (int i = 0; i < aP->instances; i++)
    {
      CorDbInstance* iP = &aP->instanceV[i];

      if (iP->modifiedAtNs < start)                    // an earlier life's
        continue;

      if ((iP->modifiedAtNs > atNs) && (axis == TROE_AT_MODIFIED))
        continue;

      uint64_t t = instanceTime(iP, axis);

      if ((t == 0) || (t > atNs))
        continue;

      int b;
      for (b = 0; b < bestN; b++)
      {
        if (sameDataset(aP->instanceV[bestV[b]].datasetId, iP->datasetId))
          break;
      }

      if (b == bestN)
        bestV[bestN++] = i;
      else if (t >= instanceTime(&aP->instanceV[bestV[b]], axis))
        bestV[b] = i;
    }

    CorNode* attrP = NULL;

    for (int b = 0; b < bestN; b++)
    {
      CorDbInstance* iP = &aP->instanceV[bestV[b]];

      if (iP->deletedAtNs != 0)                        // deleted at T
        continue;

      CorNode* instP = corDbHistoryInstanceDecode(iP, kaP);

      if (instP == NULL)
        continue;

      CorNode* idP = corTreeLookup(instP, "instanceId");   // the decoder puts it in for the temporal answer - the store's instance has none
      if (idP != NULL)
        corTreeChildRemove(instP, idP);

      if (iP->modifiedAtNs > modifiedNs)
        modifiedNs = iP->modifiedAtNs;

      if (strcmp(aP->name, LD_VOCAB_SCOPE) == 0)       // the Scope's history is kept as a Property: the entity's member again
      {
        CorNode* valueP = corTreeLookup(instP, "value");

        if (valueP != NULL)
        {
          corTreeChildRemove(instP, valueP);
          valueP->name = (char*) LD_VOCAB_SCOPE;
          scopeP       = valueP;
        }
        continue;
      }

      if (attrP == NULL)
        attrP = corTreeObject(kaP, aP->name);

      instP->name = (char*) ((iP->datasetId != NULL) ? iP->datasetId : "@none");
      corTreeChildAdd(attrP, instP);
    }

    if (attrP != NULL)
    {
      if (firstAttrP == NULL)
        firstAttrP = attrP;
      else
        lastAttrP->next = attrP;
      lastAttrP = attrP;
    }
  }

  if (scopeP != NULL)
    corTreeChildAdd(entityP, scopeP);

  corTreeChildAdd(entityP, corTreeInteger(kaP, "createdAt",  (long long) start));
  corTreeChildAdd(entityP, corTreeInteger(kaP, "modifiedAt", (long long) modifiedNs));

  for (CorNode* attrP = firstAttrP; attrP != NULL; )
  {
    CorNode* nextP = attrP->next;

    attrP->next = NULL;
    corTreeChildAdd(entityP, attrP);
    attrP = nextP;
  }

  return entityP;
}



// -----------------------------------------------------------------------------
//
// corDbTimepointRetrieve - TroeDriver.entityAtRetrieve
//
int corDbTimepointRetrieve(Tenant* tenantP, const char* entityId, uint64_t atNs, int axis, CorNode** entityPP)
{
  CorDbStore* storeP = corDbStoreOf(tenantP);

  *entityPP = NULL;

  if ((storeP == NULL) || (storeP->historyP == NULL))
    return TROE_NOT_FOUND;

  corDbHistoryDrain(storeP);                           // what is queued, applied first
  pthread_mutex_lock(&storeP->histMutex);

  CorDbHistEntity* eP = corDbHistoryEntity(storeP->historyP, entityId, NULL, false);

  if (eP != NULL)
    *entityPP = entityAt(eP, atNs, axis, corRest.kallocP);

  pthread_mutex_unlock(&storeP->histMutex);

  return (*entityPP != NULL) ? TROE_OK : TROE_NOT_FOUND;
}



// -----------------------------------------------------------------------------
//
// corDbTimepointQuery - TroeDriver.entityAtQuery
//
// Every entity of the history rebuilt at atNs (only the ones named, when the filter names ids), then the
// store's own query over them - its matching, ordering, paging and count. The rebuilt entities are the
// request's: the history mutex is held while they are built, not while they are queried.
//
int corDbTimepointQuery(Tenant* tenantP, DbQueryFilter* filterP, uint64_t atNs, int axis, CorNode** arrayPP)
{
  CorDbStore* storeP = corDbStoreOf(tenantP);
  CorNode*    listP  = corTreeArray(corRest.kallocP, NULL);

  *arrayPP = NULL;

  if (listP == NULL)
    return TROE_ERR;

  if ((storeP != NULL) && (storeP->historyP != NULL))
  {
    corDbHistoryDrain(storeP);
    pthread_mutex_lock(&storeP->histMutex);

    if ((filterP != NULL) && (filterP->idV != NULL) && (filterP->idV[0] != NULL))
    {
      for (char** idP = filterP->idV; *idP != NULL; idP++)
      {
        CorDbHistEntity* eP = corDbHistoryEntity(storeP->historyP, *idP, NULL, false);
        CorNode*         atP = (eP != NULL) ? entityAt(eP, atNs, axis, corRest.kallocP) : NULL;

        if (atP != NULL)
          corTreeChildAdd(listP, atP);
      }
    }
    else
    {
      for (CorDbHistEntity* eP = storeP->historyP->first; eP != NULL; eP = eP->next)
      {
        CorNode* atP = entityAt(eP, atNs, axis, corRest.kallocP);

        if (atP != NULL)
          corTreeChildAdd(listP, atP);
      }
    }

    pthread_mutex_unlock(&storeP->histMutex);
  }

  return (corDbEntityQueryList(listP, filterP, arrayPP) == DB_OK) ? TROE_OK : TROE_ERR;
}

#endif  // TROE_DRIVER_ENTITY_AT
