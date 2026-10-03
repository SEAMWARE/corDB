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
#include <stdlib.h>                                    // free
#include <string.h>                                    // strcmp
#include <time.h>                                      // clock_gettime

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
// entityOf - the entity's history, created on its first record
//
static CorDbHistEntity* entityOf(CorDbStore* storeP, CorNode* entityP)
{
  CorNode* idP = corTreeLookup(entityP, "id");

  if ((idP == NULL) || (idP->type != CorString))
    return NULL;

  CorDbHistEntity* eP = corDbHistoryEntity(storeP->historyP, idP->value.s, typeOf(entityP), true);

  if (eP == NULL)
    COR_E("corDB: out of memory for the history of '%s'", idP->value.s);

  return eP;
}



// -----------------------------------------------------------------------------
//
// entityEvent - an entity-level record: the index and the history log
//
static void entityEvent(CorDbStore* storeP, CorDbHistEntity* eP, const char* entityOp, uint64_t atNs, CorAlloc* kaP)
{
  CorBinBuffer body;

  if (corDbHistoryEntityEvent(eP, entityOp, atNs, kaP, &body) == false)
    COR_E("corDB: out of memory recording '%s' %s", eP->id, entityOp);
  else
    corDbPersistHistAppend(storeP->persistP, body.buf, body.len);

  free(body.buf);
}



// -----------------------------------------------------------------------------
//
// instanceAppend - one instance: the index and the history log
//
static void instanceAppend(CorDbStore* storeP, CorDbHistEntity* eP, const char* attrName, const char* datasetId,
                           CorNode* instanceP, uint64_t deletedAtNs, CorAlloc* kaP)
{
  CorDbInstance* iP = corDbHistoryInstanceAdd(eP, attrName, datasetId, instanceP, deletedAtNs, kaP);

  if (iP == NULL)
  {
    COR_E("corDB: out of memory recording an instance of '%s' of '%s'", attrName, eP->id);
    return;
  }

  corDbPersistHistAppend(storeP->persistP, iP->body, iP->bodyLen);
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
static void allInstances(CorDbStore* storeP, CorDbHistEntity* eP, CorNode* attrP, CorAlloc* kaP)
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
static void removedInstances(CorDbStore* storeP, CorDbHistEntity* eP, const char* attrName, CorNode* preP, CorNode* postP,
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
// corDbHistoryCreated -
//
void corDbHistoryCreated(CorDbStore* storeP, CorNode* entityP, CorAlloc* kaP)
{
  if ((storeP == NULL) || (storeP->historyP == NULL) || (entityP == NULL))
    return;

  CorDbHistEntity* eP = entityOf(storeP, entityP);

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

  CorDbHistEntity* eP = entityOf(storeP, newEntityP);

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

  CorDbHistEntity* eP = entityOf(storeP, liveEntityP);

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

  CorDbHistEntity* eP = entityOf(storeP, goneEntityP);

  if (eP != NULL)
    entityEvent(storeP, eP, "deleted", timeOf(NULL), kaP);
}
