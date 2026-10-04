//
// FILE            corDbTroeWrite.c
//
// AUTHOR          Ken Zangelin
//
// Copyright 2026 Seamware
// SPDX-License-Identifier: Apache-2.0
//
// The temporal API's own writes (`--troe corDB`): create and add history (§ 5.6.11, § 5.6.12), delete
// an entity's or an attribute's (§ 5.6.16, § 5.6.13), modify and delete one instance (§ 5.6.14,
// § 5.6.15) - the correction path; history otherwise comes in from current state (corDbHistoryWrite.c).
//
// Under the history mutex, what the writes have queued applied first. Every change is a record of the
// history log: an added instance as any other (and an entity's 'created' event), a removal or a
// modification as a { id, histOp, ... } record that corDbHistoryOpApply applies - live, right after
// logging it, and at recovery.
//
#include <pthread.h>                                   // pthread_mutex_lock, pthread_mutex_unlock
#include <stdbool.h>                                   // bool
#include <stdlib.h>                                    // free
#include <string.h>                                    // strcmp

#include "corLog/corLog.h"                             // COR_E
#include "corAlloc/CorAlloc.h"                         // CorAlloc
#include "corAlloc/corAlloc.h"                         // corAlloc
#include "corTree/CorNode.h"                           // CorNode
#include "corTree/corTreeBin.h"                        // corTreeBinEncode, CorBinBuffer
#include "corTree/corTreeBuilder.h"                    // corTreeObject, corTreeString, corTreeBoolean, corTreeInteger, corTreeChildAdd
#include "corTree/corTreeClone.h"                      // corTreeClone
#include "corTree/corTreeLookup.h"                     // corTreeLookup
#include "corRest/CorRestState.h"                      // corRest
#include "corNgsild/ldApiEntityToDbModel.h"            // ldApiEntityToDbModel
#include "corNgsild/ldBinCodec.h"                      // ldBinCodec
#include "corNgsild/ldCheckDateTime.h"                 // ldIsoToNanoseconds
#include "corNgsild/ldTermId.h"                        // ldTermId, ldNodeRename

#include "db/Tenant.h"                                 // Tenant
#include "troe/TroeDriver.h"                           // TROE_*

#include "corDB/corDbHistory.h"                        // corDbHistory*
#include "corDB/corDbHistoryWrite.h"                   // corDbHistoryDrain
#include "corDB/corDbPersist.h"                        // corDbPersistHistAppend
#include "corDB/corDbStore.h"                          // corDbStoreOf
#include "corDB/corDbTroeWrite.h"                      // Own interface



// -----------------------------------------------------------------------------
//
// historyLock / historyUnlock - the tenant's history, what is queued applied, under its mutex
//
static CorDbStore* historyLock(Tenant* tenantP)
{
  CorDbStore* storeP = corDbStoreOf(tenantP);

  if ((storeP == NULL) || (storeP->historyP == NULL))
    return NULL;

  corDbHistoryDrain(storeP);
  pthread_mutex_lock(&storeP->histMutex);
  return storeP;
}

static void historyUnlock(CorDbStore* storeP)
{
  pthread_mutex_unlock(&storeP->histMutex);
}



// -----------------------------------------------------------------------------
//
// logTree - a record of the history log, from its tree
//
static void logTree(CorDbStore* storeP, CorNode* treeP)
{
  CorBinBuffer buf = { NULL, 0, 0 };

  if (corTreeBinEncode(treeP, &ldBinCodec, NULL, &buf) == true)
    corDbPersistHistAppend(storeP->persistP, buf.buf, buf.len);
  else
    COR_E("corDB: a temporal write could not be encoded for the history log");

  free(buf.buf);
}



// -----------------------------------------------------------------------------
//
// opApply - a removal or a modification: logged, then applied by the code that recovery uses too
//
static int opApply(CorDbStore* storeP, CorNode* opP, CorAlloc* kaP)
{
  logTree(storeP, opP);

  return (corDbHistoryOpApply(storeP->historyP, opP, kaP) == 0) ? TROE_OK : TROE_ERR;
}



// -----------------------------------------------------------------------------
//
// isAttributeMember - an EntityTemporal member that is an attribute (not id, type, scope, a timestamp, @...)
//
static bool isAttributeMember(CorNode* nP)
{
  if ((nP->name == NULL) || (nP->name[0] == '@'))
    return false;

  switch (ldTermId(nP))
  {
  case CorTermId:
  case CorTermType:
  case CorTermScope:
  case CorTermCreatedAt:
  case CorTermModifiedAt:
    return false;
  default:
    return (nP->type == CorArray) || (nP->type == CorObject);
  }
}



// -----------------------------------------------------------------------------
//
// storeForm - an instance as the API gives it, as the store keeps it (the form a history record holds):
// one-attribute entity through ldApiEntityToDbModel - "object" / "languageMap" / ... become "value",
// timestamps nanoseconds. The datasetId comes back separately (NULL: the default instance); a supplied
// instanceId is dropped (it is the system's to give, § 5.6.11).
//
static CorNode* storeForm(const char* attrName, CorNode* apiInstanceP, const char** datasetIdP, CorAlloc* kaP)
{
  CorNode* instP = corTreeClone(kaP, apiInstanceP);
  CorNode* idP   = (instP != NULL) ? corTreeLookup(instP, "instanceId") : NULL;

  if (instP == NULL)
    return NULL;

  if (idP != NULL)
    corTreeChildRemove(instP, idP);

  CorNode* entityP = corTreeObject(kaP, NULL);

  ldNodeRename(instP, (char*) attrName);
  instP->next = NULL;
  corTreeChildAdd(entityP, corTreeString(kaP, "id", "urn:ngsi-ld:corDB:instance"));
  corTreeChildAdd(entityP, corTreeString(kaP, "type", "Instance"));
  corTreeChildAdd(entityP, instP);

  ldApiEntityToDbModel(entityP, kaP, (int64_t) corRest.requestStartTime);

  CorNode* attrP = corTreeLookup(entityP, attrName);
  CorNode* dbP   = ((attrP != NULL) && (attrP->type == CorObject)) ? attrP->value.head : NULL;

  if (dbP == NULL)
    return NULL;

  *datasetIdP = ((dbP->name != NULL) && (strcmp(dbP->name, "@none") != 0)) ? dbP->name : NULL;
  dbP->name   = NULL;
  dbP->next   = NULL;

  return dbP;
}



// -----------------------------------------------------------------------------
//
// timeSet - an integer timestamp member of an instance, set or added
//
static void timeSet(CorNode* instP, const char* name, uint64_t ns, CorAlloc* kaP)
{
  CorNode* tP = corTreeLookup(instP, name);

  if (tP != NULL)
  {
    tP->type    = CorInt;
    tP->value.i = (long long) ns;
  }
  else
    corTreeChildAdd(instP, corTreeInteger(kaP, name, (long long) ns));
}



// -----------------------------------------------------------------------------
//
// instancesAdd - every instance of every attribute of an EntityTemporal fragment, appended
//
// Each a millisecond after the one before (createdAt = modifiedAt = the request's time + n ms), as
// timescale does: the order they were given stays the order written.
//
static int instancesAdd(CorDbStore* storeP, CorDbHistEntity* eP, CorNode* rootP, CorAlloc* kaP)
{
  uint64_t atNs = corRest.requestStartTime;

  for (CorNode* attrP = rootP->value.head; attrP != NULL; attrP = attrP->next)
  {
    if (isAttributeMember(attrP) == false)
      continue;

    CorNode* firstP = (attrP->type == CorArray) ? attrP->value.head : attrP;

    for (CorNode* apiP = firstP; apiP != NULL; apiP = (attrP->type == CorArray) ? apiP->next : NULL)
    {
      if (apiP->type != CorObject)
        continue;

      const char* datasetId = NULL;
      CorNode*    instP     = storeForm(attrP->name, apiP, &datasetId, kaP);

      if (instP == NULL)
        return TROE_ERR;

      timeSet(instP, "createdAt",  atNs, kaP);
      timeSet(instP, "modifiedAt", atNs, kaP);
      atNs += 1000000;

      CorDbHistRecord rec;

      if (corDbHistoryRecordEncode(eP->id, corDbHistoryEntityTypesJoined(eP, kaP), attrP->name, datasetId, instP, 0, kaP, &rec) == false)
        return TROE_ERR;

      if (corDbHistoryRecordAdd(storeP->historyP, eP, &rec) == NULL)
        return TROE_ERR;

      corDbPersistHistAppend(storeP->persistP, rec.body, rec.bodyLen);
    }
  }

  return TROE_OK;
}



// -----------------------------------------------------------------------------
//
// typeOf - an EntityTemporal's type: the name, or the names joined by '\n' (corDbHistory.h)
//
static const char* typeOf(CorNode* rootP)
{
  CorNode* typeP = corTreeLookup(rootP, "type");

  if (typeP == NULL)
    return NULL;
  if (typeP->type == CorString)
    return typeP->value.s;
  if (typeP->type != CorArray)
    return NULL;

  int len = 0;

  for (CorNode* tP = typeP->value.head; tP != NULL; tP = tP->next)
  {
    if (tP->type == CorString)
      len += (int) strlen(tP->value.s) + 1;
  }

  char* joined = (char*) corAlloc(corRest.kallocP, len + 1);

  if (joined == NULL)
    return NULL;

  joined[0] = 0;
  for (CorNode* tP = typeP->value.head; tP != NULL; tP = tP->next)
  {
    if (tP->type != CorString)
      continue;
    if (joined[0] != 0)
      strcat(joined, "\n");
    strcat(joined, tP->value.s);
  }

  return (joined[0] != 0) ? joined : NULL;
}



// -----------------------------------------------------------------------------
//
// corDbTroeCreate - POST /temporal/entities: create, or - the entity there already - append (§ 5.6.11.4)
//
int corDbTroeCreate(Tenant* tenantP, CorNode* rootP)
{
  CorNode* idP = (rootP != NULL) ? corTreeLookup(rootP, "id") : NULL;

  if ((idP == NULL) || (idP->type != CorString) || (idP->value.s[0] == 0))
    return TROE_ERR;

  CorDbStore* storeP = historyLock(tenantP);

  if (storeP == NULL)
    return TROE_ERR;

  CorAlloc*        kaP     = corRest.kallocP;
  bool             existed = (corDbHistoryEntity(storeP->historyP, idP->value.s, NULL, false) != NULL);
  CorDbHistEntity* eP      = corDbHistoryEntity(storeP->historyP, idP->value.s, typeOf(rootP), true);
  int              rc      = TROE_ERR;

  if (eP != NULL)
  {
    if (existed == false)
    {
      CorBinBuffer event;

      if (corDbHistoryEntityEvent(eP, "created", corRest.requestStartTime, kaP, &event) == true)
        corDbPersistHistAppend(storeP->persistP, event.buf, event.len);
      free(event.buf);
    }

    rc = instancesAdd(storeP, eP, rootP, kaP);
  }

  historyUnlock(storeP);

  if (rc != TROE_OK)
    return rc;

  return existed ? TROE_UPDATED : TROE_OK;
}



// -----------------------------------------------------------------------------
//
// corDbTroeAttrsAdd - POST /temporal/entities/{id}/attrs: the entity's history must exist
//
int corDbTroeAttrsAdd(Tenant* tenantP, const char* entityId, CorNode* rootP)
{
  CorDbStore* storeP = historyLock(tenantP);

  if (storeP == NULL)
    return TROE_ERR;

  CorDbHistEntity* eP = corDbHistoryEntity(storeP->historyP, entityId, NULL, false);
  int              rc = TROE_NOT_FOUND;

  if (eP != NULL)
  {
    const char* types = typeOf(rootP);

    if (types != NULL)
      corDbHistoryEntityTypes(storeP->historyP, eP, types, false);   // § 11.2.3.4: the new names added
    rc = instancesAdd(storeP, eP, rootP, corRest.kallocP);
  }

  historyUnlock(storeP);
  return rc;
}



// -----------------------------------------------------------------------------
//
// opTree - { id, histOp, attr? }
//
static CorNode* opTree(const char* entityId, const char* histOp, const char* attrName, CorAlloc* kaP)
{
  CorNode* opP = corTreeObject(kaP, NULL);

  corTreeChildAdd(opP, corTreeString(kaP, "id", entityId));
  corTreeChildAdd(opP, corTreeString(kaP, "histOp", histOp));
  if (attrName != NULL)
    corTreeChildAdd(opP, corTreeString(kaP, "attr", attrName));

  return opP;
}



// -----------------------------------------------------------------------------
//
// corDbTroeDelete - DELETE /temporal/entities/{id}: the entity's whole history
//
int corDbTroeDelete(Tenant* tenantP, const char* entityId)
{
  CorDbStore* storeP = historyLock(tenantP);

  if (storeP == NULL)
    return TROE_NOT_FOUND;

  int rc = TROE_NOT_FOUND;

  if (corDbHistoryEntity(storeP->historyP, entityId, NULL, false) != NULL)
    rc = opApply(storeP, opTree(entityId, "entityRemoved", NULL, corRest.kallocP), corRest.kallocP);

  historyUnlock(storeP);
  return rc;
}



// -----------------------------------------------------------------------------
//
// corDbTroeAttrDelete - DELETE /temporal/entities/{id}/attrs/{attr}: all its instances (deleteAll),
// those of one datasetId, or the default ones (none given)
//
int corDbTroeAttrDelete(Tenant* tenantP, const char* entityId, const char* attrName, const char* datasetId, bool deleteAll)
{
  CorDbStore* storeP = historyLock(tenantP);

  if (storeP == NULL)
    return TROE_NOT_FOUND;

  CorAlloc*        kaP = corRest.kallocP;
  CorDbHistEntity* eP  = corDbHistoryEntity(storeP->historyP, entityId, NULL, false);
  int              rc  = TROE_NOT_FOUND;

  if ((eP != NULL) && (corDbHistoryAttrLookup(eP, attrName) != NULL))
  {
    CorNode* opP = opTree(entityId, "attrRemoved", attrName, kaP);

    if (datasetId != NULL)
      corTreeChildAdd(opP, corTreeString(kaP, "datasetId", datasetId));
    if (deleteAll)
      corTreeChildAdd(opP, corTreeBoolean(kaP, "deleteAll", true));

    rc = opApply(storeP, opP, kaP);
  }

  historyUnlock(storeP);
  return rc;
}



// -----------------------------------------------------------------------------
//
// instanceOf - the instance a PATCH .../{instanceId} body holds: the body itself, or the first
// instance of its first attribute (a fragment) - timescale accepts both
//
static CorNode* instanceOf(CorNode* bodyP)
{
  if ((bodyP == NULL) || (bodyP->type != CorObject))
    return NULL;

  if (corTreeLookup(bodyP, "type") != NULL)
    return bodyP;

  for (CorNode* nP = bodyP->value.head; nP != NULL; nP = nP->next)
  {
    if (isAttributeMember(nP) == false)
      continue;

    return (nP->type == CorArray) ? nP->value.head : nP;
  }

  return NULL;
}



// -----------------------------------------------------------------------------
//
// corDbTroeInstanceModify - PATCH /temporal/entities/{id}/attrs/{attr}/{instanceId} (§ 5.6.14)
//
// The value replaced, observedAt if one is given, modifiedAt now; the rest of the instance - its
// type, createdAt, instanceId - kept.
//
int corDbTroeInstanceModify(Tenant* tenantP, const char* entityId, const char* attrName, const char* instanceId, CorNode* rootP)
{
  CorNode* apiP = instanceOf(rootP);

  if (apiP == NULL)
    return TROE_ERR;

  CorDbStore* storeP = historyLock(tenantP);

  if (storeP == NULL)
    return TROE_NOT_FOUND;

  CorAlloc*        kaP = corRest.kallocP;
  CorDbHistEntity* eP  = corDbHistoryEntity(storeP->historyP, entityId, NULL, false);
  CorDbHistAttr*   aP  = (eP != NULL) ? corDbHistoryAttrLookup(eP, attrName) : NULL;
  int              ix  = (aP != NULL) ? corDbHistoryInstanceIndex(aP, instanceId, kaP) : -1;
  int              rc  = TROE_NOT_FOUND;

  if (ix >= 0)
  {
    const char* datasetId = NULL;
    CorNode*    newP      = storeForm(attrName, apiP, &datasetId, kaP);
    CorNode*    instP     = corDbHistoryInstanceDecode(&aP->instanceV[ix], kaP);   // with its instanceId

    rc = TROE_ERR;

    if ((newP != NULL) && (instP != NULL))
    {
      CorNode* valueP    = corTreeLookup(newP, "value");
      CorNode* oldValueP = corTreeLookup(instP, "value");
      CorNode* observedP = corTreeLookup(apiP, "observedAt");

      if (valueP != NULL)
      {
        if (oldValueP != NULL)
          corTreeChildRemove(instP, oldValueP);
        corTreeChildRemove(newP, valueP);
        corTreeChildAdd(instP, valueP);
      }

      if ((observedP != NULL) && (observedP->type == CorString))
      {
        int64_t ns = ldIsoToNanoseconds(observedP->value.s);
        if (ns > 0)
          timeSet(instP, "observedAt", (uint64_t) ns, kaP);
      }

      timeSet(instP, "modifiedAt", corRest.requestStartTime, kaP);

      CorNode* opP = opTree(entityId, "instanceModified", attrName, kaP);

      corTreeChildAdd(opP, corTreeString(kaP, "instanceId", instanceId));
      ldNodeRename(instP, "instance");
      instP->next = NULL;
      corTreeChildAdd(opP, instP);

      rc = opApply(storeP, opP, kaP);
    }
  }

  historyUnlock(storeP);
  return rc;
}



// -----------------------------------------------------------------------------
//
// corDbTroeInstanceDelete - DELETE /temporal/entities/{id}/attrs/{attr}/{instanceId} (§ 5.6.15)
//
int corDbTroeInstanceDelete(Tenant* tenantP, const char* entityId, const char* attrName, const char* instanceId)
{
  CorDbStore* storeP = historyLock(tenantP);

  if (storeP == NULL)
    return TROE_NOT_FOUND;

  CorAlloc*        kaP = corRest.kallocP;
  CorDbHistEntity* eP  = corDbHistoryEntity(storeP->historyP, entityId, NULL, false);
  CorDbHistAttr*   aP  = (eP != NULL) ? corDbHistoryAttrLookup(eP, attrName) : NULL;
  int              rc  = TROE_NOT_FOUND;

  if ((aP != NULL) && (corDbHistoryInstanceIndex(aP, instanceId, kaP) >= 0))
  {
    CorNode* opP = opTree(entityId, "instanceRemoved", attrName, kaP);

    corTreeChildAdd(opP, corTreeString(kaP, "instanceId", instanceId));
    rc = opApply(storeP, opP, kaP);
  }

  historyUnlock(storeP);
  return rc;
}
