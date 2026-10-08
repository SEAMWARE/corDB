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
// § 5.6.15) - the correction path; history otherwise comes in from current state (corDbHistoryWrite.c),
// or from another store (corDbTroeHistoryImport - coraine-import).
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
#include "corNgsild/ldTypes.h"                         // ldAttrTypeToString, LdAttrType

#include "db/Tenant.h"                                 // Tenant
#include "troe/TroeDriver.h"                           // TROE_*, TroeEvent, TroeOp*

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



#ifdef TROE_DRIVER_HISTORY_IMPORT
// -----------------------------------------------------------------------------
//
// importTypes - an imported event's entity type(s): its snapshot's "type" (several joined by '\n'),
// else the single name the event carries
//
static const char* importTypes(const TroeEvent* evP)
{
  const char* types = (evP->entitySnapshot != NULL) ? typeOf(evP->entitySnapshot) : NULL;

  return (types != NULL) ? types : evP->entityType;
}



// -----------------------------------------------------------------------------
//
// importInstance - the one instance an imported attribute event names: its snapshot is the attribute's
// wrapper { datasetId|"@none": instance }
//
static CorNode* importInstance(const TroeEvent* evP)
{
  CorNode* wrapP = evP->attrSnapshot;

  if ((wrapP == NULL) || (wrapP->type != CorObject) || (wrapP->value.head == NULL))
    return NULL;

  if ((evP->datasetId != NULL) && (evP->datasetId[0] != 0))
  {
    CorNode* namedP = corTreeLookup(wrapP, evP->datasetId);

    if (namedP != NULL)
      return namedP;
  }

  return wrapP->value.head;
}



// -----------------------------------------------------------------------------
//
// tombstoneType - the type of the instance a deletion records: the one it gives, else the type of the
// attribute's last recorded instance (a deleted instance keeps its Attribute's type - § 5.3.2.5), else
// Property
//
static const char* tombstoneType(CorDbHistEntity* eP, const char* attrName, CorNode* srcP, CorAlloc* kaP)
{
  CorNode* typeP = corTreeLookup(srcP, "type");

  if ((srcP->kind >= LdAttrProperty) && (srcP->kind <= LdAttrJsonProperty))
    return ldAttrTypeToString((LdAttrType) srcP->kind);
  if ((typeP != NULL) && (typeP->type == CorString))
    return typeP->value.s;

  CorDbHistAttr* aP    = corDbHistoryAttrLookup(eP, attrName);
  CorNode*       lastP = ((aP != NULL) && (aP->instances > 0)) ? corDbHistoryInstanceDecode(&aP->instanceV[aP->instances - 1], kaP) : NULL;

  if (lastP != NULL)
  {
    typeP = corTreeLookup(lastP, "type");

    if ((lastP->kind >= LdAttrProperty) && (lastP->kind <= LdAttrJsonProperty))
      return ldAttrTypeToString((LdAttrType) lastP->kind);
    if ((typeP != NULL) && (typeP->type == CorString))
      return typeP->value.s;
  }

  return "Property";
}



// -----------------------------------------------------------------------------
//
// importRecord - an imported attribute event as a history record: the instance with the times and the
// instanceId the event carries; a deletion as a tombstone (the kind, urn:ngsi-ld:null, deletedAt)
//
static bool importRecord(CorDbHistEntity* eP, const TroeEvent* evP, CorAlloc* kaP, CorDbHistRecord* recP)
{
  CorNode*    srcP      = importInstance(evP);
  const char* datasetId = ((evP->datasetId != NULL) && (evP->datasetId[0] != 0)) ? evP->datasetId : NULL;
  uint64_t    createdAt = (evP->createdAtNs != 0) ? evP->createdAtNs : evP->modifiedAtNs;
  uint64_t    deletedAt = (evP->op == TroeOpAttrDeleted) ? evP->modifiedAtNs : 0;
  CorNode*    instP;

  if ((srcP == NULL) || (srcP->type != CorObject))
    return false;

  if (deletedAt != 0)
  {
    instP = corTreeObject(kaP, NULL);
    corTreeChildAdd(instP, corTreeString(kaP, "type", tombstoneType(eP, evP->attrName, srcP, kaP)));
    corTreeChildAdd(instP, corTreeString(kaP, "value", "urn:ngsi-ld:null"));
    corTreeChildAdd(instP, corTreeInteger(kaP, "deletedAt", (long long) deletedAt));
  }
  else
  {
    instP = corTreeClone(kaP, srcP);
    if (instP == NULL)
      return false;

    instP->name = NULL;
    instP->next = NULL;

    CorNode* idP = corTreeLookup(instP, "instanceId");
    if (idP != NULL)
      corTreeChildRemove(instP, idP);
  }

  timeSet(instP, "createdAt",  createdAt,          kaP);
  timeSet(instP, "modifiedAt", evP->modifiedAtNs,  kaP);

  //
  // The source's instanceId: the encoder takes the one the instance carries (none: one generated -
  // a source that had none, an entity's created row)
  //
  if ((evP->instanceId != NULL) && (evP->instanceId[0] != 0))
    corTreeChildAdd(instP, corTreeString(kaP, "instanceId", evP->instanceId));

  return corDbHistoryRecordEncode(eP->id, corDbHistoryEntityTypesJoined(eP, kaP), evP->attrName, datasetId, instP, deletedAt, kaP, recP);
}



// -----------------------------------------------------------------------------
//
// importDuplicate - is the event's instanceId in the attribute's history already?
//
// A re-import is what puts one there: compared by its times first (the index has them), decoded only
// where they are equal.
//
static bool importDuplicate(CorDbHistEntity* eP, const TroeEvent* evP, CorAlloc* kaP)
{
  if ((evP->instanceId == NULL) || (evP->instanceId[0] == 0))
    return false;

  CorDbHistAttr* aP        = corDbHistoryAttrLookup(eP, evP->attrName);
  uint64_t       createdAt = (evP->createdAtNs != 0) ? evP->createdAtNs : evP->modifiedAtNs;

  if (aP == NULL)
    return false;

  for (int ix = aP->instances - 1; ix >= 0; ix--)
  {
    CorDbInstance* iP = &aP->instanceV[ix];

    if ((iP->modifiedAtNs != evP->modifiedAtNs) || (iP->createdAtNs != createdAt))
      continue;

    CorNode* instP = corDbHistoryInstanceDecode(iP, kaP);
    CorNode* idP   = (instP != NULL) ? corTreeLookup(instP, "instanceId") : NULL;

    if ((idP != NULL) && (idP->type == CorString) && (strcmp(idP->value.s, evP->instanceId) == 0))
      return true;
  }

  return false;
}



// -----------------------------------------------------------------------------
//
// importCheck - a batch that would put an instanceId in twice is refused whole, before anything is
// written: the first event of each attribute in the batch is looked for (a re-import repeats them all)
//
static bool importCheck(CorDbStore* storeP, const TroeEvent* listHead, CorAlloc* kaP)
{
  for (const TroeEvent* evP = listHead; evP != NULL; evP = evP->next)
  {
    if ((evP->op < TroeOpAttrCreated) || (evP->attrName == NULL) || (evP->entityId == NULL))
      continue;

    bool first = true;

    for (const TroeEvent* prevP = listHead; prevP != evP; prevP = prevP->next)
    {
      if ((prevP->attrName != NULL) && (prevP->entityId != NULL) &&
          (strcmp(prevP->attrName, evP->attrName) == 0) && (strcmp(prevP->entityId, evP->entityId) == 0))
      {
        first = false;
        break;
      }
    }

    if (first == false)
      continue;

    CorDbHistEntity* eP = corDbHistoryEntity(storeP->historyP, evP->entityId, NULL, false);

    if ((eP != NULL) && (importDuplicate(eP, evP, kaP) == true))
    {
      COR_E("corDB: imported history: the instance '%s' of '%s' of '%s' is there already", evP->instanceId, evP->attrName, evP->entityId);
      return false;
    }
  }

  return true;
}



// -----------------------------------------------------------------------------
//
// corDbTroeHistoryImport - TroeDriver.historyImport: history from another store (coraine-import)
//
// Written as it comes: the entity events at their time, every instance with the instanceId, createdAt,
// modifiedAt (and observedAt, in the instance) the event carries - into the index and the history log,
// as the temporal API's own writes. Nothing else is recorded for it (the import writes the current state
// with corNgsild.troeSkip). One batch is one tenant; refused whole when it would repeat an instanceId.
//
int corDbTroeHistoryImport(const TroeEvent* listHead, int count)
{
  (void) count;

  if (listHead == NULL)
    return TROE_OK;

  CorDbStore* storeP = historyLock(listHead->tenantP);

  if (storeP == NULL)
    return TROE_ERR;

  CorAlloc* kaP = corRest.kallocP;
  int       rc  = (importCheck(storeP, listHead, kaP) == true) ? TROE_OK : TROE_ERR;

  for (const TroeEvent* evP = listHead; (evP != NULL) && (rc == TROE_OK); evP = evP->next)
  {
    if (evP->entityId == NULL)
    {
      rc = TROE_ERR;
      break;
    }

    const char*      types = importTypes(evP);
    CorDbHistEntity* eP    = corDbHistoryEntity(storeP->historyP, evP->entityId, types, true);

    if (eP == NULL)
    {
      rc = TROE_ERR;
      break;
    }

    if (evP->op < TroeOpAttrCreated)
    {
      const char* entityOp = (evP->op == TroeOpEntityCreated) ? "created" : (evP->op == TroeOpEntityReplaced) ? "replaced" : "deleted";

      if ((types != NULL) && (evP->op != TroeOpEntityDeleted))
        corDbHistoryEntityTypes(storeP->historyP, eP, types, true);

      CorBinBuffer event;

      if (corDbHistoryEntityEvent(eP, entityOp, evP->modifiedAtNs, kaP, &event) == true)
        corDbPersistHistAppend(storeP->persistP, event.buf, event.len);
      else
        rc = TROE_ERR;

      free(event.buf);
      continue;
    }

    CorDbHistRecord rec;

    if ((evP->attrName == NULL) || (importRecord(eP, evP, kaP, &rec) == false) || (corDbHistoryRecordAdd(storeP->historyP, eP, &rec) == NULL))
    {
      COR_E("corDB: imported history: an instance of '%s' of '%s' could not be recorded", (evP->attrName != NULL) ? evP->attrName : "?", evP->entityId);
      rc = TROE_ERR;
      break;
    }

    corDbPersistHistAppend(storeP->persistP, rec.body, rec.bodyLen);
  }

  historyUnlock(storeP);
  return rc;
}
#endif  // TROE_DRIVER_HISTORY_IMPORT
