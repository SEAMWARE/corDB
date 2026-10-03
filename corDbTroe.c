//
// FILE            corDbTroe.c
//
// AUTHOR          Ken Zangelin
//
// Copyright 2026 Seamware
// SPDX-License-Identifier: Apache-2.0
//
// corDB as the TRoE driver (`--database corDB --troe corDB`): the broker takes troeRegister from this
// plugin - the current-state one - so history is the same store, the same lock and the same log
// (corDbHistory.h). The write sites record it; the broker's TRoE events are not used (their hooks are
// NULL). What this file serves is the temporal API: the reads, and the temporal writes that correct
// history.
//
#include <stdbool.h>                                   // bool
#include <stdint.h>                                    // uint64_t
#include <string.h>                                    // strcmp

#include "corAlloc/CorAlloc.h"                         // CorAlloc
#include "corBase/corTimeIso.h"                        // corTimeIso
#include "corTree/CorNode.h"                           // CorNode
#include "corTree/corTreeBuilder.h"                    // corTreeObject, corTreeArray, corTreeString, corTreeChildAdd
#include "corTree/corTreeLookup.h"                     // corTreeLookup
#include "corAlloc/corAlloc.h"                         // corAlloc
#include "corRest/CorRestState.h"                      // corRest
#include "corNgsild/ldTermId.h"                        // ldNodeRename

#include "db/Tenant.h"                                 // Tenant
#include "troe/TroeDriver.h"                           // TroeDriver, TroeQueryFilter, TroeRangeInfo, TROE_*

#include "corDB/corDbHistory.h"                        // CorDbHistory, corDbHistoryOn, corDbHistoryEntity, corDbHistoryInstanceDecode
#include "corDB/corDbStore.h"                          // COR_DB_READ



// -----------------------------------------------------------------------------
//
// INSTANCE_CAP - the per-attribute page when neither firstN nor lastN is given (timescale's too)
//
enum { INSTANCE_CAP = 100 };



// -----------------------------------------------------------------------------
//
// valueMember - the member that holds an instance's value in the API, by its kind
//
// The store keeps every kind's value under "value" (ldApiEntityToDbModel); a temporal answer uses the
// kind's own member, as timescale's does.
//
static const char* valueMember(CorNode* instanceP)
{
  CorNode* typeP = corTreeLookup(instanceP, "type");

  if ((typeP == NULL) || (typeP->type != CorString))
    return "value";

  const char* t = typeP->value.s;

  if (strcmp(t, "Relationship") == 0)      return "object";
  if (strcmp(t, "LanguageProperty") == 0)  return "languageMap";
  if (strcmp(t, "VocabProperty") == 0)     return "vocab";
  if (strcmp(t, "JsonProperty") == 0)      return "json";
  if (strcmp(t, "ListProperty") == 0)      return "valueList";
  if (strcmp(t, "ListRelationship") == 0)  return "objectList";

  return "value";
}



// -----------------------------------------------------------------------------
//
// isoMember - a timestamp member, ns in the store, ISO text in the answer
//
// createdAt, modifiedAt and deletedAt to the microsecond, observedAt to the millisecond, an all-zero
// fraction left out - as the core API and timescale render them.
//
static void isoMember(CorNode* nodeP, int fracDigits, CorAlloc* kaP)
{
  if (nodeP->type != CorInt)
    return;

  char* buf = (char*) corAlloc(kaP, 32);

  if (buf == NULL)
    return;

  corTimeIso((int64_t) nodeP->value.i, fracDigits, false, buf);
  nodeP->type    = CorString;
  nodeP->value.s = buf;
}



// -----------------------------------------------------------------------------
//
// toApi - a stored instance, decoded, as the temporal API renders it
//
static void toApi(CorNode* instanceP, CorAlloc* kaP)
{
  const char* member = valueMember(instanceP);

  for (CorNode* nP = instanceP->value.head; nP != NULL; nP = nP->next)
  {
    if (nP->name == NULL)
      continue;

    if ((strcmp(nP->name, "value") == 0) && (strcmp(member, "value") != 0))
    {
      if (strcmp(member, "languageMap") == 0)          // a LanguageProperty's tombstone is a map: {"@none": null}
      {
        if ((nP->type == CorString) && (strcmp(nP->value.s, "urn:ngsi-ld:null") == 0))
        {
          CorNode* noneP = corTreeString(kaP, "@none", "urn:ngsi-ld:null");

          ldNodeRename(nP, "languageMap");
          nP->type            = CorObject;
          nP->value.head = NULL;
          nP->value.tail      = NULL;
          corTreeChildAdd(nP, noneP);
          continue;
        }
      }
      ldNodeRename(nP, member);                        // the name AND its term id - a bare rename leaves the id stale
    }
    else if ((strcmp(nP->name, "createdAt") == 0) || (strcmp(nP->name, "modifiedAt") == 0) || (strcmp(nP->name, "deletedAt") == 0))
      isoMember(nP, 6, kaP);
    else if (strcmp(nP->name, "observedAt") == 0)
      isoMember(nP, 3, kaP);
  }
}



// -----------------------------------------------------------------------------
//
// attrWanted - is the attribute in ?attrs (none given: all are)
//
static bool attrWanted(TroeQueryFilter* fP, const char* attrName)
{
  if ((fP == NULL) || (fP->attrV == NULL) || (fP->attrV[0] == NULL))
    return true;

  for (char** aP = fP->attrV; *aP != NULL; aP++)
  {
    if (strcmp(*aP, attrName) == 0)
      return true;
  }

  return false;
}



// -----------------------------------------------------------------------------
//
// temporalEntity - one entity's temporal representation, in kaP
//
static CorNode* temporalEntity(CorDbHistEntity* eP, TroeQueryFilter* fP, TroeRangeInfo* rangeP, CorAlloc* kaP)
{
  CorNode* entityP = corTreeObject(kaP, NULL);

  corTreeChildAdd(entityP, corTreeString(kaP, "id", eP->id));
  if (eP->type != NULL)
    corTreeChildAdd(entityP, corTreeString(kaP, "type", eP->type));

  int  cap      = INSTANCE_CAP;
  bool backward = false;

  if ((fP != NULL) && (fP->lastN > 0))
  {
    cap      = fP->lastN;
    backward = true;
  }
  else if ((fP != NULL) && (fP->firstN > 0))
    cap = fP->firstN;

  for (CorDbHistAttr* aP = eP->attrs; aP != NULL; aP = aP->next)
  {
    if (attrWanted(fP, aP->name) == false)
      continue;

    CorNode* arrayP = corTreeArray(kaP, aP->name);
    int      first  = 0;
    int      last   = aP->instances;                  // exclusive

    if (aP->instances > cap)
    {
      if (rangeP != NULL)
        rangeP->hasMore = true;

      if (backward)
        first = aP->instances - cap;
      else
        last = cap;
    }

    for (int i = first; i < last; i++)
    {
      CorNode* instP = corDbHistoryInstanceDecode(&aP->instanceV[i], kaP);

      if (instP == NULL)
        continue;

      toApi(instP, kaP);
      corTreeChildAdd(arrayP, instP);
    }

    if (arrayP->value.head != NULL)
      corTreeChildAdd(entityP, arrayP);
  }

  if (rangeP != NULL)
    rangeP->size = cap;

  return entityP;
}



// -----------------------------------------------------------------------------
//
// corDbTroeRetrieve - GET /temporal/entities/{id}
//
static int corDbTroeRetrieve(Tenant* tenantP, const char* entityId, TroeQueryFilter* fP, CorNode** resultPP, TroeRangeInfo* rangeP)
{
  COR_DB_READ(tenantP);

  *resultPP = NULL;

  if ((corDbLockedStore == NULL) || (corDbLockedStore->historyP == NULL))
    return TROE_NOT_FOUND;

  CorDbHistEntity* eP = corDbHistoryEntity(corDbLockedStore->historyP, entityId, NULL, false);

  if (eP == NULL)
    return TROE_NOT_FOUND;

  *resultPP = temporalEntity(eP, fP, rangeP, corRest.kallocP);
  return TROE_OK;
}



// -----------------------------------------------------------------------------
//
// corDbTroeInit / corDbTroeClose - the history lives and dies with the store (corDbInit, corDbClose)
//
static int  corDbTroeInit(void)  { return TROE_OK; }
static void corDbTroeClose(void) { }



// -----------------------------------------------------------------------------
//
// troeRegister - the broker calls it from this plugin when `--troe corDB` goes with `--database corDB`
//
// Before the current-state plugin's init: so every store is built with its history.
//
void troeRegister(TroeDriver* driverP)
{
  corDbHistoryOn = true;

  driverP->alias                  = "corDB";
  driverP->version                = PLUGIN_VERSION;
  driverP->init                   = corDbTroeInit;
  driverP->close                  = corDbTroeClose;
  driverP->entityTemporalRetrieve = corDbTroeRetrieve;
}
