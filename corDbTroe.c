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
#include <stdlib.h>                                    // qsort
#include <string.h>                                    // strcmp

#include "corAlloc/CorAlloc.h"                         // CorAlloc
#include "corBase/corTimeIso.h"                        // corTimeIso
#include "corTree/CorNode.h"                           // CorNode
#include "corTree/corTreeBuilder.h"                    // corTreeObject, corTreeArray, corTreeString, corTreeChildAdd
#include "corTree/corTreeLookup.h"                     // corTreeLookup
#include "corAlloc/corAlloc.h"                         // corAlloc
#include "corRest/CorRestState.h"                      // corRest
#include "corNgsild/ldCheckDateTime.h"                 // ldIsoToNanoseconds
#include "corNgsild/ldTermId.h"                        // ldNodeRename

#include "db/Tenant.h"                                 // Tenant
#include "troe/TroeDriver.h"                           // TroeDriver, TroeQueryFilter, TroeRangeInfo, TROE_*

#include "corDB/corDbHistory.h"                        // CorDbHistory, corDbHistoryOn, corDbHistoryEntity, corDbHistoryInstanceDecode
#include "corDB/corDbHistoryWrite.h"                   // corDbHistoryDrain
#include "corDB/corDbStore.h"                          // corDbStoreOf



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
// datasetWanted - is the instance's datasetId in ?datasetId ("@none": the default instance)
//
static bool datasetWanted(TroeQueryFilter* fP, const char* datasetId)
{
  if ((fP == NULL) || (fP->datasetIdV == NULL) || (fP->datasetIdV[0] == NULL))
    return true;

  for (char** dP = fP->datasetIdV; *dP != NULL; dP++)
  {
    if (strcmp(*dP, "@none") == 0)
    {
      if (datasetId == NULL)
        return true;
    }
    else if ((datasetId != NULL) && (strcmp(*dP, datasetId) == 0))
      return true;
  }

  return false;
}



// -----------------------------------------------------------------------------
//
// Axis - the timestamp ?timeproperty filters and orders on (§ 4.11, default observedAt)
//
// createdAt is the time of the instances that CREATED their attribute, deletedAt the time of the
// tombstones: an instance has those only when it is one; modifiedAt and observedAt are every instance's.
//
typedef enum { AxisObservedAt, AxisModifiedAt, AxisCreatedAt, AxisDeletedAt } Axis;

static Axis axisOf(TroeQueryFilter* fP)
{
  const char* tp = (fP != NULL) ? fP->timeproperty : NULL;

  if (tp == NULL)                      return AxisObservedAt;
  if (strcmp(tp, "modifiedAt") == 0)   return AxisModifiedAt;
  if (strcmp(tp, "createdAt") == 0)    return AxisCreatedAt;
  if (strcmp(tp, "deletedAt") == 0)    return AxisDeletedAt;
  return AxisObservedAt;
}

static bool onAxis(CorDbInstance* iP, Axis axis)
{
  if (axis == AxisCreatedAt)
    return (iP->deletedAtNs == 0) && (iP->createdAtNs != 0) && (iP->createdAtNs == iP->modifiedAtNs);

  if (axis == AxisDeletedAt)
    return iP->deletedAtNs != 0;

  return true;
}

static uint64_t axisNs(CorDbInstance* iP, Axis axis)
{
  switch (axis)
  {
  case AxisObservedAt:  return iP->observedAtNs;
  case AxisDeletedAt:   return iP->deletedAtNs;
  default:              return iP->modifiedAtNs;
  }
}



// -----------------------------------------------------------------------------
//
// Window - ?timerel on the axis (§ 4.11): before is exclusive, after inclusive, between [timeAt, endTimeAt)
//
// An instance without the axis' timestamp is in no window - and, with no window, after every instance
// that has one, in either direction.
//
typedef struct Window
{
  int       rel;                                       // 0 none, 1 before, 2 after, 3 between
  uint64_t  fromNs;
  uint64_t  toNs;
} Window;

static uint64_t isoNs(const char* iso)
{
  int64_t ns = (iso != NULL) ? ldIsoToNanoseconds(iso) : 0;
  return (ns > 0) ? (uint64_t) ns : 0;
}

static void windowOf(TroeQueryFilter* fP, Window* wP)
{
  memset(wP, 0, sizeof(*wP));

  if ((fP == NULL) || (fP->timerel == NULL))
    return;

  wP->fromNs = (fP->timeAtNs != 0) ? fP->timeAtNs : isoNs(fP->timeAtIso);

  if (strcmp(fP->timerel, "before") == 0)        wP->rel = 1;
  else if (strcmp(fP->timerel, "after") == 0)    wP->rel = 2;
  else if (strcmp(fP->timerel, "between") == 0)
  {
    wP->rel  = 3;
    wP->toNs = (fP->endTimeAtNs != 0) ? fP->endTimeAtNs : isoNs(fP->endTimeAtIso);
  }
}

static bool inWindow(Window* wP, uint64_t ns)
{
  switch (wP->rel)
  {
  case 0:  return true;
  case 1:  return (ns != 0) && (ns < wP->fromNs);
  case 2:  return (ns != 0) && (ns >= wP->fromNs);
  default: return (ns != 0) && (ns >= wP->fromNs) && (ns < wP->toNs);
  }
}



// -----------------------------------------------------------------------------
//
// Pick - an instance that passed the filters, with what orders it
//
// Order: the default instance first, then by datasetId; within one, by the axis - ascending, or
// descending for ?lastN - an instance without the timestamp last either way; equal times in the order
// they were written (descending: the reverse).
//
typedef struct Pick
{
  CorDbInstance*  iP;
  const char*     datasetId;
  uint64_t        ns;
  int             ix;                                  // the order written
  bool            backward;
} Pick;

static int pickCompare(const void* a, const void* b)
{
  const Pick* pA = (const Pick*) a;
  const Pick* pB = (const Pick*) b;

  if ((pA->datasetId == NULL) != (pB->datasetId == NULL))
    return (pA->datasetId == NULL) ? -1 : 1;

  if (pA->datasetId != NULL)
  {
    int c = strcmp(pA->datasetId, pB->datasetId);
    if (c != 0)
      return c;
  }

  if ((pA->ns == 0) != (pB->ns == 0))
    return (pA->ns == 0) ? 1 : -1;

  if (pA->ns != pB->ns)
    return ((pA->ns < pB->ns) != pA->backward) ? -1 : 1;

  return ((pA->ix < pB->ix) != pA->backward) ? -1 : 1;
}



// -----------------------------------------------------------------------------
//
// isoString - ns as the API's ISO text, to the microsecond (an all-zero fraction left out)
//
static const char* isoString(uint64_t ns, CorAlloc* kaP)
{
  char* buf = (char*) corAlloc(kaP, 32);

  if (buf != NULL)
    corTimeIso((int64_t) ns, 6, false, buf);

  return buf;
}



// -----------------------------------------------------------------------------
//
// temporalEntity - one entity's temporal representation, in kaP - timescale's answer, from the index
//
static CorNode* temporalEntity(CorDbHistEntity* eP, TroeQueryFilter* fP, TroeRangeInfo* rangeP, CorAlloc* kaP)
{
  CorNode* entityP = corTreeObject(kaP, NULL);

  corTreeChildAdd(entityP, corTreeString(kaP, "id", eP->id));
  if (eP->type != NULL)
    corTreeChildAdd(entityP, corTreeString(kaP, "type", eP->type));

  int  lastN    = (fP != NULL) ? fP->lastN   : 0;
  int  firstN   = (fP != NULL) ? fP->firstN  : 0;
  int  offsetN  = (fP != NULL) ? fP->offsetN : 0;
  int  cap      = ((fP != NULL) && (fP->instanceCap > 0)) ? fP->instanceCap : INSTANCE_CAP;
  int  page     = (lastN > 0) ? lastN : ((firstN > 0) ? firstN : cap);
  bool backward = (lastN > 0);
  Axis axis     = axisOf(fP);
  Window window;

  windowOf(fP, &window);

  uint64_t modifiedNs = (eP->createdAtNs > eP->deletedAtNs) ? eP->createdAtNs : eP->deletedAtNs;
  uint64_t minNs      = 0;
  uint64_t maxNs      = 0;

  for (CorDbHistAttr* aP = eP->attrs; aP != NULL; aP = aP->next)
  {
    for (int i = 0; i < aP->instances; i++)              // the entity's modifiedAt: its last write, filters or not
    {
      if (aP->instanceV[i].modifiedAtNs > modifiedNs)
        modifiedNs = aP->instanceV[i].modifiedAtNs;
    }

    if (attrWanted(fP, aP->name) == false)
      continue;

    Pick* pickV = (Pick*) corAlloc(kaP, (aP->instances + 1) * sizeof(Pick));
    int   picks = 0;

    if (pickV == NULL)
      continue;

    for (int i = 0; i < aP->instances; i++)
    {
      CorDbInstance* iP = &aP->instanceV[i];

      if ((datasetWanted(fP, iP->datasetId) == false) || (onAxis(iP, axis) == false))
        continue;

      uint64_t ns = axisNs(iP, axis);

      if (inWindow(&window, ns) == false)
        continue;

      pickV[picks++] = (Pick) { iP, iP->datasetId, ns, i, backward };
    }

    if (picks == 0)
      continue;

    qsort(pickV, picks, sizeof(Pick), pickCompare);

    CorNode* arrayP = corTreeArray(kaP, aP->name);

    //
    // The page is per datasetId (§ 6.4.7.3): offsetN skipped, then at most `page`, in each
    //
    for (int p0 = 0; p0 < picks; )
    {
      int p1 = p0 + 1;

      while ((p1 < picks) && (((pickV[p1].datasetId == NULL) && (pickV[p0].datasetId == NULL)) ||
                              ((pickV[p1].datasetId != NULL) && (pickV[p0].datasetId != NULL) && (strcmp(pickV[p1].datasetId, pickV[p0].datasetId) == 0))))
        ++p1;

      if ((p1 - p0 > offsetN + page) && (rangeP != NULL))
        rangeP->hasMore = true;

      int from = p0 + offsetN;
      int to   = (from + page < p1) ? from + page : p1;

      for (int p = from; p < to; p++)
      {
        CorNode* instP = corDbHistoryInstanceDecode(pickV[p].iP, kaP);

        if (instP == NULL)
          continue;

        //
        // The store keys an instance by its datasetId rather than carrying it as a member: the answer
        // carries it
        //
        if ((pickV[p].datasetId != NULL) && (corTreeLookup(instP, "datasetId") == NULL))
          corTreeChildAdd(instP, corTreeString(kaP, "datasetId", pickV[p].datasetId));

        toApi(instP, kaP);
        corTreeChildAdd(arrayP, instP);

        if (pickV[p].ns != 0)
        {
          if ((minNs == 0) || (pickV[p].ns < minNs))  minNs = pickV[p].ns;
          if (pickV[p].ns > maxNs)                    maxNs = pickV[p].ns;
        }
      }

      p0 = p1;
    }

    if (arrayP->value.head != NULL)
      corTreeChildAdd(entityP, arrayP);
  }

  //
  // createdAt and modifiedAt of the entity (sysAttrs - the broker strips them unless asked), and its
  // deletedAt when its last event is its deletion - which is not stripped: it says the entity is gone
  //
  if (eP->createdAtNs != 0)
    corTreeChildAdd(entityP, corTreeString(kaP, "createdAt", isoString(eP->createdAtNs, kaP)));
  if (modifiedNs != 0)
    corTreeChildAdd(entityP, corTreeString(kaP, "modifiedAt", isoString(modifiedNs, kaP)));
  if (eP->deletedAtNs != 0)
    corTreeChildAdd(entityP, corTreeString(kaP, "deletedAt", isoString(eP->deletedAtNs, kaP)));

  if (rangeP != NULL)
  {
    if (minNs != 0)
    {
      const char* minIso = isoString(minNs, kaP);
      if ((rangeP->rangeStartIso == NULL) || (strcmp(minIso, rangeP->rangeStartIso) < 0))
        rangeP->rangeStartIso = minIso;
    }
    if (maxNs != 0)
    {
      const char* maxIso = isoString(maxNs, kaP);
      if ((rangeP->rangeEndIso == NULL) || (strcmp(maxIso, rangeP->rangeEndIso) > 0))
        rangeP->rangeEndIso = maxIso;
    }
    if (rangeP->size == 0)
      rangeP->size = page;
  }

  return entityP;
}



// -----------------------------------------------------------------------------
//
// corDbTroeRetrieve - GET /temporal/entities/{id}
//
static int corDbTroeRetrieve(Tenant* tenantP, const char* entityId, TroeQueryFilter* fP, CorNode** resultPP, TroeRangeInfo* rangeP)
{
  CorDbStore* storeP = corDbStoreOf(tenantP);

  *resultPP = NULL;

  if ((storeP == NULL) || (storeP->historyP == NULL))
    return TROE_NOT_FOUND;

  //
  // The history index is the history mutex's, not the store lock's: what is queued applied first
  //
  corDbHistoryDrain(storeP);
  pthread_mutex_lock(&storeP->histMutex);

  int              rc = TROE_NOT_FOUND;
  CorDbHistEntity* eP = corDbHistoryEntity(storeP->historyP, entityId, NULL, false);

  if (eP != NULL)
  {
    *resultPP = temporalEntity(eP, fP, rangeP, corRest.kallocP);
    rc        = TROE_OK;
  }

  pthread_mutex_unlock(&storeP->histMutex);
  return rc;
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
