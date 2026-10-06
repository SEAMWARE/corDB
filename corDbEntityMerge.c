//
// FILE            corDbEntityMerge.c
//
// AUTHOR          Ken Zangelin
//
// Copyright 2026 Seamware
// SPDX-License-Identifier: Apache-2.0
//
// corDB change-set persistence for Merge Entity / Partial Attribute Update.
//
// The NGSI-LD merge itself is done by the broker against the request-arena tree
// returned by db.entityRetrieve; this file applies the resulting change report
// to the LIVE stored entity. Only the attributes the report names are touched —
// a PATCH on one attribute of a 2000-attribute entity does not re-clone the
// whole entity.
//
// The tenant store uses a malloc-backed allocator, so any node grafted into the
// live tree is cloned with the NULL (malloc) allocator; replaced/removed nodes
// are corTreeFree'd.
//

#include <stdbool.h>                                 // bool
#include <string.h>                                   // strcmp

#include "corTree/CorNode.h"                          // CorNode
#include "corTree/corTreeLookup.h"                    // corTreeLookup
#include "corTree/corTreeClone.h"                     // corTreeClone
#include "corTree/corTreeFree.h"                      // corTreeFree
#include "corTree/corTreeBuilder.h"                   // corTreeChildRemove, corTreeChildAdd
#include "corTree/corTreeChildReplace.h"              // corTreeChildReplace

#include "corNgsild/LdVocab.h"                         // LD_VOCAB_MODIFIED_AT, LD_VOCAB_SCOPE
#include "corNgsild/ldEntityMerge.h"                   // LdMergeReport

#include "db/DbDriver.h"                              // DB_OK, DB_NOT_FOUND, DB_INVALID_GEOMETRY, Tenant
#include "shared/geoMatch.h"                          // geoEntityValidate
#include "corDB/corDbSysTimes.h"                      // corDbTreeIn, corDbTreeOut, corDbFullView
#include "corDB/corDbIndex.h"        // corDbIndexLookup
#include "corDB/corDbPersist.h"                      // corDbPersistAppend
#include "corDB/corDbHistoryWrite.h"                 // corDbHistoryCreated, ...Replaced, ...Merged, ...Deleted
#include "corRest/CorRestState.h"                    // corRest (kallocP - the history's scratch)
#include "corDB/corDbStore.h"          // corDbEntities
#include "corDB/corDbEntityMerge.h"    // Own interface



// -----------------------------------------------------------------------------
//
// replaceOrAdd - graft a malloc-clone of `srcNode` into `live` under `name`,
// replacing (and freeing) any existing same-named child.
//
static void replaceOrAdd(CorNode* live, const char* name, CorNode* srcNode)
{
  if (srcNode == NULL)
    return;

  //
  // NULL allocator == malloc == store lifetime. In the store's form: the times its entity's createdAt
  // gives left out (corDbSysTimes.h)
  //
  CorNode* clone = corDbTreeIn(srcNode, corDbCreatedAt(live, 0));
  CorNode* old  = corTreeLookup(live, name);

  if (old != NULL)
  {
    corTreeChildReplace(live, old, clone);
    corTreeFree(old);
  }
  else
    corTreeChildAdd(live, clone);
}



// -----------------------------------------------------------------------------
//
// corDbApplyReportToLive - apply a merge report to a live stored entity.
//
// `merged` is the already-merged request-arena tree the report was produced
// against; the new attribute wrappers (and refreshed modifiedAt/type/scope) are
// copied from it into `live`. Shared by the single-entity and batch paths.
//
void corDbApplyReportToLive(CorNode* live, CorNode* merged, LdMergeReport* reportP)
{
  bool anyChange = false;

  if (reportP != NULL && reportP->changes != NULL)
  {
    for (CorNode* change = reportP->changes->value.head; change != NULL; change = change->next)
    {
      CorNode* attrNameP = corTreeLookup(change, "attr");
      CorNode* reasonP  = corTreeLookup(change, "reason");

      if (attrNameP == NULL || reasonP == NULL || attrNameP->type != CorString || reasonP->type != CorString)
        continue;

      const char* attrName = attrNameP->value.s;
      const char* reason   = reasonP->value.s;

      if (strcmp(reason, "attributeDeleted") == 0)
      {
        CorNode* old = corTreeLookup(live, attrName);
        if (old != NULL)
        {
          corTreeChildRemove(live, old);
          corTreeFree(old);
          anyChange = true;
        }
      }
      else
      {
        replaceOrAdd(live, attrName, corTreeLookup(merged, attrName));
        anyChange = true;
      }
    }
  }

  if (anyChange)
  {
    replaceOrAdd(live, LD_VOCAB_MODIFIED_AT, corTreeLookup(merged, LD_VOCAB_MODIFIED_AT));
    replaceOrAdd(live, "type",               corTreeLookup(merged, "type"));
    replaceOrAdd(live, LD_VOCAB_SCOPE,        corTreeLookup(merged, LD_VOCAB_SCOPE));
  }
}



// -----------------------------------------------------------------------------
//
// corDbMergedNames - the members a merge report says changed, and what a change refreshes beside them
// (corDbApplyReportToLive: modifiedAt, type, scope); -1 when there are more than 'max'
//
int corDbMergedNames(LdMergeReport* reportP, const char** names, int max)
{
  int n = 0;

  if ((reportP != NULL) && (reportP->changes != NULL))
  {
    for (CorNode* change = reportP->changes->value.head; change != NULL; change = change->next)
    {
      CorNode* attrNameP = corTreeLookup(change, "attr");

      if ((attrNameP == NULL) || (attrNameP->type != CorString))
        continue;

      if (n == max - 3)
        return -1;

      names[n++] = attrNameP->value.s;
    }
  }

  names[n++] = LD_VOCAB_MODIFIED_AT;
  names[n++] = "type";
  names[n++] = LD_VOCAB_SCOPE;

  return n;
}



// -----------------------------------------------------------------------------
//
// corDbPersistMerged - the log record of a merge applied to 'live': the attributes the report
// names, and what a change refreshes beside them (corDbApplyReportToLive) - not the entity
//
void corDbPersistMerged(CorDbPersist* persistP, CorNode* live, LdMergeReport* reportP)
{
  const char* names[64];

  if (persistP == NULL)
    return;

  int n = corDbMergedNames(reportP, names, 64);

  if (n < 0)
    corDbPersistAppend(persistP, CorDbLogEntityPut, live);   // that many attributes: the entity
  else
    corDbPersistAppendAttrs(persistP, live, names, n);
}



// -----------------------------------------------------------------------------
//
// mergePrepare - BEFORE the write lock: what corDbApplyReportToLive would clone into the store under it -
// the members the report changed and, with any change, modifiedAt, type and scope - each a malloc clone
// in the store's form (corDbSysTimes.h), the children of one request-arena object
//
static CorNode* mergePrepare(CorNode* merged, LdMergeReport* reportP)
{
  CorNode* prepP           = corTreeObject(corRest.kallocP, NULL);
  int64_t  entityCreatedAt = corDbCreatedAt(merged, 0);
  bool     anyChange       = false;

  if ((prepP == NULL) || (reportP == NULL) || (reportP->changes == NULL))
    return prepP;

  for (CorNode* change = reportP->changes->value.head; change != NULL; change = change->next)
  {
    CorNode* attrNameP = corTreeLookup(change, "attr");
    CorNode* reasonP   = corTreeLookup(change, "reason");

    if ((attrNameP == NULL) || (reasonP == NULL) || (attrNameP->type != CorString) || (reasonP->type != CorString))
      continue;

    anyChange = true;

    if (strcmp(reasonP->value.s, "attributeDeleted") == 0)
      continue;

    CorNode* srcP = corTreeLookup(merged, attrNameP->value.s);

    if ((srcP != NULL) && (corTreeLookup(prepP, attrNameP->value.s) == NULL))
      corTreeChildAdd(prepP, corDbTreeIn(srcP, entityCreatedAt));
  }

  if (anyChange)
  {
    const char* refreshed[] = { LD_VOCAB_MODIFIED_AT, "type", LD_VOCAB_SCOPE };

    for (int ix = 0; ix < 3; ix++)
    {
      CorNode* srcP = corTreeLookup(merged, refreshed[ix]);

      if ((srcP != NULL) && (corTreeLookup(prepP, refreshed[ix]) == NULL))
        corTreeChildAdd(prepP, corTreeClone(NULL, srcP));
    }
  }

  return prepP;
}



// -----------------------------------------------------------------------------
//
// prepFree - what mergePrepare cloned and nobody took
//
static void prepFree(CorNode* prepP)
{
  CorNode* mP = (prepP != NULL) ? prepP->value.head : NULL;

  while (mP != NULL)
  {
    CorNode* nextP = mP->next;

    corTreeFree(mP);
    mP = nextP;
  }

  if (prepP != NULL)
  {
    prepP->value.head = NULL;
    prepP->value.tail = NULL;
  }
}



// -----------------------------------------------------------------------------
//
// takeOrAdd - a member mergePrepare made (already the store's) moved into the live entity, replacing (and
// freeing) a same-named one
//
static void takeOrAdd(CorNode* live, CorNode* prepP, const char* name)
{
  CorNode* nodeP = corTreeLookup(prepP, name);

  if (nodeP == NULL)
    return;

  corTreeChildRemove(prepP, nodeP);

  CorNode* oldP = corTreeLookup(live, name);

  if (oldP != NULL)
  {
    corTreeChildReplace(live, oldP, nodeP);
    corTreeFree(oldP);
  }
  else
    corTreeChildAdd(live, nodeP);
}



// -----------------------------------------------------------------------------
//
// applyPrepared - corDbApplyReportToLive, with what it would clone made before the lock (mergePrepare)
//
static void applyPrepared(CorNode* live, CorNode* prepP, LdMergeReport* reportP)
{
  bool anyChange = false;

  if ((reportP != NULL) && (reportP->changes != NULL))
  {
    for (CorNode* change = reportP->changes->value.head; change != NULL; change = change->next)
    {
      CorNode* attrNameP = corTreeLookup(change, "attr");
      CorNode* reasonP   = corTreeLookup(change, "reason");

      if ((attrNameP == NULL) || (reasonP == NULL) || (attrNameP->type != CorString) || (reasonP->type != CorString))
        continue;

      if (strcmp(reasonP->value.s, "attributeDeleted") == 0)
      {
        CorNode* oldP = corTreeLookup(live, attrNameP->value.s);

        if (oldP != NULL)
        {
          corTreeChildRemove(live, oldP);
          corTreeFree(oldP);
        }
      }
      else
        takeOrAdd(live, prepP, attrNameP->value.s);

      anyChange = true;
    }
  }

  if (anyChange)
  {
    takeOrAdd(live, prepP, LD_VOCAB_MODIFIED_AT);
    takeOrAdd(live, prepP, "type");
    takeOrAdd(live, prepP, LD_VOCAB_SCOPE);
  }
}



// -----------------------------------------------------------------------------
//
// changesApply - under the write lock: what mergePrepare made moved into the live entity
//
static int changesApply(Tenant* tenantP, const char* entityId, CorNode* mergedEntity, LdMergeReport* reportP, CorNode* prepP)
{
  COR_DB_WRITE(tenantP);

  // Re-validate the GeoProperty values of the COMPLETE merged entity before it
  // touches the store. A PATCH/merge fragment that omits the attribute type is
  // validated as a plain Property (geo check skipped), so a wholesale-replaced
  // GeoProperty value such as {"type":"Polygon"} (no coordinates) would slip
  // through and persist as broken geometry. Same DB_INVALID_GEOMETRY → 400
  // contract as create.
  if (!geoEntityValidate(mergedEntity))
    return DB_INVALID_GEOMETRY;

  CorNode* entities = corDbEntities(tenantP);

  //
  // One hop via the id index instead of a walk of the whole store with a
  // corTreeLookup per entity. The loop shape is kept so the body below is unchanged:
  // indexed, it runs exactly once for the hit and not at all for a miss;
  // unindexed - a store that predates the index - it walks as it always did.
  //
  CorDbStore* idxStoreP = corDbStoreOf(tenantP);
  CorNode*    idxHitP   = corDbIndexLookup(idxStoreP, entityId);
  bool        indexed   = (idxStoreP != NULL) && (idxStoreP->idToPrevEntity != NULL);

  for (CorNode* eP = indexed ? idxHitP : entities->value.head;
       eP != NULL;
       eP = indexed ? NULL : eP->next)
  {
    CorNode* idP = corTreeLookup(eP, "id");
    if (idP != NULL && idP->type == CorString && strcmp(idP->value.s, entityId) == 0)
    {
      applyPrepared(eP, prepP, reportP);
      corDbPersistMerged(corDbLockedStore->persistP, eP, reportP);
      corDbHistoryMerged(corDbLockedStore, eP, reportP, corRest.kallocP);
      return DB_OK;
    }
  }

  return DB_NOT_FOUND;
}



// -----------------------------------------------------------------------------
//
// corDbEntityChangesApply - persist a merged single entity (DB driver entry): what goes into the store is
// cloned before the write lock, and only moved in under it
//
int corDbEntityChangesApply(Tenant* tenantP, const char* entityId,
                            CorNode* mergedEntity, LdMergeReport* reportP)
{
  CorNode* prepP = mergePrepare(mergedEntity, reportP);
  int      r     = changesApply(tenantP, entityId, mergedEntity, reportP, prepP);

  prepFree(prepP);                                    // what was not taken (not found, invalid geometry)
  return r;
}
