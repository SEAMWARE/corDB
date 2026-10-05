//
// FILE            corDbEntityBulkMerge.c
//
// AUTHOR          Ken Zangelin
//
// Copyright 2026 Seamware
// SPDX-License-Identifier: Apache-2.0
//
// corDB Batch Merge persistence — the store is in-memory, so the two phases
// the broker brackets the merge with are plain loops:
//
//   corDbEntityBulkRetrieve     clone each current stored entity into the
//                               request arena (same-id fragments share one
//                               clone so the broker's sequential merges
//                               accumulate). The broker then runs ldEntityMerge.
//   corDbEntityBulkChangesApply apply each fragment's change report to the live
//                               stored entity.
//

#include <stddef.h>                                       // NULL
#include <string.h>                                       // strcmp

#include "corDB/corDbIndex.h"                         // corDbIndexLookup
#include <stdlib.h>                                       // calloc, free
#include "corTree/corTreeFree.h"                          // corTreeFree
#include "corTree/corTreeChildReplace.h"                  // corTreeChildReplace
#include "corTree/corTreeBuilder.h"                       // corTreeChildAdd, corTreeChildRemove
#include "corTree/CorNode.h"                              // CorNode
#include "corTree/corTreeClone.h"                         // corTreeClone
#include "corTree/corTreeLookup.h"                        // corTreeLookup

#include "corRest/CorRestState.h"                           // corRest (kallocP for arena clones)

#include "corNgsild/ldEntityMerge.h"                       // LdMergeReport

#include "db/DbDriver.h"                                  // DB_OK, DB_ERR, Tenant
#include "corDB/corDbPersist.h"                      // corDbPersistAppend
#include "corDB/corDbHistoryWrite.h"                 // corDbHistoryCreated, ...Replaced, ...Merged, ...Deleted
#include "corDB/corDbStore.h"              // corDbEntities
#include "corDB/corDbEntityMerge.h"        // corDbApplyReportToLive
#include "corDB/corDbEntityBulkMerge.h"    // Own interface



// -----------------------------------------------------------------------------
//
// liveById - locate the live stored entity with the given id
//
static CorNode* liveById(CorNode* entities, const char* id)
{
  for (CorNode* eP = entities->value.head; eP != NULL; eP = eP->next)
  {
    CorNode* idP = corTreeLookup(eP, "id");
    if (idP != NULL && idP->type == CorString && strcmp(idP->value.s, id) == 0)
      return eP;
  }
  return NULL;
}



// -----------------------------------------------------------------------------
//
// corDbEntityBulkRetrieve - Batch Merge Phase 1: clone current stored entities.
//
// `targetsV` is a caller-allocated, zeroed array parallel to the fragments.
// Each slot gets a request-arena clone of the live entity, or stays NULL when
// no such entity exists. Same-id fragments share one clone.
//
int corDbEntityBulkRetrieve(Tenant* tenantP, CorNode* fragmentsArr, CorNode** targetsV)
{
  COR_DB_READ(tenantP);

  if (fragmentsArr == NULL || fragmentsArr->type != CorArray)
    return DB_ERR;

  CorNode*    entities = corDbEntities(tenantP);
  CorDbStore* storeP   = corDbStoreOf(tenantP);
  bool        indexed  = (storeP != NULL) && (storeP->idToPrevEntity != NULL);

  int k = 0;
  for (CorNode* fragP = fragmentsArr->value.head; fragP != NULL; fragP = fragP->next, k++)
  {
    if (targetsV[k] != NULL)
      continue;

    CorNode* idP = corTreeLookup(fragP, "id");
    if (idP == NULL || idP->type != CorString)
      continue;

    //
    // One hop via the id index - liveById walked the whole store for every entity of the batch
    // (the walk stays for a store with no index)
    //
    CorNode* live = indexed ? corDbIndexLookup(storeP, idP->value.s) : liveById(entities, idP->value.s);
    if (live == NULL)
      continue;  // slot stays NULL -> DB_NOT_FOUND in the broker

    CorNode* shared = corTreeClone(corRest.kallocP, live);

    int j = 0;
    for (CorNode* f2 = fragmentsArr->value.head; f2 != NULL; f2 = f2->next, j++)
    {
      if (targetsV[j] != NULL)
        continue;
      CorNode* id2 = corTreeLookup(f2, "id");
      if (id2 != NULL && id2->type == CorString && strcmp(id2->value.s, idP->value.s) == 0)
        targetsV[j] = shared;
    }
  }

  return DB_OK;
}



// -----------------------------------------------------------------------------
//
// corDbEntityBulkChangesApply - Batch Merge Phase 2: apply each fragment's
// change report to its live stored entity.
//
//
// What each entity's write sets is prepared BEFORE the lock, from the merged entity the broker made -
// its state after the write, the state the whole-entity path (corDbEntityBulkUpdate) writes too: the
// changed members cloned for the store, the log record encoded. Under the lock: an index lookup, the
// members swapped in, the record appended, the history recorded. After it: the members swapped out
// are freed. Done under the lock (clones, encoding, a walk of the store for each entity), a batch
// update of 20 ran 13 % below the whole-entity path.
//
typedef struct EntityChange
{
  const char*  id;
  CorNode*     setV[64];                             // clones of the members the write sets (malloc - the store's)
  int          nSet;
  const char*  delV[64];                             // the members it removes
  int          nDel;
  int          preIx;                                // its log record, encoded (CorDbPre), -1: none
  bool         underLock;                            // more than 61 members named: applied as before, under the lock
} EntityChange;

int corDbEntityBulkChangesApply(Tenant* tenantP, CorNode* fragmentsArr,
                                CorNode** mergedTargetsV, LdMergeReport* reportsV,
                                int* resultsV)
{
  if (fragmentsArr == NULL || fragmentsArr->type != CorArray)
    return DB_ERR;

  int n = 0;
  for (CorNode* c = fragmentsArr->value.head; c != NULL; c = c->next) n++;

  EntityChange* chV = (EntityChange*) calloc((n > 0) ? n : 1, sizeof(EntityChange));

  if (chV == NULL)
    return DB_ERR;

  COR_DB_PRE(pre);
  bool persist = corDbPersistOn();

  //
  // Before the lock
  //
  int i = 0;
  for (CorNode* fragP = fragmentsArr->value.head; fragP != NULL; fragP = fragP->next, i++)
  {
    CorNode* idP = corTreeLookup(fragP, "id");

    chV[i].preIx = -1;

    if ((resultsV[i] != DB_OK) || (mergedTargetsV[i] == NULL) || (idP == NULL) || (idP->type != CorString))
      continue;

    const char* names[64];
    int         nn = corDbMergedNames(&reportsV[i], names, 64);

    chV[i].id = idP->value.s;

    if (nn < 0)                                      // that many attributes: applied under the lock, as before
    {
      chV[i].underLock = true;
      continue;
    }

    for (int k = 0; k < nn; k++)
    {
      CorNode* mP = corTreeLookup(mergedTargetsV[i], names[k]);

      if (mP != NULL)
        chV[i].setV[chV[i].nSet++] = corTreeClone(NULL, mP);   // NULL allocator: malloc, the store's lifetime
      else
        chV[i].delV[chV[i].nDel++] = names[k];
    }

    if (persist)
      chV[i].preIx = corDbPersistPreAddAttrs(&pre, mergedTargetsV[i], names, nn);
  }

  //
  // Under the lock
  //
  bool anyOk = false;
  {
    COR_DB_WRITE(tenantP);

    CorDbStore* storeP = corDbStoreOf(tenantP);
    CorNode*    entities = corDbEntities(tenantP);

    for (i = 0; i < n; i++)
    {
      EntityChange* chP = &chV[i];

      if (chP->id == NULL)
        continue;

      CorNode* live = ((storeP != NULL) && (storeP->idToPrevEntity != NULL)) ? corDbIndexLookup(storeP, chP->id) : liveById(entities, chP->id);

      if (live == NULL)
      {
        resultsV[i] = DB_NOT_FOUND;                  // gone between the broker's read and this write
        continue;
      }

      if (chP->underLock)
      {
        corDbApplyReportToLive(live, mergedTargetsV[i], &reportsV[i]);
        corDbPersistMerged(corDbLockedStore->persistP, live, &reportsV[i]);
        corDbHistoryMerged(corDbLockedStore, live, &reportsV[i], corRest.kallocP);
        anyOk = true;
        continue;
      }

      //
      // A member swapped out goes into the slot of the clone that replaced it - freed after the lock
      //
      for (int k = 0; k < chP->nSet; k++)
      {
        CorNode* newP = chP->setV[k];
        CorNode* oldP = (newP != NULL) ? corTreeLookup(live, newP->name) : NULL;

        if (newP == NULL)
          continue;

        if (oldP != NULL)
          corTreeChildReplace(live, oldP, newP);
        else
          corTreeChildAdd(live, newP);

        chP->setV[k] = oldP;
      }

      for (int k = 0; k < chP->nDel; k++)
      {
        CorNode* oldP = corTreeLookup(live, chP->delV[k]);

        if (oldP != NULL)
          corTreeChildRemove(live, oldP);

        chP->delV[k] = (const char*) oldP;           // freed after the lock, as a node
      }

      if ((chP->preIx >= 0) && (pre.lenV[chP->preIx] >= 0))
        corDbPersistAppendPre(corDbLockedStore->persistP, CorDbLogAttrsPut, &pre, chP->preIx, NULL);
      else
        corDbPersistMerged(corDbLockedStore->persistP, live, &reportsV[i]);

      corDbHistoryMerged(corDbLockedStore, live, &reportsV[i], corRest.kallocP);
      anyOk = true;
    }
  }

  //
  // After the lock: what was swapped out, and the clones of the entities not written
  //
  for (i = 0; i < n; i++)
  {
    for (int k = 0; k < chV[i].nSet; k++)
      if (chV[i].setV[k] != NULL)
        corTreeFree(chV[i].setV[k]);

    for (int k = 0; (chV[i].id != NULL) && (resultsV[i] == DB_OK) && (k < chV[i].nDel); k++)
      if (chV[i].delV[k] != NULL)
        corTreeFree((CorNode*) chV[i].delV[k]);
  }

  free(chV);

  return anyOk ? DB_OK : DB_ERR;
}
