//
// FILE            corDbTenantDrop.c
//
// AUTHOR          Ken Zangelin
//
// Copyright 2026 Seamware
// SPDX-License-Identifier: Apache-2.0
//
#include <pthread.h>                                     // pthread_rwlock_*
#include <stdlib.h>                                      // free

#include "corLog/corLog.h"                               // COR_T
#include "corHash/corHash.h"                             // corHashRelease
#include "corTree/CorNode.h"                             // CorNode
#include "corTree/corTreeFree.h"                         // corTreeFree
#include "corTree/corTreeLookup.h"                       // corTreeLookup

#include "db/DbDriver.h"                                 // DB_OK
#include "db/Tenant.h"                                   // Tenant
#include "corDB/corDbHistoryWrite.h"                     // corDbHistoryDrop
#include "corDB/corDbPersist.h"                          // corDbPersistDrop
#include "corDB/corDbStore.h"                            // CorDbStore, corDbStoreRetire
#include "corDB/corDbTenantDrop.h"                       // Own interface



// -----------------------------------------------------------------------------
//
// containerEmpty - every member of an array (or object) freed
//
static void containerEmpty(CorNode* containerP)
{
  if (containerP == NULL)
    return;

  CorNode* mP = containerP->value.head;

  while (mP != NULL)
  {
    CorNode* nextP = mP->next;

    corTreeFree(mP);
    mP = nextP;
  }

  containerP->value.head = NULL;
  containerP->value.tail = NULL;
}



// -----------------------------------------------------------------------------
//
// corDbTenantDrop -
//
// The store struct is not freed: a request that took it before the drop may still be waiting for its lock,
// and finds it empty. The tenant forgets it, so the next use builds a new one (corDbStoreOf), and the store
// is RETIRED - kept, with its log's node, until the broker releases the tenant (corDbTenantRelease), when
// nothing can reach either any more.
//
int corDbTenantDrop(Tenant* tenantP)
{
  CorDbStore* storeP = (CorDbStore*) __atomic_load_n(&tenantP->pluginData, __ATOMIC_ACQUIRE);

  if (storeP == NULL)
    return DB_OK;

  //
  // Under the write lock: no write appends to the log any more, a snapshot in progress ends at its next
  // slice (its cursor cleared, as a writer that unlinks the entity under it does), and the store is emptied
  //
  pthread_rwlock_wrlock(&storeP->lock);

  CorDbPersist* persistP = storeP->persistP;

  storeP->persistP   = NULL;
  storeP->snapCursor = NULL;

  containerEmpty(corTreeLookup(storeP->tree, "entities"));
  containerEmpty(corTreeLookup(storeP->tree, "subscriptions"));
  containerEmpty(corTreeLookup(storeP->tree, "registrations"));
  containerEmpty(corTreeLookup(storeP->tree, "docs"));

  if (storeP->idToPrevEntity != NULL)
    corHashRelease(storeP->idToPrevEntity);
  if (storeP->idxOld != NULL)
    corHashRelease(storeP->idxOld);

  storeP->idToPrevEntity = NULL;
  storeP->idxOld         = NULL;
  storeP->idxSlots       = 0;
  storeP->idxCount       = 0;
  storeP->idxOldMoved    = 0;

  pthread_rwlock_unlock(&storeP->lock);

  corDbHistoryDrop(storeP);                            // under the history's own mutex - a drain may be running

  corDbPersistDrop(persistP);                          // its files and its directory
  __atomic_store_n(&tenantP->pluginData, NULL, __ATOMIC_RELEASE);

  storeP->droppedPersistP = persistP;                  // the node stays in the flusher's list until the release
  corDbStoreRetire(tenantP, storeP);

  COR_T(0, "corDB: tenant '%s' dropped", tenantP->name);
  return DB_OK;
}
