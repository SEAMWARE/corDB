//
// FILE            corDbClose.c
//
// AUTHOR          Ken Zangelin
//
// Copyright 2026 Seamware
// SPDX-License-Identifier: Apache-2.0
//
#include <pthread.h>                                     // pthread_rwlock_destroy
#include <stdlib.h>                                      // free

#include "corLog/corLog.h"                               // COR_I

#include "corHash/corHash.h"                           // corHashRelease
#include "corTree/corTreeFree.h"                         // corTreeFree

#include "db/Tenant.h"                                   // tenant0, tenantList
#include "corDB/corDbGeoMatch.h"          // corDbGeoClose
#include "corDB/corDbHistory.h"                  // corDbHistoryFree
#include "corDB/corDbHistoryWrite.h"             // corDbHistoryDrain
#include "corDB/corDbPersist.h"                  // corDbPersistClose
#include "corDB/corDbStore.h"        // CorDbStore
#include "corDB/corDbClose.h"             // Own interface



// -----------------------------------------------------------------------------
//
// corDbFreeTenantStore - free the per-tenant CorNode tree
//
static void corDbFreeTenantStore(Tenant* tenantP)
{
  if (tenantP->pluginData != NULL)
  {
    CorDbStore* storeP = (CorDbStore*) tenantP->pluginData;

    //
    // The tree first, then the lock, then the struct that holds both. Nothing
    // else is running by now - corDbClose is called once, at shutdown, after
    // the HTTP server has stopped - so there is no last writer to wait for.
    //
    if (storeP->idToPrevEntity != NULL)
      corHashRelease(storeP->idToPrevEntity);

    if (storeP->idxOld != NULL)
      corHashRelease(storeP->idxOld);

    corTreeFree(storeP->tree);

    if (storeP->historyP != NULL)
    {
      corDbHistoryFree(storeP->historyP);
      free(storeP->historyP);
    }

    pthread_rwlock_destroy(&storeP->lock);
    free(storeP);

    tenantP->pluginData = NULL;
  }
}



// -----------------------------------------------------------------------------
//
// corDbClose -
//
void corDbClose(void)
{
  //
  // What history is still queued, applied - then the logs, written, synced, closed - while every store
  // still exists (§ 5a)
  //
  corDbHistoryDrain((CorDbStore*) tenant0.pluginData);
  for (Tenant* tP = tenantList; tP != NULL; tP = tP->next)
    corDbHistoryDrain((CorDbStore*) tP->pluginData);

  corDbPersistClose();

  corDbFreeTenantStore(&tenant0);

  for (Tenant* tP = tenantList; tP != NULL; tP = tP->next)
    corDbFreeTenantStore(tP);

  corDbGeoClose();
  corDbHistoryScratchClose();
  COR_I("corDB: closed (all tenant stores freed)");
}
