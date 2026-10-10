//
// FILE            corDbClose.c
//
// AUTHOR          Ken Zangelin
//
// Copyright 2026 Seamware
// SPDX-License-Identifier: Apache-2.0
//
#include "corLog/corLog.h"                               // COR_I

#include "db/Tenant.h"                                   // tenant0, tenantList
#include "corDB/corDbGeoMatch.h"          // corDbGeoClose
#include "corDB/corDbHistory.h"                  // corDbHistoryScratchClose
#include "corDB/corDbHistoryWrite.h"             // corDbHistoryDrain
#include "corDB/corDbPersist.h"                  // corDbPersistClose
#include "corDB/corDbStore.h"        // CorDbStore, corDbStoreFree, corDbStoreRetiredFreeAll
#include "corDB/corDbClose.h"             // Own interface



// -----------------------------------------------------------------------------
//
// corDbFreeTenantStore - free the per-tenant CorNode tree
//
static void corDbFreeTenantStore(Tenant* tenantP)
{
  //
  // Nothing else is running by now - corDbClose is called once, at shutdown, after the HTTP server has
  // stopped and corDbPersistClose has closed every log - so there is no last writer to wait for
  //
  corDbStoreFree((CorDbStore*) tenantP->pluginData);
  tenantP->pluginData = NULL;
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

  corDbStoreRetiredFreeAll();                        // dropped and never released

  corDbGeoClose();
  corDbHistoryScratchClose();
  COR_I("corDB: closed (all tenant stores freed)");
}
