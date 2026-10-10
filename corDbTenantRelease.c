//
// FILE            corDbTenantRelease.c
//
// AUTHOR          Ken Zangelin
//
// Copyright 2026 Seamware
// SPDX-License-Identifier: Apache-2.0
//
#include <stddef.h>                                      // NULL

#include "corLog/corLog.h"                               // COR_T

#include "db/DbDriver.h"                                 // DB_OK
#include "db/Tenant.h"                                   // Tenant
#include "corDB/corDbHistoryWrite.h"                     // corDbHistoryDrain
#include "corDB/corDbPersist.h"                          // corDbPersistRelease
#include "corDB/corDbStore.h"                            // CorDbStore, corDbStoreFree, corDbStoreRetiredRelease
#include "corDB/corDbTenantRelease.h"                    // Own interface



// -----------------------------------------------------------------------------
//
// corDbTenantRelease -
//
// Who else could hold a store when this runs:
//
//   - a request, a loop, a cache: no - that is the broker's promise for this call (DbDriver.h, tenantRelease)
//   - the snapshot thread (corDbPersist.c), which reaches a store through its log's node, not through the
//     tenant: corDbPersistRelease waits for a snapshot it has claimed and then takes the node out of its
//     list, so after it nothing reaches the store
//
int corDbTenantRelease(Tenant* tenantP)
{
  CorDbStore* storeP = (CorDbStore*) __atomic_exchange_n(&tenantP->pluginData, NULL, __ATOMIC_ACQ_REL);

  if (storeP != NULL)
  {
    if (__atomic_load_n(&storeP->histQHead, __ATOMIC_ACQUIRE) != NULL)
      corDbHistoryDrain(storeP);                       // queued history applied - and logged - before the log closes

    corDbPersistRelease(storeP->persistP);
    storeP->persistP = NULL;
    corDbStoreFree(storeP);
  }

  corDbStoreRetiredRelease(tenantP);

  COR_T(0, "corDB: tenant '%s' released", tenantP->name);
  return DB_OK;
}
