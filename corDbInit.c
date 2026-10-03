//
// FILE            corDbInit.c
//
// AUTHOR          Ken Zangelin
//
// Copyright 2026 Seamware
// SPDX-License-Identifier: Apache-2.0
//
#include "corLog/corLog.h"                               // COR_I

#include "db/Tenant.h"                                   // tenant0
#include "corDB/corDbStore.h"             // corDbTenantStore
#include "corDB/corDbPersist.h"                  // corDbPersistInit
#include "corDB/corDbGeoMatch.h"          // corDbGeoInit
#include "corDB/corDbInit.h"              // Own interface



// -----------------------------------------------------------------------------
//
// corDbInit -
//
int corDbInit(void)
{
  //
  // The flusher before the first store: a store opens its log as it is built
  //
  if (corDbPersistInit() == false)
    return -1;

  //
  // Create the store for the default tenant eagerly
  //
  corDbTenantStore(&tenant0);
  corDbGeoInit();

  COR_I("corDB: in-memory store ready (per-tenant CorNode trees, GEOS enabled)");
  return 0;
}
