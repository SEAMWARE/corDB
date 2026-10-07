//
// FILE            corDbInit.c
//
// AUTHOR          Ken Zangelin
//
// Copyright 2026 Seamware
// SPDX-License-Identifier: Apache-2.0
//
#include <string.h>                                         // strcmp

#include "corLog/corLog.h"                               // COR_I, COR_E
#include "corDB/corDbGlobals.h"                          // corDbLockPrefer, corDbLockWriters

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
  // --dbLockPrefer: whom a tenant's store lock lets in first when readers and writers both wait - glibc's
  // default, readers, or writers (PTHREAD_RWLOCK_PREFER_WRITER_NONRECURSIVE_NP). Which is faster is the
  // workload's (coraine doc/performance.md, "Writes that notify, and which writer the lock lets in
  // first"). Writers first is safe: corDB never takes its store's read lock twice in one thread.
  //
  if      (strcmp(corDbLockPrefer, "reads")  == 0) corDbLockWriters = false;
  else if (strcmp(corDbLockPrefer, "writes") == 0) corDbLockWriters = true;
  else
  {
    COR_E("corDB: --dbLockPrefer '%s': reads or writes", corDbLockPrefer);
    return -1;
  }

  //
  // The flusher before the first store: a store opens its log as it is built
  //
  if (corDbPersistInit() == false)
    return -1;

  //
  // Create the store for the default tenant eagerly
  //
  corDbTenantStore(&tenant0);
  corDbPersistTenants();                             // and every other tenant --dbDir knows
  corDbGeoInit();

#if COR_DB_RAM_ONLY
  COR_I("ramDB: in-memory store ready, in RAM only - a restart starts empty (per-tenant CorNode trees, GEOS enabled)");
#else
  COR_I("corDB: in-memory store ready (per-tenant CorNode trees, GEOS enabled)");
#endif
  return 0;
}
