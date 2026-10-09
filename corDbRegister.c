//
// FILE            corDbRegister.c
//
// AUTHOR          Ken Zangelin
//
// Copyright 2026 Seamware
// SPDX-License-Identifier: Apache-2.0
//
#define PLUGIN_VERSION "0.2.0"

#include <string.h>                                    // memset

#include "corNgsild/LdSubCache.h"                       // LdSubCacheItem
#include "db/DbDriver.h"                               // DbDriver
#include "db/DbQueryFilter.h"                          // DbQueryFilter
#include "shared/geoMatch.h"                           // csrGeoMatchOverlap, csrGeoMatchExact

#include "corDB/corDbContext.h"                      // corDbContextSave, ...Delete, ...List, ...Get
#include "corDB/corDbSubscriptionStatsFlush.h"       // corDbSubscriptionStatsFlush
#include "corDB/corDbSnapshot.h"                     // corDbSnapshotCreate, ...Query, ...Update, ...Delete
#include "corDB/corDbTenantDrop.h"                   // corDbTenantDrop
#include "corDB/corDbGlobals.h"        // corDbArgV
#include "corDB/corDbInit.h"           // corDbInit
#include "corDB/corDbClose.h"          // corDbClose
#include "corDB/corDbStore.h"          // corDbTenantStore
#include "corDB/corDbEntityCreate.h"   // corDbEntityCreate
#include "corDB/corDbEntityBulkCreate.h" // corDbEntityBulkCreate
#include "corDB/corDbEntityBulkUpdate.h" // corDbEntityBulkUpdate
#include "corDB/corDbEntityBulkMerge.h"  // corDbEntityBulkMerge
#include "corDB/corDbEntityBulkDelete.h" // corDbEntityBulkDelete
#include "corDB/corDbEntityRetrieve.h" // corDbEntityRetrieve
#include "corDB/corDbEntityQuery.h"    // corDbEntityQuery
#include "corDB/corDbEntityDelete.h"   // corDbEntityDelete
#include "corDB/corDbEntityMerge.h"    // corDbEntityMerge
#include "corDB/corDbEntityReplace.h"  // corDbEntityReplace
#include "corDB/corDbEntityAttrsSet.h" // corDbEntityAttrsSet
#include "corDB/corDbTypeList.h"       // corDbTypeList
#include "corDB/corDbAttrList.h"       // corDbAttrList
#if COR_FEATURE_SUBSCRIPTIONS
#include "corDB/corDbSubscriptionCreate.h"    // corDbSubscriptionCreate
#include "corDB/corDbSubscriptionRetrieve.h"  // corDbSubscriptionRetrieve
#include "corDB/corDbSubscriptionQuery.h"     // corDbSubscriptionQuery
#include "corDB/corDbSubscriptionUpdate.h"    // corDbSubscriptionUpdate
#include "corDB/corDbSubscriptionReplace.h"   // corDbSubscriptionReplace
#include "corDB/corDbSubscriptionDelete.h"    // corDbSubscriptionDelete
#endif
#include "corDB/corDbDoc.h"                           // corDbDocCreate, ...
#if COR_FEATURE_REGISTRATIONS
#include "corDB/corDbRegistrationCreate.h"    // corDbRegistrationCreate
#include "corDB/corDbRegistrationRetrieve.h"  // corDbRegistrationRetrieve
#include "corDB/corDbRegistrationQuery.h"     // corDbRegistrationQuery
#include "corDB/corDbRegistrationUpdate.h"    // corDbRegistrationUpdate
#include "corDB/corDbRegistrationDelete.h"    // corDbRegistrationDelete
#endif
#include "corDB/corDbGeoMatch.h"             // corDbGeoMatch
#include "shared/dbPluginAbi.h"                          // dbPluginAbiBrokerCheck



// -----------------------------------------------------------------------------
//
// corDbGeoMatchCb - generic geo match callback
//
static bool corDbGeoMatchCb(CorNode* entityP, LdGeoRel* geoRel, const char* geometry,
                             const char* coordinates, const char* geoproperty)
{
  DbQueryFilter filter;

  memset(&filter, 0, sizeof(filter));
  filter.geoRel       = geoRel;
  filter.geometry      = (char*) geometry;
  filter.coordinates   = (char*) coordinates;
  filter.geoproperty   = (char*) geoproperty;

  return corDbGeoMatch(entityP, &filter, NULL);
}



// -----------------------------------------------------------------------------
//
// corDbTenantSetup - create the per-tenant store tree on first use
//
static int corDbTenantSetup(Tenant* tenantP)
{
  corDbTenantStore(tenantP);
  return 0;
}



// -----------------------------------------------------------------------------
//
// dbRegister -
//
void dbRegister(DbDriver* driverP)
{
  dbPluginAbiBrokerCheck("DB");                     // exits on a broker built against another interface

#if COR_DB_RAM_ONLY
  driverP->alias           = "ramDB";
#else
  driverP->alias           = "corDB";
#endif
  driverP->version         = PLUGIN_VERSION;
  driverP->args            = corDbArgV;
  driverP->init            = corDbInit;
  driverP->close           = corDbClose;
  driverP->entityCreate     = corDbEntityCreate;
  driverP->entityBulkCreate = corDbEntityBulkCreate;
  driverP->entityBulkUpdate = corDbEntityBulkUpdate;
  driverP->entityBulkRetrieve     = corDbEntityBulkRetrieve;
  driverP->entityBulkChangesApply = corDbEntityBulkChangesApply;
  driverP->entityBulkDelete = corDbEntityBulkDelete;
  driverP->entityRetrieve  = corDbEntityRetrieve;
  driverP->entityQuery     = corDbEntityQuery;
  driverP->entityDelete    = corDbEntityDelete;
  driverP->entityChangesApply = corDbEntityChangesApply;
  driverP->entityReplace   = corDbEntityReplace;
  driverP->entityAttrsSet  = corDbEntityAttrsSet;
  driverP->typeList        = corDbTypeList;
  driverP->attrList        = corDbAttrList;
  //
  // With COR_FEATURE_SUBSCRIPTIONS off these sources leave the plugin build
  // (CMakeLists.txt) and the slots stay NULL - the driver-slot convention for
  // "this driver does not do that". Nothing calls them: the routes that would
  // are answered by corNotInThisBuild before any driver is reached.
  //
#if COR_FEATURE_SUBSCRIPTIONS
  driverP->subscriptionCreate   = corDbSubscriptionCreate;
  driverP->subscriptionRetrieve = corDbSubscriptionRetrieve;
  driverP->subscriptionQuery    = corDbSubscriptionQuery;
  driverP->subscriptionUpdate   = corDbSubscriptionUpdate;
  driverP->subscriptionReplace  = corDbSubscriptionReplace;
  driverP->subscriptionDelete   = corDbSubscriptionDelete;
  driverP->subscriptionList     = corDbSubscriptions;
#endif
  //
  // With COR_FEATURE_REGISTRATIONS off these sources leave the plugin build
  // (CMakeLists.txt) and the slots stay NULL - the driver-slot convention for
  // "this driver does not do that". Nothing calls them: the routes that would
  // are answered by corNotInThisBuild before any driver is reached.
  //
#if COR_FEATURE_REGISTRATIONS
  driverP->registrationCreate   = corDbRegistrationCreate;
  driverP->registrationRetrieve = corDbRegistrationRetrieve;
  driverP->registrationQuery    = corDbRegistrationQuery;
  driverP->registrationUpdate   = corDbRegistrationUpdate;
  driverP->registrationDelete   = corDbRegistrationDelete;
  driverP->registrationList     = corDbRegistrations;
#endif
  driverP->contextSave     = corDbContextSave;
  driverP->contextDelete   = corDbContextDelete;
  driverP->contextList     = corDbContextList;
  driverP->contextGet      = corDbContextGet;
  driverP->subscriptionStatsFlush = corDbSubscriptionStatsFlush;
  driverP->snapshotCreate  = corDbSnapshotCreate;
  driverP->snapshotQuery   = corDbSnapshotQuery;
  driverP->snapshotUpdate  = corDbSnapshotUpdate;
  driverP->snapshotDelete  = corDbSnapshotDelete;
  driverP->tenantDrop      = corDbTenantDrop;
  driverP->docCreate       = corDbDocCreate;
  driverP->docRetrieve     = corDbDocRetrieve;
  driverP->docQuery        = corDbDocQuery;
  driverP->docReplace      = corDbDocReplace;
  driverP->docDelete       = corDbDocDelete;

  driverP->tenantSetup     = corDbTenantSetup;
  driverP->geoMatchFunc    = corDbGeoMatchCb;
  driverP->csrGeoMatchFunc      = csrGeoMatchOverlap;
  driverP->csrGeoMatchExactFunc = csrGeoMatchExact;
}
