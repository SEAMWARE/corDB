//
// FILE            corDbStore.c
//
// AUTHOR          Ken Zangelin
//
// Copyright 2026 Seamware
// SPDX-License-Identifier: Apache-2.0
//
#include <pthread.h>                                 // pthread_rwlock_init
#include <stddef.h>                                  // NULL
#include <stdlib.h>                                  // malloc, free

#include "corTree/CorNode.h"                         // CorNode
#include "corTree/corTreeBuilder.h"                  // corTreeObject, corTreeArray, corTreeChildAdd
#include "corTree/corTreeFree.h"                     // corTreeFree
#include "corTree/corTreeLookup.h"                   // corTreeLookup

#include "db/Tenant.h"                               // Tenant

#include "corDB/corDbHistory.h"                      // CorDbHistory, corDbHistoryOn
#include "corDB/corDbHistoryWrite.h"                 // corDbHistoryDrain
#include "corDB/corDbPersist.h"                      // corDbPersistOpen, corDbPersistSyncWait
#include "corDB/corDbStore.h"         // Own interface



// -----------------------------------------------------------------------------
//
// createMutex - one store is built at a time
//
// Building a store is rare (once per tenant) and, with --dbDir, opens the tenant's log - which two
// builders racing for one tenant must not both do. The first request on a tenant takes it; every
// later one finds the store published and never comes here.
//
static pthread_mutex_t createMutex = PTHREAD_MUTEX_INITIALIZER;



// -----------------------------------------------------------------------------
//
// corDbTenantStore - return (or create) the per-tenant CorNode tree
//
//
// corDbStoreOf - the tenant's store, created on first use
//
CorDbStore* corDbStoreOf(Tenant* tenantP)
{
  CorDbStore* existingP = (CorDbStore*) __atomic_load_n(&tenantP->pluginData, __ATOMIC_ACQUIRE);

  if (existingP != NULL)
    return existingP;

  pthread_mutex_lock(&createMutex);

  existingP = (CorDbStore*) __atomic_load_n(&tenantP->pluginData, __ATOMIC_ACQUIRE);
  if (existingP != NULL)                             // built while this thread waited for the mutex
  {
    pthread_mutex_unlock(&createMutex);
    return existingP;
  }

  //
  // First access for this tenant - build the store using malloc (NULL
  // allocator): it outlives every request, so it cannot come from a per-request
  // kalloc buffer.
  //
  CorDbStore* storeP = (CorDbStore*) malloc(sizeof(CorDbStore));

  if (storeP == NULL)
  {
    pthread_mutex_unlock(&createMutex);
    return NULL;
  }

  CorNode* store        = corTreeObject(NULL, NULL);
  CorNode* entities     = corTreeArray(NULL, "entities");
  CorNode* subscriptions = corTreeArray(NULL, "subscriptions");
  CorNode* registrations = corTreeArray(NULL, "registrations");

  corTreeChildAdd(store, entities);
  corTreeChildAdd(store, subscriptions);
  corTreeChildAdd(store, registrations);

  storeP->tree           = store;
  storeP->entities       = entities;
  storeP->idToPrevEntity = NULL;                     // built on the first entity
  storeP->idxSlots = 0;
  storeP->idxCount = 0;
  storeP->snapCursor = NULL;
  storeP->historyP   = corDbHistoryOn ? (struct CorDbHistory*) calloc(1, sizeof(CorDbHistory)) : NULL;
  storeP->histQHead  = NULL;
  storeP->histQTail  = NULL;
  pthread_mutex_init(&storeP->histQMutex, NULL);
  pthread_mutex_init(&storeP->histMutex, NULL);
  pthread_rwlock_init(&storeP->lock, NULL);
  storeP->persistP = corDbPersistOpen(tenantP, storeP);   // the log replayed into it - NULL without --dbDir

  //
  // Published complete, under the mutex. Two requests arriving together for a tenant with no store
  // yet - the first requests after startup, on ANY tenant, the default one included - each built one,
  // and the second assignment replaced the first: the entities already put in it were gone, while
  // their requests had answered 201. Now the second waits for the mutex and finds the first's.
  //
  __atomic_store_n(&tenantP->pluginData, storeP, __ATOMIC_RELEASE);
  pthread_mutex_unlock(&createMutex);

  return storeP;
}



// -----------------------------------------------------------------------------
//
// corDbStoreRead / corDbStoreWrite - take the lock, hand back the store
//
// A NULL store (out of memory on first touch) is handed back unlocked, and
// corDbStoreUnlock knows not to unlock it. The alternative - failing to lock and
// carrying on - is the bug this whole file exists to prevent.
//
CorDbStore* corDbStoreRead(Tenant* tenantP)
{
  CorDbStore* storeP = corDbStoreOf(tenantP);

  if (storeP != NULL)
    pthread_rwlock_rdlock(&storeP->lock);

  return storeP;
}



CorDbStore* corDbStoreWrite(Tenant* tenantP)
{
  CorDbStore* storeP = corDbStoreOf(tenantP);

  if (storeP != NULL)
    pthread_rwlock_wrlock(&storeP->lock);

  return storeP;
}



// -----------------------------------------------------------------------------
//
// corDbStoreUnlock - the cleanup handler behind COR_DB_READ / COR_DB_WRITE
//
// Called by the compiler on every exit from the scope that declared the lock,
// including returns the author forgot about. Takes a POINTER to the variable
// because that is the cleanup attribute's contract.
//
void corDbStoreUnlock(CorDbStore** storePP)
{
  if ((storePP != NULL) && (*storePP != NULL))
  {
    pthread_rwlock_unlock(&(*storePP)->lock);

    if (__atomic_load_n(&(*storePP)->histQHead, __ATOMIC_ACQUIRE) != NULL)
      corDbHistoryDrain(*storePP);                   // the history this write enqueued - before the answer, so a read sees it

    corDbPersistSyncWait();                          // --dbSync request only, and only after a write
  }
}



// -----------------------------------------------------------------------------
//
CorNode* corDbTenantStore(Tenant* tenantP)
{
  CorDbStore* storeP = corDbStoreOf(tenantP);

  return (storeP != NULL) ? storeP->tree : NULL;
}





// -----------------------------------------------------------------------------
//
// corDbEntities - return the "entities" array for a tenant
//
CorNode* corDbEntities(Tenant* tenantP)
{
  CorNode* store = corDbTenantStore(tenantP);

  return corTreeLookup(store, "entities");
}



// -----------------------------------------------------------------------------
//
// corDbSubscriptions - return the "subscriptions" array for a tenant
//
CorNode* corDbSubscriptions(Tenant* tenantP)
{
  CorNode* store = corDbTenantStore(tenantP);

  return corTreeLookup(store, "subscriptions");
}



// -----------------------------------------------------------------------------
//
// corDbRegistrations - return the "registrations" array for a tenant
//
CorNode* corDbRegistrations(Tenant* tenantP)
{
  CorNode* store = corDbTenantStore(tenantP);

  return corTreeLookup(store, "registrations");
}
