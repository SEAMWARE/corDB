//
// FILE            corDbStore.c
//
// AUTHOR          Ken Zangelin
//
// Copyright 2026 Seamware
// SPDX-License-Identifier: Apache-2.0
//
#include <pthread.h>                                 // pthread_rwlock_init, pthread_rwlockattr_*

#include <stddef.h>                                  // NULL
#include <stdlib.h>                                  // malloc, free

#include "corTree/CorNode.h"                         // CorNode
#include "corTree/corTreeBuilder.h"                  // corTreeObject, corTreeArray, corTreeChildAdd
#include "corTree/corTreeFree.h"                     // corTreeFree
#include "corTree/corTreeLookup.h"                   // corTreeLookup
#include "corHash/corHash.h"                         // corHashRelease

#include "corRest/corRestProblem.h"                 // corRestProblemLate, COR_REST_ERROR_INTERNAL

#include "db/Tenant.h"                               // Tenant

#include "corDB/corDbHistory.h"                      // CorDbHistory, corDbHistoryOn, corDbHistoryFree
#include "corDB/corDbHistoryWrite.h"                 // corDbHistoryDrain
#include "corDB/corDbPersist.h"                      // corDbPersistOpen, corDbPersistSyncWait, corDbPersistRelease
#include "corDB/corDbGlobals.h"                       // corDbLockWriters
#if COR_FEATURE_TYPE_INDEX
#include "corDB/corDbTypeIndex.h"                    // corDbTypeIndexFree
#endif
#if COR_FEATURE_DB_INDEXES
#include "corDB/corDbValueIndex.h"                   // corDbValueIndexDrop
#endif
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
  CorNode* docs          = corTreeObject(NULL, "docs");    // the document store: one array per collection

  corTreeChildAdd(store, entities);
  corTreeChildAdd(store, subscriptions);
  corTreeChildAdd(store, registrations);
  corTreeChildAdd(store, docs);

  storeP->tree           = store;
  storeP->entities       = entities;
  storeP->idToPrevEntity = NULL;                     // built on the first entity
  storeP->idxSlots = 0;
  storeP->idxCount = 0;
  storeP->idxOld = NULL;
  storeP->idxOldMoved = 0;
  storeP->snapCursor = NULL;
  storeP->writersWaiting = 0;
  storeP->historyP   = corDbHistoryOn ? (struct CorDbHistory*) calloc(1, sizeof(CorDbHistory)) : NULL;
  storeP->histQHead  = NULL;
  storeP->histQTail  = NULL;
  storeP->retiredOwnerP   = NULL;
  storeP->retiredNext     = NULL;
  storeP->droppedPersistP = NULL;
  pthread_mutex_init(&storeP->histQMutex, NULL);
  pthread_mutex_init(&storeP->histMutex, NULL);
#if COR_FEATURE_TYPE_INDEX
  storeP->typeIndexP = NULL;                         // built by the first query that can use it
  pthread_mutex_init(&storeP->typeIndexMutex, NULL);
#endif
#if COR_FEATURE_DB_INDEXES
  storeP->valueIndexP = NULL;                        // built by the first query that can use it
  pthread_mutex_init(&storeP->valueIndexMutex, NULL);
#endif
  {
    pthread_rwlockattr_t lockAttr;                     // --dbLockPrefer (corDbInit)

    pthread_rwlockattr_init(&lockAttr);
    if (corDbLockWriters)
      pthread_rwlockattr_setkind_np(&lockAttr, PTHREAD_RWLOCK_PREFER_WRITER_NONRECURSIVE_NP);
    pthread_rwlock_init(&storeP->lock, &lockAttr);
    pthread_rwlockattr_destroy(&lockAttr);
  }
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
// The retired stores - dropped, not yet released (CorDbStore.retiredOwnerP). A list, not a field of the
// tenant: a tenant dropped and used again builds a new store, and may be dropped again before it is
// released - each drop retires one.
//
static pthread_mutex_t retiredMutex = PTHREAD_MUTEX_INITIALIZER;
static CorDbStore*     retiredList  = NULL;



// -----------------------------------------------------------------------------
//
// corDbStoreFree -
//
void corDbStoreFree(CorDbStore* storeP)
{
  if (storeP == NULL)
    return;

  if (storeP->idToPrevEntity != NULL)
    corHashRelease(storeP->idToPrevEntity);

  if (storeP->idxOld != NULL)
    corHashRelease(storeP->idxOld);

#if COR_FEATURE_TYPE_INDEX
  corDbTypeIndexFree(storeP);
  pthread_mutex_destroy(&storeP->typeIndexMutex);
#endif
#if COR_FEATURE_DB_INDEXES
  corDbValueIndexDrop(storeP);
  pthread_mutex_destroy(&storeP->valueIndexMutex);
#endif

  corTreeFree(storeP->tree);

  if (storeP->historyP != NULL)
  {
    corDbHistoryFree(storeP->historyP);
    free(storeP->historyP);
  }

  pthread_mutex_destroy(&storeP->histQMutex);
  pthread_mutex_destroy(&storeP->histMutex);
  pthread_rwlock_destroy(&storeP->lock);
  free(storeP);
}



// -----------------------------------------------------------------------------
//
// corDbStoreRetire -
//
void corDbStoreRetire(Tenant* tenantP, CorDbStore* storeP)
{
  pthread_mutex_lock(&retiredMutex);
  storeP->retiredOwnerP = tenantP;
  storeP->retiredNext   = retiredList;
  retiredList           = storeP;
  pthread_mutex_unlock(&retiredMutex);
}



// -----------------------------------------------------------------------------
//
// corDbStoreRetiredRelease -
//
void corDbStoreRetiredRelease(Tenant* tenantP)
{
  CorDbStore*  mineP = NULL;
  CorDbStore** prevPP;

  pthread_mutex_lock(&retiredMutex);
  prevPP = &retiredList;
  while (*prevPP != NULL)
  {
    CorDbStore* storeP = *prevPP;

    if (storeP->retiredOwnerP == tenantP)
    {
      *prevPP             = storeP->retiredNext;
      storeP->retiredNext = mineP;
      mineP               = storeP;
    }
    else
      prevPP = &storeP->retiredNext;
  }
  pthread_mutex_unlock(&retiredMutex);

  while (mineP != NULL)                              // freed outside the mutex: a log node's release may wait for a snapshot's slice
  {
    CorDbStore* nextP = mineP->retiredNext;

    corDbPersistRelease(mineP->droppedPersistP);
    corDbStoreFree(mineP);
    mineP = nextP;
  }
}



// -----------------------------------------------------------------------------
//
// corDbStoreRetiredFreeAll -
//
void corDbStoreRetiredFreeAll(void)
{
  pthread_mutex_lock(&retiredMutex);
  CorDbStore* storeP = retiredList;
  retiredList = NULL;
  pthread_mutex_unlock(&retiredMutex);

  while (storeP != NULL)
  {
    CorDbStore* nextP = storeP->retiredNext;

    corDbStoreFree(storeP);                          // its log node went with corDbPersistClose
    storeP = nextP;
  }
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
  {
    __atomic_add_fetch(&storeP->writersWaiting, 1, __ATOMIC_RELAXED);
    pthread_rwlock_wrlock(&storeP->lock);
    __atomic_sub_fetch(&storeP->writersWaiting, 1, __ATOMIC_RELAXED);
  }

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

    //
    // A write the store took but could not keep is not answered as a success: the operation has
    // returned by now, so the answer is replaced (corRestProblemLate) - outside a request (a load, a
    // flush of subscription counters) the error is logged and that is all
    //
    CorDbKeep keep = corDbPersistSyncWait();           // the wait: --dbSync request only, and only after a write

    if (keep == CorDbNotLogged)
      corRestProblemLate(503, COR_REST_ERROR_INTERNAL, "Service Unavailable",
                         "the write is in the broker's memory but not in its log (corDB --dbDir) - a restart loses it; the broker's log says why");
    else if (keep == CorDbNotSynced)
      corRestProblemLate(503, COR_REST_ERROR_INTERNAL, "Service Unavailable",
                         "a sync of the log to the disk failed, for this write's record or one before it (corDB --dbDir) - a restart may lose it; the broker's log says why");
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
// corDbStoreDocs - a collection of the document store, made on its first use if 'create'
//
// The caller holds the store's lock (a write lock to create).
//
CorNode* corDbStoreDocs(CorDbStore* storeP, const char* collection, bool create)
{
  CorNode* docsP = corTreeLookup(storeP->tree, "docs");
  CorNode* collP = corTreeLookup(docsP, collection);

  if ((collP == NULL) && (create == true))
  {
    collP = corTreeArray(NULL, collection);

    if (collP != NULL)
      corTreeChildAdd(docsP, collP);
  }

  return collP;
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
