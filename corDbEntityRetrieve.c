//
// FILE            corDbEntityRetrieve.c
//
// AUTHOR          Ken Zangelin
//
// Copyright 2026 Seamware
// SPDX-License-Identifier: Apache-2.0
//
#include <string.h>                                   // strcmp

#include "corTree/CorNode.h"                          // CorNode
#include "corTree/corTreeClone.h"                     // corTreeClone
#include "corTree/corTreeLookup.h"                    // corTreeLookup
#include "corRest/CorRestState.h"                       // corRest

#include "db/DbDriver.h"                              // DB_OK, DB_NOT_FOUND, Tenant
#include "corDB/corDbSysTimes.h"                      // corDbTreeIn, corDbTreeOut
#include "corDB/corDbIndex.h"        // corDbIndexLookup
#include "corDB/corDbStore.h"          // corDbEntities
#include "corDB/corDbEntityRetrieve.h" // Own interface



// -----------------------------------------------------------------------------
//
// entityRetrieve - under the store's read lock
//
static int entityRetrieve(Tenant* tenantP, const char* entityId, CorNode** entityPP, int64_t* createdAtP)
{
  COR_DB_READ(tenantP);

  CorNode* entities = corDbEntities(tenantP);

  //
  // O(1) via the id index. The walk below is the fallback for a store built
  // before the index existed - it cannot happen in a running broker, but a NULL
  // index must not mean "entity not found".
  //
  {
    CorNode* hitP = corDbIndexLookup(corDbStoreOf(tenantP), entityId);

    if (hitP != NULL)
    {
      *entityPP = corDbEntityCopy(corRest.kallocP, hitP, createdAtP);
      return DB_OK;
    }

    if (corDbStoreOf(tenantP)->idToPrevEntity != NULL)
      return DB_NOT_FOUND;                           // indexed, and it is not there
  }

  for (CorNode* eP = entities->value.head; eP != NULL; eP = eP->next)
  {
    CorNode* idP = corTreeLookup(eP, "id");

    if (idP != NULL && idP->type == CorString && strcmp(idP->value.s, entityId) == 0)
    {
      // Clone into the request arena (freed at request end), matching mongoc's
      // retrieve. A NULL (malloc) clone would leak — no caller frees the result;
      // they all consume it within the request (render / merge / replace-copy).
      *entityPP = corDbEntityCopy(corRest.kallocP, eP, createdAtP);
      return DB_OK;
    }
  }

  return DB_NOT_FOUND;
}



// -----------------------------------------------------------------------------
//
// corDbEntityRetrieve - the copy under the store's lock, its inherited times put back after it
//
int corDbEntityRetrieve(Tenant* tenantP, const char* entityId, CorNode** entityPP)
{
  int64_t createdAt = 0;
  int     r         = entityRetrieve(tenantP, entityId, entityPP, &createdAt);

  if (r == DB_OK)
    corDbEntityCopyFinish(corRest.kallocP, *entityPP, createdAt);

  return r;
}
