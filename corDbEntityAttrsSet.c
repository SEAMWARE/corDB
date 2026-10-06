//
// FILE            corDbEntityAttrsSet.c
//
// AUTHOR          Ken Zangelin
//
// Copyright 2026 Seamware
// SPDX-License-Identifier: Apache-2.0
//

#include <stdbool.h>                                 // bool
#include <string.h>                                   // strcmp

#include "corTree/corTreeFree.h"                      // corTreeFree
#include "corTree/corTreeChildReplace.h"              // corTreeChildReplace
#include "corTree/CorNode.h"                          // CorNode
#include "corTree/corTreeLookup.h"                    // corTreeLookup

#include "corNgsild/ldEntityAttrsSet.h"                // ldEntityAttrsSet
#include "corNgsild/LdVocab.h"                         // LD_VOCAB_MODIFIED_AT, LD_VOCAB_SCOPE

#include "db/DbDriver.h"                              // DB_OK, DB_NOT_FOUND
#include "corDB/corDbSysTimes.h"                      // corDbTreeIn, corDbTreeOut, corDbFullView
#include "corDB/corDbIndex.h"        // corDbIndexLookup
#include "corDB/corDbPersist.h"                      // corDbPersistAppend
#include "corDB/corDbHistoryWrite.h"                 // corDbHistoryCreated, ...Replaced, ...Merged, ...Deleted
#include "corRest/CorRestState.h"                    // corRest (kallocP - the history's scratch)
#include "corDB/corDbStore.h"          // corDbEntities
#include "corDB/corDbEntityAttrsSet.h" // Own interface



#if COR_DB_SYS_TIMES
// -----------------------------------------------------------------------------
//
// attrsTimes - the fragment's attributes of a live entity, in place: their instances' inherited times put
// in (fill - ldEntityAttrsSet keeps an instance's createdAt) or the inherited ones taken out again
// (corDbSysTimes.h)
//
static void attrsTimes(CorNode* eP, CorNode* fragmentDb, bool fill)
{
  int64_t entityCreatedAt = corDbCreatedAt(eP, 0);

  for (CorNode* fP = (fragmentDb != NULL) ? fragmentDb->value.head : NULL; fP != NULL; fP = fP->next)
  {
    CorNode* attrP = corTreeLookup(eP, fP->name);

    if (fill == true)
      corDbAttrTimesFill(attrP, entityCreatedAt);
    else
      corDbAttrTimesDrop(attrP, entityCreatedAt);
  }
}
#endif



// -----------------------------------------------------------------------------
//
// corDbEntityAttrsSet -
//
int corDbEntityAttrsSet(Tenant* tenantP, const char* entityId,
                        CorNode* fragmentDb, bool overwriteScope,
                        uint64_t ts, LdMergeReport* reportP)
{
  COR_DB_WRITE(tenantP);

  CorNode* entities = corDbEntities(tenantP);
  if (entities == NULL)
    return DB_NOT_FOUND;

  //
  // One hop via the id index instead of a walk of the whole store with a
  // corTreeLookup per entity. The loop shape is kept so the body below is unchanged:
  // indexed, it runs exactly once for the hit and not at all for a miss;
  // unindexed - a store that predates the index - it walks as it always did.
  //
  CorDbStore* idxStoreP = corDbStoreOf(tenantP);
  CorNode*    idxHitP   = corDbIndexLookup(idxStoreP, entityId);
  bool        indexed   = (idxStoreP != NULL) && (idxStoreP->idToPrevEntity != NULL);

  for (CorNode* eP = indexed ? idxHitP : entities->value.head;
       eP != NULL;
       eP = indexed ? NULL : eP->next)
  {
    CorNode* idP = corTreeLookup(eP, "id");
    if (idP != NULL && idP->type == CorString && strcmp(idP->value.s, entityId) == 0)
    {
      // NULL allocator → malloc heap (tenant store lifetime)
#if COR_DB_SYS_TIMES
      attrsTimes(eP, fragmentDb, true);               // what the attributes inherit, in place - ldEntityAttrsSet keeps it
#endif
      ldEntityAttrsSet(eP, fragmentDb, overwriteScope, ts, reportP, NULL);
#if COR_DB_SYS_TIMES
      attrsTimes(eP, fragmentDb, false);              // and the store's form again: only the new times stay
#endif

      //
      // The log record: the members the fragment names, as the entity has them now, and what the
      // write refreshes beside them - not the entity (corDbPersistAppendAttrs)
      //
      const char* names[64];
      int         n    = 0;
      bool        many = false;

      for (CorNode* mP = (fragmentDb != NULL) ? fragmentDb->value.head : NULL; mP != NULL; mP = mP->next)
      {
        if (n == 61)
        {
          many = true;
          break;
        }
        names[n++] = mP->name;
      }

      if (many)                                      // that many attributes: the entity
        corDbPersistAppend(corDbLockedStore->persistP, CorDbLogEntityPut, eP);
      else
      {
        names[n++] = LD_VOCAB_MODIFIED_AT;
        names[n++] = "type";
        names[n++] = LD_VOCAB_SCOPE;
        corDbPersistAppendAttrs(corDbLockedStore->persistP, eP, names, n);
      }
      corDbHistoryMerged(corDbLockedStore, corDbFullView(eP, corRest.kallocP), reportP, corRest.kallocP);
      return DB_OK;
    }
  }

  return DB_NOT_FOUND;
}
