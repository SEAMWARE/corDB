//
// FILE            corDbReplay.c
//
// AUTHOR          Ken Zangelin
//
// Copyright 2026 Seamware
// SPDX-License-Identifier: Apache-2.0
//
#include <stdbool.h>                                   // bool
#include <string.h>                                    // strcmp

#include "corLog/corLog.h"                             // COR_E
#include "corTree/CorNode.h"                           // CorNode
#include "corTree/corTreeBuilder.h"                    // corTreeChildAdd, corTreeChildRemove
#include "corTree/corTreeChildReplace.h"               // corTreeChildReplace
#include "corTree/corTreeClone.h"                      // corTreeClone
#include "corTree/corTreeFree.h"                       // corTreeFree
#include "corTree/corTreeLookup.h"                     // corTreeLookup

#include "corDB/corDbIndex.h"                          // corDbIndexLookup, corDbIndexLink, corDbIndexReplace, corDbIndexUnlink
#include "corDB/corDbLog.h"                            // CorDbLogRecord
#include "corDB/corDbStore.h"                          // CorDbStore
#include "corDB/corDbReplay.h"                         // Own interface



// -----------------------------------------------------------------------------
//
// byId - the member of a subscription or registration array with this id
//
static CorNode* byId(CorNode* arrayP, const char* id)
{
  for (CorNode* nodeP = arrayP->value.head; nodeP != NULL; nodeP = nodeP->next)
  {
    CorNode* idP = corTreeLookup(nodeP, "id");

    if ((idP != NULL) && (idP->type == CorString) && (strcmp(idP->value.s, id) == 0))
      return nodeP;
  }

  return NULL;
}



// -----------------------------------------------------------------------------
//
// idOf - a PUT's id (in the body) or a DELETE's (the body)
//
static const char* idOf(CorNode* bodyP)
{
  if (bodyP == NULL)
    return NULL;

  if (bodyP->type == CorString)
    return bodyP->value.s;

  CorNode* idP = corTreeLookup(bodyP, "id");

  return ((idP != NULL) && (idP->type == CorString)) ? idP->value.s : NULL;
}



// -----------------------------------------------------------------------------
//
// put - a subscription or registration: replaced in place, or added at the end
//
static bool put(CorNode* arrayP, const char* id, CorNode* bodyP)
{
  CorNode* cloneP = corTreeClone(NULL, bodyP);

  if (cloneP == NULL)
    return false;

  CorNode* oldP = byId(arrayP, id);

  if (oldP != NULL)
  {
    corTreeChildReplace(arrayP, oldP, cloneP);
    corTreeFree(oldP);
  }
  else
    corTreeChildAdd(arrayP, cloneP);

  return true;
}



// -----------------------------------------------------------------------------
//
// drop - a subscription or registration removed, if it is there
//
static void drop(CorNode* arrayP, const char* id)
{
  CorNode* oldP = byId(arrayP, id);

  if (oldP != NULL)
  {
    corTreeChildRemove(arrayP, oldP);
    corTreeFree(oldP);
  }
}



// -----------------------------------------------------------------------------
//
// corDbReplay -
//
bool corDbReplay(CorDbStore* storeP, CorDbLogRecord* recP)
{
  const char* id = idOf(recP->bodyP);

  if (id == NULL)
  {
    COR_E("corDB: log record %llu (op %d) has no id", (unsigned long long) recP->seq, recP->op);
    return false;
  }

  CorNode* subsP = corTreeLookup(storeP->tree, "subscriptions");
  CorNode* regsP = corTreeLookup(storeP->tree, "registrations");

  switch (recP->op)
  {
  case CorDbLogEntityPut:
  {
    CorNode* cloneP = corTreeClone(NULL, recP->bodyP);

    if (cloneP == NULL)
      return false;

    CorNode* oldP = corDbIndexLookup(storeP, id);

    if (oldP != NULL)
    {
      corDbIndexReplace(storeP, oldP, cloneP);
      corTreeFree(oldP);
    }
    else
      corDbIndexLink(storeP, cloneP);

    return true;
  }

  case CorDbLogAttrsPut:
  {
    //
    // Some members of an entity, each whole, and the members it no longer has (corDbPersistAppendAttrs):
    // a member it already has is replaced where it is, a new one goes to the end - where the write put it
    //
    CorNode* entityP = corDbIndexLookup(storeP, id);

    if (entityP == NULL)
    {
      COR_E("corDB: log record %llu changes attributes of '%s', which is not in the store", (unsigned long long) recP->seq, id);
      return false;
    }

    CorNode* attrsP   = corTreeLookup(recP->bodyP, "attrs");
    CorNode* deletedP = corTreeLookup(recP->bodyP, "deleted");

    for (CorNode* mP = (attrsP != NULL) ? attrsP->value.head : NULL; mP != NULL; mP = mP->next)
    {
      CorNode* cloneP = corTreeClone(NULL, mP);

      if (cloneP == NULL)
        return false;

      CorNode* oldP = corTreeLookup(entityP, mP->name);

      if (oldP != NULL)
      {
        corTreeChildReplace(entityP, oldP, cloneP);
        corTreeFree(oldP);
      }
      else
        corTreeChildAdd(entityP, cloneP);
    }

    for (CorNode* nP = (deletedP != NULL) ? deletedP->value.head : NULL; nP != NULL; nP = nP->next)
    {
      CorNode* oldP = (nP->type == CorString) ? corTreeLookup(entityP, nP->value.s) : NULL;

      if (oldP != NULL)
      {
        corTreeChildRemove(entityP, oldP);
        corTreeFree(oldP);
      }
    }

    return true;
  }

  case CorDbLogEntityDelete:
  {
    CorNode* oldP = corDbIndexLookup(storeP, id);

    if (oldP != NULL)
    {
      corDbIndexUnlink(storeP, oldP);
      corTreeFree(oldP);
    }

    return true;
  }

  case CorDbLogSubPut:     return put(subsP, id, recP->bodyP);
  case CorDbLogSubDelete:  drop(subsP, id); return true;
  case CorDbLogRegPut:     return put(regsP, id, recP->bodyP);
  case CorDbLogRegDelete:  drop(regsP, id); return true;

  case CorDbLogDocPut:
  case CorDbLogDocDelete:
  {
    CorNode* collNameP = corTreeLookup(recP->bodyP, "collection");

    if ((collNameP == NULL) || (collNameP->type != CorString))
    {
      COR_E("corDB: log record %llu: a document record without its collection", (unsigned long long) recP->seq);
      return false;
    }

    CorNode* collP = corDbStoreDocs(storeP, collNameP->value.s, true);

    if (collP == NULL)
      return false;

    if (recP->op == CorDbLogDocDelete)
    {
      drop(collP, id);
      return true;
    }

    CorNode* docP = corTreeLookup(recP->bodyP, "doc");

    if ((docP == NULL) || (docP->type != CorObject))
    {
      COR_E("corDB: log record %llu: a document record without its document", (unsigned long long) recP->seq);
      return false;
    }

    docP->name = NULL;                                // an element of the collection's array, as the write stored it
    return put(collP, id, docP);
  }

  default:
    COR_E("corDB: log record %llu: op %d is not one this broker writes", (unsigned long long) recP->seq, recP->op);
    return false;
  }
}
