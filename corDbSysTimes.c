//
// FILE            corDbSysTimes.c
//
// AUTHOR          Ken Zangelin
//
// Copyright 2026 Seamware
// SPDX-License-Identifier: Apache-2.0
//
#include <string.h>                                   // strcmp

#include "corTree/CorNode.h"                          // CorNode
#include "corTree/corTreeBuilder.h"                   // corTreeObject, corTreeArray, corTreeInteger, corTreeChildAdd
#include "corTree/corTreeClone.h"                     // corTreeClone
#include "corTree/corTreeFree.h"                      // corTreeFree
#include "corTree/corTreeLookup.h"                    // corTreeLookup
#include "corNgsild/CorTerm.h"                        // CorTermCreatedAt, CorTermModifiedAt

#include "corDB/corDbSysTimes.h"                      // Own interface



// -----------------------------------------------------------------------------
//
// memberTime - a member's integer value (0: none)
//
static int64_t memberTime(CorNode* nodeP, const char* name)
{
  CorNode* tP = ((nodeP != NULL) && (nodeP->type == CorObject)) ? corTreeLookup(nodeP, name) : NULL;

  return ((tP != NULL) && (tP->type == CorInt)) ? (int64_t) tP->value.i : 0;
}



// -----------------------------------------------------------------------------
//
// isCreatedAt / isModifiedAt - the system timestamp members (integers)
//
static inline bool isCreatedAt(CorNode* mP)
{
  return (mP->type == CorInt) && (mP->name != NULL) && (mP->name[0] == 'c') && (strcmp(mP->name, "createdAt") == 0);
}

static inline bool isModifiedAt(CorNode* mP)
{
  return (mP->type == CorInt) && (mP->name != NULL) && (mP->name[0] == 'm') && (strcmp(mP->name, "modifiedAt") == 0);
}



// -----------------------------------------------------------------------------
//
// headCreatedAt - a store entity's createdAt: its FIRST member (corDbTreeIn puts it there - one hop),
// else looked up (an entity stored whole)
//
static inline int64_t headCreatedAt(CorNode* nodeP)
{
  if ((nodeP == NULL) || (nodeP->type != CorObject))
    return 0;

  CorNode* hP = nodeP->value.head;

  return ((hP != NULL) && isCreatedAt(hP)) ? (int64_t) hP->value.i : memberTime(nodeP, "createdAt");
}



// -----------------------------------------------------------------------------
//
// corDbCreatedAt / corDbModifiedAt -
//
int64_t corDbCreatedAt(CorNode* nodeP, int64_t parentCreatedAt)
{
  int64_t t = (COR_DB_SYS_TIMES == 1) ? headCreatedAt(nodeP) : memberTime(nodeP, "createdAt");

  return ((t == 0) && (COR_DB_SYS_TIMES == 1)) ? parentCreatedAt : t;
}

int64_t corDbModifiedAt(CorNode* nodeP, int64_t parentCreatedAt)
{
  int64_t t = memberTime(nodeP, "modifiedAt");

  return ((t == 0) && (COR_DB_SYS_TIMES == 1)) ? corDbCreatedAt(nodeP, parentCreatedAt) : t;
}



#if COR_DB_SYS_TIMES
// -----------------------------------------------------------------------------
//
// opaque - a member whose content is the user's: a value is never looked into
//
static bool opaque(const char* name)
{
  return (name != NULL) && ((strcmp(name, "value") == 0) || (strcmp(name, "object") == 0) || (strcmp(name, "json") == 0) ||
                            (strcmp(name, "vocab") == 0) || (strcmp(name, "languageMap") == 0) ||
                            (strcmp(name, "valueList") == 0) || (strcmp(name, "objectList") == 0));
}






// -----------------------------------------------------------------------------
//
// treeIn - a tree into the store, one pass, the clone's own: every createdAt / modifiedAt below the entity
// equal to the entity's createdAt left out (the entity is created whole, with one time - an object written
// since keeps its own), and the entity's modifiedAt while it is its createdAt
//
static CorNode* treeIn(CorNode* srcP, int64_t entityCreatedAt, bool inValue, int level)
{
  if ((srcP->type != CorObject) && (srcP->type != CorArray))
    return corTreeClone(NULL, srcP);

  CorNode* nodeP = (srcP->type == CorObject) ? corTreeObject(NULL, srcP->name) : corTreeArray(NULL, srcP->name);

  if (nodeP == NULL)
    return NULL;

  nodeP->flags  = srcP->flags;
  nodeP->termId = srcP->termId;

  bool timed = (srcP->type == CorObject) && (inValue == false) && ((level == 0) || (level >= 2));

  if (level == 0)                                     // the entity's createdAt FIRST: found in one hop
  {
    CorNode* tP = corTreeInteger(NULL, "createdAt", entityCreatedAt);

    if (tP == NULL)
    {
      corTreeFree(nodeP);
      return NULL;
    }

    tP->termId = CorTermCreatedAt;
    corTreeChildAdd(nodeP, tP);
  }

  for (CorNode* mP = srcP->value.head; mP != NULL; mP = mP->next)
  {
    if ((level == 0) && isCreatedAt(mP))
      continue;

    if (timed && (mP->value.i == entityCreatedAt) && (isModifiedAt(mP) || ((level >= 2) && isCreatedAt(mP))))
      continue;

    CorNode* cP = treeIn(mP, entityCreatedAt, inValue || ((srcP->type == CorObject) && opaque(mP->name)), (srcP->type == CorObject) ? level + 1 : level);

    if (cP == NULL)
    {
      corTreeFree(nodeP);
      return NULL;
    }

    corTreeChildAdd(nodeP, cP);
  }

  return nodeP;
}



// -----------------------------------------------------------------------------
//
// treeOut - a store tree out, one pass, the clone's own: what is left out put back - the entity's
// createdAt, right before an object's own modifiedAt or, with neither, both last (where corNgsild puts
// them); an object below the entity is one with a "type"
//
static CorNode* treeOut(CorAlloc* kaP, CorNode* storeP, int64_t entityCreatedAt, bool inValue, int level)
{
  if ((storeP->type != CorObject) && (storeP->type != CorArray))
    return corTreeClone(kaP, storeP);

  CorNode* nodeP = (storeP->type == CorObject) ? corTreeObject(kaP, storeP->name) : corTreeArray(kaP, storeP->name);

  if (nodeP == NULL)
    return NULL;

  nodeP->flags  = storeP->flags;
  nodeP->termId = storeP->termId;

  bool timed   = (storeP->type == CorObject) && (inValue == false) && ((level == 0) || (level >= 2)) && (entityCreatedAt != 0);
  bool hasType = (level == 0);
  bool seenC   = false;
  bool seenM   = false;

  for (CorNode* mP = storeP->value.head; mP != NULL; mP = mP->next)
  {
    if (timed && (mP->type == CorInt) && (mP->name != NULL))
    {
      if (isCreatedAt(mP))
      {
        if (level == 0)                               // the store keeps it first - out, it goes where it was
          continue;
        seenC = true;
      }
      else if (isModifiedAt(mP))
      {
        if (seenC == false)
        {
          CorNode* tP = corTreeInteger(kaP, "createdAt", entityCreatedAt);

          tP->termId = CorTermCreatedAt;
          corTreeChildAdd(nodeP, tP);
          seenC = true;
        }
        seenM = true;
      }
    }
    else if (timed && (hasType == false) && (mP->name != NULL) && (mP->name[0] == 't') && (strcmp(mP->name, "type") == 0))
      hasType = true;

    CorNode* cP = treeOut(kaP, mP, entityCreatedAt, inValue || ((storeP->type == CorObject) && opaque(mP->name)), (storeP->type == CorObject) ? level + 1 : level);

    if (cP == NULL)
      return NULL;

    corTreeChildAdd(nodeP, cP);
  }

  if (timed && hasType)
  {
    if (seenC == false)
    {
      CorNode* tP = corTreeInteger(kaP, "createdAt", entityCreatedAt);

      tP->termId = CorTermCreatedAt;
      corTreeChildAdd(nodeP, tP);
    }

    if (seenM == false)
    {
      CorNode* tP = corTreeInteger(kaP, "modifiedAt", entityCreatedAt);

      tP->termId = CorTermModifiedAt;
      corTreeChildAdd(nodeP, tP);
    }
  }

  return nodeP;
}



// -----------------------------------------------------------------------------
//
// corDbTreeIn / corDbTreeOut -
//
// An entity (entityCreatedAt 0) is level 0 - its createdAt the one its objects inherit. An attribute
// (its dataset wrapper) is level 1, 'entityCreatedAt' its entity's.
//
CorNode* corDbTreeIn(CorNode* srcP, int64_t entityCreatedAt)
{
  if (entityCreatedAt == 0)
  {
    int64_t createdAt = memberTime(srcP, "createdAt");

    if (createdAt == 0)                               // no createdAt (none to inherit): stored as it is
      return corTreeClone(NULL, srcP);

    return treeIn(srcP, createdAt, false, 0);
  }

  return treeIn(srcP, entityCreatedAt, false, 1);
}

CorNode* corDbTreeOut(CorAlloc* kaP, CorNode* storeP, int64_t entityCreatedAt)
{
  if (entityCreatedAt == 0)
  {
    int64_t createdAt = headCreatedAt(storeP);

    if (createdAt == 0)
      return corTreeClone(kaP, storeP);

    return treeOut(kaP, storeP, createdAt, false, 0);
  }

  return treeOut(kaP, storeP, entityCreatedAt, false, 1);
}

#else

CorNode* corDbTreeIn(CorNode* srcP, int64_t parentCreatedAt)                  { (void) parentCreatedAt; return corTreeClone(NULL, srcP); }
CorNode* corDbTreeOut(CorAlloc* kaP, CorNode* storeP, int64_t parentCreatedAt) { (void) parentCreatedAt; return corTreeClone(kaP, storeP); }

#endif
