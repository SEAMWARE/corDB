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
// corDbCreatedAt / corDbModifiedAt -
//
int64_t corDbCreatedAt(CorNode* nodeP, int64_t parentCreatedAt)
{
  int64_t t = memberTime(nodeP, "createdAt");

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

static bool isTime(CorNode* mP, const char* name)
{
  return (mP->type == CorInt) && (mP->name != NULL) && (strcmp(mP->name, name) == 0);
}



// -----------------------------------------------------------------------------
//
// treeIn - the object's own createdAt (0: none) is 'createdAt'; its children's parent is it
//
// 'level': 0 the entity (keeps both), 1 an attribute's dataset wrapper (no times), 2+ an instance, a
// sub-attribute - an object with times.
//
static CorNode* treeIn(CorNode* srcP, int64_t parentCreatedAt, bool inValue, int level)
{
  if ((srcP->type != CorObject) && (srcP->type != CorArray))
    return corTreeClone(NULL, srcP);

  CorNode* nodeP = (srcP->type == CorObject) ? corTreeObject(NULL, srcP->name) : corTreeArray(NULL, srcP->name);

  if (nodeP == NULL)
    return NULL;

  nodeP->flags  = srcP->flags;
  nodeP->termId = srcP->termId;

  //
  // This object's times: what it inherits, what it keeps
  //
  int64_t createdAt  = memberTime(srcP, "createdAt");
  int64_t modifiedAt = memberTime(srcP, "modifiedAt");
  bool    timed      = (srcP->type == CorObject) && (inValue == false) && ((createdAt != 0) || (modifiedAt != 0));
  bool    dropC      = timed && (level > 0) && (createdAt == parentCreatedAt);
  int64_t ownC       = (createdAt != 0) ? createdAt : parentCreatedAt;
  bool    dropM      = timed && (level > 0) && (modifiedAt == ownC);

  for (CorNode* mP = srcP->value.head; mP != NULL; mP = mP->next)
  {
    if (timed && dropC && isTime(mP, "createdAt"))
      continue;
    if (timed && dropM && isTime(mP, "modifiedAt"))
      continue;

    bool     childInValue = inValue || ((srcP->type == CorObject) && opaque(mP->name));
    int64_t  childParent  = (timed == true) ? ownC : parentCreatedAt;   // a dataset wrapper passes its parent's on
    CorNode* cP           = treeIn(mP, childParent, childInValue, (srcP->type == CorObject) ? level + 1 : level);

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
// isAttrObject - an object that carries times: an instance or a sub-attribute (it has a "type", or is
// the entity) - not a dataset wrapper, not a value
//
static bool hasType(CorNode* nodeP)
{
  CorNode* tP = corTreeLookup(nodeP, "type");

  return (tP != NULL) && (tP->type == CorString);
}



// -----------------------------------------------------------------------------
//
// treeOut -
//
static CorNode* treeOut(CorAlloc* kaP, CorNode* storeP, int64_t parentCreatedAt, bool inValue, int level)
{
  if ((storeP->type != CorObject) && (storeP->type != CorArray))
    return corTreeClone(kaP, storeP);

  CorNode* nodeP = (storeP->type == CorObject) ? corTreeObject(kaP, storeP->name) : corTreeArray(kaP, storeP->name);

  if (nodeP == NULL)
    return NULL;

  nodeP->flags  = storeP->flags;
  nodeP->termId = storeP->termId;

  bool    timed     = (storeP->type == CorObject) && (inValue == false) && ((level == 0) || ((level >= 2) && hasType(storeP)));
  int64_t createdAt = memberTime(storeP, "createdAt");
  int64_t ownC      = (createdAt != 0) ? createdAt : parentCreatedAt;

  for (CorNode* mP = storeP->value.head; mP != NULL; mP = mP->next)
  {
    bool     childInValue = inValue || ((storeP->type == CorObject) && opaque(mP->name));
    int64_t  childParent  = (timed == true) ? ownC : parentCreatedAt;
    CorNode* cP           = treeOut(kaP, mP, childParent, childInValue, (storeP->type == CorObject) ? level + 1 : level);

    if (cP == NULL)
      return NULL;

    corTreeChildAdd(nodeP, cP);
  }

  if ((timed == true) && (ownC != 0))
  {
    //
    // What it inherited, put back where corNgsild puts it: last
    //
    if (corTreeLookup(storeP, "createdAt") == NULL)
    {
      CorNode* tP = corTreeInteger(kaP, "createdAt", ownC);
      tP->termId  = CorTermCreatedAt;
      corTreeChildAdd(nodeP, tP);
    }

    if (corTreeLookup(storeP, "modifiedAt") == NULL)
    {
      CorNode* tP = corTreeInteger(kaP, "modifiedAt", ownC);
      tP->termId  = CorTermModifiedAt;
      corTreeChildAdd(nodeP, tP);
    }
  }

  return nodeP;
}



// -----------------------------------------------------------------------------
//
// corDbTreeIn / corDbTreeOut -
//
// An entity is level 0; an attribute (its dataset wrapper) level 1, its parent the entity's createdAt.
//
CorNode* corDbTreeIn(CorNode* srcP, int64_t parentCreatedAt)
{
  return treeIn(srcP, parentCreatedAt, false, (parentCreatedAt == 0) ? 0 : 1);
}

CorNode* corDbTreeOut(CorAlloc* kaP, CorNode* storeP, int64_t parentCreatedAt)
{
  return treeOut(kaP, storeP, parentCreatedAt, false, (parentCreatedAt == 0) ? 0 : 1);
}

#else

CorNode* corDbTreeIn(CorNode* srcP, int64_t parentCreatedAt)                  { (void) parentCreatedAt; return corTreeClone(NULL, srcP); }
CorNode* corDbTreeOut(CorAlloc* kaP, CorNode* storeP, int64_t parentCreatedAt) { (void) parentCreatedAt; return corTreeClone(kaP, storeP); }

#endif
