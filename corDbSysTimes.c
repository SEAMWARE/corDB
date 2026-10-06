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
  if (name == NULL)
    return false;

  switch (name[0])
  {
  case 'v': return (strcmp(name, "value") == 0) || (strcmp(name, "vocab") == 0) || (strcmp(name, "valueList") == 0);
  case 'o': return (strcmp(name, "object") == 0) || (strcmp(name, "objectList") == 0);
  case 'j': return (strcmp(name, "json") == 0);
  case 'l': return (strcmp(name, "languageMap") == 0);
  }

  return false;
}






// -----------------------------------------------------------------------------
//
// INHERIT_C / INHERIT_M - a store object below the entity whose createdAt / modifiedAt is not there: the
// entity's createdAt. CorNode flag bits 0x04 and 0x08 (0x01, 0x02 and 0xF0 are corJsonld's). Store-only:
// an out-copy never carries them, and the log does not keep them - corDbTreeIn sets them again on replay.
//
#define INHERIT_C   0x04
#define INHERIT_M   0x08
#define INHERITS    (INHERIT_C | INHERIT_M)



// -----------------------------------------------------------------------------
//
// timeNode - a createdAt / modifiedAt member (in an out-copy)
//
static CorNode* timeNode(CorAlloc* kaP, bool created, int64_t t)
{
  CorNode* tP = corTreeInteger(kaP, created ? "createdAt" : "modifiedAt", t);

  if (tP != NULL)
    tP->termId = created ? CorTermCreatedAt : CorTermModifiedAt;

  return tP;
}



// -----------------------------------------------------------------------------
//
// treeIn - a tree into the store, one pass, the clone's own: every createdAt / modifiedAt below the entity
// equal to the entity's createdAt left out (the entity is created whole, with one time - an object written
// since keeps its own), and the entity's modifiedAt while it is its createdAt. An object below the entity
// (one with a "type") whose time is not there - left out now, or absent already (a log record) - is
// marked INHERIT_C / INHERIT_M: the out-copy then does not have to look.
//
static CorNode* treeIn(CorNode* srcP, int64_t entityCreatedAt, bool inValue, int level)
{
  CorNode* nodeP = (srcP->type == CorObject) ? corTreeObject(NULL, srcP->name) : corTreeArray(NULL, srcP->name);

  if (nodeP == NULL)
    return NULL;

  nodeP->flags  = srcP->flags & ~INHERITS;
  nodeP->termId = srcP->termId;

  bool timed   = (srcP->type == CorObject) && (inValue == false) && ((level == 0) || (level >= 2));
  bool hasType = false;
  bool keptC   = false;
  bool keptM   = false;

  if (level == 0)                                     // the entity's createdAt FIRST: found in one hop
  {
    CorNode* tP = timeNode(NULL, true, entityCreatedAt);

    if (tP == NULL)
    {
      corTreeFree(nodeP);
      return NULL;
    }

    corTreeChildAdd(nodeP, tP);
  }

  for (CorNode* mP = srcP->value.head; mP != NULL; mP = mP->next)
  {
    CorNode* cP;

    if ((mP->type != CorObject) && (mP->type != CorArray))
    {
      if (timed && (mP->type == CorInt))
      {
        if (isCreatedAt(mP))
        {
          if ((level == 0) || (mP->value.i == entityCreatedAt))
            continue;
          keptC = true;
        }
        else if (isModifiedAt(mP))
        {
          if (mP->value.i == entityCreatedAt)
            continue;
          keptM = true;
        }
      }
      else if (timed && (mP->type == CorString) && (mP->name != NULL) && (mP->name[0] == 't') && (strcmp(mP->name, "type") == 0))
        hasType = true;

      cP = corTreeClone(NULL, mP);
    }
    else
      cP = treeIn(mP, entityCreatedAt, inValue || ((srcP->type == CorObject) && opaque(mP->name)), (srcP->type == CorObject) ? level + 1 : level);

    if (cP == NULL)
    {
      corTreeFree(nodeP);
      return NULL;
    }

    corTreeChildAdd(nodeP, cP);
  }

  if (timed && (level >= 2) && hasType)
  {
    if (keptC == false) nodeP->flags |= INHERIT_C;
    if (keptM == false) nodeP->flags |= INHERIT_M;
  }

  return nodeP;
}



// -----------------------------------------------------------------------------
//
// treeOut - a store tree out: a clone, but for a marked object (INHERIT_C / INHERIT_M), whose missing
// times are put back - a createdAt right before its object's modifiedAt, both last when neither is there
// (where corNgsild puts them). No member is looked at in an unmarked object.
//
static CorNode* treeOut(CorAlloc* kaP, CorNode* storeP, int64_t entityCreatedAt)
{
  CorNode* nodeP = (storeP->type == CorObject) ? corTreeObject(kaP, storeP->name) : corTreeArray(kaP, storeP->name);

  if (nodeP == NULL)
    return NULL;

  unsigned char inherits = storeP->flags & INHERITS;

  nodeP->flags  = storeP->flags & ~INHERITS;
  nodeP->termId = storeP->termId;

  for (CorNode* mP = storeP->value.head; mP != NULL; mP = mP->next)
  {
    if (((inherits & INHERIT_C) != 0) && isModifiedAt(mP))
    {
      corTreeChildAdd(nodeP, timeNode(kaP, true, entityCreatedAt));
      inherits &= ~INHERIT_C;
    }

    CorNode* cP = ((mP->type != CorObject) && (mP->type != CorArray)) ? corTreeClone(kaP, mP) : treeOut(kaP, mP, entityCreatedAt);

    if (cP == NULL)
      return NULL;

    corTreeChildAdd(nodeP, cP);
  }

  if ((inherits & INHERIT_C) != 0) corTreeChildAdd(nodeP, timeNode(kaP, true,  entityCreatedAt));
  if ((inherits & INHERIT_M) != 0) corTreeChildAdd(nodeP, timeNode(kaP, false, entityCreatedAt));

  return nodeP;
}



// -----------------------------------------------------------------------------
//
// entityOut - a store ENTITY out: its createdAt (first in the store) back in its place - right before its
// modifiedAt, or last with the entity's modifiedAt (left out while it is the createdAt) after it
//
static CorNode* entityOut(CorAlloc* kaP, CorNode* storeP, int64_t createdAt)
{
  CorNode* nodeP = corTreeObject(kaP, storeP->name);

  if (nodeP == NULL)
    return NULL;

  nodeP->flags  = storeP->flags & ~INHERITS;
  nodeP->termId = storeP->termId;

  bool doneC = false;
  bool seenM = false;

  for (CorNode* mP = storeP->value.head; mP != NULL; mP = mP->next)
  {
    if (mP->type == CorInt)
    {
      if (isCreatedAt(mP))
        continue;                                     // the store keeps it first

      if (isModifiedAt(mP))
      {
        corTreeChildAdd(nodeP, timeNode(kaP, true, createdAt));
        doneC = true;
        seenM = true;
      }
    }

    CorNode* cP = ((mP->type != CorObject) && (mP->type != CorArray)) ? corTreeClone(kaP, mP) : treeOut(kaP, mP, createdAt);

    if (cP == NULL)
      return NULL;

    corTreeChildAdd(nodeP, cP);
  }

  if (doneC == false) corTreeChildAdd(nodeP, timeNode(kaP, true,  createdAt));
  if (seenM == false) corTreeChildAdd(nodeP, timeNode(kaP, false, createdAt));

  return nodeP;
}



// -----------------------------------------------------------------------------
//
// corDbAttrTimesFill / corDbAttrTimesDrop - an attribute of the store, in place: its instances' times
// put in (each missing one the entity's createdAt - two small nodes, no copy) / the ones equal to the
// entity's createdAt taken out again
//
void corDbAttrTimesFill(CorNode* attrP, int64_t entityCreatedAt)
{
  if ((attrP == NULL) || (attrP->type != CorObject))
    return;

  for (CorNode* instP = attrP->value.head; instP != NULL; instP = instP->next)
  {
    if ((instP->type != CorObject) || ((instP->flags & INHERITS) == 0))
      continue;

    if ((instP->flags & INHERIT_C) != 0) corTreeChildAdd(instP, timeNode(NULL, true,  entityCreatedAt));
    if ((instP->flags & INHERIT_M) != 0) corTreeChildAdd(instP, timeNode(NULL, false, entityCreatedAt));

    instP->flags &= ~INHERITS;
  }
}

void corDbAttrTimesDrop(CorNode* attrP, int64_t entityCreatedAt)
{
  if ((attrP == NULL) || (attrP->type != CorObject) || (entityCreatedAt == 0))
    return;

  for (CorNode* instP = attrP->value.head; instP != NULL; instP = instP->next)
  {
    if (instP->type != CorObject)
      continue;

    CorNode* mP = instP->value.head;

    while (mP != NULL)
    {
      CorNode* nextP = mP->next;

      if ((mP->type == CorInt) && (mP->value.i == entityCreatedAt))
      {
        unsigned char mark = isCreatedAt(mP) ? INHERIT_C : isModifiedAt(mP) ? INHERIT_M : 0;

        if (mark != 0)
        {
          corTreeChildRemove(instP, mP);
          corTreeFree(mP);
          instP->flags |= mark;
        }
      }

      mP = nextP;
    }
  }
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
  if ((srcP->type != CorObject) && (srcP->type != CorArray))
    return corTreeClone(NULL, srcP);

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
  if ((storeP->type != CorObject) && (storeP->type != CorArray))
    return corTreeClone(kaP, storeP);

  if (entityCreatedAt == 0)
  {
    int64_t createdAt = headCreatedAt(storeP);

    if (createdAt == 0)
      return corTreeClone(kaP, storeP);

    return entityOut(kaP, storeP, createdAt);
  }

  return treeOut(kaP, storeP, entityCreatedAt);
}

#else

CorNode* corDbTreeIn(CorNode* srcP, int64_t parentCreatedAt)                  { (void) parentCreatedAt; return corTreeClone(NULL, srcP); }
void     corDbAttrTimesFill(CorNode* attrP, int64_t entityCreatedAt)            { (void) attrP; (void) entityCreatedAt; }
void     corDbAttrTimesDrop(CorNode* attrP, int64_t entityCreatedAt)            { (void) attrP; (void) entityCreatedAt; }
CorNode* corDbTreeOut(CorAlloc* kaP, CorNode* storeP, int64_t parentCreatedAt) { (void) parentCreatedAt; return corTreeClone(kaP, storeP); }

#endif
