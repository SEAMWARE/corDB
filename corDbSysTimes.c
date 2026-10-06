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
#include "corTree/corTreeClone.h"                     // corTreeClone, corTreeCloneMarked
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
// frontCreatedAt - a store entity's createdAt member where the store keeps it: first, or second after the
// "id" the id index puts first (corDbIndex.c, idFirst) - NULL when it is not there; *prevPP its predecessor
//
static inline CorNode* frontCreatedAt(CorNode* nodeP, CorNode** prevPP)
{
  CorNode* hP = nodeP->value.head;

  *prevPP = NULL;

  if (hP == NULL)
    return NULL;

  if (isCreatedAt(hP))
    return hP;

  if ((hP->next != NULL) && isCreatedAt(hP->next))
  {
    *prevPP = hP;
    return hP->next;
  }

  return NULL;
}



// -----------------------------------------------------------------------------
//
// headCreatedAt - a store entity's createdAt: at its front (corDbTreeIn puts it there - a hop or two),
// else looked up (an entity stored whole)
//
static inline int64_t headCreatedAt(CorNode* nodeP)
{
  if ((nodeP == NULL) || (nodeP->type != CorObject))
    return 0;

  CorNode* prevP;
  CorNode* cP = frontCreatedAt(nodeP, &prevP);

  return (cP != NULL) ? (int64_t) cP->value.i : memberTime(nodeP, "createdAt");
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
// corNgsild's ldEntityAttrsSet knows them: an instance it replaces keeps an inherited createdAt inherited,
// and its report's preValue has every time in place.
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
  else if (level == 0)
    nodeP->flags |= INHERIT_C;                        // the entity: its createdAt is FIRST - out, it goes back in its place

  return nodeP;
}



// -----------------------------------------------------------------------------
//
// timesBack - corTreeCloneMarked's callback for a marked object (INHERIT_C / INHERIT_M): its missing times
// put back in the clone - a createdAt right before its modifiedAt, both last when neither is there (where
// corNgsild puts them)
//
static void timesBack(CorAlloc* kaP, CorNode* copyP, CorNode* origP, void* ctx)
{
  int64_t  entityCreatedAt = *((int64_t*) ctx);
  CorNode* headP           = copyP->value.head;
  CorNode* frontPrevP      = NULL;
  CorNode* frontP          = ((origP->flags & INHERIT_C) != 0) ? frontCreatedAt(copyP, &frontPrevP) : NULL;

  if (frontP != NULL)
  {
    //
    // The entity: its createdAt (at the front in the store) right before its modifiedAt - or last, with
    // the modifiedAt it leaves out while it is the createdAt. (A marked instance has no createdAt.)
    //
    if (frontPrevP == NULL)
      copyP->value.head = frontP->next;
    else
      frontPrevP->next = frontP->next;

    if (copyP->value.tail == frontP)
      copyP->value.tail = frontPrevP;

    headP = frontP;                                   // the node to put back

    CorNode* prevP = NULL;
    CorNode* mP    = copyP->value.head;

    while ((mP != NULL) && (isModifiedAt(mP) == false))
    {
      prevP = mP;
      mP    = mP->next;
    }

    if (mP == NULL)
    {
      corTreeChildAdd(copyP, headP);
      corTreeChildAdd(copyP, timeNode(kaP, false, entityCreatedAt));
    }
    else
    {
      headP->next = mP;

      if (prevP == NULL)
        copyP->value.head = headP;
      else
        prevP->next = headP;
    }

    return;
  }

  if ((origP->flags & INHERIT_C) != 0)
  {
    CorNode* tP    = timeNode(kaP, true, entityCreatedAt);
    CorNode* tailP = copyP->value.tail;

    if ((tailP != NULL) && (isModifiedAt(tailP) == true))
    {
      //
      // Right before the modifiedAt, which is last (corNgsild puts it there): the two nodes swap what
      // they hold, and the one holding the modifiedAt goes last - no walk
      //
      CorNode tmp = *tailP;

      tailP->name   = tP->name;   tailP->value  = tP->value;   tailP->termId = tP->termId;   tailP->flags = tP->flags;
      tP->name      = tmp.name;   tP->value     = tmp.value;   tP->termId    = tmp.termId;   tP->flags    = tmp.flags;

      corTreeChildAdd(copyP, tP);
    }
    else
    {
      CorNode* prevP = NULL;
      CorNode* mP    = headP;

      while ((mP != NULL) && (isModifiedAt(mP) == false))
      {
        prevP = mP;
        mP    = mP->next;
      }

      if (mP == NULL)
        corTreeChildAdd(copyP, tP);
      else
      {
        tP->next = mP;

        if (prevP == NULL)
          copyP->value.head = tP;
        else
          prevP->next = tP;
      }
    }
  }

  if ((origP->flags & INHERIT_M) != 0)
    corTreeChildAdd(copyP, timeNode(kaP, false, entityCreatedAt));
}



// -----------------------------------------------------------------------------
//
// treeOut - a store tree out: corTree's own clone (one call - not a call per node across the library),
// timesBack on each marked object
//
static CorNode* treeOut(CorAlloc* kaP, CorNode* storeP, int64_t entityCreatedAt)
{
  return corTreeCloneMarked(kaP, storeP, INHERITS, timesBack, &entityCreatedAt);
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
// timesPut - a copy made with corTreeClone (the marks copied too): every marked object's times put back,
// the marks cleared
//
static void timesPut(CorAlloc* kaP, CorNode* nodeP, int64_t entityCreatedAt)
{
  for (CorNode* mP = nodeP->value.head; mP != NULL; mP = mP->next)
  {
    //
    // Not into a value: nothing in a value is marked. corJsonld marks a value node (its value-kind
    // bits, 0xF0); one without them (a store rebuilt from its files) is walked, and finds nothing
    //
    if (((mP->type == CorObject) || (mP->type == CorArray)) && ((mP->flags & 0xF0) == 0))
      timesPut(kaP, mP, entityCreatedAt);
  }

  if ((nodeP->flags & INHERITS) != 0)
  {
    timesBack(kaP, nodeP, nodeP, &entityCreatedAt);
    nodeP->flags &= ~INHERITS;
  }
}



// -----------------------------------------------------------------------------
//
// corDbEntityCopy / corDbEntityCopyFinish - corDbTreeOut of a store ENTITY in two steps: the copy under the
// store's lock - corTreeClone, as without inherited times - and the times put back after it, on the
// request's own copy
//
CorNode* corDbEntityCopy(CorAlloc* kaP, CorNode* storeP, int64_t* createdAtP)
{
  *createdAtP = (((storeP->flags & INHERIT_C) != 0) && (storeP->type == CorObject)) ? headCreatedAt(storeP) : 0;

  if (*createdAtP == 0)
    return corDbTreeOut(kaP, storeP, 0);              // not in the store's form: whole already - or the old way

  return corTreeClone(kaP, storeP);
}

void corDbEntityCopyFinish(CorAlloc* kaP, CorNode* copyP, int64_t createdAt)
{
  if ((copyP != NULL) && (createdAt != 0))
    timesPut(kaP, copyP, createdAt);
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

    if ((storeP->flags & INHERIT_C) != 0)             // the store's form: one clone, timesBack on what is marked
      return treeOut(kaP, storeP, createdAt);

    return entityOut(kaP, storeP, createdAt);
  }

  return treeOut(kaP, storeP, entityCreatedAt);
}

#else

CorNode* corDbTreeIn(CorNode* srcP, int64_t parentCreatedAt)                  { (void) parentCreatedAt; return corTreeClone(NULL, srcP); }
CorNode* corDbEntityCopy(CorAlloc* kaP, CorNode* storeP, int64_t* createdAtP) { *createdAtP = 0; return corTreeClone(kaP, storeP); }
void     corDbEntityCopyFinish(CorAlloc* kaP, CorNode* copyP, int64_t createdAt) { (void) kaP; (void) copyP; (void) createdAt; }
CorNode* corDbTreeOut(CorAlloc* kaP, CorNode* storeP, int64_t parentCreatedAt) { (void) parentCreatedAt; return corTreeClone(kaP, storeP); }

#endif
