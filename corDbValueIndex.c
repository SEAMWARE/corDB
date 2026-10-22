//
// FILE            corDbValueIndex.c
//
// AUTHOR          Ken Zangelin
//
// Copyright 2026 Seamware
// SPDX-License-Identifier: Apache-2.0
//
// A declaration's index is a skip list of (value, entity) entries: the values of the attribute an instance
// or an array element holds - a boolean, a number, a string, in that order of kinds - ordered by kind, then
// value, then the entity's place in the store. Each entity has its entries chained (VxEntity.entries), so a
// write finds them without searching, and checks them against the entity as it now is: a write that leaves
// the attribute's values as they were changes nothing.
//
// What a value is, is what the q matcher compares (corNgsild ldEntityMatch.c): an instance's "value", or its
// "object" (a Relationship), or the attribute itself when it is not an object - every instance, and every
// scalar of an array. Numbers are kept as the double the matcher compares (an integer converted, as it
// converts it), and a date-time term's nanoseconds go through the same conversion: the conversion keeps
// order, so a range over the doubles holds every value the exact comparison can match - the candidates are
// matched as before.
//
#include <math.h>                                    // INFINITY, sqrt
#include <pthread.h>                                 // pthread_mutex_lock
#include <stdbool.h>                                 // bool
#include <stdint.h>                                  // int64_t, uintptr_t, INT64_MIN
#include <stdlib.h>                                  // malloc, calloc, free, qsort, strtod
#include <string.h>                                  // strcmp, strdup, strlen

#include "corAlloc/corAlloc.h"                       // corAlloc
#include "corTree/CorNode.h"                         // CorNode
#include "corTree/corTreeLookup.h"                   // corTreeLookup
#include "corRest/CorRestState.h"                    // corRest
#include "corNgsild/LdQ.h"                           // LdQNode, LdQTerm
#include "corNgsild/LdVocab.h"                       // LD_VOCAB_CREATED_AT, LD_VOCAB_MODIFIED_AT, LD_VOCAB_EXPIRES_AT
#include "corNgsild/ldCheckDateTime.h"               // ldIsoToNanoseconds

#include "corDB/corDbIndexes.h"                      // corDbIndexesCovering, corDbIndexesMax, CorDbIndexDecl
#include "corDB/corDbValueIndex.h"                   // Own interface



// -----------------------------------------------------------------------------
//
// The kinds of a value, in their order in a skip list
//
#define KIND_BOOL     1
#define KIND_NUMBER   2
#define KIND_STRING   3

#define LEVELS       20                              // a skip list's levels: 4^20 entries before it degrades



// -----------------------------------------------------------------------------
//
// VxEntry - a value of an entity in a declaration's skip list
//
typedef struct VxEntry
{
  unsigned char      kind;
  unsigned char      levels;
  unsigned short     declIx;
  double             n;                              // KIND_NUMBER - KIND_BOOL: 0 or 1
  int64_t            seq;                            // the entity's - here, so a comparison reads one entry, not its entity too
  char*              s;                              // KIND_STRING: a copy - the entity's may be freed before the entry
  struct VxEntity*   entP;
  struct VxEntry*    sibling;                        // the entity's next entry
  struct VxEntry*    fwd[];                          // 'levels' of them
} VxEntry;



// -----------------------------------------------------------------------------
//
// VxEntity - an entity of the store with a value of a declared attribute: its place, its entries
//
// Only those: an entity with none of them - nearly every entity, for a rare attribute - costs the index
// nothing. Their places are numbers in the store's order, SEQ_GAP apart when they are given at the end, the
// middle of the gap when an entity gains a value where it is (seqBefore); a gap used up numbers them all
// again (seqRenumber).
//
#define SEQ_GAP  ((int64_t) 1 << 20)

typedef struct VxEntity
{
  CorNode*          entityP;
  int64_t           seq;                             // its place in the store's list
  struct VxEntity*  mapNext;
  struct VxEntity*  orderPrev;                       // the index's entities in the store's order
  struct VxEntity*  orderNext;
  VxEntry*          entries;                         // by declaration, in the order the values are found
} VxEntity;



// -----------------------------------------------------------------------------
//
// VxDecl - a declaration and its skip list
//
typedef struct VxDecl
{
  CorDbIndexDecl  decl;
  VxEntry*        headP;                             // LEVELS forward pointers, no value
  int             levels;                            // the levels in use
  int64_t         entries;
  int64_t         bytes;
  int64_t         accesses;                          // the queries it served - atomic
  int64_t         updates;                           // the writes that changed it - under the write lock
} VxDecl;



// -----------------------------------------------------------------------------
//
// EntityBlock - entities are allocated in blocks, not one malloc each
//
#define ENTITY_BLOCK  1024

typedef struct EntityBlock
{
  struct EntityBlock*  next;
  VxEntity             entityV[ENTITY_BLOCK];
} EntityBlock;



// -----------------------------------------------------------------------------
//
// CorDbValueIndex - a store's declarations, and its entities by pointer
//
// The map is chained through the entities themselves (mapNext). It grows as the type index's does
// (corDbTypeIndex.c): a table MAP_GROWTH times larger takes over, and the old one's buckets move into it
// MAP_MOVE at a time, at every change - never all at once under the write lock.
//
#define MAP_BUCKETS  1024
#define MAP_LOAD        2
#define MAP_GROWTH      8
#define MAP_MOVE       32

typedef struct CorDbValueIndex
{
  VxDecl*        declV;
  int            declN;                              // 0: no declaration covers the tenant - nothing kept
  VxEntity**     map;
  unsigned int   mapMask;
  int            mapCount;
  VxEntity**     oldMap;                             // a growth in progress
  unsigned int   oldMask;
  unsigned int   oldMoved;
  EntityBlock*   blocks;
  int            blockUsed;
  VxEntity*      freeEntities;                       // chained through mapNext
  VxEntity*      orderHead;
  VxEntity*      orderTail;
  int64_t        storeN;                             // the store's entities - the planner's n
  uint64_t       rand;                               // the skip lists' levels
} CorDbValueIndex;



// -----------------------------------------------------------------------------
//
// VxKey - a value looked for or added
//
typedef struct VxKey
{
  int          kind;
  double       n;
  const char*  s;
} VxKey;



// -----------------------------------------------------------------------------
//
// keyCompare - a key against an entry's value
//
static int keyCompare(const VxKey* kP, const VxEntry* eP)
{
  if (kP->kind != eP->kind)
    return (kP->kind < eP->kind) ? -1 : 1;

  if (kP->kind == KIND_STRING)
    return strcmp(kP->s, eP->s);

  return (kP->n < eP->n) ? -1 : (kP->n > eP->n) ? 1 : 0;
}



// -----------------------------------------------------------------------------
//
// entryCompare - a key and a place against an entry: value, then the place in the store
//
static int entryCompare(const VxKey* kP, int64_t seq, const VxEntry* eP)
{
  int c = keyCompare(kP, eP);

  if (c != 0)
    return c;

  return (seq < eP->seq) ? -1 : (seq > eP->seq) ? 1 : 0;
}



// -----------------------------------------------------------------------------
//
// levelRandom - an entry's levels: one more with a probability of 1/4
//
static int levelRandom(CorDbValueIndex* vxP)
{
  int levels = 1;

  vxP->rand ^= vxP->rand << 13;
  vxP->rand ^= vxP->rand >> 7;
  vxP->rand ^= vxP->rand << 17;

  for (uint64_t r = vxP->rand; ((r & 3) == 0) && (levels < LEVELS); r >>= 2)
    ++levels;

  return levels;
}



// -----------------------------------------------------------------------------
//
// listInsert - an entry of the entity's in a declaration's skip list; NULL: out of memory
//
static VxEntry* listInsert(CorDbValueIndex* vxP, int declIx, const VxKey* kP, VxEntity* entP)
{
  VxDecl*   dP     = &vxP->declV[declIx];
  VxEntry*  updV[LEVELS];
  VxEntry*  xP     = dP->headP;
  int       levels = levelRandom(vxP);

  for (int l = LEVELS - 1; l >= 0; l--)
  {
    while ((xP->fwd[l] != NULL) && (entryCompare(kP, entP->seq, xP->fwd[l]) > 0))
      xP = xP->fwd[l];
    updV[l] = xP;
  }

  size_t   size = sizeof(VxEntry) + levels * sizeof(VxEntry*);
  VxEntry* eP   = (VxEntry*) malloc(size);

  if (eP == NULL)
    return NULL;

  eP->kind    = kP->kind;
  eP->levels  = levels;
  eP->declIx  = declIx;
  eP->n       = kP->n;
  eP->seq     = entP->seq;
  eP->s       = NULL;
  eP->entP    = entP;
  eP->sibling = NULL;

  if (kP->kind == KIND_STRING)
  {
    eP->s = strdup(kP->s);

    if (eP->s == NULL)
    {
      free(eP);
      return NULL;
    }

    size += strlen(eP->s) + 1;
  }

  for (int l = 0; l < levels; l++)
  {
    eP->fwd[l]      = updV[l]->fwd[l];
    updV[l]->fwd[l] = eP;
  }

  if (levels > dP->levels)
    dP->levels = levels;

  dP->entries += 1;
  dP->bytes   += size;

  return eP;
}



// -----------------------------------------------------------------------------
//
// listRemove - an entry out of its skip list, freed
//
static void listRemove(CorDbValueIndex* vxP, VxEntry* eP)
{
  VxDecl*  dP = &vxP->declV[eP->declIx];
  VxKey    k  = { eP->kind, eP->n, eP->s };
  VxEntry* xP = dP->headP;

  for (int l = dP->levels - 1; l >= 0; l--)
  {
    while ((xP->fwd[l] != NULL) && (xP->fwd[l] != eP) && (entryCompare(&k, eP->seq, xP->fwd[l]) > 0))
      xP = xP->fwd[l];

    if ((l < eP->levels) && (xP->fwd[l] == eP))
      xP->fwd[l] = eP->fwd[l];
  }

  dP->entries -= 1;
  dP->bytes   -= sizeof(VxEntry) + eP->levels * sizeof(VxEntry*) + ((eP->s != NULL) ? strlen(eP->s) + 1 : 0);

  free(eP->s);
  free(eP);
}



// -----------------------------------------------------------------------------
//
// The entity map - an entity -> its VxEntity
//
static inline unsigned int ptrHash(CorNode* entityP)
{
  uintptr_t p = (uintptr_t) entityP;

  p ^= p >> 17;
  p *= 0xed5ad4bbu;
  p ^= p >> 11;

  return (unsigned int) p;
}

//
// mapBucket - the bucket an entity is in: the old table's while its bucket there has not moved yet
//
static VxEntity** mapBucket(CorDbValueIndex* vxP, CorNode* entityP)
{
  unsigned int h = ptrHash(entityP);

  if (vxP->oldMap != NULL)
  {
    unsigned int oldIx = h & vxP->oldMask;

    if (oldIx >= vxP->oldMoved)
      return &vxP->oldMap[oldIx];
  }

  return &vxP->map[h & vxP->mapMask];
}

//
// mapMigrate - up to 'buckets' of the old table moved into the current one; the old freed when empty
//
static void mapMigrate(CorDbValueIndex* vxP, unsigned int buckets)
{
  if (vxP->oldMap == NULL)
    return;

  for (unsigned int n = 0; (n < buckets) && (vxP->oldMoved <= vxP->oldMask); n++, vxP->oldMoved++)
  {
    VxEntity* entP = vxP->oldMap[vxP->oldMoved];

    while (entP != NULL)
    {
      VxEntity*  nextP   = entP->mapNext;
      VxEntity** bucketP = &vxP->map[ptrHash(entP->entityP) & vxP->mapMask];

      entP->mapNext = *bucketP;
      *bucketP      = entP;
      entP          = nextP;
    }

    vxP->oldMap[vxP->oldMoved] = NULL;
  }

  if (vxP->oldMoved > vxP->oldMask)
  {
    free(vxP->oldMap);
    vxP->oldMap   = NULL;
    vxP->oldMask  = 0;
    vxP->oldMoved = 0;
  }
}

//
// mapGrow - a larger table takes over; the old one moves into it bit by bit (mapMigrate)
//
static void mapGrow(CorDbValueIndex* vxP)
{
  if (vxP->oldMap != NULL)
    return;

  unsigned int  buckets = (vxP->mapMask + 1) * MAP_GROWTH;
  VxEntity**    newP    = (VxEntity**) calloc(buckets, sizeof(VxEntity*));

  if (newP == NULL)
    return;                                          // longer chains; slower, not wrong

  vxP->oldMap   = vxP->map;
  vxP->oldMask  = vxP->mapMask;
  vxP->oldMoved = 0;
  vxP->map      = newP;
  vxP->mapMask  = buckets - 1;
}

static VxEntity* mapFind(CorDbValueIndex* vxP, CorNode* entityP)
{
  for (VxEntity* entP = *mapBucket(vxP, entityP); entP != NULL; entP = entP->mapNext)
  {
    if (entP->entityP == entityP)
      return entP;
  }

  return NULL;
}

static void mapAdd(CorDbValueIndex* vxP, VxEntity* entP)
{
  mapMigrate(vxP, MAP_MOVE);

  VxEntity** bucketP = mapBucket(vxP, entP->entityP);

  entP->mapNext  = *bucketP;
  *bucketP       = entP;
  vxP->mapCount += 1;

  if (vxP->mapCount > (int) (vxP->mapMask + 1) * MAP_LOAD)
    mapGrow(vxP);
}

static VxEntity* mapRemove(CorDbValueIndex* vxP, CorNode* entityP)
{
  mapMigrate(vxP, MAP_MOVE);

  for (VxEntity** pP = mapBucket(vxP, entityP); *pP != NULL; pP = &(*pP)->mapNext)
  {
    VxEntity* entP = *pP;

    if (entP->entityP == entityP)
    {
      *pP            = entP->mapNext;
      entP->mapNext  = NULL;
      vxP->mapCount -= 1;
      return entP;
    }
  }

  return NULL;
}



// -----------------------------------------------------------------------------
//
// entityAlloc / entityFree
//
static VxEntity* entityAlloc(CorDbValueIndex* vxP)
{
  VxEntity* entP = vxP->freeEntities;

  if (entP != NULL)
    vxP->freeEntities = entP->mapNext;
  else
  {
    if ((vxP->blocks == NULL) || (vxP->blockUsed == ENTITY_BLOCK))
    {
      EntityBlock* blockP = (EntityBlock*) malloc(sizeof(EntityBlock));

      if (blockP == NULL)
        return NULL;

      blockP->next   = vxP->blocks;
      vxP->blocks    = blockP;
      vxP->blockUsed = 0;
    }

    entP = &vxP->blocks->entityV[vxP->blockUsed++];
  }

  memset(entP, 0, sizeof(VxEntity));
  return entP;
}

static void entityFree(CorDbValueIndex* vxP, VxEntity* entP)
{
  entP->entityP     = NULL;
  entP->mapNext     = vxP->freeEntities;
  vxP->freeEntities = entP;
}



// -----------------------------------------------------------------------------
//
// typeOf - is the entity of the type? "type" is a string or an array of strings
//
static bool typeOf(CorNode* entityP, const char* type)
{
  CorNode* typeP = corTreeLookup(entityP, "type");

  if (typeP == NULL)
    return false;

  if (typeP->type == CorString)
    return strcmp(typeP->value.s, type) == 0;

  if (typeP->type == CorArray)
  {
    for (CorNode* tP = typeP->value.head; tP != NULL; tP = tP->next)
    {
      if ((tP->type == CorString) && (strcmp(tP->value.s, type) == 0))
        return true;
    }
  }

  return false;
}



// -----------------------------------------------------------------------------
//
// KeyWalk - what keysEach does with each value of an entity: compare it with the entity's entries (same), or
// add it (an entry)
//
typedef struct KeyWalk
{
  CorDbValueIndex*  vxP;
  VxEntity*         entP;
  int               declIx;
  bool              add;
  VxEntry*          declFirstP;                      // the declaration's first entry of the entity
  VxEntry*          cursorP;                         // same: the entry the next value is compared with
  VxEntry*          tailP;                           // add: the entity's last entry
  bool              stop;                            // same: a difference - add: out of memory
} KeyWalk;

//
// keySeen - a value the declaration has for the entity already (the same value in two instances, twice in
// an array): from declFirstP up to, not including, toP
//
static bool keySeen(const VxKey* kP, VxEntry* fromP, VxEntry* toP)
{
  for (VxEntry* eP = fromP; (eP != NULL) && (eP != toP); eP = eP->sibling)
  {
    if (keyCompare(kP, eP) == 0)
      return true;
  }

  return false;
}

static void keyTake(KeyWalk* wP, const VxKey* kP)
{
  if (wP->stop == true)
    return;

  if (wP->add == false)
  {
    if (keySeen(kP, wP->declFirstP, wP->cursorP) == true)
      return;

    if ((wP->cursorP == NULL) || (wP->cursorP->declIx != wP->declIx) || (keyCompare(kP, wP->cursorP) != 0))
    {
      wP->stop = true;
      return;
    }

    if (wP->declFirstP == NULL)
      wP->declFirstP = wP->cursorP;
    wP->cursorP = wP->cursorP->sibling;
    return;
  }

  if (keySeen(kP, wP->declFirstP, NULL) == true)
    return;

  VxEntry* eP = listInsert(wP->vxP, wP->declIx, kP, wP->entP);

  if (eP == NULL)
  {
    wP->stop = true;
    return;
  }

  if (wP->tailP == NULL)
    wP->entP->entries = eP;
  else
    wP->tailP->sibling = eP;
  wP->tailP = eP;

  if (wP->declFirstP == NULL)
    wP->declFirstP = eP;
}

//
// valueKeys - a value: a scalar, or the scalars of an array
//
static void valueKeys(KeyWalk* wP, CorNode* vP, bool inArray)
{
  VxKey k = { 0, 0, NULL };

  switch (vP->type)
  {
  case CorInt:      k.kind = KIND_NUMBER; k.n = (double) vP->value.i;          break;
  case CorFloat:    k.kind = KIND_NUMBER; k.n = vP->value.f;                   break;
  case CorString:   k.kind = KIND_STRING; k.s = vP->value.s;                   break;
  case CorBoolean:  k.kind = KIND_BOOL;   k.n = (vP->value.b == true) ? 1 : 0; break;
  case CorArray:
    if (inArray == false)
    {
      for (CorNode* elemP = vP->value.head; elemP != NULL; elemP = elemP->next)
        valueKeys(wP, elemP, true);
    }
    return;
  default:
    return;
  }

  keyTake(wP, &k);
}

//
// instanceValue - an instance's value: "value", or "object" (a Relationship)
//
static CorNode* instanceValue(CorNode* instP)
{
  CorNode* vP = corTreeLookup(instP, "value");

  return (vP != NULL) ? vP : corTreeLookup(instP, "object");
}

//
// keysEach - the values of the declaration's attribute in the entity, as the q matcher finds them: the
// attribute itself when it is not an object, its value (the API's shape), or every instance's
//
static void keysEach(KeyWalk* wP, CorNode* entityP)
{
  VxDecl* dP = &wP->vxP->declV[wP->declIx];

  wP->declFirstP = NULL;

  if ((dP->decl.type != NULL) && (typeOf(entityP, dP->decl.type) == false))
    return;

  CorNode* wrapperP = corTreeLookup(entityP, dP->decl.attr);

  if (wrapperP == NULL)
    return;

  if (wrapperP->type != CorObject)
  {
    valueKeys(wP, wrapperP, false);
    return;
  }

  CorNode* vP = instanceValue(wrapperP);

  if (vP != NULL)
  {
    valueKeys(wP, vP, false);
    return;
  }

  for (CorNode* instP = wrapperP->value.head; instP != NULL; instP = instP->next)
  {
    if ((instP->type == CorObject) && ((vP = instanceValue(instP)) != NULL))
      valueKeys(wP, vP, false);
  }
}



// -----------------------------------------------------------------------------
//
// entriesAdd - the entity's entries, from its values; false: out of memory
//
static bool entriesAdd(CorDbValueIndex* vxP, VxEntity* entP)
{
  KeyWalk w = { vxP, entP, 0, true, NULL, NULL, NULL, false };

  entP->entries = NULL;

  for (w.declIx = 0; (w.declIx < vxP->declN) && (w.stop == false); w.declIx++)
    keysEach(&w, entP->entityP);

  return w.stop == false;
}



// -----------------------------------------------------------------------------
//
// entriesRemove - the entity's entries out of their skip lists
//
static void entriesRemove(CorDbValueIndex* vxP, VxEntity* entP)
{
  VxEntry* eP = entP->entries;

  while (eP != NULL)
  {
    VxEntry* nextP = eP->sibling;

    listRemove(vxP, eP);
    eP = nextP;
  }

  entP->entries = NULL;
}



// -----------------------------------------------------------------------------
//
// keysAny - has the entity a value of a declared attribute - would it have entries? A walk that compares
// with no entries stops at the first value.
//
static bool keysAny(CorDbValueIndex* vxP, CorNode* entityP)
{
  KeyWalk w = { vxP, NULL, 0, false, NULL, NULL, NULL, false };

  for (w.declIx = 0; (w.declIx < vxP->declN) && (w.stop == false); w.declIx++)
    keysEach(&w, entityP);

  return w.stop;
}



// -----------------------------------------------------------------------------
//
// seqRenumber - the index's entities numbered again, SEQ_GAP apart, in their order; their entries' copies too
// (the order of a skip list does not change: every number keeps its place among the others)
//
static void seqRenumber(CorDbValueIndex* vxP)
{
  int64_t seq = 0;

  for (VxEntity* entP = vxP->orderHead; entP != NULL; entP = entP->orderNext)
  {
    seq      += SEQ_GAP;
    entP->seq = seq;

    for (VxEntry* eP = entP->entries; eP != NULL; eP = eP->sibling)
      eP->seq = seq;
  }
}



// -----------------------------------------------------------------------------
//
// orderInsert - the entity into the order, before nextP (NULL: at the end), and its place
//
static void orderInsert(CorDbValueIndex* vxP, VxEntity* entP, VxEntity* nextP)
{
  VxEntity* prevP = (nextP != NULL) ? nextP->orderPrev : vxP->orderTail;

  if (nextP == NULL)
    entP->seq = ((prevP != NULL) ? prevP->seq : 0) + SEQ_GAP;
  else
  {
    if (nextP->seq - ((prevP != NULL) ? prevP->seq : 0) < 2)
      seqRenumber(vxP);

    int64_t lo = (prevP != NULL) ? prevP->seq : 0;

    entP->seq = lo + (nextP->seq - lo) / 2;
  }

  entP->orderPrev = prevP;
  entP->orderNext = nextP;

  if (prevP != NULL)
    prevP->orderNext = entP;
  else
    vxP->orderHead = entP;

  if (nextP != NULL)
    nextP->orderPrev = entP;
  else
    vxP->orderTail = entP;
}

static void orderRemove(CorDbValueIndex* vxP, VxEntity* entP)
{
  if (entP->orderPrev != NULL)
    entP->orderPrev->orderNext = entP->orderNext;
  else
    vxP->orderHead = entP->orderNext;

  if (entP->orderNext != NULL)
    entP->orderNext->orderPrev = entP->orderPrev;
  else
    vxP->orderTail = entP->orderPrev;
}



// -----------------------------------------------------------------------------
//
// updatesCount - one write more for every declaration the entity has entries of (grouped by declaration)
//
static void updatesCount(CorDbValueIndex* vxP, VxEntity* entP)
{
  int declIx = -1;

  for (VxEntry* eP = entP->entries; eP != NULL; eP = eP->sibling)
  {
    if (eP->declIx != declIx)
    {
      declIx = eP->declIx;
      vxP->declV[declIx].updates += 1;
    }
  }
}



// -----------------------------------------------------------------------------
//
// entriesRefresh - the entity's entries, as its values now are: a declaration whose values are as they were
// keeps its entries - nearly every write - and the others' are taken out and added again; false: out of memory
//
// The entries are grouped by declaration, in the declarations' order: a declaration's are a segment of the
// entity's list.
//
static bool entriesRefresh(CorDbValueIndex* vxP, VxEntity* entP)
{
  VxEntry* oldP  = entP->entries;
  VxEntry* tailP = NULL;

  entP->entries = NULL;

  for (int declIx = 0; declIx < vxP->declN; declIx++)
  {
    VxEntry* segP = ((oldP != NULL) && (oldP->declIx == declIx)) ? oldP : NULL;
    VxEntry* lastP = NULL;

    for (VxEntry* eP = segP; (eP != NULL) && (eP->declIx == declIx); eP = eP->sibling)
      lastP = eP;

    VxEntry* restP = (lastP != NULL) ? lastP->sibling : oldP;
    KeyWalk  same  = { vxP, entP, declIx, false, NULL, segP, NULL, false };

    keysEach(&same, entP->entityP);

    if ((same.stop == false) && ((same.cursorP == NULL) || (same.cursorP->declIx != declIx)))
    {
      if (segP != NULL)                              // as it was: the segment stays
      {
        if (tailP == NULL)
          entP->entries = segP;
        else
          tailP->sibling = segP;
        tailP          = lastP;
        lastP->sibling = NULL;
      }
    }
    else
    {
      for (VxEntry* eP = segP; (eP != NULL) && (eP != restP); )
      {
        VxEntry* nextP = eP->sibling;

        listRemove(vxP, eP);
        eP = nextP;
      }

      KeyWalk add = { vxP, entP, declIx, true, NULL, NULL, tailP, false };

      keysEach(&add, entP->entityP);
      tailP = add.tailP;
      vxP->declV[declIx].updates += 1;

      if (add.stop == true)                          // out of memory: the rest's entries out too
      {
        if (tailP != NULL)
          tailP->sibling = restP;
        else
          entP->entries = restP;
        return false;
      }
    }

    oldP = restP;
  }

  return true;
}



// -----------------------------------------------------------------------------
//
// indexFree - every skip list, the entities, the declarations
//
static void indexFree(CorDbValueIndex* vxP)
{
  if (vxP == NULL)
    return;

  for (int ix = 0; ix < vxP->declN; ix++)
  {
    VxDecl* dP = &vxP->declV[ix];

    for (VxEntry* eP = (dP->headP != NULL) ? dP->headP->fwd[0] : NULL; eP != NULL;)
    {
      VxEntry* nextP = eP->fwd[0];

      free(eP->s);
      free(eP);
      eP = nextP;
    }

    free(dP->headP);
    free(dP->decl.attr);
    free(dP->decl.type);
  }

  while (vxP->blocks != NULL)
  {
    EntityBlock* nextP = vxP->blocks->next;

    free(vxP->blocks);
    vxP->blocks = nextP;
  }

  free(vxP->declV);
  free(vxP->map);
  free(vxP->oldMap);
  free(vxP);
}



// -----------------------------------------------------------------------------
//
// indexBuild - the index of the store's list as it is, for the declarations that cover the tenant
//
static CorDbValueIndex* indexBuild(CorDbStore* storeP, Tenant* tenantP)
{
  CorDbValueIndex* vxP = (CorDbValueIndex*) calloc(1, sizeof(CorDbValueIndex));

  if (vxP == NULL)
    return NULL;

  int              declMax = corDbIndexesMax();
  CorDbIndexDecl*  declV   = (CorDbIndexDecl*) calloc(declMax, sizeof(CorDbIndexDecl));

  vxP->rand  = 0x9e3779b97f4a7c15ull;
  vxP->declV = (VxDecl*) calloc(declMax, sizeof(VxDecl));

  int declN = (declV != NULL) ? corDbIndexesCovering(tenantP, declV, declMax) : -1;

  if ((vxP->declV == NULL) || (declN < 0))
  {
    free(declV);
    indexFree(vxP);
    return NULL;
  }

  for (int ix = 0; ix < declN; ix++)
  {
    vxP->declV[ix].decl  = declV[ix];
    vxP->declV[ix].headP = (VxEntry*) calloc(1, sizeof(VxEntry) + LEVELS * sizeof(VxEntry*));
    vxP->declN           = ix + 1;

    if (vxP->declV[ix].headP == NULL)
    {
      for (int jx = ix + 1; jx < declN; jx++)
      {
        free(declV[jx].attr);
        free(declV[jx].type);
      }
      free(declV);
      indexFree(vxP);
      return NULL;
    }
  }

  free(declV);

  if (vxP->declN == 0)                               // nothing to keep: the writes do nothing
    return vxP;

  unsigned int buckets = MAP_BUCKETS;
  int          n       = 0;

  for (CorNode* eP = storeP->entities->value.head; eP != NULL; eP = eP->next)
    ++n;

  while (n > (int) buckets * MAP_LOAD)
    buckets *= MAP_GROWTH;

  vxP->map     = (VxEntity**) calloc(buckets, sizeof(VxEntity*));
  vxP->mapMask = buckets - 1;

  if (vxP->map == NULL)
  {
    indexFree(vxP);
    return NULL;
  }

  vxP->storeN = n;

  for (CorNode* eP = storeP->entities->value.head; eP != NULL; eP = eP->next)
  {
    if (keysAny(vxP, eP) == false)
      continue;

    VxEntity* entP = entityAlloc(vxP);

    if (entP == NULL)
    {
      indexFree(vxP);
      return NULL;
    }

    entP->entityP = eP;
    orderInsert(vxP, entP, NULL);
    mapAdd(vxP, entP);

    if (entriesAdd(vxP, entP) == false)
    {
      indexFree(vxP);
      return NULL;
    }
  }

  return vxP;
}



// -----------------------------------------------------------------------------
//
// corDbValueIndexOf -
//
CorDbValueIndex* corDbValueIndexOf(CorDbStore* storeP, Tenant* tenantP)
{
  CorDbValueIndex* vxP = __atomic_load_n(&storeP->valueIndexP, __ATOMIC_ACQUIRE);

  if (vxP != NULL)
    return (vxP->declN > 0) ? vxP : NULL;

  if (corDbIndexesCovering(tenantP, NULL, 0) == 0)   // nothing declared: no lock, nothing built
    return NULL;

  pthread_mutex_lock(&storeP->valueIndexMutex);

  vxP = __atomic_load_n(&storeP->valueIndexP, __ATOMIC_ACQUIRE);

  if (vxP == NULL)
  {
    vxP = indexBuild(storeP, tenantP);
    __atomic_store_n(&storeP->valueIndexP, vxP, __ATOMIC_RELEASE);
  }

  pthread_mutex_unlock(&storeP->valueIndexMutex);

  return ((vxP != NULL) && (vxP->declN > 0)) ? vxP : NULL;
}



// -----------------------------------------------------------------------------
//
// CandSet - entities a node of the q can match (VxEntity*, any order, maybe twice) - in the request's memory
//
typedef struct CandSet
{
  VxEntity**  v;
  int         n;
  int         size;
  int         max;                                   // more than that: the index does not serve (false)
} CandSet;

static bool candAdd(CandSet* setP, VxEntity* entP)
{
  if (setP->n >= setP->max)
    return false;

  if (setP->n == setP->size)
  {
    int         size = (setP->size == 0) ? 64 : setP->size * 2;
    VxEntity**  v    = (VxEntity**) corAlloc(corRest.kallocP, size * sizeof(VxEntity*));

    if (v == NULL)
      return false;

    if (setP->n > 0)
      memcpy(v, setP->v, setP->n * sizeof(VxEntity*));

    setP->v    = v;
    setP->size = size;
  }

  setP->v[setP->n++] = entP;
  return true;
}



// -----------------------------------------------------------------------------
//
// rangeTake - the entities of a declaration's values of one kind from lo to hi (strings: hi NULL - to the
// last string)
//
static bool rangeTake(VxDecl* dP, int kind, double nLo, double nHi, const char* sLo, const char* sHi, CandSet* setP)
{
  VxKey     lo = { kind, nLo, sLo };
  VxEntry*  xP = dP->headP;

  for (int l = dP->levels - 1; l >= 0; l--)
  {
    while ((xP->fwd[l] != NULL) && (entryCompare(&lo, INT64_MIN, xP->fwd[l]) > 0))
      xP = xP->fwd[l];
  }

  for (VxEntry* eP = xP->fwd[0]; (eP != NULL) && (eP->kind == kind); eP = eP->fwd[0])
  {
    if ((kind == KIND_STRING) && (sHi != NULL) && (strcmp(eP->s, sHi) > 0))
      break;
    if ((kind != KIND_STRING) && (eP->n > nHi))
      break;

    if (candAdd(setP, eP->entP) == false)
      return false;
  }

  return true;
}



// -----------------------------------------------------------------------------
//
// sysAttr - an attribute the q matcher reads from the entity itself, not from an attribute
//
static bool sysAttr(const char* attr)
{
  return (strcmp(attr, "createdAt") == 0) || (strcmp(attr, "modifiedAt") == 0) ||
         (strcmp(attr, LD_VOCAB_CREATED_AT) == 0) || (strcmp(attr, LD_VOCAB_MODIFIED_AT) == 0) || (strcmp(attr, LD_VOCAB_EXPIRES_AT) == 0);
}



// -----------------------------------------------------------------------------
//
// declFor - the declaration that serves a term on the attribute: one narrowed to a type every match has
// (typeV, the query's), else one of every type. NULL: none.
//
static VxDecl* declFor(CorDbValueIndex* vxP, const char* attr, char** typeV)
{
  VxDecl* anyP = NULL;

  for (int ix = 0; ix < vxP->declN; ix++)
  {
    VxDecl* dP = &vxP->declV[ix];

    if (strcmp(dP->decl.attr, attr) != 0)
      continue;

    if (dP->decl.type == NULL)
    {
      anyP = dP;
      continue;
    }

    for (int tx = 0; (typeV != NULL) && (typeV[tx] != NULL); tx++)
    {
      if (strcmp(typeV[tx], dP->decl.type) == 0)
        return dP;
    }
  }

  return anyP;
}



// -----------------------------------------------------------------------------
//
// termSet - the entities whose values a term can match; false: no declaration serves it
//
// The operators that need a value: ==, a range, a list, <, <=, >, >=. Not !=, !~= (an entity matches by a
// value it does not have), not ~= (a regular expression has no range), not existence (an attribute whose
// value is an object has no entry), not a path into the attribute (sub-attribute, [member]).
//
static bool termSet(CorDbValueIndex* vxP, char** typeV, LdQTerm* tP, CandSet* setP)
{
  if ((tP->subPathN > 0) || (tP->valuePathN > 0) || sysAttr(tP->attr))
    return false;

  VxDecl* dP = declFor(vxP, tP->attr, typeV);

  if (dP == NULL)
    return false;

  double  n  = 0;
  bool    ok = true;

  if (tP->valueType == LdQNumber)
    n = tP->value.n;
  else if (tP->valueType == LdQDateTime)
    n = (double) tP->value.ns;

  switch (tP->op)
  {
  case LdQEqual:
    switch (tP->valueType)
    {
    case LdQNumber:
    case LdQDateTime:  ok = rangeTake(dP, KIND_NUMBER, n, n, NULL, NULL, setP);                                         break;
    case LdQString:    ok = rangeTake(dP, KIND_STRING, 0, 0, tP->value.s, tP->value.s, setP);                           break;
    case LdQBool:      ok = rangeTake(dP, KIND_BOOL, tP->value.b ? 1 : 0, tP->value.b ? 1 : 0, NULL, NULL, setP);      break;
    case LdQRange:     ok = rangeTake(dP, KIND_NUMBER, tP->value.numRange.lo, tP->value.numRange.hi, NULL, NULL, setP); break;
    case LdQValueList:
      for (int ix = 0; (ix < tP->value.list.count) && (ok == true); ix++)
      {
        const char* item = tP->value.list.values[ix];

        switch (tP->value.list.itemTypeV[ix])
        {
        case LdQNumber:   n  = strtod(item, NULL);                    ok = rangeTake(dP, KIND_NUMBER, n, n, NULL, NULL, setP); break;
        case LdQDateTime: n  = (double) ldIsoToNanoseconds(item);     ok = rangeTake(dP, KIND_NUMBER, n, n, NULL, NULL, setP); break;
        case LdQString:   ok = rangeTake(dP, KIND_STRING, 0, 0, item, item, setP);                                             break;
        case LdQBool:     n  = (strcmp(item, "true") == 0) ? 1 : 0;   ok = rangeTake(dP, KIND_BOOL, n, n, NULL, NULL, setP);   break;
        default:          return false;
        }
      }
      break;
    default:
      return false;
    }
    break;

  case LdQGreater:
  case LdQGreaterEq:
    if ((tP->valueType == LdQNumber) || (tP->valueType == LdQDateTime))
      ok = rangeTake(dP, KIND_NUMBER, n, INFINITY, NULL, NULL, setP);
    else if (tP->valueType == LdQString)
      ok = rangeTake(dP, KIND_STRING, 0, 0, tP->value.s, NULL, setP);
    else
      return false;
    break;

  case LdQLess:
  case LdQLessEq:
    if ((tP->valueType == LdQNumber) || (tP->valueType == LdQDateTime))
      ok = rangeTake(dP, KIND_NUMBER, -INFINITY, n, NULL, NULL, setP);
    else if (tP->valueType == LdQString)
      ok = rangeTake(dP, KIND_STRING, 0, 0, "", tP->value.s, setP);
    else
      return false;
    break;

  default:
    return false;
  }

  if (ok == true)
    __atomic_add_fetch(&dP->accesses, 1, __ATOMIC_RELAXED);

  return ok;
}



// -----------------------------------------------------------------------------
//
// nodeSet - the entities a node of the q can match; false: no declaration serves it
//
// An AND: its member with the fewest - every match is among them. An OR: all its members' - every one of them
// served.
//
static bool nodeSet(CorDbValueIndex* vxP, char** typeV, LdQNode* nodeP, CandSet* setP)
{
  if (nodeP == NULL)
    return false;

  if (nodeP->type == LdQTermNode)
    return termSet(vxP, typeV, &nodeP->term, setP);

  if (nodeP->type == LdQAndNode)
  {
    CandSet best  = { NULL, 0, 0, setP->max };
    bool    found = false;

    for (int ix = 0; ix < nodeP->group.count; ix++)
    {
      CandSet s = { NULL, 0, 0, setP->max };

      if (nodeSet(vxP, typeV, nodeP->group.childV[ix], &s) == false)
        continue;

      if ((found == false) || (s.n < best.n))
        best = s;
      found = true;
    }

    for (int ix = 0; ix < best.n; ix++)              // added to what the caller has: an OR's other members
    {
      if (candAdd(setP, best.v[ix]) == false)
        return false;
    }

    return found;
  }

  if (nodeP->type == LdQOrNode)
  {
    for (int ix = 0; ix < nodeP->group.count; ix++)
    {
      if (nodeSet(vxP, typeV, nodeP->group.childV[ix], setP) == false)
        return false;
    }

    return true;
  }

  return false;
}



// -----------------------------------------------------------------------------
//
// seqCompare - VxEntity* by place, for qsort
//
static int seqCompare(const void* a, const void* b)
{
  int64_t sa = (*(VxEntity* const*) a)->seq;
  int64_t sb = (*(VxEntity* const*) b)->seq;

  return (sa < sb) ? -1 : (sa > sb) ? 1 : 0;
}



// -----------------------------------------------------------------------------
//
// corDbValueIndexCandidates -
//
int corDbValueIndexCandidates(CorDbValueIndex* vxP, DbQueryFilter* filterP, int needed, CorDbValueCand** candVP)
{
  if ((vxP == NULL) || (filterP == NULL) || (filterP->qExpr == NULL))
    return -1;

  //
  // The types every match has: the one type of a query by one type, an AND group's - a declaration narrowed
  // to one of them serves the query
  //
  char** typeV = NULL;

  if (filterP->typeExpr != NULL)
    typeV = (filterP->typeExpr->groupCount == 1) ? filterP->typeExpr->groupV[0].typeV : NULL;
  else if ((filterP->typeV != NULL) && (filterP->typeV[0] != NULL) && (filterP->typeV[1] == NULL))
    typeV = filterP->typeV;

  //
  // A page of 'needed' matches: the walk stops at the page, the index gathers and sorts every candidate. Of n
  // entities, c candidates - the walk reads about needed * n / c entities to its page, the index c: the index
  // when c is at most sqrt(needed * n). Measured (coraine doc/performance.md, "corDB's declared indexes"): a
  // page of 20 of a q that half the store matches is 3 times slower from the index. Every match wanted (a
  // count, an unpaged query): the index - it matches the candidates only.
  //
  CandSet set = { NULL, 0, 0, INT32_MAX };

  if (needed > 0)
  {
    double max = sqrt((double) needed * (double) vxP->storeN);

    set.max = (max < 64) ? 64 : (max > INT32_MAX) ? INT32_MAX : (int) max;
  }

  if (nodeSet(vxP, typeV, filterP->qExpr, &set) == false)
    return -1;

  if (set.n > 1)
    qsort(set.v, set.n, sizeof(VxEntity*), seqCompare);

  CorDbValueCand* candV = (CorDbValueCand*) corAlloc(corRest.kallocP, (set.n + 1) * sizeof(CorDbValueCand));
  int             n     = 0;

  if (candV == NULL)
    return -1;

  for (int ix = 0; ix < set.n; ix++)
  {
    if ((n > 0) && (candV[n - 1].seq == set.v[ix]->seq))
      continue;

    candV[n].entityP = set.v[ix]->entityP;
    candV[n].seq     = set.v[ix]->seq;
    ++n;
  }

  *candVP = candV;
  return n;
}



// -----------------------------------------------------------------------------
//
// corDbValueIndexSeq -
//
int64_t corDbValueIndexSeq(CorDbValueIndex* vxP, CorNode* entityP)
{
  VxEntity* entP = ((vxP != NULL) && (vxP->declN > 0)) ? mapFind(vxP, entityP) : NULL;

  return (entP != NULL) ? entP->seq : -1;
}



// -----------------------------------------------------------------------------
//
// indexOff - out of memory in the middle of a change: the index no longer says the truth, and goes. The next
// query that can use one builds it again.
//
static void indexOff(CorDbStore* storeP)
{
  corDbValueIndexDrop(storeP);
}



// -----------------------------------------------------------------------------
//
// entityIn - an entity with a value of a declared attribute, not in the index yet, in it: its place before the
// next of the index's entities after it in the store (fromP: the store's entity after it). false: out of memory
//
// The one write that walks the store: from the entity to the next one with a declared value. An entity
// created with one is the store's last - nothing to walk.
//
static bool entityIn(CorDbValueIndex* vxP, CorNode* entityP, CorNode* fromP)
{
  VxEntity* nextP = NULL;

  for (CorNode* eP = fromP; (eP != NULL) && (nextP == NULL); eP = eP->next)
    nextP = mapFind(vxP, eP);

  VxEntity* entP = entityAlloc(vxP);

  if (entP == NULL)
    return false;

  entP->entityP = entityP;
  orderInsert(vxP, entP, nextP);
  mapAdd(vxP, entP);

  if (entriesAdd(vxP, entP) == false)
    return false;

  updatesCount(vxP, entP);
  return true;
}

//
// entityOut - the entity's entries, its place, out of the index
//
static void entityOut(CorDbValueIndex* vxP, VxEntity* entP)
{
  updatesCount(vxP, entP);
  entriesRemove(vxP, entP);
  orderRemove(vxP, entP);
  entityFree(vxP, entP);
}



// -----------------------------------------------------------------------------
//
// corDbValueIndexLink - an entity without a value of a declared attribute: nothing but the count
//
void corDbValueIndexLink(CorDbStore* storeP, CorNode* entityP)
{
  CorDbValueIndex* vxP = storeP->valueIndexP;

  if ((vxP == NULL) || (vxP->declN == 0))
    return;

  vxP->storeN += 1;

  if (keysAny(vxP, entityP) == false)
    return;

  if (entityIn(vxP, entityP, NULL) == false)
    indexOff(storeP);
}



// -----------------------------------------------------------------------------
//
// corDbValueIndexUnlink -
//
void corDbValueIndexUnlink(CorDbStore* storeP, CorNode* entityP)
{
  CorDbValueIndex* vxP = storeP->valueIndexP;

  if ((vxP == NULL) || (vxP->declN == 0))
    return;

  vxP->storeN -= 1;

  if (keysAny(vxP, entityP) == false)                // not in the index
    return;

  VxEntity* entP = mapRemove(vxP, entityP);

  if (entP == NULL)
  {
    indexOff(storeP);                                // in it, if it said the truth
    return;
  }

  entityOut(vxP, entP);
}



// -----------------------------------------------------------------------------
//
// corDbValueIndexReplace - newP takes oldP's place in the store, and so oldP's place in the index
//
void corDbValueIndexReplace(CorDbStore* storeP, CorNode* oldP, CorNode* newP)
{
  CorDbValueIndex* vxP = storeP->valueIndexP;

  if ((vxP == NULL) || (vxP->declN == 0))
    return;

  bool wasIn = keysAny(vxP, oldP);
  bool isIn  = keysAny(vxP, newP);

  if ((wasIn == false) && (isIn == false))           // nearly every replace, for a rare attribute
    return;

  VxEntity* entP = (wasIn == true) ? mapRemove(vxP, oldP) : NULL;

  if ((wasIn == true) && (entP == NULL))
  {
    indexOff(storeP);
    return;
  }

  if (entP == NULL)                                  // it gains a value: its place, where it is
  {
    if (entityIn(vxP, newP, newP->next) == false)
      indexOff(storeP);
    return;
  }

  if (isIn == false)
  {
    entityOut(vxP, entP);
    return;
  }

  entP->entityP = newP;
  mapAdd(vxP, entP);

  if (entriesRefresh(vxP, entP) == false)
    indexOff(storeP);
}



// -----------------------------------------------------------------------------
//
// declTouched - can a write of these members (or of the entity's types) have changed a declaration's entries?
//
static bool declTouched(CorDbValueIndex* vxP, const char** namesV, int namesN, bool retyped)
{
  for (int ix = 0; ix < vxP->declN; ix++)
  {
    VxDecl* dP = &vxP->declV[ix];

    if ((dP->decl.type != NULL) && (retyped == true))
      return true;

    for (int n = 0; n < namesN; n++)
    {
      if (strcmp(namesV[n], dP->decl.attr) == 0)
        return true;
    }
  }

  return false;
}



// -----------------------------------------------------------------------------
//
// corDbValueIndexChanged -
//
void corDbValueIndexChanged(CorDbStore* storeP, CorNode* entityP, const char** namesV, int namesN, bool retyped)
{
  CorDbValueIndex* vxP = storeP->valueIndexP;

  if ((vxP == NULL) || (vxP->declN == 0))
    return;

  if ((namesN >= 0) && (declTouched(vxP, namesV, namesN, retyped) == false))   // nearly every write: no declared attribute
    return;

  VxEntity* entP = mapFind(vxP, entityP);

  if (entP == NULL)                                  // not in the index: in it if it gained a value
  {
    if ((keysAny(vxP, entityP) == true) && (entityIn(vxP, entityP, entityP->next) == false))
      indexOff(storeP);
    return;
  }

  if (entriesRefresh(vxP, entP) == false)
  {
    indexOff(storeP);
    return;
  }

  if (entP->entries == NULL)                         // it lost its last value: out of the index
  {
    mapRemove(vxP, entityP);
    orderRemove(vxP, entP);
    entityFree(vxP, entP);
  }
}



// -----------------------------------------------------------------------------
//
// corDbValueIndexStats -
//
bool corDbValueIndexStats(CorDbStore* storeP, const char* declId, int64_t* entriesP, int64_t* bytesP, int64_t* accessesP, int64_t* updatesP)
{
  CorDbValueIndex* vxP = __atomic_load_n(&storeP->valueIndexP, __ATOMIC_ACQUIRE);

  for (int ix = 0; (vxP != NULL) && (ix < vxP->declN); ix++)
  {
    VxDecl* dP = &vxP->declV[ix];

    if (strcmp(dP->decl.id, declId) == 0)
    {
      *entriesP  = dP->entries;
      *bytesP    = dP->bytes;
      *accessesP = __atomic_load_n(&dP->accesses, __ATOMIC_RELAXED);
      *updatesP  = dP->updates;
      return true;
    }
  }

  return false;
}



// -----------------------------------------------------------------------------
//
// corDbValueIndexDrop -
//
void corDbValueIndexDrop(CorDbStore* storeP)
{
  indexFree(storeP->valueIndexP);
  __atomic_store_n(&storeP->valueIndexP, NULL, __ATOMIC_RELEASE);
}
