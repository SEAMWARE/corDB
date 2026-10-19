//
// FILE            corDbTypeIndex.c
//
// AUTHOR          Ken Zangelin
//
// Copyright 2026 Seamware
// SPDX-License-Identifier: Apache-2.0
//
#include <pthread.h>                                 // pthread_mutex_lock
#include <stdbool.h>                                 // bool
#include <stdint.h>                                  // uintptr_t
#include <stdlib.h>                                  // malloc, calloc, free
#include <string.h>                                  // strcmp, strdup

#include "corHash/corHash.h"                         // corHashTableCreate, corHashItemAdd, ...
#include "corTree/CorNode.h"                         // CorNode
#include "corTree/corTreeLookup.h"                   // corTreeLookup

#include "corDB/corDbIndex.h"                        // corDbIndexPrev
#include "corDB/corDbTypeIndex.h"                    // Own interface



// -----------------------------------------------------------------------------
//
// CorDbTypeList - the entities of one type, in the store's order
//
typedef struct CorDbTypeList
{
  char*          type;
  CorDbTypeRef*  head;
  CorDbTypeRef*  tail;
  int            count;
} CorDbTypeList;



// -----------------------------------------------------------------------------
//
// RefBlock - refs are allocated in blocks, not one malloc each: a malloc'd 48-byte ref costs 64
//
#define REF_BLOCK  1024

typedef struct RefBlock
{
  struct RefBlock*  next;
  CorDbTypeRef      refV[REF_BLOCK];
} RefBlock;



// -----------------------------------------------------------------------------
//
// CorDbTypeIndex - the lists by type, and the entity map: an entity -> its first ref
//
// The map is chained through the refs themselves (mapNext), so it costs a bucket array and no entry. It grows
// as the id index does (corDbIndex.c): a table MAP_GROWTH times larger takes over, and the old one's buckets
// move into it MAP_MOVE at a time, at every change - never all at once under the write lock.
//
#define MAP_BUCKETS  1024
#define MAP_LOAD        2
#define MAP_GROWTH      8
#define MAP_MOVE       32

typedef struct CorDbTypeIndex
{
  CorHashTable*   lists;                             // type -> CorDbTypeList
  CorDbTypeRef**  map;
  unsigned int    mapMask;
  int             mapCount;
  CorDbTypeRef**  oldMap;                            // a growth in progress
  unsigned int    oldMask;
  unsigned int    oldMoved;                          // the old buckets moved so far
  RefBlock*       blocks;
  int             blockUsed;                         // refs handed out of the first block
  CorDbTypeRef*   freeRefs;                          // freed refs, chained through 'next'
  CorDbTypeList*  lastListP;                         // the list a link found last (listGet)
} CorDbTypeIndex;



// -----------------------------------------------------------------------------
//
// typeHash / typeCompare - the lists' table
//
static unsigned int typeHash(const char* name)
{
  unsigned int h = 2166136261u;

  for (const unsigned char* p = (const unsigned char*) name; *p != 0; p++)
    h = (h ^ *p) * 16777619u;

  return h;
}

static int typeCompare(const char* name, void* itemP)
{
  return strcmp(name, ((CorDbTypeList*) itemP)->type);
}



// -----------------------------------------------------------------------------
//
// refAlloc / refFree
//
static CorDbTypeRef* refAlloc(CorDbTypeIndex* ixP)
{
  CorDbTypeRef* refP = ixP->freeRefs;

  if (refP != NULL)
    ixP->freeRefs = refP->next;
  else
  {
    if ((ixP->blocks == NULL) || (ixP->blockUsed == REF_BLOCK))
    {
      RefBlock* blockP = (RefBlock*) malloc(sizeof(RefBlock));

      if (blockP == NULL)
        return NULL;

      blockP->next   = ixP->blocks;
      ixP->blocks    = blockP;
      ixP->blockUsed = 0;
    }

    refP = &ixP->blocks->refV[ixP->blockUsed++];
  }

  memset(refP, 0, sizeof(CorDbTypeRef));
  return refP;
}

static void refFree(CorDbTypeIndex* ixP, CorDbTypeRef* refP)
{
  refP->next    = ixP->freeRefs;
  ixP->freeRefs = refP;
}



// -----------------------------------------------------------------------------
//
// The entity map - an entity -> its first ref
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
static CorDbTypeRef** mapBucket(CorDbTypeIndex* ixP, CorNode* entityP)
{
  unsigned int h = ptrHash(entityP);

  if (ixP->oldMap != NULL)
  {
    unsigned int oldIx = h & ixP->oldMask;

    if (oldIx >= ixP->oldMoved)
      return &ixP->oldMap[oldIx];
  }

  return &ixP->map[h & ixP->mapMask];
}

//
// mapMigrate - up to 'buckets' of the old table moved into the current one; the old freed when empty
//
static void mapMigrate(CorDbTypeIndex* ixP, unsigned int buckets)
{
  if (ixP->oldMap == NULL)
    return;

  for (unsigned int n = 0; (n < buckets) && (ixP->oldMoved <= ixP->oldMask); n++, ixP->oldMoved++)
  {
    CorDbTypeRef* refP = ixP->oldMap[ixP->oldMoved];

    while (refP != NULL)
    {
      CorDbTypeRef*  nextP   = refP->mapNext;
      CorDbTypeRef** bucketP = &ixP->map[ptrHash(refP->entityP) & ixP->mapMask];

      refP->mapNext = *bucketP;
      *bucketP      = refP;
      refP          = nextP;
    }

    ixP->oldMap[ixP->oldMoved] = NULL;
  }

  if (ixP->oldMoved > ixP->oldMask)
  {
    free(ixP->oldMap);
    ixP->oldMap   = NULL;
    ixP->oldMask  = 0;
    ixP->oldMoved = 0;
  }
}

//
// mapGrow - a larger table takes over; the old one moves into it bit by bit (mapMigrate). Not while a growth
// is still in progress: the next change finishes that one first.
//
static void mapGrow(CorDbTypeIndex* ixP)
{
  if (ixP->oldMap != NULL)
    return;

  unsigned int    buckets = (ixP->mapMask + 1) * MAP_GROWTH;
  CorDbTypeRef**  newP    = (CorDbTypeRef**) calloc(buckets, sizeof(CorDbTypeRef*));

  if (newP == NULL)
    return;                                          // longer chains; slower, not wrong

  ixP->oldMap   = ixP->map;
  ixP->oldMask  = ixP->mapMask;
  ixP->oldMoved = 0;
  ixP->map      = newP;
  ixP->mapMask  = buckets - 1;
}

static CorDbTypeRef* mapFind(CorDbTypeIndex* ixP, CorNode* entityP)
{
  for (CorDbTypeRef* refP = *mapBucket(ixP, entityP); refP != NULL; refP = refP->mapNext)
  {
    if (refP->entityP == entityP)
      return refP;
  }

  return NULL;
}

static void mapAdd(CorDbTypeIndex* ixP, CorDbTypeRef* firstP)
{
  mapMigrate(ixP, MAP_MOVE);

  CorDbTypeRef** bucketP = mapBucket(ixP, firstP->entityP);

  firstP->mapNext = *bucketP;
  *bucketP        = firstP;
  ixP->mapCount  += 1;

  if (ixP->mapCount > (int) (ixP->mapMask + 1) * MAP_LOAD)
    mapGrow(ixP);
}

//
// mapRemove - the entity's first ref out of the map, returned; NULL: not in it
//
static CorDbTypeRef* mapRemove(CorDbTypeIndex* ixP, CorNode* entityP)
{
  mapMigrate(ixP, MAP_MOVE);

  for (CorDbTypeRef** pP = mapBucket(ixP, entityP); *pP != NULL; pP = &(*pP)->mapNext)
  {
    CorDbTypeRef* refP = *pP;

    if (refP->entityP == entityP)
    {
      *pP            = refP->mapNext;
      refP->mapNext  = NULL;
      ixP->mapCount -= 1;
      return refP;
    }
  }

  return NULL;
}



// -----------------------------------------------------------------------------
//
// The lists
//

//
// listGet - the type's list; create: made if there is none (a link, the write lock held)
//
// A link checks the list it found last before the table: creates come in runs of one type, and the table
// costs a hash over the whole expanded type name - most of a link's time, which is spent under the write
// lock. Only a link reads and sets lastListP: a query's lookup (create false) runs under the read lock,
// beside other queries.
//
static CorDbTypeList* listGet(CorDbTypeIndex* ixP, const char* type, bool create)
{
  if ((create == true) && (ixP->lastListP != NULL) && (strcmp(ixP->lastListP->type, type) == 0))
    return ixP->lastListP;

  CorDbTypeList* listP = (CorDbTypeList*) corHashItemLookup(ixP->lists, type);

  if (create == true)
    ixP->lastListP = listP;

  if ((listP != NULL) || (create == false))
    return listP;

  listP = (CorDbTypeList*) calloc(1, sizeof(CorDbTypeList));

  if (listP == NULL)
    return NULL;

  listP->type = strdup(type);

  if ((listP->type == NULL) || (corHashItemAdd(ixP->lists, listP->type, listP) != 0))
  {
    free(listP->type);
    free(listP);
    return NULL;
  }

  ixP->lastListP = listP;
  return listP;
}

//
// listInsert - the ref after afterP in the list; afterP NULL: at the head
//
static void listInsert(CorDbTypeList* listP, CorDbTypeRef* afterP, CorDbTypeRef* refP)
{
  refP->listP = listP;
  refP->prev  = afterP;
  refP->next  = (afterP != NULL) ? afterP->next : listP->head;

  if (refP->next != NULL)
    refP->next->prev = refP;
  else
    listP->tail = refP;

  if (afterP != NULL)
    afterP->next = refP;
  else
    listP->head = refP;

  listP->count += 1;
}

//
// listRemove - the ref out of its list; a list left empty is dropped (types come and go)
//
static void listRemove(CorDbTypeIndex* ixP, CorDbTypeRef* refP)
{
  CorDbTypeList* listP = refP->listP;

  if (refP->prev != NULL)
    refP->prev->next = refP->next;
  else
    listP->head = refP->next;

  if (refP->next != NULL)
    refP->next->prev = refP->prev;
  else
    listP->tail = refP->prev;

  refP->prev   = NULL;
  refP->next   = NULL;
  listP->count -= 1;

  if (listP->count == 0)
  {
    if (ixP->lastListP == listP)
      ixP->lastListP = NULL;

    corHashItemRemove(ixP->lists, listP->type);
    free(listP->type);
    free(listP);
  }
}



// -----------------------------------------------------------------------------
//
// TypeIter / typeNext - an entity's types, one at a time: "type" is a string or an array of strings
//
typedef struct TypeIter
{
  CorNode*  typeP;                                   // the entity's "type"
  CorNode*  atP;                                     // the array's element returned last
  bool      started;
} TypeIter;

static const char* typeNext(CorNode* entityP, TypeIter* itP)
{
  if (itP->started == false)
  {
    itP->started = true;
    itP->typeP   = corTreeLookup(entityP, "type");

    if (itP->typeP == NULL)
      return NULL;

    if (itP->typeP->type == CorString)
      return itP->typeP->value.s;

    if (itP->typeP->type != CorArray)
      return NULL;

    itP->atP = itP->typeP->value.head;
  }
  else if ((itP->typeP == NULL) || (itP->typeP->type != CorArray) || (itP->atP == NULL))
    return NULL;
  else
    itP->atP = itP->atP->next;

  while ((itP->atP != NULL) && (itP->atP->type != CorString))
    itP->atP = itP->atP->next;

  return (itP->atP != NULL) ? itP->atP->value.s : NULL;
}

static bool typeOf(CorNode* entityP, const char* type)
{
  TypeIter it = { NULL, NULL, false };

  for (const char* t = typeNext(entityP, &it); t != NULL; t = typeNext(entityP, &it))
  {
    if (strcmp(t, type) == 0)
      return true;
  }

  return false;
}



// -----------------------------------------------------------------------------
//
// refIn - the ref of a chain (an entity's refs) in a list; NULL: none
//
static CorDbTypeRef* refIn(CorDbTypeRef* firstP, CorDbTypeList* listP)
{
  for (CorDbTypeRef* refP = firstP; refP != NULL; refP = refP->sibling)
  {
    if (refP->listP == listP)
      return refP;
  }

  return NULL;
}



// -----------------------------------------------------------------------------
//
// placeAfter - the ref an entity's ref goes after in a type's list, so that the list keeps the store's order:
// the nearest entity BEFORE it in the store that is in the list. NULL: none - the head.
//
// Backwards from the entity, one id-index lookup a step (corDbIndexPrev); for an entity the id index does
// not hold, the walk from the start of the store. Only where a type is GAINED - an entity's types changed;
// created entities go to the end.
//
static CorDbTypeRef* placeAfter(CorDbStore* storeP, CorDbTypeIndex* ixP, CorNode* entityP, CorDbTypeList* listP)
{
  if (listP->count == 0)
    return NULL;

  if (corDbIndexLookup(storeP, corDbEntityId(entityP)) == entityP)
  {
    for (CorNode* pP = corDbIndexPrev(storeP, entityP); pP != NULL; pP = corDbIndexPrev(storeP, pP))
    {
      CorDbTypeRef* refP = refIn(mapFind(ixP, pP), listP);

      if (refP != NULL)
        return refP;
    }

    return NULL;
  }

  CorDbTypeRef* lastP = NULL;

  for (CorNode* pP = storeP->entities->value.head; (pP != NULL) && (pP != entityP); pP = pP->next)
  {
    CorDbTypeRef* refP = refIn(mapFind(ixP, pP), listP);

    if (refP != NULL)
      lastP = refP;
  }

  return lastP;
}



// -----------------------------------------------------------------------------
//
// entityAdd - the entity's refs, one per type: at the end of each list (atEnd: it is the store's last), or
// where the store's order puts it. False: out of memory - the index is then wrong, and dropped.
//
static bool entityAdd(CorDbStore* storeP, CorDbTypeIndex* ixP, CorNode* entityP, CorDbTypeRef* firstP, bool atEnd)
{
  CorDbTypeRef* lastP = firstP;
  TypeIter      it    = { NULL, NULL, false };

  while ((lastP != NULL) && (lastP->sibling != NULL))
    lastP = lastP->sibling;

  for (const char* type = typeNext(entityP, &it); type != NULL; type = typeNext(entityP, &it))
  {
    CorDbTypeList* listP = listGet(ixP, type, true);

    if (listP == NULL)
      return false;

    if (refIn(firstP, listP) != NULL)                // the same type twice, or kept by a replace
      continue;

    CorDbTypeRef* refP = refAlloc(ixP);

    if (refP == NULL)
      return false;

    refP->entityP = entityP;
    listInsert(listP, atEnd ? listP->tail : placeAfter(storeP, ixP, entityP, listP), refP);

    if (lastP == NULL)
      firstP = refP;
    else
      lastP->sibling = refP;
    lastP = refP;
  }

  if (firstP != NULL)
    mapAdd(ixP, firstP);

  return true;
}



// -----------------------------------------------------------------------------
//
// indexFree - every list, the map, the refs
//
static void listFree(CorDbTypeIndex* ixP)
{
  for (int i = 0; i < ixP->lists->arraySize; i++)
  {
    for (CorHashListItem* itemP = ixP->lists->array[i]; itemP != NULL; itemP = itemP->next)
    {
      CorDbTypeList* listP = (CorDbTypeList*) itemP->data;

      free(listP->type);
      free(listP);
    }
  }
}

static void indexFree(CorDbTypeIndex* ixP)
{
  if (ixP == NULL)
    return;

  if (ixP->lists != NULL)
  {
    listFree(ixP);
    corHashRelease(ixP->lists);
  }

  while (ixP->blocks != NULL)
  {
    RefBlock* nextP = ixP->blocks->next;

    free(ixP->blocks);
    ixP->blocks = nextP;
  }

  free(ixP->map);
  free(ixP->oldMap);
  free(ixP);
}



// -----------------------------------------------------------------------------
//
// indexBuild - the index of the store's list as it is
//
static CorDbTypeIndex* indexBuild(CorDbStore* storeP)
{
  CorDbTypeIndex* ixP = (CorDbTypeIndex*) calloc(1, sizeof(CorDbTypeIndex));

  if (ixP == NULL)
    return NULL;

  unsigned int buckets = MAP_BUCKETS;
  int          n       = 0;

  for (CorNode* eP = storeP->entities->value.head; eP != NULL; eP = eP->next)
    ++n;

  while (n > (int) buckets * MAP_LOAD)
    buckets *= MAP_GROWTH;

  ixP->lists   = corHashTableCreate(NULL, typeHash, typeCompare, 1024);
  ixP->map     = (CorDbTypeRef**) calloc(buckets, sizeof(CorDbTypeRef*));
  ixP->mapMask = buckets - 1;

  if ((ixP->lists == NULL) || (ixP->map == NULL))
  {
    indexFree(ixP);
    return NULL;
  }

  for (CorNode* eP = storeP->entities->value.head; eP != NULL; eP = eP->next)
  {
    if (entityAdd(storeP, ixP, eP, NULL, true) == false)
    {
      indexFree(ixP);
      return NULL;
    }
  }

  return ixP;
}



// -----------------------------------------------------------------------------
//
// corDbTypeIndexOf -
//
CorDbTypeIndex* corDbTypeIndexOf(CorDbStore* storeP)
{
  CorDbTypeIndex* ixP = __atomic_load_n(&storeP->typeIndexP, __ATOMIC_ACQUIRE);

  if (ixP != NULL)
    return ixP;

  pthread_mutex_lock(&storeP->typeIndexMutex);

  ixP = __atomic_load_n(&storeP->typeIndexP, __ATOMIC_ACQUIRE);

  if (ixP == NULL)
  {
    ixP = indexBuild(storeP);
    __atomic_store_n(&storeP->typeIndexP, ixP, __ATOMIC_RELEASE);
  }

  pthread_mutex_unlock(&storeP->typeIndexMutex);

  return ixP;
}



// -----------------------------------------------------------------------------
//
// corDbTypeIndexHead -
//
CorDbTypeRef* corDbTypeIndexHead(CorDbTypeIndex* ixP, const char* type, int* countP)
{
  CorDbTypeList* listP = listGet(ixP, type, false);

  *countP = (listP != NULL) ? listP->count : 0;

  return (listP != NULL) ? listP->head : NULL;
}



// -----------------------------------------------------------------------------
//
// corDbTypeIndexRef -
//
CorDbTypeRef* corDbTypeIndexRef(CorDbTypeIndex* ixP, CorNode* entityP, const char* type)
{
  CorDbTypeList* listP = listGet(ixP, type, false);

  return (listP != NULL) ? refIn(mapFind(ixP, entityP), listP) : NULL;
}



// -----------------------------------------------------------------------------
//
// dropIndex - out of memory in the middle of a change: the index no longer says the truth, and goes. The next
// query that can use one builds it again.
//
static void dropIndex(CorDbStore* storeP)
{
  indexFree(storeP->typeIndexP);
  __atomic_store_n(&storeP->typeIndexP, NULL, __ATOMIC_RELEASE);
}



// -----------------------------------------------------------------------------
//
// corDbTypeIndexLink -
//
void corDbTypeIndexLink(CorDbStore* storeP, CorNode* entityP)
{
  CorDbTypeIndex* ixP = storeP->typeIndexP;

  if ((ixP != NULL) && (entityAdd(storeP, ixP, entityP, NULL, true) == false))
    dropIndex(storeP);
}



// -----------------------------------------------------------------------------
//
// corDbTypeIndexUnlink -
//
void corDbTypeIndexUnlink(CorDbStore* storeP, CorNode* entityP)
{
  CorDbTypeIndex* ixP = storeP->typeIndexP;

  if (ixP == NULL)
    return;

  CorDbTypeRef* refP = mapRemove(ixP, entityP);

  while (refP != NULL)
  {
    CorDbTypeRef* nextP = refP->sibling;

    listRemove(ixP, refP);
    refFree(ixP, refP);
    refP = nextP;
  }
}



// -----------------------------------------------------------------------------
//
// corDbTypeIndexReplace -
//
// The refs of the types both have now name newP, where they are - the place in the store does not change.
// A type only oldP had: its ref goes. A type only newP has: a ref where the store's order puts it.
//
void corDbTypeIndexReplace(CorDbStore* storeP, CorNode* oldP, CorNode* newP)
{
  CorDbTypeIndex* ixP = storeP->typeIndexP;

  if (ixP == NULL)
    return;

  CorDbTypeRef* refP   = mapRemove(ixP, oldP);
  CorDbTypeRef* firstP = NULL;
  CorDbTypeRef* lastP  = NULL;

  while (refP != NULL)
  {
    CorDbTypeRef* nextP = refP->sibling;

    refP->sibling = NULL;

    if (typeOf(newP, refP->listP->type) == true)
    {
      refP->entityP = newP;

      if (lastP == NULL)
        firstP = refP;
      else
        lastP->sibling = refP;
      lastP = refP;
    }
    else
    {
      listRemove(ixP, refP);
      refFree(ixP, refP);
    }

    refP = nextP;
  }

  if (entityAdd(storeP, ixP, newP, firstP, false) == false)
    dropIndex(storeP);
}



// -----------------------------------------------------------------------------
//
// corDbTypeIndexRetype -
//
void corDbTypeIndexRetype(CorDbStore* storeP, CorNode* entityP)
{
  CorDbTypeIndex* ixP = storeP->typeIndexP;

  if (ixP == NULL)
    return;

  //
  // Nearly every merge leaves the types as they were: then nothing changes - every ref's type is the
  // entity's, and every type of the entity has its ref
  //
  CorDbTypeRef* firstP = mapFind(ixP, entityP);
  bool          same   = true;
  TypeIter      it     = { NULL, NULL, false };

  for (CorDbTypeRef* refP = firstP; (refP != NULL) && same; refP = refP->sibling)
    same = typeOf(entityP, refP->listP->type);

  for (const char* type = typeNext(entityP, &it); (type != NULL) && same; type = typeNext(entityP, &it))
  {
    CorDbTypeList* listP = listGet(ixP, type, false);

    same = (listP != NULL) && (refIn(firstP, listP) != NULL);
  }

  if (same == false)
    corDbTypeIndexReplace(storeP, entityP, entityP);
}



// -----------------------------------------------------------------------------
//
// corDbTypeIndexFree -
//
void corDbTypeIndexFree(CorDbStore* storeP)
{
  dropIndex(storeP);
}
