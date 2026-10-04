//
// FILE            corDbHistory.c
//
// AUTHOR          Ken Zangelin
//
// Copyright 2026 Seamware
// SPDX-License-Identifier: Apache-2.0
//
#include <pthread.h>                                   // pthread_key_create, pthread_getspecific, pthread_once
#include <stdbool.h>                                   // bool
#include <stdint.h>                                    // uint64_t
#include <stdlib.h>                                    // malloc, realloc, free
#include <string.h>                                    // strcmp, strdup, memcpy

#include "corLog/corLog.h"                             // COR_E
#include "corAlloc/CorAlloc.h"                         // CorAlloc
#include "corAlloc/corAlloc.h"                         // corAlloc
#include "corHash/corHash.h"                           // corHashTableCreate, corHashItemAdd, corHashItemLookup, corHashItemRemove
#include "corTree/CorNode.h"                           // CorNode
#include "corTree/corTreeBin.h"                        // corTreeBinEncode, corTreeBinDecode, CorBinBuffer
#include "corTree/corTreeBuilder.h"                    // corTreeObject, corTreeString, corTreeInteger, corTreeChildAdd
#include "corTree/corTreeClone.h"                      // corTreeClone
#include "corTree/corTreeLookup.h"                     // corTreeLookup

#include "corNgsild/ldBinCodec.h"                      // ldBinCodec
#include "corNgsild/ldCheckDateTime.h"                 // ldIsoToNanoseconds
#include "corNgsild/ldIdGenerate.h"                    // ldIdGenerate

#include "corDB/corDbHistory.h"                        // Own interface



// -----------------------------------------------------------------------------
//
// corDbHistoryOn -
//
bool corDbHistoryOn = false;



// -----------------------------------------------------------------------------
//
// idHash / idCompare - the entity-id table (corHash keeps no key: the entity carries it)
//
static unsigned int idHash(const char* name)
{
  uint32_t h = 2166136261u;                            // FNV-1a

  for (const unsigned char* p = (const unsigned char*) name; *p != 0; p++)
    h = (h ^ *p) * 16777619u;

  return h;
}

static int idCompare(const char* name, void* itemP)
{
  return strcmp(name, ((CorDbHistEntity*) itemP)->id);
}



// -----------------------------------------------------------------------------
//
// nameCompare / intern - one copy of each attribute name and entity type per tenant
//
static int nameCompare(const char* name, void* itemP)
{
  return strcmp(name, (const char*) itemP);
}

static const char* intern(CorDbHistory* hP, const char* name)
{
  if (name == NULL)
    return NULL;

  if (hP->names == NULL)
  {
    hP->names = corHashTableCreate(NULL, idHash, nameCompare, 4096);  // a tenant's names are few: no growth
    if (hP->names == NULL)
      return NULL;
  }

  const char* p = (const char*) corHashItemLookup(hP->names, name);

  if (p != NULL)
    return p;

  if (hP->namesN == hP->namesSize)
  {
    int    size = (hP->namesSize == 0) ? 64 : hP->namesSize * 2;
    char** v    = (char**) realloc(hP->nameV, size * sizeof(char*));

    if (v == NULL)
      return NULL;

    hP->nameV     = v;
    hP->namesSize = size;
  }

  char* copyP = strdup(name);

  if (copyP == NULL)
    return NULL;

  hP->nameV[hP->namesN++] = copyP;
  corHashItemAdd(hP->names, copyP, copyP);

  return copyP;
}



// -----------------------------------------------------------------------------
//
// tableGrow - a table twice the size, with every entity in it (corHash does not rehash)
//
static bool tableGrow(CorDbHistory* hP)
{
  int           slots = (hP->slots == 0) ? 1024 : hP->slots * 2;
  CorHashTable* newP  = corHashTableCreate(NULL, idHash, idCompare, slots);

  if (newP == NULL)
    return (hP->byId != NULL);                         // keep the old one: slower, not wrong

  for (CorDbHistEntity* eP = hP->first; eP != NULL; eP = eP->next)
    corHashItemAdd(newP, eP->id, eP);

  if (hP->byId != NULL)
    corHashRelease(hP->byId);

  hP->byId  = newP;
  hP->slots = slots;
  return true;
}



// -----------------------------------------------------------------------------
//
// corDbHistoryEntity -
//
CorDbHistEntity* corDbHistoryEntity(CorDbHistory* hP, const char* entityId, const char* entityType, bool create)
{
  CorDbHistEntity* eP = (hP->byId != NULL) ? (CorDbHistEntity*) corHashItemLookup(hP->byId, entityId) : NULL;

  if (eP != NULL)
  {
    if (entityType != NULL)
      corDbHistoryEntityTypes(hP, eP, entityType, false);

    return eP;
  }

  if (create == false)
    return NULL;

  if (((hP->byId == NULL) || (hP->count >= hP->slots * 2)) && (tableGrow(hP) == false))
    return NULL;

  eP = (CorDbHistEntity*) calloc(1, sizeof(CorDbHistEntity));
  if (eP == NULL)
    return NULL;

  eP->id = strdup(entityId);
  if (eP->id == NULL)
  {
    free(eP);
    return NULL;
  }

  if (entityType != NULL)
    corDbHistoryEntityTypes(hP, eP, entityType, true);

  if (hP->last != NULL)
    hP->last->next = eP;
  else
    hP->first = eP;
  hP->last = eP;

  corHashItemAdd(hP->byId, eP->id, eP);
  ++hP->count;

  return eP;
}



// -----------------------------------------------------------------------------
//
// corDbHistoryEntityTypesJoined - an entity's types as one string, the names joined by '\n'
//
const char* corDbHistoryEntityTypesJoined(CorDbHistEntity* eP, CorAlloc* kaP)
{
  if (eP->typeN <= 1)
    return eP->type;

  int len = 0;

  for (int i = 0; i < eP->typeN; i++)
    len += (int) strlen(eP->typeV[i]) + 1;

  char* joined = (char*) corAlloc(kaP, len + 1);

  if (joined == NULL)
    return eP->type;

  joined[0] = 0;
  for (int i = 0; i < eP->typeN; i++)
  {
    if (i > 0)
      strcat(joined, "\n");
    strcat(joined, eP->typeV[i]);
  }

  return joined;
}



// -----------------------------------------------------------------------------
//
// corDbHistoryEntityTypes -
//
void corDbHistoryEntityTypes(CorDbHistory* hP, CorDbHistEntity* eP, const char* types, bool replace)
{
  if (replace)
    eP->typeN = 0;

  const char* p = types;

  while (*p != 0)
  {
    const char* end = strchr(p, '\n');
    int         len = (end != NULL) ? (int) (end - p) : (int) strlen(p);
    char        name[1024];

    if ((len > 0) && (len < (int) sizeof(name)))
    {
      memcpy(name, p, len);
      name[len] = 0;

      const char* typeP = intern(hP, name);
      bool        have  = false;

      for (int i = 0; (i < eP->typeN) && (have == false); i++)
        have = (eP->typeV[i] == typeP);              // interned: one pointer per name

      if ((typeP != NULL) && (have == false))
      {
        if (eP->typeN == eP->typeSize)
        {
          int          size = (eP->typeSize == 0) ? 2 : eP->typeSize * 2;
          const char** v    = (const char**) realloc(eP->typeV, size * sizeof(char*));

          if (v == NULL)
            break;
          eP->typeV    = v;
          eP->typeSize = size;
        }
        eP->typeV[eP->typeN++] = typeP;
      }
    }

    if (end == NULL)
      break;
    p = end + 1;
  }

  eP->type = (eP->typeN > 0) ? eP->typeV[0] : NULL;
}



// -----------------------------------------------------------------------------
//
// attrOf - the entity's history of one attribute, created on its first instance
//
static CorDbHistAttr* attrOf(CorDbHistory* hP, CorDbHistEntity* eP, const char* attrName)
{
  for (CorDbHistAttr* aP = eP->attrs; aP != NULL; aP = aP->next)
  {
    if (strcmp(aP->name, attrName) == 0)
      return aP;
  }

  CorDbHistAttr* aP = (CorDbHistAttr*) calloc(1, sizeof(CorDbHistAttr));

  if (aP == NULL)
    return NULL;

  aP->name = intern(hP, attrName);
  if (aP->name == NULL)
  {
    free(aP);
    return NULL;
  }

  aP->instanceV = aP->inlineV;
  aP->size      = sizeof(aP->inlineV) / sizeof(aP->inlineV[0]);

  if (eP->lastAttr != NULL)
    eP->lastAttr->next = aP;
  else
    eP->attrs = aP;
  eP->lastAttr = aP;

  return aP;
}



// -----------------------------------------------------------------------------
//
// arenaCopy - len bytes into the tenant's history arena (8-aligned); NULL out of memory
//
static char* arenaCopy(CorDbHistory* hP, const void* src, size_t len)
{
  CorDbHistChunk* cP = hP->chunks;

  if ((cP == NULL) || (cP->used + len > cP->size))
  {
    size_t size = (len > 1024 * 1024) ? len : 1024 * 1024;

    cP = (CorDbHistChunk*) malloc(sizeof(CorDbHistChunk) + size);
    if (cP == NULL)
      return NULL;

    cP->next   = hP->chunks;
    cP->used   = 0;
    cP->size   = size;
    hP->chunks = cP;
  }

  char* p = &cP->data[cP->used];

  memcpy(p, src, len);
  cP->used += (len + 7) & ~((size_t) 7);
  if (cP->used > cP->size)
    cP->used = cP->size;

  return p;
}



// -----------------------------------------------------------------------------
//
// encodeExact - a tree encoded into this thread's reusable buffer, then copied to one malloc of exactly
// its size: a record encoded into a fresh buffer grew it by realloc after realloc and shrank it with
// one more - a quarter of a batch create was malloc
//
//
// The buffer is the thread's, kept between calls - and freed when the thread ends (a key's destructor):
// a plain __thread pointer was lost with every worker thread, 256 bytes each (valgrind).
//
static pthread_key_t  scratchKey;
static pthread_once_t scratchOnce = PTHREAD_ONCE_INIT;

static void scratchFree(void* p)
{
  CorBinBuffer* bP = (CorBinBuffer*) p;

  free(bP->buf);
  free(bP);
}

static bool scratchKeyMade = false;

static void scratchKeyCreate(void)
{
  scratchKeyMade = (pthread_key_create(&scratchKey, scratchFree) == 0);
}



// -----------------------------------------------------------------------------
//
// corDbHistoryScratchClose - at corDbClose: the calling thread's buffer freed, and the key deleted - a
// worker that ends after the plugin is unloaded must not run a destructor that is gone with it
//
void corDbHistoryScratchClose(void)
{
  if (scratchKeyMade == false)
    return;

  CorBinBuffer* bP = (CorBinBuffer*) pthread_getspecific(scratchKey);

  if (bP != NULL)
  {
    pthread_setspecific(scratchKey, NULL);
    scratchFree(bP);
  }

  pthread_key_delete(scratchKey);
  scratchKeyMade = false;
}

static CorBinBuffer* scratchGet(void)
{
  pthread_once(&scratchOnce, scratchKeyCreate);

  if (scratchKeyMade == false)
    return NULL;

  CorBinBuffer* bP = (CorBinBuffer*) pthread_getspecific(scratchKey);

  if (bP == NULL)
  {
    bP = (CorBinBuffer*) calloc(1, sizeof(CorBinBuffer));
    if ((bP != NULL) && (pthread_setspecific(scratchKey, bP) != 0))
    {
      free(bP);
      bP = NULL;
    }
  }

  return bP;
}

static bool encodeExact(CorNode* treeP, char** bodyPP, int* lenP)
{
  CorBinBuffer* scratchP = scratchGet();

  if (scratchP == NULL)
    return false;

  scratchP->len = 0;

  if (corTreeBinEncode(treeP, &ldBinCodec, NULL, scratchP) == false)
    return false;

  char* bodyP = (char*) malloc(scratchP->len);

  if (bodyP == NULL)
    return false;

  memcpy(bodyP, scratchP->buf, scratchP->len);
  *bodyPP = bodyP;
  *lenP   = scratchP->len;

  return true;
}



// -----------------------------------------------------------------------------
//
// encodeInto - the same, copied into kaP (a request's arena): a record lives until it is queued,
// where it is copied again - no malloc of its own
//
static bool encodeInto(CorNode* treeP, CorAlloc* kaP, char** bodyPP, int* lenP)
{
  CorBinBuffer* scratchP = scratchGet();

  if (scratchP == NULL)
    return false;

  scratchP->len = 0;

  if (corTreeBinEncode(treeP, &ldBinCodec, NULL, scratchP) == false)
    return false;

  char* bodyP = (char*) corAlloc(kaP, scratchP->len);

  if (bodyP == NULL)
    return false;

  memcpy(bodyP, scratchP->buf, scratchP->len);
  *bodyPP = bodyP;
  *lenP   = scratchP->len;

  return true;
}



// -----------------------------------------------------------------------------
//
// instanceScan - the members a record needs, in one pass over the instance: instanceId and the four
// timestamps (a lookup each was five walks of the same members)
//
static uint64_t nsOf(CorNode* tP)
{
  if (tP->type == CorInt)
    return (tP->value.i > 0) ? (uint64_t) tP->value.i : 0;

  if (tP->type == CorString)
  {
    int64_t ns = ldIsoToNanoseconds(tP->value.s);
    return (ns > 0) ? (uint64_t) ns : 0;
  }

  return 0;
}

static CorNode* instanceScan(CorNode* instanceP, CorDbHistRecord* recP)
{
  CorNode* instanceIdP = NULL;

  for (CorNode* mP = instanceP->value.head; mP != NULL; mP = mP->next)
  {
    const char* n = mP->name;

    if (n == NULL)
      continue;

    switch (n[0])
    {
    case 'i': if (strcmp(n, "instanceId") == 0) instanceIdP        = mP;      break;
    case 'o': if (strcmp(n, "observedAt") == 0) recP->observedAtNs = nsOf(mP); break;
    case 'c': if (strcmp(n, "createdAt")  == 0) recP->createdAtNs  = nsOf(mP); break;
    case 'm': if (strcmp(n, "modifiedAt") == 0) recP->modifiedAtNs = nsOf(mP); break;
    case 'd': if (strcmp(n, "deletedAt")  == 0) recP->deletedAtNs  = nsOf(mP); break;
    }
  }

  return instanceIdP;
}



// -----------------------------------------------------------------------------
//
// corDbHistoryRecordEncode -
//
bool corDbHistoryRecordEncode(const char* entityId, const char* entityType, const char* attrName, const char* datasetId,
                              CorNode* instanceP, uint64_t deletedAtNs, CorAlloc* kaP, CorDbHistRecord* recP)
{
  memset(recP, 0, sizeof(*recP));

  //
  // The instanceId: the one the instance carries (the temporal API's own writes), or one generated.
  // It goes into the RECORD, beside the instance - not into the instance, which may be the live store's
  // and would have to be cloned to carry it (a whole copy of every instance on every write, measured)
  //
  CorNode*    instanceIdP = (instanceP->type == CorObject) ? instanceScan(instanceP, recP) : NULL;
  const char* instanceId  = ((instanceIdP != NULL) && (instanceIdP->type == CorString)) ? instanceIdP->value.s : NULL;

  if (instanceId == NULL)
  {
    instanceId = ldIdGenerate(kaP, "Instance");
    if (instanceId == NULL)
      return false;
  }

  //
  // { id, type?, attr, datasetId?, deletedAt?, instanceId, instance } - "instance" a SHALLOW copy of the
  // instance's node: its members are the instance's own, only read by the encoder
  //
  CorNode* treeP = corTreeObject(kaP, NULL);
  CorNode* wrapP = (CorNode*) corAlloc(kaP, sizeof(CorNode));

  if ((treeP == NULL) || (wrapP == NULL))
    return false;

  *wrapP      = *instanceP;
  wrapP->name = (char*) "instance";
  wrapP->next = NULL;

  bool namedDataset = (datasetId != NULL) && (datasetId[0] != 0) && (strcmp(datasetId, "@none") != 0);

  corTreeChildAdd(treeP, corTreeString(kaP, "id", entityId));
  if (entityType != NULL)
    corTreeChildAdd(treeP, corTreeString(kaP, "type", entityType));
  corTreeChildAdd(treeP, corTreeString(kaP, "attr", attrName));
  if (namedDataset)
    corTreeChildAdd(treeP, corTreeString(kaP, "datasetId", datasetId));
  if (deletedAtNs != 0)
    corTreeChildAdd(treeP, corTreeInteger(kaP, "deletedAt", (long long) deletedAtNs));
  corTreeChildAdd(treeP, corTreeString(kaP, "instanceId", instanceId));
  corTreeChildAdd(treeP, wrapP);

  if (encodeInto(treeP, kaP, &recP->body, &recP->bodyLen) == false)
    return false;

  recP->attrName     = attrName;
  recP->instanceId   = instanceId;
  recP->datasetId    = namedDataset ? datasetId : NULL;
  recP->instanceP    = instanceP;
  if (deletedAtNs != 0)
    recP->deletedAtNs = deletedAtNs;

  return true;
}



// -----------------------------------------------------------------------------
//
// corDbHistoryRecordAdd -
//
CorDbInstance* corDbHistoryRecordAdd(CorDbHistory* hP, CorDbHistEntity* eP, CorDbHistRecord* recP)
{
  CorDbHistAttr* aP = attrOf(hP, eP, recP->attrName);

  if (aP == NULL)
    return NULL;

  if (aP->instances == aP->size)
  {
    int            size = aP->size * 2;
    CorDbInstance* v;

    if (aP->instanceV == aP->inlineV)                // out of the inline ones: the first array
    {
      v = (CorDbInstance*) malloc(size * sizeof(CorDbInstance));
      if (v != NULL)
        memcpy(v, aP->inlineV, aP->instances * sizeof(CorDbInstance));
    }
    else
      v = (CorDbInstance*) realloc(aP->instanceV, size * sizeof(CorDbInstance));

    if (v == NULL)
      return NULL;

    aP->instanceV = v;
    aP->size      = size;
  }

  CorDbInstance* iP = &aP->instanceV[aP->instances];

  //
  // The instanceId is in the record - a reader finds it there; no copy of it per instance
  //
  memset(iP, 0, sizeof(*iP));
  iP->datasetId    = (recP->datasetId != NULL) ? arenaCopy(hP, recP->datasetId, strlen(recP->datasetId) + 1) : NULL;
  iP->body         = arenaCopy(hP, recP->body, recP->bodyLen);
  iP->bodyLen      = recP->bodyLen;
  iP->observedAtNs = recP->observedAtNs;
  iP->createdAtNs  = recP->createdAtNs;
  iP->modifiedAtNs = recP->modifiedAtNs;
  iP->deletedAtNs  = recP->deletedAtNs;

  if ((iP->body == NULL) || ((recP->datasetId != NULL) && (iP->datasetId == NULL)))
    return NULL;

  ++aP->instances;
  return iP;
}



// -----------------------------------------------------------------------------
//
// corDbHistoryInstanceAdd -
//
CorDbInstance* corDbHistoryInstanceAdd(CorDbHistory* hP, CorDbHistEntity* eP, const char* attrName, const char* datasetId,
                                       CorNode* instanceP, uint64_t deletedAtNs, CorAlloc* kaP)
{
  CorDbHistRecord rec;

  if (corDbHistoryRecordEncode(eP->id, eP->type, attrName, datasetId, instanceP, deletedAtNs, kaP, &rec) == false)
    return NULL;

  return corDbHistoryRecordAdd(hP, eP, &rec);
}



// -----------------------------------------------------------------------------
//
// corDbHistoryEntityEvent -
//
void corDbHistoryEntityEventApply(CorDbHistEntity* eP, const char* entityOp, uint64_t atNs)
{
  if (strcmp(entityOp, "deleted") == 0)
    eP->deletedAtNs = atNs;
  else
  {
    eP->deletedAtNs = 0;                               // created again after a delete: alive again
    if ((eP->createdAtNs == 0) || (strcmp(entityOp, "created") == 0))
      eP->createdAtNs = atNs;
  }
}



bool corDbHistoryEntityEventEncode(const char* entityId, const char* entityType, const char* entityOp, uint64_t atNs, CorAlloc* kaP, CorBinBuffer* outP)
{
  outP->buf  = NULL;
  outP->len  = 0;
  outP->size = 0;

  CorNode* recP = corTreeObject(kaP, NULL);

  if (recP == NULL)
    return false;

  corTreeChildAdd(recP, corTreeString(kaP, "id", entityId));
  if (entityType != NULL)
    corTreeChildAdd(recP, corTreeString(kaP, "type", entityType));
  corTreeChildAdd(recP, corTreeString(kaP, "entityOp", entityOp));
  corTreeChildAdd(recP, corTreeInteger(kaP, "at", (long long) atNs));

  if (encodeExact(recP, &outP->buf, &outP->len) == false)
    return false;

  outP->size = outP->len;
  return true;
}



bool corDbHistoryEntityEvent(CorDbHistEntity* eP, const char* entityOp, uint64_t atNs, CorAlloc* kaP, CorBinBuffer* outP)
{
  corDbHistoryEntityEventApply(eP, entityOp, atNs);
  return corDbHistoryEntityEventEncode(eP->id, corDbHistoryEntityTypesJoined(eP, kaP), entityOp, atNs, kaP, outP);
}



// -----------------------------------------------------------------------------
//
// corDbHistoryInstanceDecode -
//
CorNode* corDbHistoryInstanceDecode(CorDbInstance* iP, CorAlloc* kaP)
{
  const char* error = NULL;
  CorNode*    recP  = corTreeBinDecode(iP->body, iP->bodyLen, &ldBinCodec, NULL, kaP, &error);
  CorNode*    instP = (recP != NULL) ? corTreeLookup(recP, "instance") : NULL;

  if (instP == NULL)
  {
    COR_E("corDB: a history instance does not decode: %s", (error != NULL) ? error : "no instance member");
    return NULL;
  }

  instP->name = NULL;
  instP->next = NULL;

  //
  // The instanceId sits beside the instance in the record (so a write never clones an instance to give
  // it one) - an answer has it in the instance
  //
  CorNode* idP = corTreeLookup(recP, "instanceId");

  if ((idP != NULL) && (corTreeLookup(instP, "instanceId") == NULL))
  {
    corTreeChildRemove(recP, idP);
    corTreeChildAdd(instP, idP);
  }

  return instP;
}



// -----------------------------------------------------------------------------
//
// corDbHistoryAttrLookup - the entity's history of one attribute; NULL: none
//
CorDbHistAttr* corDbHistoryAttrLookup(CorDbHistEntity* eP, const char* attrName)
{
  for (CorDbHistAttr* aP = eP->attrs; aP != NULL; aP = aP->next)
  {
    if (strcmp(aP->name, attrName) == 0)
      return aP;
  }

  return NULL;
}



// -----------------------------------------------------------------------------
//
// attrFree / attrUnlink -
//
static void attrFree(CorDbHistAttr* aP)
{
  if (aP->instanceV != aP->inlineV)
    free(aP->instanceV);
  free(aP);                                          // its name is interned; the bodies are the arena's
}

static void attrUnlink(CorDbHistEntity* eP, CorDbHistAttr* aP)
{
  CorDbHistAttr* prevP = NULL;

  for (CorDbHistAttr* xP = eP->attrs; xP != NULL; prevP = xP, xP = xP->next)
  {
    if (xP != aP)
      continue;

    if (prevP != NULL)
      prevP->next = aP->next;
    else
      eP->attrs = aP->next;

    if (eP->lastAttr == aP)
      eP->lastAttr = prevP;

    attrFree(aP);
    return;
  }
}



// -----------------------------------------------------------------------------
//
// corDbHistoryEntityRemove - an entity's whole history gone from the index (the temporal API's
// delete, § 5.6.16 - not a deletion of the entity, which history records)
//
void corDbHistoryEntityRemove(CorDbHistory* hP, CorDbHistEntity* eP)
{
  CorDbHistEntity* prevP = NULL;

  for (CorDbHistEntity* xP = hP->first; xP != NULL; prevP = xP, xP = xP->next)
  {
    if (xP != eP)
      continue;

    if (prevP != NULL)
      prevP->next = eP->next;
    else
      hP->first = eP->next;

    if (hP->last == eP)
      hP->last = prevP;
    break;
  }

  if (hP->byId != NULL)
    corHashItemRemove(hP->byId, eP->id);
  --hP->count;

  CorDbHistAttr* nextP;

  for (CorDbHistAttr* aP = eP->attrs; aP != NULL; aP = nextP)
  {
    nextP = aP->next;
    attrFree(aP);
  }

  free(eP->typeV);
  free(eP->id);
  free(eP);
}



// -----------------------------------------------------------------------------
//
// corDbHistoryAttrRemove - the instances of an attribute: all of them (deleteAll), or those of one
// datasetId (NULL: the default instance); the attribute goes when none is left. Returns how many went.
//
int corDbHistoryAttrRemove(CorDbHistEntity* eP, CorDbHistAttr* aP, const char* datasetId, bool deleteAll)
{
  int kept = 0;

  for (int i = 0; i < aP->instances; i++)
  {
    CorDbInstance* iP   = &aP->instanceV[i];
    bool           gone = deleteAll ||
                          ((datasetId == NULL) && (iP->datasetId == NULL)) ||
                          ((datasetId != NULL) && (iP->datasetId != NULL) && (strcmp(datasetId, iP->datasetId) == 0));

    if (gone == false)
      aP->instanceV[kept++] = *iP;
  }

  int removed = aP->instances - kept;

  aP->instances = kept;
  if (kept == 0)
    attrUnlink(eP, aP);

  return removed;
}



// -----------------------------------------------------------------------------
//
// corDbHistoryInstanceIndex - where the instance with this instanceId is; -1: not there
//
// The instanceId is in the instance's record, not beside it in the index: found by decoding - the
// temporal API's instance operations are rare, a reader's scan is not.
//
int corDbHistoryInstanceIndex(CorDbHistAttr* aP, const char* instanceId, CorAlloc* kaP)
{
  for (int i = 0; i < aP->instances; i++)
  {
    CorNode* instP = corDbHistoryInstanceDecode(&aP->instanceV[i], kaP);
    CorNode* idP   = (instP != NULL) ? corTreeLookup(instP, "instanceId") : NULL;

    if ((idP != NULL) && (idP->type == CorString) && (strcmp(idP->value.s, instanceId) == 0))
      return i;
  }

  return -1;
}



// -----------------------------------------------------------------------------
//
// corDbHistoryInstanceRemoveAt / corDbHistoryInstanceReplaceAt -
//
void corDbHistoryInstanceRemoveAt(CorDbHistEntity* eP, CorDbHistAttr* aP, int ix)
{
  memmove(&aP->instanceV[ix], &aP->instanceV[ix + 1], (aP->instances - ix - 1) * sizeof(CorDbInstance));
  --aP->instances;

  if (aP->instances == 0)
    attrUnlink(eP, aP);
}

bool corDbHistoryInstanceReplaceAt(CorDbHistory* hP, CorDbHistAttr* aP, int ix, CorDbHistRecord* recP)
{
  CorDbInstance* iP   = &aP->instanceV[ix];
  char*          body = arenaCopy(hP, recP->body, recP->bodyLen);

  if (body == NULL)
    return false;

  iP->body         = body;                           // the old one stays in the arena, unreferenced
  iP->bodyLen      = recP->bodyLen;
  iP->observedAtNs = recP->observedAtNs;
  iP->createdAtNs  = recP->createdAtNs;
  iP->modifiedAtNs = recP->modifiedAtNs;
  iP->deletedAtNs  = recP->deletedAtNs;

  return true;
}



// -----------------------------------------------------------------------------
//
// corDbHistoryOpApply - a temporal-API write's record, applied to the index: what a live write does
// after logging it, and what recovery does replaying it - one code for both
//
// { id, histOp, attr?, datasetId?, deleteAll?, instanceId?, instance? }, histOp one of entityRemoved,
// attrRemoved, instanceRemoved, instanceModified. Returns TROE-style: 0 done, -2 nothing to apply it to.
//
int corDbHistoryOpApply(CorDbHistory* hP, CorNode* opP, CorAlloc* kaP)
{
  CorNode* idP   = corTreeLookup(opP, "id");
  CorNode* hOpP  = corTreeLookup(opP, "histOp");
  CorNode* attrP = corTreeLookup(opP, "attr");

  if ((idP == NULL) || (idP->type != CorString) || (hOpP == NULL) || (hOpP->type != CorString))
    return -1;

  CorDbHistEntity* eP = corDbHistoryEntity(hP, idP->value.s, NULL, false);

  if (eP == NULL)
    return -2;

  if (strcmp(hOpP->value.s, "entityRemoved") == 0)
  {
    corDbHistoryEntityRemove(hP, eP);
    return 0;
  }

  CorDbHistAttr* aP = ((attrP != NULL) && (attrP->type == CorString)) ? corDbHistoryAttrLookup(eP, attrP->value.s) : NULL;

  if (aP == NULL)
    return -2;

  if (strcmp(hOpP->value.s, "attrRemoved") == 0)
  {
    CorNode* dsP  = corTreeLookup(opP, "datasetId");
    CorNode* allP = corTreeLookup(opP, "deleteAll");

    corDbHistoryAttrRemove(eP, aP, ((dsP != NULL) && (dsP->type == CorString)) ? dsP->value.s : NULL,
                           (allP != NULL) && (allP->type == CorBoolean) && allP->value.b);
    return 0;
  }

  CorNode* instanceIdP = corTreeLookup(opP, "instanceId");
  int      ix          = ((instanceIdP != NULL) && (instanceIdP->type == CorString)) ? corDbHistoryInstanceIndex(aP, instanceIdP->value.s, kaP) : -1;

  if (ix < 0)
    return -2;

  if (strcmp(hOpP->value.s, "instanceRemoved") == 0)
  {
    corDbHistoryInstanceRemoveAt(eP, aP, ix);
    return 0;
  }

  if (strcmp(hOpP->value.s, "instanceModified") == 0)
  {
    CorNode*        instP = corTreeLookup(opP, "instance");
    CorDbHistRecord rec;

    if ((instP == NULL) || (corDbHistoryRecordEncode(eP->id, eP->type, aP->name, aP->instanceV[ix].datasetId, instP, 0, kaP, &rec) == false))
      return -1;

    return corDbHistoryInstanceReplaceAt(hP, aP, ix, &rec) ? 0 : -1;
  }

  return -1;
}



// -----------------------------------------------------------------------------
//
// corDbHistoryFree -
//
void corDbHistoryFree(CorDbHistory* hP)
{
  CorDbHistEntity* nextEP;

  for (CorDbHistEntity* eP = hP->first; eP != NULL; eP = nextEP)
  {
    nextEP = eP->next;

    CorDbHistAttr* nextAP;

    for (CorDbHistAttr* aP = eP->attrs; aP != NULL; aP = nextAP)
    {
      nextAP = aP->next;

      if (aP->instanceV != aP->inlineV)
        free(aP->instanceV);
      free(aP);                                      // its name is interned: freed below
    }

    free(eP->typeV);
    free(eP->id);
    free(eP);
  }

  if (hP->byId != NULL)
    corHashRelease(hP->byId);

  if (hP->names != NULL)
    corHashRelease(hP->names);

  for (int i = 0; i < hP->namesN; i++)
    free(hP->nameV[i]);
  free(hP->nameV);

  CorDbHistChunk* nextCP;

  for (CorDbHistChunk* cP = hP->chunks; cP != NULL; cP = nextCP)
  {
    nextCP = cP->next;
    free(cP);                                        // the bodies and dataset ids of every instance in it
  }

  memset(hP, 0, sizeof(*hP));
}
