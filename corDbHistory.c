//
// FILE            corDbHistory.c
//
// AUTHOR          Ken Zangelin
//
// Copyright 2026 Seamware
// SPDX-License-Identifier: Apache-2.0
//
#include <stdbool.h>                                   // bool
#include <stdint.h>                                    // uint64_t
#include <stdlib.h>                                    // malloc, realloc, free
#include <string.h>                                    // strcmp, strdup, memcpy

#include "corLog/corLog.h"                             // COR_E
#include "corAlloc/CorAlloc.h"                         // CorAlloc
#include "corAlloc/corAlloc.h"                         // corAlloc
#include "corHash/corHash.h"                           // corHashTableCreate, corHashItemAdd, corHashItemLookup
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
    if ((entityType != NULL) && ((eP->type == NULL) || (strcmp(eP->type, entityType) != 0)))
      eP->type = intern(hP, entityType);               // the newest type the entity was written with

    return eP;
  }

  if (create == false)
    return NULL;

  if (((hP->byId == NULL) || (hP->count >= hP->slots * 2)) && (tableGrow(hP) == false))
    return NULL;

  eP = (CorDbHistEntity*) calloc(1, sizeof(CorDbHistEntity));
  if (eP == NULL)
    return NULL;

  eP->id   = strdup(entityId);
  eP->type = intern(hP, entityType);

  if ((eP->id == NULL) || ((entityType != NULL) && (eP->type == NULL)))
  {
    free(eP->id);
    free(eP);
    return NULL;
  }

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
static __thread CorBinBuffer scratch = { NULL, 0, 0 };

static bool encodeExact(CorNode* treeP, char** bodyPP, int* lenP)
{
  scratch.len = 0;

  if (corTreeBinEncode(treeP, &ldBinCodec, NULL, &scratch) == false)
    return false;

  char* bodyP = (char*) malloc(scratch.len);

  if (bodyP == NULL)
    return false;

  memcpy(bodyP, scratch.buf, scratch.len);
  *bodyPP = bodyP;
  *lenP   = scratch.len;

  return true;
}



// -----------------------------------------------------------------------------
//
// encodeInto - the same, copied into kaP (a request's arena): a record lives until it is queued,
// where it is copied again - no malloc of its own
//
static bool encodeInto(CorNode* treeP, CorAlloc* kaP, char** bodyPP, int* lenP)
{
  scratch.len = 0;

  if (corTreeBinEncode(treeP, &ldBinCodec, NULL, &scratch) == false)
    return false;

  char* bodyP = (char*) corAlloc(kaP, scratch.len);

  if (bodyP == NULL)
    return false;

  memcpy(bodyP, scratch.buf, scratch.len);
  *bodyPP = bodyP;
  *lenP   = scratch.len;

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
  return corDbHistoryEntityEventEncode(eP->id, eP->type, entityOp, atNs, kaP, outP);
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
