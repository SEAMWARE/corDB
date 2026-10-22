//
// FILE            corDbIndexes.c
//
// AUTHOR          Ken Zangelin
//
// Copyright 2026 Seamware
// SPDX-License-Identifier: Apache-2.0
//
// The declarations (corDbIndexes.h): --dbIndexes's first, then the admin API's. A declaration is an index in
// the store of every tenant it covers (corDbValueIndex.h), built by the first query that can use it - and at
// once in every store there is, when the admin API declares it. <id> is the declaration's: 8 hex digits of a
// CRC-32C of its attribute, type and tenant, as the mongoc plugin has it.
//
#include <stdbool.h>                                 // bool
#include <stdint.h>                                  // uint32_t, int64_t
#include <stdio.h>                                   // snprintf
#include <stdlib.h>                                  // free
#include <string.h>                                  // strcmp, strdup, strlen
#include <pthread.h>                                 // pthread_mutex_t

#include "corBase/corCrc32c.h"                       // corCrc32c
#include "corBase/corStringSplit.h"                  // corStringSplit
#include "corAlloc/CorAlloc.h"                       // CorAlloc
#include "corAlloc/corAlloc.h"                       // corAlloc
#include "corAlloc/corAllocBufferInit.h"             // corAllocBufferInit
#include "corAlloc/corAllocBufferReset.h"            // corAllocBufferReset
#include "corAlloc/corAllocStrdup.h"                 // corAllocStrdup
#include "corLog/corLog.h"                           // COR_E, COR_W
#include "corTree/CorNode.h"                         // CorNode
#include "corTree/corTreeBuilder.h"                  // corTreeArray, corTreeObject, corTreeString, corTreeInteger, corTreeBoolean, corTreeChildAdd
#include "corTree/corTreeLookup.h"                   // corTreeLookup
#include "corJson/CorJson.h"                         // CorJson
#include "corJson/corJsonCreate.h"                   // corJsonCreate
#include "corJson/corJsonParse.h"                    // corJsonParse
#include "corJsonld/corLdInit.h"                     // corLdCoreContext
#include "corJsonld/corLdExpand.h"                   // corLdExpand
#include "corRest/CorRestState.h"                    // corRest

#include "db/DbDriver.h"                             // DB_OK, DB_ERR, DB_NOT_FOUND, DB_ALREADY_EXISTS, DB_BAD_INPUT
#include "db/Tenant.h"                               // Tenant, tenant0, tenantList, tenantApiName

#include "corDB/corDbGlobals.h"                      // corDbIndexesOption
#include "corDB/corDbStore.h"                        // CorDbStore, corDbStoreRead, corDbStoreWrite, corDbStoreUnlock
#include "corDB/corDbDoc.h"                          // corDbDocCreate, corDbDocDelete, corDbDocQueryIn
#include "corDB/corDbValueIndex.h"                   // corDbValueIndexOf, corDbValueIndexDrop, corDbValueIndexStats
#include "corDB/corDbIndexes.h"                      // Own interface



// -----------------------------------------------------------------------------
//
// IndexDecl - one declaration
//
typedef struct IndexDecl
{
  char   id[12];         // 8 hex digits
  char*  attr;           // expanded
  char*  type;           // expanded - NULL: every type
  char*  tenant;         // NULL: every tenant
  bool   api;            // made through /admin/indexes (kept in the default tenant's store) - false: --dbIndexes
} IndexDecl;



// -----------------------------------------------------------------------------
//
// declV - the declarations, --dbIndexes's first. declN is read without the mutex, to know there are none.
//
static IndexDecl        declV[64];
static int              declN       = 0;
static pthread_mutex_t  declMutex   = PTHREAD_MUTEX_INITIALIZER;
static const int        declMax     = sizeof(declV) / sizeof(declV[0]);



// -----------------------------------------------------------------------------
//
// declIdSet - the id of a declaration: its attribute, type and tenant
//
static void declIdSet(IndexDecl* dP)
{
  uint32_t crc = corCrc32c(0, dP->attr, strlen(dP->attr) + 1);

  crc = corCrc32c(crc, (dP->type   != NULL)? dP->type   : "", (dP->type   != NULL)? strlen(dP->type)   + 1 : 1);
  crc = corCrc32c(crc, (dP->tenant != NULL)? dP->tenant : "", (dP->tenant != NULL)? strlen(dP->tenant) + 1 : 1);

  snprintf(dP->id, sizeof(dP->id), "%08x", crc);
}



// -----------------------------------------------------------------------------
//
// declLookup - the declaration of an id (declMutex held) - -1: none
//
static int declLookup(const char* id)
{
  for (int ix = 0; ix < declN; ix++)
  {
    if (strcmp(declV[ix].id, id) == 0)
      return ix;
  }

  return -1;
}



// -----------------------------------------------------------------------------
//
// declFree -
//
static void declFree(IndexDecl* dP)
{
  free(dP->attr);
  free(dP->type);
  free(dP->tenant);
}



// -----------------------------------------------------------------------------
//
// declAdd - a declaration appended (declMutex held): its strings copied. DB_OK, DB_ALREADY_EXISTS (the
// same attribute, type and tenant), DB_BAD_INPUT (more than declMax) - errorText says which
//
static int declAdd(const char* attr, const char* type, const char* tenant, bool api, char* errorText, int errorTextLen)
{
  IndexDecl d = { "", strdup(attr), (type != NULL)? strdup(type) : NULL, (tenant != NULL)? strdup(tenant) : NULL, api };

  declIdSet(&d);

  int ix = declLookup(d.id);
  if (ix >= 0)
  {
    snprintf(errorText, errorTextLen, "'%s' is declared already (%s, by %s)", attr, d.id, declV[ix].api? "the admin API" : "--dbIndexes");
    declFree(&d);
    return DB_ALREADY_EXISTS;
  }

  if (declN >= declMax)
  {
    snprintf(errorText, errorTextLen, "%d declared indexes at most", declMax);
    declFree(&d);
    return DB_BAD_INPUT;
  }

  declV[declN] = d;
  __atomic_store_n(&declN, declN + 1, __ATOMIC_RELEASE);
  return DB_OK;
}



// -----------------------------------------------------------------------------
//
// declCovers - does a declaration cover a tenant? A Snapshot's own tenant is the tenant it was taken on
//
static bool declCovers(IndexDecl* dP, Tenant* tenantP)
{
  return (dP->tenant == NULL) || (strcmp(dP->tenant, tenantApiName(tenantP)) == 0);
}



// -----------------------------------------------------------------------------
//
// storesEach - every tenant that has a store, the default one first: f returns false to stop
//
typedef bool (*StoreFunc)(Tenant* tenantP, CorDbStore* storeP, void* dataP);

static bool storesEach(StoreFunc f, void* dataP)
{
  for (Tenant* tP = &tenant0; tP != NULL; tP = (tP == &tenant0)? tenantList : tP->next)
  {
    if (__atomic_load_n(&tP->pluginData, __ATOMIC_ACQUIRE) == NULL)
      continue;

    if (f(tP, (CorDbStore*) tP->pluginData, dataP) == false)
      return false;
  }

  return true;
}



// -----------------------------------------------------------------------------
//
// rebuildIn - a store's value indexes dropped, under its write lock, and - for a declaration that covers the
// tenant - built again at once (dataP: the declaration; NULL: built by the next query that can use them)
//
static bool rebuildIn(Tenant* tenantP, CorDbStore* storeP, void* dataP)
{
  IndexDecl* dP = (IndexDecl*) dataP;

  (void) storeP;

  {
    CorDbStore* lockedP = corDbStoreWrite(tenantP);

    if (lockedP != NULL)
      corDbValueIndexDrop(lockedP);
    corDbStoreUnlock(&lockedP);
  }

  if ((dP != NULL) && (declCovers(dP, tenantP) == true))
  {
    CorDbStore* lockedP = corDbStoreRead(tenantP);

    if (lockedP != NULL)
      corDbValueIndexOf(lockedP, tenantP);
    corDbStoreUnlock(&lockedP);
  }

  return true;
}



// -----------------------------------------------------------------------------
//
// StatsWork / statsIn - a declaration's index in a store: its entries, bytes, and the query terms it served
//
typedef struct StatsWork
{
  IndexDecl*  declP;
  CorNode*    tenantsP;
} StatsWork;

static bool statsIn(Tenant* tenantP, CorDbStore* storeP, void* dataP)
{
  StatsWork*  wP       = (StatsWork*) dataP;
  int64_t     entries  = 0;
  int64_t     bytes    = 0;
  int64_t     accesses = 0;
  bool        built;

  (void) storeP;

  if (declCovers(wP->declP, tenantP) == false)
    return true;

  {
    CorDbStore* lockedP = corDbStoreRead(tenantP);

    built = (lockedP != NULL) && corDbValueIndexStats(lockedP, wP->declP->id, &entries, &bytes, &accesses);
    corDbStoreUnlock(&lockedP);
  }

  CorNode* tP = corTreeObject(corRest.kallocP, NULL);

  corTreeChildAdd(tP, corTreeString(corRest.kallocP, "tenant", tenantP->name));
  if (tenantP->snapshotId != NULL)
    corTreeChildAdd(tP, corTreeString(corRest.kallocP, "snapshot", tenantP->snapshotId));
  corTreeChildAdd(tP, corTreeBoolean(corRest.kallocP, "built", built));
  corTreeChildAdd(tP, corTreeInteger(corRest.kallocP, "entries", entries));
  corTreeChildAdd(tP, corTreeInteger(corRest.kallocP, "bytes", bytes));
  corTreeChildAdd(tP, corTreeInteger(corRest.kallocP, "accesses", accesses));
  corTreeChildAdd(wP->tenantsP, tP);

  return true;
}



// -----------------------------------------------------------------------------
//
// declToTree - a declaration as the admin API shows it; its tenants with stats (declMutex NOT held: the stores'
// locks are taken)
//
static CorNode* declToTree(IndexDecl* dP, bool stats)
{
  CorNode* declP = corTreeObject(corRest.kallocP, NULL);

  corTreeChildAdd(declP, corTreeString(corRest.kallocP, "id", dP->id));
  corTreeChildAdd(declP, corTreeString(corRest.kallocP, "attribute", dP->attr));
  if (dP->type != NULL)
    corTreeChildAdd(declP, corTreeString(corRest.kallocP, "type", dP->type));
  if (dP->tenant != NULL)
    corTreeChildAdd(declP, corTreeString(corRest.kallocP, "tenant", dP->tenant));
  corTreeChildAdd(declP, corTreeString(corRest.kallocP, "declaredBy", dP->api? "api" : "dbIndexes"));

  if (stats == true)
  {
    StatsWork w = { dP, corTreeArray(corRest.kallocP, "tenants") };

    storesEach(statsIn, &w);
    corTreeChildAdd(declP, w.tenantsP);
  }

  return declP;
}



// -----------------------------------------------------------------------------
//
// declSave - a declaration of the admin API kept in the default tenant's store, a document of its own
//
static int declSave(IndexDecl* dP)
{
  CorNode* docP = corTreeObject(corRest.kallocP, NULL);

  corTreeChildAdd(docP, corTreeString(corRest.kallocP, "attribute", dP->attr));
  if (dP->type != NULL)
    corTreeChildAdd(docP, corTreeString(corRest.kallocP, "type", dP->type));
  if (dP->tenant != NULL)
    corTreeChildAdd(docP, corTreeString(corRest.kallocP, "tenant", dP->tenant));

  return corDbDocCreate(&tenant0, "dbIndexes", dP->id, docP);
}



// -----------------------------------------------------------------------------
//
// declsLoad - the admin API's declarations, from the default tenant's store
//
static void declsLoad(void)
{
  static char  buf[4096];
  CorAlloc     alloc;
  CorNode*     arrayP = NULL;
  char         errorText[256];

  corAllocBufferInit(&alloc, buf, sizeof(buf), 16 * 1024, NULL, "dbIndexesLoad");

  if (corDbDocQueryIn(&tenant0, "dbIndexes", &alloc, &arrayP) == DB_OK)
  {
    for (CorNode* docP = arrayP->value.head; docP != NULL; docP = docP->next)
    {
      CorNode* attrP   = corTreeLookup(docP, "attribute");
      CorNode* typeP   = corTreeLookup(docP, "type");
      CorNode* tenantP = corTreeLookup(docP, "tenant");

      if ((attrP == NULL) || (attrP->type != CorString))
        continue;

      if (declAdd(attrP->value.s,
                  ((typeP   != NULL) && (typeP->type   == CorString))? typeP->value.s   : NULL,
                  ((tenantP != NULL) && (tenantP->type == CorString))? tenantP->value.s : NULL,
                  true, errorText, sizeof(errorText)) == DB_BAD_INPUT)
        COR_W("corDB: the declared index of '%s' is left out: %s", attrP->value.s, errorText);
    }
  }

  corAllocBufferReset(&alloc, false);
}



// -----------------------------------------------------------------------------
//
// optionDeclare - one declaration of --dbIndexes: its names expanded with the core @context (a name
// with a ':' is taken as it is)
//
static bool optionDeclare(CorAlloc* allocP, const char* attr, const char* type, const char* tenant)
{
  char  errorText[256];
  char* attrIri = corLdExpand(corLdCoreContext(), attr, allocP, NULL, NULL);
  char* typeIri = (type != NULL)? corLdExpand(corLdCoreContext(), type, allocP, NULL, NULL) : NULL;

  if (declAdd((attrIri != NULL)? attrIri : attr, (typeIri != NULL)? typeIri : type, tenant, false, errorText, sizeof(errorText)) != DB_OK)
  {
    COR_E("corDB: --dbIndexes: %s", errorText);
    return false;
  }

  return true;
}



// -----------------------------------------------------------------------------
//
// optionParse - --dbIndexes: attribute names, comma-separated - or a JSON array of
// { "attribute": , "type"?: , "tenant"?: }
//
static bool optionParse(const char* option)
{
  static char  buf[4096];
  CorAlloc     alloc;
  bool         ok = true;

  corAllocBufferInit(&alloc, buf, sizeof(buf), 16 * 1024, NULL, "dbIndexes");

  char* text = corAllocStrdup(&alloc, option);

  while ((*text == ' ') || (*text == '\t'))
    text++;

  if (*text == '[')
  {
    CorJson  cj;

    corJsonCreate(&cj, &alloc);

    CorNode* treeP = corJsonParse(&cj, text);

    if ((treeP == NULL) || (treeP->type != CorArray))
    {
      COR_E("corDB: --dbIndexes: not a JSON array of { \"attribute\", \"type\", \"tenant\" }");
      ok = false;
    }

    for (CorNode* declP = (treeP != NULL)? treeP->value.head : NULL; (ok == true) && (declP != NULL); declP = declP->next)
    {
      CorNode* attrP   = (declP->type == CorObject)? corTreeLookup(declP, "attribute") : NULL;
      CorNode* typeP   = (declP->type == CorObject)? corTreeLookup(declP, "type")      : NULL;
      CorNode* tenantP = (declP->type == CorObject)? corTreeLookup(declP, "tenant")    : NULL;

      if ((attrP == NULL) || (attrP->type != CorString) || ((typeP != NULL) && (typeP->type != CorString)) || ((tenantP != NULL) && (tenantP->type != CorString)))
      {
        COR_E("corDB: --dbIndexes: each declaration is { \"attribute\": <string>, \"type\"?: <string>, \"tenant\"?: <string> }");
        ok = false;
      }
      else
        ok = optionDeclare(&alloc, attrP->value.s, (typeP != NULL)? typeP->value.s : NULL, (tenantP != NULL)? tenantP->value.s : NULL);
    }
  }
  else
  {
    char* nameV[64];
    int   nameN = corStringSplit(text, ',', nameV, sizeof(nameV) / sizeof(nameV[0]));

    for (int ix = 0; (ok == true) && (ix < nameN); ix++)
    {
      if (nameV[ix][0] != 0)
        ok = optionDeclare(&alloc, nameV[ix], NULL, NULL);
    }
  }

  corAllocBufferReset(&alloc, false);
  return ok;
}



// -----------------------------------------------------------------------------
//
// corDbIndexesInit -
//
bool corDbIndexesInit(void)
{
  if ((corDbIndexesOption != NULL) && (corDbIndexesOption[0] != 0) && (optionParse(corDbIndexesOption) == false))
    return false;

  declsLoad();
  return true;
}



// -----------------------------------------------------------------------------
//
// corDbIndexesMax -
//
int corDbIndexesMax(void)
{
  return declMax;
}



// -----------------------------------------------------------------------------
//
// corDbIndexesCovering -
//
int corDbIndexesCovering(Tenant* tenantP, CorDbIndexDecl* outV, int outMax)
{
  if (outV == NULL)
    return __atomic_load_n(&declN, __ATOMIC_ACQUIRE);

  int n = 0;

  pthread_mutex_lock(&declMutex);

  for (int ix = 0; (ix < declN) && (n < outMax); ix++)
  {
    if (declCovers(&declV[ix], tenantP) == false)
      continue;

    memcpy(outV[n].id, declV[ix].id, sizeof(outV[n].id));
    outV[n].attr = strdup(declV[ix].attr);
    outV[n].type = (declV[ix].type != NULL)? strdup(declV[ix].type) : NULL;

    if ((outV[n].attr == NULL) || ((declV[ix].type != NULL) && (outV[n].type == NULL)))
    {
      for (int jx = 0; jx <= n; jx++)
      {
        free(outV[jx].attr);
        free(outV[jx].type);
      }

      pthread_mutex_unlock(&declMutex);
      return -1;
    }

    ++n;
  }

  pthread_mutex_unlock(&declMutex);
  return n;
}



// -----------------------------------------------------------------------------
//
// corDbIndexList - the declarations copied, then each one's stores read with declMutex released (see
// corDbIndexAdd)
//
int corDbIndexList(CorNode** arrayPP)
{
  CorNode*    arrayP = corTreeArray(corRest.kallocP, NULL);
  IndexDecl*  copyV  = (IndexDecl*) corAlloc(corRest.kallocP, declMax * sizeof(IndexDecl));
  int         copyN  = 0;

  if (copyV == NULL)
    return DB_ERR;

  pthread_mutex_lock(&declMutex);

  for (int ix = 0; ix < declN; ix++, copyN++)
  {
    copyV[copyN]        = declV[ix];
    copyV[copyN].attr   = corAllocStrdup(corRest.kallocP, declV[ix].attr);
    copyV[copyN].type   = (declV[ix].type   != NULL)? corAllocStrdup(corRest.kallocP, declV[ix].type)   : NULL;
    copyV[copyN].tenant = (declV[ix].tenant != NULL)? corAllocStrdup(corRest.kallocP, declV[ix].tenant) : NULL;
  }

  pthread_mutex_unlock(&declMutex);

  for (int ix = 0; ix < copyN; ix++)
    corTreeChildAdd(arrayP, declToTree(&copyV[ix], true));

  *arrayPP = arrayP;
  return DB_OK;
}



// -----------------------------------------------------------------------------
//
// declRemove - the declaration of an id taken out of declV (declMutex held), into *dP; false: none
//
static bool declRemove(const char* id, IndexDecl* dP)
{
  int ix = declLookup(id);

  if (ix < 0)
    return false;

  *dP = declV[ix];

  for (int jx = ix; jx < declN - 1; jx++)
    declV[jx] = declV[jx + 1];
  __atomic_store_n(&declN, declN - 1, __ATOMIC_RELEASE);

  return true;
}



// -----------------------------------------------------------------------------
//
// corDbIndexAdd - a declaration of the admin API: kept, then its index built in every store of a tenant it
// covers
//
// No store's lock is waited for with declMutex held - a query that builds an index takes the mutex under its
// store's lock: the document is written, and the stores rebuilt, with the mutex released. The declaration
// is copied for that: another request may delete it meanwhile.
//
int corDbIndexAdd(const char* attr, const char* type, const char* tenant, CorNode** declPP, char* errorText, int errorTextLen)
{
  IndexDecl copy = { "", NULL, NULL, NULL, true };

  pthread_mutex_lock(&declMutex);

  int rc = declAdd(attr, type, tenant, true, errorText, errorTextLen);

  if (rc == DB_OK)
  {
    IndexDecl* dP = &declV[declN - 1];

    memcpy(copy.id, dP->id, sizeof(copy.id));
    copy.attr   = corAllocStrdup(corRest.kallocP, dP->attr);
    copy.type   = (dP->type   != NULL)? corAllocStrdup(corRest.kallocP, dP->type)   : NULL;
    copy.tenant = (dP->tenant != NULL)? corAllocStrdup(corRest.kallocP, dP->tenant) : NULL;
    *declPP     = declToTree(dP, false);
  }

  pthread_mutex_unlock(&declMutex);

  if (rc != DB_OK)
    return rc;

  int saved = declSave(&copy);

  if (saved != DB_OK)
  {
    IndexDecl d;

    snprintf(errorText, errorTextLen, "keeping the declaration in the default tenant's store: %s", (saved == DB_ALREADY_EXISTS)? "there already" : "failed");

    pthread_mutex_lock(&declMutex);
    bool removed = declRemove(copy.id, &d);
    pthread_mutex_unlock(&declMutex);

    if (removed == true)
      declFree(&d);

    return DB_ERR;
  }

  storesEach(rebuildIn, &copy);
  return DB_OK;
}



// -----------------------------------------------------------------------------
//
// corDbIndexDelete - a declaration of the admin API taken away: its document deleted, its indexes dropped.
// One of --dbIndexes is not (DB_BAD_INPUT): it is the start's, and comes back with it
//
int corDbIndexDelete(const char* id, char* errorText, int errorTextLen)
{
  IndexDecl  d;
  int        rc = DB_OK;

  pthread_mutex_lock(&declMutex);

  int ix = declLookup(id);

  if (ix < 0)
  {
    snprintf(errorText, errorTextLen, "no declared index '%s'", id);
    rc = DB_NOT_FOUND;
  }
  else if (declV[ix].api == false)
  {
    snprintf(errorText, errorTextLen, "'%s' is declared by --dbIndexes - taken out of it, it is gone at the next start", id);
    rc = DB_BAD_INPUT;
  }
  else
    declRemove(id, &d);

  pthread_mutex_unlock(&declMutex);

  if (rc != DB_OK)
    return rc;

  if (corDbDocDelete(&tenant0, "dbIndexes", id) == DB_ERR)
  {
    COR_E("corDB: the declared index '%s' is taken away, and its document in the default tenant's store is not: it is back at the next start", id);
  }

  storesEach(rebuildIn, NULL);                       // built again by the next query that can use them
  declFree(&d);

  return DB_OK;
}
