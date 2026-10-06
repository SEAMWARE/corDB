//
// FILE            corDbDoc.c
//
// AUTHOR          Ken Zangelin
//
// Copyright 2026 Seamware
// SPDX-License-Identifier: Apache-2.0
//
#include <string.h>                                   // strcmp

#include "corLog/corLog.h"                            // COR_E
#include "corTree/CorNode.h"                          // CorNode
#include "corTree/corTreeChildReplace.h"              // corTreeChildReplace
#include "corTree/corTreeClone.h"                     // corTreeClone
#include "corTree/corTreeFree.h"                      // corTreeFree
#include "corTree/corTreeBuilder.h"                   // corTreeObject, corTreeString, corTreeChildAdd, ...
#include "corTree/corTreeLookup.h"                    // corTreeLookup
#include "corRest/CorRestState.h"                     // corRest

#include "db/DbDriver.h"                              // DB_OK, DB_ALREADY_EXISTS, DB_NOT_FOUND, DB_ERR
#include "corDB/corDbLog.h"                           // CorDbLogDocPut, CorDbLogDocDelete
#include "corDB/corDbPersist.h"                       // corDbPersistAppend
#include "corDB/corDbStore.h"                         // COR_DB_READ, COR_DB_WRITE, corDbStoreDocs
#include "corDB/corDbDoc.h"                           // Own interface



// -----------------------------------------------------------------------------
//
// docById - a document of a collection
//
static CorNode* docById(CorNode* collP, const char* docId)
{
  for (CorNode* dP = (collP != NULL) ? collP->value.head : NULL; dP != NULL; dP = dP->next)
  {
    CorNode* idP = corTreeLookup(dP, "id");

    if ((idP != NULL) && (idP->type == CorString) && (strcmp(idP->value.s, docId) == 0))
      return dP;
  }

  return NULL;
}



// -----------------------------------------------------------------------------
//
// docClone - the document as stored: its "id" first, set to docId, whatever the tree says
//
static CorNode* docClone(const char* docId, CorNode* docP)
{
  CorNode* cloneP = corTreeObject(NULL, NULL);

  if (cloneP == NULL)
    return NULL;

  corTreeChildAdd(cloneP, corTreeString(NULL, "id", docId));

  for (CorNode* mP = docP->value.head; mP != NULL; mP = mP->next)
  {
    if ((mP->name != NULL) && (strcmp(mP->name, "id") == 0))
      continue;

    CorNode* mCloneP = corTreeClone(NULL, mP);

    if (mCloneP == NULL)
    {
      corTreeFree(cloneP);
      return NULL;
    }

    corTreeChildAdd(cloneP, mCloneP);
  }

  return cloneP;
}



// -----------------------------------------------------------------------------
//
// logPut / logDelete - the record of a write: { id, collection, doc } / { id, collection }
//
// The record names the stored document without owning it: 'doc' is linked in for the encoding and
// unlinked after - a node of the store is never moved.
//
static void logPut(CorDbPersist* persistP, const char* collection, const char* docId, CorNode* storedP)
{
  if (persistP == NULL)
    return;

  CorNode  rec  = { 0 };
  CorNode  id   = { 0 };
  CorNode  coll = { 0 };
  CorNode  doc  = *storedP;                           // a shallow copy: same members, its own name and link

  rec.type     = CorObject;
  id.type      = CorString;  id.name   = (char*) "id";          id.value.s   = (char*) docId;
  coll.type    = CorString;  coll.name = (char*) "collection";  coll.value.s = (char*) collection;
  doc.name     = (char*) "doc";
  doc.next     = NULL;

  rec.value.head = &id;
  id.next        = &coll;
  coll.next      = &doc;
  rec.value.tail = &doc;

  corDbPersistAppend(persistP, CorDbLogDocPut, &rec);
}

static void logDelete(CorDbPersist* persistP, const char* collection, const char* docId)
{
  if (persistP == NULL)
    return;

  CorNode  rec  = { 0 };
  CorNode  id   = { 0 };
  CorNode  coll = { 0 };

  rec.type     = CorObject;
  id.type      = CorString;  id.name   = (char*) "id";          id.value.s   = (char*) docId;
  coll.type    = CorString;  coll.name = (char*) "collection";  coll.value.s = (char*) collection;

  rec.value.head = &id;
  id.next        = &coll;
  rec.value.tail = &coll;

  corDbPersistAppend(persistP, CorDbLogDocDelete, &rec);
}



// -----------------------------------------------------------------------------
//
// corDbDocCreate -
//
int corDbDocCreate(Tenant* tenantP, const char* collection, const char* docId, CorNode* docP)
{
  COR_DB_WRITE(tenantP);

  CorNode* collP = corDbStoreDocs(corDbLockedStore, collection, true);

  if (collP == NULL)
    return DB_ERR;

  if (docById(collP, docId) != NULL)
    return DB_ALREADY_EXISTS;

  CorNode* cloneP = docClone(docId, docP);

  if (cloneP == NULL)
  {
    COR_E("corDB: out of memory for document '%s' (%s)", docId, collection);
    return DB_ERR;
  }

  corTreeChildAdd(collP, cloneP);
  logPut(corDbLockedStore->persistP, collection, docId, cloneP);

  return DB_OK;
}



// -----------------------------------------------------------------------------
//
// corDbDocRetrieve -
//
int corDbDocRetrieve(Tenant* tenantP, const char* collection, const char* docId, CorNode** docPP)
{
  return corDbDocRetrieveIn(tenantP, collection, docId, corRest.kallocP, docPP);
}



// -----------------------------------------------------------------------------
//
// corDbDocRetrieveIn - corDbDocRetrieve, the copy in 'allocP' (at start there is no request)
//
int corDbDocRetrieveIn(Tenant* tenantP, const char* collection, const char* docId, CorAlloc* allocP, CorNode** docPP)
{
  COR_DB_READ(tenantP);

  CorNode* dP = docById(corDbStoreDocs(corDbLockedStore, collection, false), docId);

  if (dP == NULL)
    return DB_NOT_FOUND;

  *docPP = corTreeClone(allocP, dP);

  return (*docPP != NULL) ? DB_OK : DB_ERR;
}



// -----------------------------------------------------------------------------
//
// corDbDocQuery -
//
int corDbDocQuery(Tenant* tenantP, const char* collection, CorNode** arrayPP)
{
  return corDbDocQueryIn(tenantP, collection, corRest.kallocP, arrayPP);
}



// -----------------------------------------------------------------------------
//
// corDbDocQueryIn - corDbDocQuery, the copies in 'allocP' (at start there is no request)
//
int corDbDocQueryIn(Tenant* tenantP, const char* collection, CorAlloc* allocP, CorNode** arrayPP)
{
  COR_DB_READ(tenantP);

  CorNode* collP  = corDbStoreDocs(corDbLockedStore, collection, false);
  CorNode* arrayP = corTreeArray(allocP, NULL);

  for (CorNode* dP = (collP != NULL) ? collP->value.head : NULL; dP != NULL; dP = dP->next)
    corTreeChildAdd(arrayP, corTreeClone(allocP, dP));

  *arrayPP = arrayP;
  return DB_OK;
}



// -----------------------------------------------------------------------------
//
// corDbDocReplace -
//
int corDbDocReplace(Tenant* tenantP, const char* collection, const char* docId, CorNode* docP)
{
  COR_DB_WRITE(tenantP);

  CorNode* collP = corDbStoreDocs(corDbLockedStore, collection, false);
  CorNode* oldP  = docById(collP, docId);

  if (oldP == NULL)
    return DB_NOT_FOUND;

  CorNode* cloneP = docClone(docId, docP);

  if (cloneP == NULL)
  {
    COR_E("corDB: out of memory for document '%s' (%s)", docId, collection);
    return DB_ERR;
  }

  corTreeChildReplace(collP, oldP, cloneP);
  corTreeFree(oldP);
  logPut(corDbLockedStore->persistP, collection, docId, cloneP);

  return DB_OK;
}



// -----------------------------------------------------------------------------
//
// corDbDocDelete -
//
int corDbDocDelete(Tenant* tenantP, const char* collection, const char* docId)
{
  COR_DB_WRITE(tenantP);

  CorNode* collP = corDbStoreDocs(corDbLockedStore, collection, false);
  CorNode* oldP  = docById(collP, docId);

  if (oldP == NULL)
    return DB_NOT_FOUND;

  corTreeChildRemove(collP, oldP);
  corTreeFree(oldP);
  logDelete(corDbLockedStore->persistP, collection, docId);

  return DB_OK;
}



// -----------------------------------------------------------------------------
//
// corDbDocMerge - a document's members set from a fragment, under one write lock: a member is set (replaced
// or added), a null member removed; "id" and "type" are not touched. DB_NOT_FOUND: no such document.
//
int corDbDocMerge(Tenant* tenantP, const char* collection, const char* docId, CorNode* fragmentP)
{
  COR_DB_WRITE(tenantP);

  CorNode* collP = corDbStoreDocs(corDbLockedStore, collection, false);
  CorNode* oldP  = docById(collP, docId);

  if (oldP == NULL)
    return DB_NOT_FOUND;

  CorNode* mergedP = corTreeClone(NULL, oldP);           // malloc - the store's

  if (mergedP == NULL)
  {
    COR_E("corDB: out of memory for document '%s' (%s)", docId, collection);
    return DB_ERR;
  }

  for (CorNode* fP = (fragmentP != NULL) ? fragmentP->value.head : NULL; fP != NULL; fP = fP->next)
  {
    if ((fP->name == NULL) || (strcmp(fP->name, "id") == 0) || (strcmp(fP->name, "type") == 0))
      continue;

    CorNode* currentP = corTreeLookup(mergedP, fP->name);

    if (fP->type == CorNull)
    {
      if (currentP != NULL)
      {
        corTreeChildRemove(mergedP, currentP);
        corTreeFree(currentP);
      }
      continue;
    }

    CorNode* cloneP = corTreeClone(NULL, fP);

    if (cloneP == NULL)
    {
      corTreeFree(mergedP);
      COR_E("corDB: out of memory for document '%s' (%s)", docId, collection);
      return DB_ERR;
    }

    if (currentP != NULL)
    {
      corTreeChildReplace(mergedP, currentP, cloneP);
      corTreeFree(currentP);
    }
    else
      corTreeChildAdd(mergedP, cloneP);
  }

  corTreeChildReplace(collP, oldP, mergedP);
  corTreeFree(oldP);
  logPut(corDbLockedStore->persistP, collection, docId, mergedP);

  return DB_OK;
}
