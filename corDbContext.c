//
// FILE            corDbContext.c
//
// AUTHOR          Ken Zangelin
//
// Copyright 2026 Seamware
// SPDX-License-Identifier: Apache-2.0
//
#include <string.h>                                   // strcmp, memset

#include "corAlloc/CorAlloc.h"                        // CorAlloc
#include "corAlloc/corAlloc.h"                        // corAlloc, corAllocStrdup
#include "corTree/CorNode.h"                          // CorNode
#include "corTree/corTreeBuilder.h"                   // corTreeObject, corTreeString, corTreeInteger, corTreeChildAdd
#include "corTree/corTreeFree.h"                      // corTreeFree
#include "corTree/corTreeLookup.h"                    // corTreeLookup

#include "db/DbDriver.h"                              // DB_OK, DB_ALREADY_EXISTS, DbContextRow
#include "db/Tenant.h"                                // tenant0
#include "corDB/corDbDoc.h"                           // corDbDocCreate, corDbDocReplace, corDbDocDelete, corDbDocRetrieveIn, corDbDocQueryIn
#include "corDB/corDbContext.h"                       // Own interface



// -----------------------------------------------------------------------------
//
// rowFrom - a stored document as a DbContextRow, its strings in 'allocP'
//
static void rowFrom(CorNode* docP, CorAlloc* allocP, DbContextRow* rowP)
{
  memset(rowP, 0, sizeof(DbContextRow));

  for (CorNode* mP = docP->value.head; mP != NULL; mP = mP->next)
  {
    if (mP->name == NULL)
      continue;

    if      ((strcmp(mP->name, "id")   == 0) && (mP->type == CorString)) rowP->id   = corAllocStrdup(allocP, mP->value.s);
    else if ((strcmp(mP->name, "url")  == 0) && (mP->type == CorString)) rowP->url  = corAllocStrdup(allocP, mP->value.s);
    else if ((strcmp(mP->name, "body") == 0) && (mP->type == CorString)) rowP->body = corAllocStrdup(allocP, mP->value.s);
    else if ((strcmp(mP->name, "kind") == 0) && (mP->type == CorInt))    rowP->kind = (int) mP->value.i;
  }
}



// -----------------------------------------------------------------------------
//
// corDbContextSave - upsert by id
//
int corDbContextSave(const char* id, const char* url, int kind, const char* body)
{
  CorNode* docP = corTreeObject(NULL, NULL);

  if (docP == NULL)
    return DB_ERR;

  corTreeChildAdd(docP, corTreeString(NULL, "id", id));
  if (url != NULL)
    corTreeChildAdd(docP, corTreeString(NULL, "url", url));
  corTreeChildAdd(docP, corTreeInteger(NULL, "kind", kind));
  corTreeChildAdd(docP, corTreeString(NULL, "body", (body != NULL) ? body : ""));

  int r = corDbDocCreate(&tenant0, "jsonldContexts", id, docP);

  if (r == DB_ALREADY_EXISTS)
    r = corDbDocReplace(&tenant0, "jsonldContexts", id, docP);

  corTreeFree(docP);
  return r;
}



// -----------------------------------------------------------------------------
//
// corDbContextDelete -
//
int corDbContextDelete(const char* id)
{
  return corDbDocDelete(&tenant0, "jsonldContexts", id);
}



// -----------------------------------------------------------------------------
//
// corDbContextList - every stored @context, rows and strings in 'allocP'
//
int corDbContextList(CorAlloc* allocP, DbContextRow** rowsPP, int* countP)
{
  CorNode* arrayP = NULL;

  *rowsPP = NULL;
  *countP = 0;

  int r = corDbDocQueryIn(&tenant0, "jsonldContexts", allocP, &arrayP);

  if ((r != DB_OK) || (arrayP == NULL))
    return r;

  int n = 0;
  for (CorNode* dP = arrayP->value.head; dP != NULL; dP = dP->next)
    n++;

  if (n == 0)
    return DB_OK;

  DbContextRow* rowsV = (DbContextRow*) corAlloc(allocP, n * sizeof(DbContextRow));

  if (rowsV == NULL)
    return DB_ERR;

  int ix = 0;
  for (CorNode* dP = arrayP->value.head; dP != NULL; dP = dP->next, ix++)
    rowFrom(dP, allocP, &rowsV[ix]);

  *rowsPP = rowsV;
  *countP = n;
  return DB_OK;
}



// -----------------------------------------------------------------------------
//
// corDbContextGet - one stored @context by id, its strings in 'allocP'
//
int corDbContextGet(const char* id, CorAlloc* allocP, DbContextRow* rowOut)
{
  CorNode* docP = NULL;
  int      r    = corDbDocRetrieveIn(&tenant0, "jsonldContexts", id, allocP, &docP);

  if (r != DB_OK)
    return r;

  rowFrom(docP, allocP, rowOut);
  return DB_OK;
}
