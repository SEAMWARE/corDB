#ifndef CORDB_CORDBDOC_H_
#define CORDB_CORDBDOC_H_

//
// FILE            corDbDoc.h
//
// AUTHOR          Ken Zangelin
//
// Copyright 2026 Seamware
// SPDX-License-Identifier: Apache-2.0
//
// The document store of DbDriver (docCreate ... docDelete): per tenant, one array per collection under
// the store's "docs"; persisted as CorDbLogDocPut / CorDbLogDocDelete.
//
#include "corAlloc/CorAlloc.h"                        // CorAlloc
#include "corTree/CorNode.h"                         // CorNode
#include "db/Tenant.h"                               // Tenant



extern int corDbDocCreate(Tenant* tenantP, const char* collection, const char* docId, CorNode* docP);
extern int corDbDocRetrieve(Tenant* tenantP, const char* collection, const char* docId, CorNode** docPP);
extern int corDbDocQuery(Tenant* tenantP, const char* collection, CorNode** arrayPP);
extern int corDbDocReplace(Tenant* tenantP, const char* collection, const char* docId, CorNode* docP);
extern int corDbDocDelete(Tenant* tenantP, const char* collection, const char* docId);

// -----------------------------------------------------------------------------
//
// corDbDocMerge - a document's members set from a fragment, atomically: a member set, a null one removed,
// "id" and "type" untouched. DB_OK, DB_NOT_FOUND, DB_ERR.
//
extern int corDbDocMerge(Tenant* tenantP, const char* collection, const char* docId, CorNode* fragmentP);

// -----------------------------------------------------------------------------
//
// corDbDocRetrieveIn / corDbDocQueryIn - the copies in a given allocator (corDbDocRetrieve / Query: the
// request's) - for a caller with no request, at start
//
extern int corDbDocRetrieveIn(Tenant* tenantP, const char* collection, const char* docId, CorAlloc* allocP, CorNode** docPP);
extern int corDbDocQueryIn(Tenant* tenantP, const char* collection, CorAlloc* allocP, CorNode** arrayPP);

#endif  // CORDB_CORDBDOC_H_
