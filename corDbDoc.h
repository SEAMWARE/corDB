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
#include "corTree/CorNode.h"                         // CorNode
#include "db/Tenant.h"                               // Tenant



extern int corDbDocCreate(Tenant* tenantP, const char* collection, const char* docId, CorNode* docP);
extern int corDbDocRetrieve(Tenant* tenantP, const char* collection, const char* docId, CorNode** docPP);
extern int corDbDocQuery(Tenant* tenantP, const char* collection, CorNode** arrayPP);
extern int corDbDocReplace(Tenant* tenantP, const char* collection, const char* docId, CorNode* docP);
extern int corDbDocDelete(Tenant* tenantP, const char* collection, const char* docId);

#endif  // CORDB_CORDBDOC_H_
