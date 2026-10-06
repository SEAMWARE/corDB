#ifndef CORDB_CORDBCONTEXT_H_
#define CORDB_CORDBCONTEXT_H_

//
// FILE            corDbContext.h
//
// AUTHOR          Ken Zangelin
//
// Copyright 2026 Seamware
// SPDX-License-Identifier: Apache-2.0
//
// The broker's @context store (§ 5.13: hosted, cached and implicitly created @contexts) - the DbDriver's
// context functions. Broker-wide, not per tenant: a document collection of the default tenant,
// "jsonldContexts", one document { id, url, kind, body } per @context - persisted with that tenant's log.
//
#include "corAlloc/CorAlloc.h"                        // CorAlloc
#include "db/DbDriver.h"                              // DbContextRow



extern int corDbContextSave(const char* id, const char* url, int kind, const char* body);
extern int corDbContextDelete(const char* id);
extern int corDbContextList(CorAlloc* allocP, DbContextRow** rowsPP, int* countP);
extern int corDbContextGet(const char* id, CorAlloc* allocP, DbContextRow* rowOut);

#endif  // CORDB_CORDBCONTEXT_H_
