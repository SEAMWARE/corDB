#ifndef CORDB_CORDBINDEXES_H_
#define CORDB_CORDBINDEXES_H_

//
// FILE            corDbIndexes.h
//
// AUTHOR          Ken Zangelin
//
// Copyright 2026 Seamware
// SPDX-License-Identifier: Apache-2.0
//
// The declared indexes (COR_FEATURE_DB_INDEXES - coraine doc/installation.md, "Declared indexes"): an
// attribute - narrowed to an entity type, to a tenant, if so declared - whose q value terms an index serves.
// Declared at start (--dbValueIndexes) or through the admin API (/admin/indexes - DbDriver indexList, indexAdd,
// indexDelete), the admin API's kept in the default tenant's document store (collection "dbIndexes", one
// document per declaration). The index itself is each store's: corDbValueIndex.h.
//
#include <stdbool.h>                                 // bool

#include "corTree/CorNode.h"                         // CorNode

#include "db/Tenant.h"                               // Tenant



// -----------------------------------------------------------------------------
//
// CorDbIndexDecl - a declaration, as a store's index copies it
//
typedef struct CorDbIndexDecl
{
  char   id[12];                                     // 8 hex digits
  char*  attr;                                       // expanded
  char*  type;                                       // expanded - NULL: every type
} CorDbIndexDecl;



// -----------------------------------------------------------------------------
//
// corDbIndexesInit - --dbValueIndexes, then the admin API's declarations; false: --dbValueIndexes is wrong (said)
//
// After the stores are loaded: the admin API's are in the default tenant's.
//
extern bool corDbIndexesInit(void);



// -----------------------------------------------------------------------------
//
// corDbIndexesCovering - the declarations that cover a tenant, copied into declV (malloc'd strings - the
// caller frees them); how many. -1: out of memory. 0 without a lock when nothing is declared.
//
extern int corDbIndexesCovering(Tenant* tenantP, CorDbIndexDecl* declV, int declMax);



// -----------------------------------------------------------------------------
//
// corDbIndexesMax - how many declarations there can be
//
extern int corDbIndexesMax(void);



// -----------------------------------------------------------------------------
//
// The DbDriver functions (db/DbDriver.h)
//
extern int corDbIndexList(CorNode** arrayPP);
extern int corDbIndexAdd(const char* attr, const char* type, const char* tenant, CorNode** declPP, char* errorText, int errorTextLen);
extern int corDbIndexDelete(const char* id, char* errorText, int errorTextLen);

#endif  // CORDB_CORDBINDEXES_H_
