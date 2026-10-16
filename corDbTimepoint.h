#ifndef CORDB_CORDBTIMEPOINT_H_
#define CORDB_CORDBTIMEPOINT_H_

//
// FILE            corDbTimepoint.h
//
// AUTHOR          Ken Zangelin
//
// Copyright 2026 Seamware
// SPDX-License-Identifier: Apache-2.0
//
#include <stdint.h>                                    // uint64_t

#include "corTree/CorNode.h"                           // CorNode
#include "db/DbQueryFilter.h"                          // DbQueryFilter
#include "db/Tenant.h"                                 // Tenant
#include "troe/TroeDriver.h"                           // TROE_DRIVER_ENTITY_AT

#ifdef TROE_DRIVER_ENTITY_AT

// -----------------------------------------------------------------------------
//
// corDbTimepointRetrieve / corDbTimepointQuery - TroeDriver.entityAtRetrieve / entityAtQuery: the entity,
// the entities, as they were at atNs, in the store's model (corDbTimepoint.c)
//
extern int corDbTimepointRetrieve(Tenant* tenantP, const char* entityId, uint64_t atNs, int axis, CorNode** entityPP);
extern int corDbTimepointQuery(Tenant* tenantP, DbQueryFilter* filterP, uint64_t atNs, int axis, CorNode** arrayPP);

#endif

#endif  // CORDB_CORDBTIMEPOINT_H_
