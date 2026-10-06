#ifndef CORDB_CORDBSNAPSHOT_H_
#define CORDB_CORDBSNAPSHOT_H_

//
// FILE            corDbSnapshot.h
//
// AUTHOR          Ken Zangelin
//
// Copyright 2026 Seamware
// SPDX-License-Identifier: Apache-2.0
//
// NGSI-LD Snapshots (§ 5.16) - the DbDriver's snapshot functions. A Snapshot's description is a document of
// the originating tenant's "snapshots" collection (corDbDoc.h); its entities live in a tenant of their own,
// made by the broker (snapshotTenant.c), which drops it with corDbTenantDrop.
//
#include "corTree/CorNode.h"                          // CorNode
#include "db/Tenant.h"                                // Tenant



extern int corDbSnapshotCreate(Tenant* tenantP, const char* snapId, CorNode* snapP);
extern int corDbSnapshotQuery(Tenant* tenantP, CorNode** arrayPP);
extern int corDbSnapshotUpdate(Tenant* tenantP, const char* snapId, CorNode* fragmentP);
extern int corDbSnapshotDelete(Tenant* tenantP, const char* snapId);

#endif  // CORDB_CORDBSNAPSHOT_H_
