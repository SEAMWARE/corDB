//
// FILE            corDbSnapshot.c
//
// AUTHOR          Ken Zangelin
//
// Copyright 2026 Seamware
// SPDX-License-Identifier: Apache-2.0
//
#include "corTree/CorNode.h"                          // CorNode
#include "db/Tenant.h"                                // Tenant
#include "corDB/corDbDoc.h"                           // corDbDocCreate, corDbDocQuery, corDbDocMerge, corDbDocDelete
#include "corDB/corDbSnapshot.h"                      // Own interface



// -----------------------------------------------------------------------------
//
// corDbSnapshotCreate / Query / Update / Delete - the "snapshots" collection of the originating tenant
//
int corDbSnapshotCreate(Tenant* tenantP, const char* snapId, CorNode* snapP)
{
  return corDbDocCreate(tenantP, "snapshots", snapId, snapP);
}

int corDbSnapshotQuery(Tenant* tenantP, CorNode** arrayPP)
{
  return corDbDocQuery(tenantP, "snapshots", arrayPP);
}

int corDbSnapshotUpdate(Tenant* tenantP, const char* snapId, CorNode* fragmentP)
{
  return corDbDocMerge(tenantP, "snapshots", snapId, fragmentP);
}

int corDbSnapshotDelete(Tenant* tenantP, const char* snapId)
{
  return corDbDocDelete(tenantP, "snapshots", snapId);
}
