#ifndef CORDB_CORDBTENANTRELEASE_H_
#define CORDB_CORDBTENANTRELEASE_H_

//
// FILE            corDbTenantRelease.h
//
// AUTHOR          Ken Zangelin
//
// Copyright 2026 Seamware
// SPDX-License-Identifier: Apache-2.0
//
#include "db/Tenant.h"                                 // Tenant



// -----------------------------------------------------------------------------
//
// corDbTenantRelease - what corDB hung on a tenant, freed: its store - its log closed first (with --dbDir:
// written, synced, out of the flusher's list; the files stay) - and the stores a drop retired
// (corDbTenantDrop). The broker calls it right before it frees the Tenant, when nothing - no request, no
// loop, no cache - can reach the tenant any more; for a Snapshot's tenant that is the snapshot's last
// unpin. DB_OK.
//
extern int corDbTenantRelease(Tenant* tenantP);

#endif  // CORDB_CORDBTENANTRELEASE_H_
