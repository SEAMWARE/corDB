#ifndef CORDB_CORDBTENANTDROP_H_
#define CORDB_CORDBTENANTDROP_H_

//
// FILE            corDbTenantDrop.h
//
// AUTHOR          Ken Zangelin
//
// Copyright 2026 Seamware
// SPDX-License-Identifier: Apache-2.0
//
#include "db/Tenant.h"                                 // Tenant



// -----------------------------------------------------------------------------
//
// corDbTenantDrop - a tenant emptied and forgotten: its entities, subscriptions, registrations, documents
// and history freed, its log closed and its directory deleted (with --dbDir). The broker drops a
// Snapshot's tenant with it (§ 5.16, a snapshot deleted or purged). A later use of the tenant's name finds
// a new, empty store. DB_OK.
//
extern int corDbTenantDrop(Tenant* tenantP);

#endif  // CORDB_CORDBTENANTDROP_H_
