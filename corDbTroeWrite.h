#ifndef CORDB_CORDBTROEWRITE_H_
#define CORDB_CORDBTROEWRITE_H_

//
// FILE            corDbTroeWrite.h
//
// AUTHOR          Ken Zangelin
//
// Copyright 2026 Seamware
// SPDX-License-Identifier: Apache-2.0
//
// The temporal API's own writes on corDB's history (the TroeDriver write entries, corDbTroe.c)
//
#include <stdbool.h>                                   // bool

#include "corTree/CorNode.h"                           // CorNode
#include "db/Tenant.h"                                 // Tenant
#include "troe/TroeDriver.h"                           // TroeEvent

extern int corDbTroeCreate(Tenant* tenantP, CorNode* rootP);
extern int corDbTroeAttrsAdd(Tenant* tenantP, const char* entityId, CorNode* rootP);
extern int corDbTroeDelete(Tenant* tenantP, const char* entityId);
extern int corDbTroeAttrDelete(Tenant* tenantP, const char* entityId, const char* attrName, const char* datasetId, bool deleteAll);
extern int corDbTroeInstanceModify(Tenant* tenantP, const char* entityId, const char* attrName, const char* instanceId, CorNode* rootP);
extern int corDbTroeInstanceDelete(Tenant* tenantP, const char* entityId, const char* attrName, const char* instanceId);

//
// corDbTroeHistoryImport - history from another store (TroeDriver.historyImport): the events written as
// they are - the source's instanceIds and times
//
extern int corDbTroeHistoryImport(const TroeEvent* listHead, int count);

#endif  // CORDB_CORDBTROEWRITE_H_
