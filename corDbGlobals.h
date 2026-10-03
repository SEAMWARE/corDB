#ifndef CORDB_CORDBGLOBALS_H_
#define CORDB_CORDBGLOBALS_H_

//
// FILE            corDbGlobals.h
//
// AUTHOR          Ken Zangelin
//
// Copyright 2026 Seamware
// SPDX-License-Identifier: Apache-2.0
//
#include "corArgs/CorArg.h"                          // CorArg



// -----------------------------------------------------------------------------
//
// corDbArgV - the plugin's options: persistence (doc/persistence.md § 8)
//
extern CorArg corDbArgV[];



// -----------------------------------------------------------------------------
//
// --dbDir, --dbSync, --dbSyncInterval
//
extern char* corDbDir;                                 // NULL: no persistence - the in-RAM store
extern char* corDbSync;                                // interval | request | none
extern int   corDbSyncInterval;                        // ms

#endif  // CORDB_CORDBGLOBALS_H_