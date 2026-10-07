#ifndef CORDB_CORDBGLOBALS_H_
#define CORDB_CORDBGLOBALS_H_

#include <stdbool.h>                                   // bool

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
// --dbDir, --dbSync, --dbSyncInterval, --dbSnapshotEvery, --dbCompress
//
extern char* corDbDir;                                 // NULL: no persistence - the in-RAM store
extern char* corDbSync;                                // interval | request | none
extern int   corDbSyncInterval;                        // ms
extern int   corDbSnapshotEvery;                       // MiB of log
extern bool  corDbCompress;                            // snapshots and finished segments compressed (zstd)
extern char* corDbLockPrefer;                          // --dbLockPrefer reads|writes
extern bool  corDbLockWriters;                         // true: a store's lock lets writers in first (corDbInit)

#endif  // CORDB_CORDBGLOBALS_H_