//
// FILE            corDbGlobals.c
//
// AUTHOR          Ken Zangelin
//
// Copyright 2026 Seamware
// SPDX-License-Identifier: Apache-2.0
//
#include "corArgs/CorArg.h"                          // CorArg

#include "corDB/corDbGlobals.h"           // Own interface



// -----------------------------------------------------------------------------
//
// --dbDir, --dbSync, --dbSyncInterval, --dbSnapshotEvery, --dbCompress
//
char* corDbDir          = NULL;
char* corDbSync         = "interval";
int   corDbSyncInterval = 100;
int   corDbSnapshotEvery = 64;
bool  corDbCompress      = false;



// -----------------------------------------------------------------------------
//
// corDbArgV - the plugin's options: persistence (doc/persistence.md § 8)
//
#if COR_DB_RAM_ONLY
//
// ramDB: corDB in RAM only - no disk, so no options for one: --dbDir is refused as the unknown option
// it is, not ignored
//
CorArg corDbArgV[] =
{
  CORARGS_END
};
#else
CorArg corDbArgV[] =
{
  { "--dbDir",          "-dbDir",          CorArgString, _vp &corDbDir,          CorArgOpt, NULL,           NULL,  NULL,       "persistent store directory (none: in RAM only)" },
  { "--dbSync",         "-dbSync",         CorArgString, _vp &corDbSync,         CorArgOpt, _vp "interval", NULL,  NULL,       "interval|request|none - when a write reaches the disk" },
  { "--dbSyncInterval", "-dbSyncInterval", CorArgInt,    _vp &corDbSyncInterval, CorArgOpt, _vp 100,        _vp 1, _vp 60000,  "ms between two syncs of the log" },
  { "--dbSnapshotEvery", "-dbSnapshotEvery", CorArgInt,  _vp &corDbSnapshotEvery, CorArgOpt, _vp 64,         _vp 1, _vp 65536,  "MiB of log after which a tenant is snapshotted" },
  { "--dbCompress",     "-dbCompress",     CorArgBool,   _vp &corDbCompress,     CorArgOpt, _vp false,      _vp false, _vp true, "compress the snapshots and the finished log segments (zstd, on the snapshot thread - the open segment never)" },
  CORARGS_END
};
#endif
