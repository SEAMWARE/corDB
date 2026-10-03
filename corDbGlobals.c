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
// --dbDir, --dbSync, --dbSyncInterval
//
char* corDbDir          = NULL;
char* corDbSync         = "interval";
int   corDbSyncInterval = 100;



// -----------------------------------------------------------------------------
//
// corDbArgV - the plugin's options: persistence (doc/persistence.md § 8)
//
CorArg corDbArgV[] =
{
  { "--dbDir",          "-dbDir",          CorArgString, _vp &corDbDir,          CorArgOpt, NULL,           NULL,  NULL,       "persistent store directory (none: in RAM only)" },
  { "--dbSync",         "-dbSync",         CorArgString, _vp &corDbSync,         CorArgOpt, _vp "interval", NULL,  NULL,       "interval|request|none - when a write reaches the disk" },
  { "--dbSyncInterval", "-dbSyncInterval", CorArgInt,    _vp &corDbSyncInterval, CorArgOpt, _vp 100,        _vp 1, _vp 60000,  "ms between two syncs of the log" },
  CORARGS_END
};
