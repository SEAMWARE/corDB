#ifndef CORDB_CORDBREPLAY_H_
#define CORDB_CORDBREPLAY_H_

//
// FILE            corDbReplay.h
//
// AUTHOR          Ken Zangelin
//
// Copyright 2026 Seamware
// SPDX-License-Identifier: Apache-2.0
//
#include <stdbool.h>                                   // bool

#include "corDB/corDbLog.h"                            // CorDbLogRecord
#include "corDB/corDbStore.h"                          // CorDbStore



// -----------------------------------------------------------------------------
//
// corDbReplay - one log record applied to a store (doc/persistence.md § 6)
//
// An effect, applied with no NGSI-LD semantics: an ENTITY_PUT of an entity the store has replaces it
// IN PLACE, so the store keeps creation order - the default query order - across a restart. The
// body is cloned into the store (malloc), so the record's own tree can go with its arena.
//
// The store is not published yet: no lock.
//
extern bool corDbReplay(CorDbStore* storeP, CorDbLogRecord* recP);

#endif  // CORDB_CORDBREPLAY_H_
