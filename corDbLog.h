#ifndef CORDB_CORDBLOG_H_
#define CORDB_CORDBLOG_H_

//
// FILE            corDbLog.h
//
// AUTHOR          Ken Zangelin
//
// Copyright 2026 Seamware
// SPDX-License-Identifier: Apache-2.0
//
// corDB's log records (doc/persistence.md § 3-4): a write's EFFECT, a 28-byte header and a cor
// binary tree. Every record decodes on its own.
//
#include <stdbool.h>                                   // bool
#include <stdint.h>                                    // uint64_t, uint32_t

#include "corAlloc/CorAlloc.h"                         // CorAlloc
#include "corTree/CorNode.h"                           // CorNode
#include "corTree/corTreeBin.h"                        // CorBinBuffer



// -----------------------------------------------------------------------------
//
// CorDbLogOp - what a record says the write did (§ 3)
//
typedef enum CorDbLogOp
{
  CorDbLogEntityPut    = 1,     // the whole entity, as stored
  CorDbLogAttrsPut     = 2,     // { "id": ..., "attrs": { the members it touched, each whole, after the change }, "deleted": [ names ]? }
  CorDbLogAttrsDelete  = 3,     // { "id": ..., "attrs": [ names ] }
  CorDbLogEntityDelete = 4,     // { "id": ... }
  CorDbLogSubPut       = 5,
  CorDbLogSubDelete    = 6,
  CorDbLogRegPut       = 7,
  CorDbLogRegDelete    = 8,
  CorDbLogBatchBegin   = 9,     // { "count": n } - replayed all or nothing
  CorDbLogBatchEnd     = 10,
  CorDbLogHistInstance = 11,     // a history record (corDbHistory.h): { id, type, attr, datasetId?, deletedAt?, instance } - hist-N.cor only
  CorDbLogDocPut       = 12,     // { "id": ..., "collection": ..., "doc": the whole document } - DbDriver docCreate/docReplace
  CorDbLogDocDelete    = 13      // { "id": ..., "collection": ... }
} CorDbLogOp;



// -----------------------------------------------------------------------------
//
// CorDbLogRecord - one record, decoded
//
typedef struct CorDbLogRecord
{
  CorDbLogOp  op;
  uint64_t    seq;              // per tenant, monotonic
  uint64_t    sysTimeNs;        // the system time of the write - createdAt/modifiedAt/deletedAt of what it wrote
  CorNode*    bodyP;
} CorDbLogRecord;

typedef enum CorDbLogStatus
{
  CorDbLogOk,                   // *recP filled, *offsetP past the record
  CorDbLogEnd,                  // no bytes left
  CorDbLogTorn                  // a short record or a bad CRC at *offsetP: everything from here is to be truncated
} CorDbLogStatus;

enum { COR_DB_LOG_HEADER_LEN = 28 };



// -----------------------------------------------------------------------------
//
// corDbLogEncode - append one record to outP (a growable malloc'd buffer, as corTreeBinEncode's)
//
extern bool corDbLogEncode(CorBinBuffer* outP, CorDbLogOp op, uint64_t seq, uint64_t sysTimeNs, CorNode* bodyP);



// -----------------------------------------------------------------------------
//
// corDbLogBodyEncode / corDbLogAppendEncoded - a record in two halves
//
// corDbLogBodyEncode appends only the body, and gives its length and CRC-32C: work that needs no
// sequence number, so it is done before the write lock. corDbLogAppendEncoded, under the lock,
// writes the header (sequence, time, the CRC finished over the header) and copies the body after it.
// The two halves together make the record corDbLogEncode makes.
//
extern bool corDbLogBodyEncode(CorBinBuffer* outP, CorNode* bodyP, int* lenP, uint32_t* crcP);
extern bool corDbLogAppendEncoded(CorBinBuffer* outP, CorDbLogOp op, uint64_t seq, uint64_t sysTimeNs, const char* body, int bodyLen, uint32_t bodyCrc);



// -----------------------------------------------------------------------------
//
// corDbLogNext - the record at *offsetP of buf; its tree from kaP, pointing into buf (buf must outlive it)
//
extern CorDbLogStatus corDbLogNext(const char* buf, int len, int* offsetP, CorAlloc* kaP, CorDbLogRecord* recP);

#endif  // CORDB_CORDBLOG_H_
