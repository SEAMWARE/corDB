//
// FILE            corDbLog.c
//
// AUTHOR          Ken Zangelin
//
// Copyright 2026 Seamware
// SPDX-License-Identifier: Apache-2.0
//
// A record (doc/persistence.md § 4), little-endian:
//
//   0   4  'c' 'r' version op
//   4   4  body length
//   8   8  sequence
//   16  8  system time, ns
//   24  4  CRC-32C of the body, continued over bytes 0-23 - body first, so a body can be encoded and
//          its CRC taken before the write lock, and only the header's 24 bytes are left for under it
//   28  n  body: the cor binary tree - the NGSI-LD codec, NO string tables, so it decodes on its own
//
#include <stdbool.h>                                   // bool
#include <stdint.h>                                    // uint32_t, uint64_t
#include <stdlib.h>                                    // realloc
#include <string.h>                                    // memcpy

#include "corBase/corCrc32c.h"                         // corCrc32c
#include "corTree/corTreeBin.h"                        // corTreeBinEncode, corTreeBinDecode
#include "corNgsild/ldBinCodec.h"                      // ldBinCodec

#include "corDbLog.h"                                  // Own interface

static const unsigned char VERSION = 1;



// -----------------------------------------------------------------------------
//
// room - make sure outP can take n more bytes
//
static bool room(CorBinBuffer* outP, int n)
{
  if (outP->len + n <= outP->size)
    return true;

  //
  // In 64 bits, and refused past an int: CorBinBuffer's length is one. Doubled in an int, a buffer
  // past 1 GiB overflowed to zero and the loop below never ended - a snapshot of a million entities
  // hung the broker's stop
  //
  long long need = (long long) outP->len + n;
  long long size = (outP->size == 0) ? 4096 : outP->size;

  if (need > 0x7FFFFFFF)
    return false;

  while (size < need)
    size *= 2;
  if (size > 0x7FFFFFFF)
    size = 0x7FFFFFFF;

  char* buf = realloc(outP->buf, (size_t) size);
  if (buf == NULL)
    return false;

  outP->buf  = buf;
  outP->size = (int) size;
  return true;
}



// -----------------------------------------------------------------------------
//
// header - the 28 bytes at h, for a body of bodyLen bytes whose CRC-32C is bodyCrc
//
static void header(char* h, CorDbLogOp op, uint64_t seq, uint64_t sysTimeNs, int bodyLen, uint32_t bodyCrc)
{
  uint32_t len = (uint32_t) bodyLen;

  h[0] = 'c';
  h[1] = 'r';
  h[2] = (char) VERSION;
  h[3] = (char) op;
  memcpy(&h[4],  &len,       4);
  memcpy(&h[8],  &seq,       8);
  memcpy(&h[16], &sysTimeNs, 8);

  uint32_t crc = corCrc32c(bodyCrc, h, 24);
  memcpy(&h[24], &crc, 4);
}



// -----------------------------------------------------------------------------
//
// corDbLogEncode -
//
bool corDbLogEncode(CorBinBuffer* outP, CorDbLogOp op, uint64_t seq, uint64_t sysTimeNs, CorNode* bodyP)
{
  int start = outP->len;

  if (room(outP, COR_DB_LOG_HEADER_LEN) == false)
    return false;

  outP->len += COR_DB_LOG_HEADER_LEN;               // the header is patched in after the body

  if ((bodyP != NULL) && (corTreeBinEncode(bodyP, &ldBinCodec, NULL, outP) == false))
  {
    outP->len = start;
    return false;
  }

  int      bodyLen = outP->len - start - COR_DB_LOG_HEADER_LEN;
  uint32_t bodyCrc = corCrc32c(0, &outP->buf[start + COR_DB_LOG_HEADER_LEN], bodyLen);

  header(&outP->buf[start], op, seq, sysTimeNs, bodyLen, bodyCrc);
  return true;
}



// -----------------------------------------------------------------------------
//
// corDbLogBodyEncode -
//
bool corDbLogBodyEncode(CorBinBuffer* outP, CorNode* bodyP, int* lenP, uint32_t* crcP)
{
  int start = outP->len;

  if (corTreeBinEncode(bodyP, &ldBinCodec, NULL, outP) == false)
  {
    outP->len = start;
    return false;
  }

  *lenP = outP->len - start;
  *crcP = corCrc32c(0, &outP->buf[start], *lenP);
  return true;
}



// -----------------------------------------------------------------------------
//
// corDbLogAppendEncoded -
//
bool corDbLogAppendEncoded(CorBinBuffer* outP, CorDbLogOp op, uint64_t seq, uint64_t sysTimeNs, const char* body, int bodyLen, uint32_t bodyCrc)
{
  if (room(outP, COR_DB_LOG_HEADER_LEN + bodyLen) == false)
    return false;

  char* h = &outP->buf[outP->len];

  memcpy(&h[COR_DB_LOG_HEADER_LEN], body, bodyLen);
  header(h, op, seq, sysTimeNs, bodyLen, bodyCrc);
  outP->len += COR_DB_LOG_HEADER_LEN + bodyLen;

  return true;
}



// -----------------------------------------------------------------------------
//
// corDbLogNext -
//
CorDbLogStatus corDbLogNext(const char* buf, int len, int* offsetP, CorAlloc* kaP, CorDbLogRecord* recP)
{
  int off = *offsetP;

  if (off >= len)
    return CorDbLogEnd;

  if (len - off < COR_DB_LOG_HEADER_LEN)
    return CorDbLogTorn;

  const char* h = &buf[off];
  uint32_t    bodyLen;
  uint32_t    crc;

  if ((h[0] != 'c') || (h[1] != 'r') || ((unsigned char) h[2] != VERSION))
    return CorDbLogTorn;

  memcpy(&bodyLen, &h[4], 4);
  if ((uint64_t) bodyLen > (uint64_t) (len - off - COR_DB_LOG_HEADER_LEN))
    return CorDbLogTorn;

  memcpy(&crc, &h[24], 4);
  if (corCrc32c(corCrc32c(0, &h[COR_DB_LOG_HEADER_LEN], bodyLen), h, 24) != crc)
    return CorDbLogTorn;

  recP->op = (CorDbLogOp) (unsigned char) h[3];
  memcpy(&recP->seq,       &h[8],  8);
  memcpy(&recP->sysTimeNs, &h[16], 8);
  recP->bodyP = NULL;

  if (bodyLen > 0)
  {
    const char* error = NULL;

    recP->bodyP = corTreeBinDecode(&h[COR_DB_LOG_HEADER_LEN], (int) bodyLen, &ldBinCodec, NULL, kaP, &error);
    if (recP->bodyP == NULL)
      return CorDbLogTorn;                           // a CRC-valid record that does not decode: a bug, not a torn write - stop all the same
  }

  *offsetP = off + COR_DB_LOG_HEADER_LEN + (int) bodyLen;
  return CorDbLogOk;
}
