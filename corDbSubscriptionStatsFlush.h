#ifndef CORDB_CORDBSUBSCRIPTIONSTATSFLUSH_H_
#define CORDB_CORDBSUBSCRIPTIONSTATSFLUSH_H_

//
// FILE            corDbSubscriptionStatsFlush.h
//
// AUTHOR          Ken Zangelin
//
// Copyright 2026 Seamware
// SPDX-License-Identifier: Apache-2.0
//
#include <stdint.h>                                   // uint64_t

#include "db/Tenant.h"                                // Tenant



// -----------------------------------------------------------------------------
//
// corDbSubscriptionStatsFlush - a subscription's notification statistics written back: timesSent and
// timesFailed increased by the deltas, lastNotification / lastSuccess / lastFailure set (0: not touched) -
// so they survive a restart. DB_OK, DB_NOT_FOUND.
//
extern int corDbSubscriptionStatsFlush(Tenant*     tenantP,
                                       const char* subId,
                                       int         deltaSent,
                                       int         deltaFailed,
                                       uint64_t    lastNotification,
                                       uint64_t    lastSuccess,
                                       uint64_t    lastFailure);

#endif  // CORDB_CORDBSUBSCRIPTIONSTATSFLUSH_H_
