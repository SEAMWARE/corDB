//
// FILE            corDbSubscriptionStatsFlush.c
//
// AUTHOR          Ken Zangelin
//
// Copyright 2026 Seamware
// SPDX-License-Identifier: Apache-2.0
//
#include <string.h>                                   // strcmp

#include "corTree/CorNode.h"                          // CorNode
#include "corTree/corTreeBuilder.h"                   // corTreeObject, corTreeInteger, corTreeChildAdd
#include "corTree/corTreeLookup.h"                    // corTreeLookup

#include "db/DbDriver.h"                              // DB_OK, DB_NOT_FOUND, DB_ERR
#include "corDB/corDbLog.h"                           // CorDbLogSubPut
#include "corDB/corDbPersist.h"                       // corDbPersistAppend
#include "corDB/corDbStore.h"                         // COR_DB_WRITE, corDbSubscriptions
#include "corDB/corDbSubscriptionStatsFlush.h"        // Own interface



// -----------------------------------------------------------------------------
//
// intMember - the integer member 'name' of 'objectP', made if it is not there (NULL: out of memory)
//
static CorNode* intMember(CorNode* objectP, const char* name)
{
  CorNode* nP = corTreeLookup(objectP, name);

  if ((nP != NULL) && (nP->type == CorInt))
    return nP;

  if (nP != NULL)                                     // not an integer - taken over
  {
    nP->type    = CorInt;
    nP->value.i = 0;
    return nP;
  }

  nP = corTreeInteger(NULL, name, 0);
  if (nP != NULL)
    corTreeChildAdd(objectP, nP);

  return nP;
}



// -----------------------------------------------------------------------------
//
// corDbSubscriptionStatsFlush -
//
int corDbSubscriptionStatsFlush(Tenant* tenantP, const char* subId, int deltaSent, int deltaFailed, uint64_t lastNotification, uint64_t lastSuccess, uint64_t lastFailure)
{
  if ((deltaSent == 0) && (deltaFailed == 0) && (lastNotification == 0) && (lastSuccess == 0) && (lastFailure == 0))
    return DB_OK;

  COR_DB_WRITE(tenantP);

  CorNode* subscriptions = corDbSubscriptions(tenantP);
  CorNode* subP          = NULL;

  for (CorNode* sP = (subscriptions != NULL) ? subscriptions->value.head : NULL; sP != NULL; sP = sP->next)
  {
    CorNode* idP = corTreeLookup(sP, "id");

    if ((idP != NULL) && (idP->type == CorString) && (strcmp(idP->value.s, subId) == 0))
    {
      subP = sP;
      break;
    }
  }

  if (subP == NULL)
    return DB_NOT_FOUND;

  CorNode* notifP = corTreeLookup(subP, "notification");

  if ((notifP == NULL) || (notifP->type != CorObject))
  {
    notifP = corTreeObject(NULL, "notification");
    if (notifP == NULL)
      return DB_ERR;
    corTreeChildAdd(subP, notifP);
  }

  CorNode* nP;

  if ((deltaSent   != 0) && ((nP = intMember(notifP, "timesSent"))   != NULL))  nP->value.i += deltaSent;
  if ((deltaFailed != 0) && ((nP = intMember(notifP, "timesFailed")) != NULL))  nP->value.i += deltaFailed;

  if ((lastNotification != 0) && ((nP = intMember(notifP, "lastNotification")) != NULL))  nP->value.i = (long long) lastNotification;
  if ((lastSuccess      != 0) && ((nP = intMember(notifP, "lastSuccess"))      != NULL))  nP->value.i = (long long) lastSuccess;
  if ((lastFailure      != 0) && ((nP = intMember(notifP, "lastFailure"))      != NULL))  nP->value.i = (long long) lastFailure;

  corDbPersistAppend(corDbLockedStore->persistP, CorDbLogSubPut, subP);

  return DB_OK;
}
