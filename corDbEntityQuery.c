//
// FILE            corDbEntityQuery.c
//
// AUTHOR          Ken Zangelin
//
// Copyright 2026 Seamware
// SPDX-License-Identifier: Apache-2.0
//
#include <regex.h>                                     // regcomp, regexec, regfree
#include <stdlib.h>                                    // strtod, qsort
#include <string.h>                                    // strcmp

#include "corAlloc/corAlloc.h"                        // corAlloc
#include "corTree/CorNode.h"                          // CorNode
#include "corTree/corTreeBuilder.h"                   // corTreeArray, corTreeClone, corTreeFloat, corTreeChildAdd
#include "corTree/corTreeClone.h"                     // corTreeClone
#include "corJson/corJsonRenderSize.h"                // corJsonFastRenderSize
#include "corTree/corTreeLookup.h"                    // corTreeLookup
#include "corLog/corLog.h"                            // COR_E
#include "corRest/CorRestState.h"                       // corRest
#include "corNgsild/LdQ.h"                              // LdQNode
#include "corNgsild/LdVocab.h"                         // LD_VOCAB_SCOPE
#include "corNgsild/LdScopeExpr.h"                     // LdScopeExpr
#include "corNgsild/LdGeoRel.h"                         // LdGeoRel, LdGeoNear
#include "corNgsild/ldEntityMatch.h"                    // ldEntityMatchType, ldEntityMatchScope, ldEntityMatchQ

#include "db/DbDriver.h"                              // DB_OK, Tenant
#include "corDB/corDbSysTimes.h"                      // corDbTreeIn, corDbTreeOut
#include "corDB/corDbStore.h"          // corDbEntities, corDbStoreOf
#include "corDB/corDbIndex.h"          // corDbIndexLookup, corDbIndexPrev, corDbEntityId
#include "corDB/corDbGeoMatch.h"       // corDbGeoMatch
#if COR_FEATURE_TYPE_INDEX
#include "corDB/corDbTypeIndex.h"                    // corDbTypeIndexOf, corDbTypeIndexHead, corDbTypeIndexRef
#endif
#include "corDB/corDbEntityQuery.h"    // Own interface



// Type, scope, and q-filter matchers are shared via ldEntityMatch.h



// matchStringV - check if value is in a NULL-terminated string array
//
static bool matchStringV(const char* value, char** strV)
{
  for (int ix = 0; strV[ix] != NULL; ix++)
  {
    if (strcmp(value, strV[ix]) == 0)
      return true;
  }
  return false;
}



// -----------------------------------------------------------------------------
//
// GeoCand - a matched entity plus its geo distance (metres; -1 if not a near
// query). Collected before pagination so a near query can be sorted by distance.
//
typedef struct GeoCand
{
  CorNode* eP;
  double  dist;
} GeoCand;



// geoCandCmp - ascending by distance (nearest first), for qsort
//
static int geoCandCmp(const void* a, const void* b)
{
  double da = ((const GeoCand*) a)->dist;
  double db = ((const GeoCand*) b)->dist;

  if (da < db) return -1;
  if (da > db) return  1;
  return 0;
}



// distCandCmp - § 7.6.2.2 sort-by-distance ordering. Entities that convey the
// GeoProperty (dist >= 0) rank ahead of those that do not (dist < 0), which sort
// last; among geo-bearing entities the order is ascending, or descending when
// distDescCand is set (dist-desc). qsort has no context arg → thread-local flag.
//
static __thread bool distDescCand;

static int distCandCmp(const void* a, const void* b)
{
  double da = ((const GeoCand*) a)->dist;
  double db = ((const GeoCand*) b)->dist;
  bool   ga = (da >= 0);
  bool   gb = (db >= 0);

  if (ga != gb) return ga ? -1 : 1;   // geo-bearing first
  if (!ga)      return 0;             // both lack the GeoProperty

  int cmp = (da < db) ? -1 : (da > db) ? 1 : 0;
  return distDescCand ? -cmp : cmp;
}



// -----------------------------------------------------------------------------
//
// qSubTimesUsed - does the q name a time the store may leave out: the entity's modifiedAt, an attribute's
// (or sub-attribute's) createdAt or modifiedAt?
//
static bool qSubTimesUsed(LdQNode* nodeP)
{
  if (nodeP == NULL)
    return false;

  if (nodeP->type == LdQTermNode)
  {
    if ((nodeP->term.subPathN == 0) && (strcmp(nodeP->term.attr, "modifiedAt") == 0))
      return true;

    for (int ix = 0; ix < nodeP->term.subPathN; ix++)
    {
      if ((strcmp(nodeP->term.subPathV[ix], "createdAt") == 0) || (strcmp(nodeP->term.subPathV[ix], "modifiedAt") == 0))
        return true;
    }

    return false;
  }

  if ((nodeP->type == LdQAndNode) || (nodeP->type == LdQOrNode))
  {
    for (int ix = 0; ix < nodeP->group.count; ix++)
    {
      if (qSubTimesUsed(nodeP->group.childV[ix]))
        return true;
    }
  }

  return false;
}


// -----------------------------------------------------------------------------
//
// entityMatch - does the stored entity match the query's filters? *geoDistanceP: its distance (-1: none)
//
static bool entityMatch(CorNode* eP, DbQueryFilter* filterP, DbQueryFilter* distFilterP, bool qSubTimes, double* geoDistanceP)
{
  CorNode* entityIdP = corTreeLookup(eP, "id");
  const char* entityId = (entityIdP != NULL && entityIdP->type == CorString) ? entityIdP->value.s : NULL;

  //
  // Filter by id
  //
  if (filterP != NULL && filterP->idV != NULL)
  {
    if (entityId == NULL || !matchStringV(entityId, filterP->idV))
      return false;
  }

  //
  // Filter by idPattern
  //
  if (filterP != NULL && filterP->idPattern != NULL)
  {
    regex_t re;

    if (entityId == NULL || regcomp(&re, filterP->idPattern, REG_EXTENDED | REG_NOSUB) != 0 || regexec(&re, entityId, 0, NULL, 0) != 0)
    {
      regfree(&re);
      return false;
    }

    regfree(&re);
  }

  //
  // Filter by type
  //
  if (filterP != NULL && filterP->typeExpr != NULL)
  {
    CorNode* typeP = corTreeLookup(eP, "type");

    if (!ldEntityMatchType(typeP, filterP->typeExpr))
      return false;
  }
  else if (filterP != NULL && filterP->typeV != NULL)
  {
    CorNode* typeP = corTreeLookup(eP, "type");

    if (typeP == NULL)
      return false;

    // Simple OR: entity type (string or array) must contain at least one of typeV
    bool found = false;

    if (typeP->type == CorString)
      found = matchStringV(typeP->value.s, filterP->typeV);
    else if (typeP->type == CorArray)
    {
      for (CorNode* elemP = typeP->value.head; elemP != NULL && !found; elemP = elemP->next)
      {
        if (elemP->type == CorString)
          found = matchStringV(elemP->value.s, filterP->typeV);
      }
    }

    if (!found)
      return false;
  }

  //
  // Filter by scope (scopeQ)
  //
  if (filterP != NULL && filterP->scopeExpr != NULL)
  {
    CorNode* scopeP = corTreeLookup(eP, LD_VOCAB_SCOPE);

    if (!ldEntityMatchScope(scopeP, filterP->scopeExpr))
      return false;
  }

  //
  // Filter by q expression
  //
  if (filterP != NULL && filterP->qExpr != NULL)
  {
    //
    // A q on the entity's modifiedAt or an attribute's own createdAt / modifiedAt reads what the store
    // may leave out - it is matched against the entity with every time in place (corDbSysTimes.h).
    // Rare: a copy each.
    //
    CorNode* matchP = (qSubTimes == true) ? corDbTreeOut(corRest.kallocP, eP, 0) : eP;

    if (!ldEntityMatchQ(matchP, filterP->qExpr))
      return false;
  }

  //
  // Geo-query filter (GEOS) — geoDistance is set for near queries, -1 otherwise
  //
  double geoDistance = -1;
  if (filterP != NULL && filterP->geoRel != NULL)
  {
    if (!corDbGeoMatch(eP, filterP, &geoDistance))
      return false;
  }
  else if (distFilterP != NULL)
  {
    // § 7.6.2.2 sort-by-distance (no filtering): distance from orderFrom to the
    // named GeoProperty. A missing/non-Point GeoProperty leaves geoDistance -1
    // (the entity is kept but ranks last).
    corDbGeoMatch(eP, distFilterP, &geoDistance);
  }

  *geoDistanceP = geoDistance;
  return true;
}



// -----------------------------------------------------------------------------
//
// candTake - a candidate of the walk: matched, it is kept; true: everything the request can use is in hand
// (see `needed` in entityQueryOver)
//
static inline bool candTake(CorNode* eP, DbQueryFilter* filterP, DbQueryFilter* distFilterP, bool qSubTimes, GeoCand* cands, int* nCandP, int needed)
{
  double geoDistance = -1;

  if (entityMatch(eP, filterP, distFilterP, qSubTimes, &geoDistance) == false)
    return false;

  cands[*nCandP].eP   = eP;
  cands[*nCandP].dist = geoDistance;
  *nCandP += 1;

  return (needed != 0) && (*nCandP >= needed);
}



#if COR_FEATURE_TYPE_INDEX
// -----------------------------------------------------------------------------
//
// indexType - the type whose list the query walks (corDbTypeIndex.h), or NULL: the store's list
//
// A type every matching entity has: the one type of a query by one type, or - an AND group, `(A;B)` - the
// member with the shortest list. A query by several types in OR (`A,B`, `A|(B;C)`) walks the store. *countP:
// the list's length (0 for a type no entity has - nothing matches).
//
static const char* indexType(CorDbStore* storeP, DbQueryFilter* filterP, struct CorDbTypeIndex** ixPP, int* countP)
{
  char** typeV = NULL;

  if ((storeP == NULL) || (filterP == NULL))
    return NULL;

  if (filterP->typeExpr != NULL)
  {
    if (filterP->typeExpr->groupCount != 1)
      return NULL;
    typeV = filterP->typeExpr->groupV[0].typeV;
  }
  else if ((filterP->typeV != NULL) && (filterP->typeV[0] != NULL) && (filterP->typeV[1] == NULL))
    typeV = filterP->typeV;

  if ((typeV == NULL) || (typeV[0] == NULL))
    return NULL;

  struct CorDbTypeIndex* ixP = corDbTypeIndexOf(storeP);

  if (ixP == NULL)
    return NULL;

  const char* bestType  = NULL;
  int         bestCount = 0;

  for (int i = 0; typeV[i] != NULL; i++)
  {
    int count = 0;

    corDbTypeIndexHead(ixP, typeV[i], &count);

    if ((bestType == NULL) || (count < bestCount))
    {
      bestType  = typeV[i];
      bestCount = count;
    }
  }

  *ixPP   = ixP;
  *countP = bestCount;
  return bestType;
}
#endif



// -----------------------------------------------------------------------------
//
// entityQueryOver - the query over a list of entities: the store's (storeP, under its read lock), or the
// caller's (storeP NULL - corDbEntityQueryList: no seek, there is no index). The page's entities copied
// as corTreeClone copies them, and for each, what corDbEntityCopyFinish needs and its geoDistance (-1:
// none) - both put in after the lock
//
static int entityQueryOver(CorDbStore* storeP, CorNode* entities, DbQueryFilter* filterP, CorNode** arrayPP, int64_t** createdAtVP, double** distVP)
{
  bool qSubTimes = (filterP != NULL) && qSubTimesUsed(filterP->qExpr);   // once per query

  CorNode* arrayP  = corTreeArray(corRest.kallocP, NULL);
  int     limit    = (filterP != NULL) ? filterP->limit  : 0;
  bool    unpaged  = (filterP != NULL) ? filterP->unpaged : false;
  int     offset   = (filterP != NULL) ? filterP->offset : 0;

  //
  // needed - how many candidates can this request possibly use? 0 means "all".
  //
  // The store is in creation order and that is the order pagination runs in, so
  // a plain page needs the first offset+limit matches and NOTHING after them.
  // Walking the rest is work whose result is discarded: at 100 000 entities a
  // `?type=X&limit=20` cost 1 008 req/s while MongoDB, which stops at the page,
  // held 28 500.
  //
  // Four things make stopping early wrong, and each is a reason the whole set
  // is genuinely used rather than merely scanned:
  //
  //   unpaged   the caller orders the result ITSELF and then paginates it (an
  //             orderBy query, § 4.23) - the first N in store order are not the
  //             first N it will keep
  //   count     totalCount below is the size of the MATCHING set, which is not
  //             known until the last entity has been judged
  //   geoRel near / distGeoproperty
  //             both qsort the candidates before paginating, so the page comes
  //             from the distance order and every candidate can reach it
  //   limit 0   count-only, or no bound at all
  //
  int needed = 0;

  if ((limit > 0) && (!unpaged) && (filterP != NULL) &&
      (!filterP->count) &&
      (filterP->distGeoproperty == NULL) &&
      ((filterP->geoRel == NULL) || (filterP->geoRel->rel != LdGeoNear)))
    needed = offset + limit;

  //
  // Size the candidate array. When the answer is bounded this is the bound, and
  // the walk that used to count the whole store to size it is not needed either
  // - that walk was itself O(store) on every single query.
  //
  int total = 0;

#if COR_FEATURE_TYPE_INDEX
  //
  // A query by type walks its type's list - every entity it can match is in it, in the store's order
  //
  struct CorDbTypeIndex* ixP     = NULL;
  int                    ixCount = 0;
  const char*            ixType  = indexType(storeP, filterP, &ixP, &ixCount);
#endif

  if (needed == 0)
  {
#if COR_FEATURE_TYPE_INDEX
    if (ixType != NULL)
      total = ixCount;
    else
#endif
    for (CorNode* eP = entities->value.head; eP != NULL; eP = eP->next) total++;
  }
  else
    total = needed;

  GeoCand* cands = (GeoCand*) corAlloc(&corRest.kalloc, sizeof(GeoCand) * (total > 0 ? total : 1));
  int      nCand = 0;

  // § 7.6.2.2 sort-by-distance — a synthetic near filter reused per entity to
  // compute the distance from the orderFrom Point to the named GeoProperty.
  DbQueryFilter  distFilter;
  LdGeoRel       distRel = { LdGeoNear, -1, -1 };
  DbQueryFilter* distFilterP = NULL;
  if (filterP != NULL && filterP->distGeoproperty != NULL && filterP->geoRel == NULL)
  {
    distFilter             = *filterP;
    distFilter.geoRel      = &distRel;
    distFilter.geometry    = (char*) "Point";
    distFilter.coordinates = filterP->distFrom;
    distFilter.geoproperty = filterP->distGeoproperty;
    distFilterP            = &distFilter;
  }

  bool        seek    = false;
  bool        back    = false;
  CorNode*    startP  = entities->value.head;
#if COR_FEATURE_TYPE_INDEX
  CorDbTypeRef* startRefP = (ixType != NULL) ? corDbTypeIndexHead(ixP, ixType, &ixCount) : NULL;
#endif

#ifdef DB_QUERY_FILTER_SEEK
  //
  // A page from a position (DbQueryFilter.seekId): the store's list is in creation order, the order of
  // the pages. The position's entity is found by its id (the index, O(1)) and the walk starts right
  // after it - or, a page before the position, right before it and backwards, one index lookup per
  // step (corDbIndexPrev). An entity that is gone since (deleted, or deleted and created again - its
  // createdAt differs): the walk from the start of the list to where its createdAt is, O(position) as
  // an offset is - entities of one batch share a createdAt, and among those the id decides.
  //
  // In a type's list, the walk starts at the position's ref there: after it, or before it and backwards.
  // A position that is gone, or no longer of the type: the store's list, as above.
  //
  seek = (storeP != NULL) && (filterP != NULL) && (filterP->seekId != NULL);
  back = seek && filterP->seekBefore;

  if (seek)
  {
    CorNode* posP = corDbIndexLookup(storeP, filterP->seekId);

    if ((posP != NULL) && (corDbCreatedAt(posP, 0) == filterP->seekCreatedAt))
    {
      startP = back ? corDbIndexPrev(storeP, posP) : posP->next;
#if COR_FEATURE_TYPE_INDEX
      if (ixType != NULL)
      {
        CorDbTypeRef* posRefP = corDbTypeIndexRef(ixP, posP, ixType);

        if (posRefP != NULL)
          startRefP = back ? posRefP->prev : posRefP->next;
        else
          ixType = NULL;
      }
#endif
    }
    else
    {
      CorNode* lastBeforeP = NULL;

      for (startP = entities->value.head; startP != NULL; startP = startP->next)
      {
        int64_t     c  = corDbCreatedAt(startP, 0);
        const char* id = corDbEntityId(startP);

        if ((c > filterP->seekCreatedAt) || ((c == filterP->seekCreatedAt) && (id != NULL) && (strcmp(id, filterP->seekId) > 0)))
          break;
        lastBeforeP = startP;
      }

      if (back)
        startP = lastBeforeP;
#if COR_FEATURE_TYPE_INDEX
      ixType = NULL;
#endif
    }
  }
#endif

  (void) seek;

#if COR_FEATURE_TYPE_INDEX
  if (ixType != NULL)
  {
    for (CorDbTypeRef* refP = startRefP; refP != NULL; refP = back ? refP->prev : refP->next)
    {
      if (candTake(refP->entityP, filterP, distFilterP, qSubTimes, cands, &nCand, needed) == true)
        break;
    }

#ifdef DEBUG
    //
    // DEBUG builds: the store's walk too - the same candidates, in the same order, or the index is wrong
    // (an entity's types changed at a write site that does not tell it). Every functional test checks it.
    //
    int  checkN   = 0;
    bool mismatch = false;

    for (CorNode* eP = startP; eP != NULL; eP = back ? corDbIndexPrev(storeP, eP) : eP->next)
    {
      double geoDistance = -1;

      if (entityMatch(eP, filterP, distFilterP, qSubTimes, &geoDistance) == false)
        continue;

      if ((checkN >= nCand) || (cands[checkN].eP != eP))
      {
        mismatch = true;
        break;
      }

      ++checkN;

      if ((needed != 0) && (checkN >= needed))
        break;
    }

    if ((mismatch == true) || (checkN != nCand))
    {
      COR_E("corDB type index: type '%s' - %d candidates from its list, the store's walk differs (at %d)", ixType, nCand, checkN);
      abort();
    }
#endif
  }
  else
#endif
  for (CorNode* eP = startP; eP != NULL; eP = back ? corDbIndexPrev(storeP, eP) : eP->next)
  {
    if (candTake(eP, filterP, distFilterP, qSubTimes, cands, &nCand, needed) == true)
      break;
  }

  if (filterP != NULL && filterP->count)
    filterP->totalCount = nCand;

  //
  // A near query returns the matches nearest-first (mongo $geoNear does the
  // same). Sort by distance before paginating so offset/limit page over the
  // distance order, not the store order. Other georels keep store order.
  //
  if (filterP != NULL && filterP->geoRel != NULL && filterP->geoRel->rel == LdGeoNear)
    qsort(cands, nCand, sizeof(GeoCand), geoCandCmp);
  else if (filterP != NULL && filterP->distGeoproperty != NULL)
  {
    // § 7.6.2.2 sort-by-distance — geo-bearing entities nearest/farthest first,
    // non-geo entities last. Paginate over this order, like the near path.
    distDescCand = filterP->distDesc;
    qsort(cands, nCand, sizeof(GeoCand), distCandCmp);
  }

  //
  // Paginate (offset/limit) and render. limit == 0 means count-only, and
  // unpaged means every match - the caller orders and paginates it itself.
  //
  if ((limit > 0) || unpaged)
  {
    int from = unpaged ? 0 : offset;
    int n    = (from < nCand) ? nCand - from : 0;

    if ((unpaged == false) && (n > limit))
      n = limit;

    *createdAtVP = (int64_t*) corAlloc(corRest.kallocP, (n + 1) * sizeof(int64_t));
    *distVP      = (double*)  corAlloc(corRest.kallocP, (n + 1) * sizeof(double));

#ifdef DB_QUERY_FILTER_MAX_BYTES
    //
    // The byte budget (DbQueryFilter.maxBytes): each entity's rendered size, as
    // corJson would render the stored entity, counted BEFORE it is copied - the
    // copy is the memory the budget is there to bound. The page ends before the
    // entity that does not fit, so it stays a prefix of the result set.
    //
    int64_t maxBytes = (filterP != NULL) ? filterP->maxBytes : 0;
    int64_t bytes    = 0;
#endif

    for (int i = from, j = 0; j < n; i++, j++)
    {
#ifdef DB_QUERY_FILTER_MAX_BYTES
      if (maxBytes > 0)
      {
        int64_t size = corJsonFastRenderSize(cands[i].eP);

        if (bytes + size > maxBytes)
        {
          filterP->budgetHit = true;
          break;
        }

        bytes += size;
      }
#endif

#ifdef DB_QUERY_FILTER_IDS_ONLY
      //
      // The ids of the matches and nothing else (DbQueryFilter.idsOnly - an EntityMap): no copy of the
      // entity, under the lock or after it.
      //
      if (filterP->idsOnly)
      {
        CorNode* idP = corTreeLookup(cands[i].eP, "id");

        if ((idP != NULL) && (idP->type == CorString))
        {
          CorNode* stubP = corTreeObject(corRest.kallocP, NULL);
          corTreeChildAdd(stubP, corTreeString(corRest.kallocP, "id", idP->value.s));
          corTreeChildAdd(arrayP, stubP);
        }
        continue;
      }
#endif

      corTreeChildAdd(arrayP, corDbEntityCopy(corRest.kallocP, cands[i].eP, &(*createdAtVP)[j]));
      (*distVP)[j] = cands[i].dist;
    }
  }

  *arrayPP = arrayP;
  return DB_OK;
}



// -----------------------------------------------------------------------------
//
// entityQuery - the store's entities, under its read lock
//
static int entityQuery(Tenant* tenantP, DbQueryFilter* filterP, CorNode** arrayPP, int64_t** createdAtVP, double** distVP)
{
  COR_DB_READ(tenantP);

  return entityQueryOver(corDbStoreOf(tenantP), corDbEntities(tenantP), filterP, arrayPP, createdAtVP, distVP);
}



// -----------------------------------------------------------------------------
//
// finish - the copies' inherited times and geoDistance, put in after the lock
//
static int finish(int r, DbQueryFilter* filterP, CorNode** arrayPP, int64_t* createdAtV, double* distV)
{
  if ((r != DB_OK) || (createdAtV == NULL))
    return r;

#ifdef DB_QUERY_FILTER_IDS_ONLY
  if ((filterP != NULL) && filterP->idsOnly)          // ids only: nothing to finish
    return r;
#endif

  int j = 0;
  for (CorNode* eP = (*arrayPP)->value.head; eP != NULL; eP = eP->next, j++)
  {
    corDbEntityCopyFinish(corRest.kallocP, eP, createdAtV[j]);

    if (distV[j] >= 0)
      corTreeChildAdd(eP, corTreeFloat(corRest.kallocP, "geoDistance", distV[j]));
  }

  return r;
}



// -----------------------------------------------------------------------------
//
// corDbEntityQueryList - the store's query over the caller's entities, in the store's model (the entities
// at a point in time - corDbTimepoint.c): the same matching, ordering, paging and count
//
int corDbEntityQueryList(CorNode* listP, DbQueryFilter* filterP, CorNode** arrayPP)
{
  int64_t* createdAtV = NULL;
  double*  distV      = NULL;
  int      r          = entityQueryOver(NULL, listP, filterP, arrayPP, &createdAtV, &distV);

  return finish(r, filterP, arrayPP, createdAtV, distV);
}



// -----------------------------------------------------------------------------
//
// corDbEntityQuery - the copies under the store's lock, their inherited times (and geoDistance, after them)
// put in after it
//
int corDbEntityQuery(Tenant* tenantP, DbQueryFilter* filterP, CorNode** arrayPP)
{
#ifdef DB_QUERY_FILTER_SEEK
  if (filterP != NULL)
    filterP->seekable = true;          // a page from a position (DbQueryFilter.seekId) - see entityQuery
#endif

  int64_t* createdAtV = NULL;
  double*  distV      = NULL;
  int      r          = entityQuery(tenantP, filterP, arrayPP, &createdAtV, &distV);

  return finish(r, filterP, arrayPP, createdAtV, distV);
}
