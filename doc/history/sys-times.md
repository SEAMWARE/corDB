# corDB system timestamps - history

How the system timestamps got to what the README's "System timestamps - one per created entity" says:
what measuring found, and what changed because of it. Newest first. Every throughput figure is coraine's
`test/perf/perfRun.sh`, release builds, broker on 8 cores, against the same code keeping every time.

## 2026-10-06 - the final form (cmp18)

- merge clones what goes into the store **before** the write lock and only moves it in under it - with or
  without inherited times: merge with every time kept went from ~119 000 to ~127 000 req/s
- the walk that puts the times back after the read lock does not go into values (corJsonld's value-kind
  bits, `0xF0`)
- the build switch (`COR_DB_SYS_TIMES`) is gone: a reader takes both forms, so an older store is read as
  it is - nothing to choose, nothing to migrate

Result: memory -21.4 % to -29.5 %; creates +2 % to +17 %, deletes +2 % to +13 %, patch at 50 connections
+4 % to +9 %; reads within ±3.4 %, merge and batch update with history -2.7 % / -3.6 % (the README's
tables).

## 2026-10-06 - the read lock holds a plain clone (cmp17)

- **corNgsild** (`ldEntityAttrsSet`) keeps an inherited `createdAt` inherited when it replaces an
  instance - no filling and dropping of times around it under the write lock - and completes the
  report's `preValue`. Without the latter, an untouched instance of a multi-instance attribute counted
  as written: `troe_corDB_attr_delete_dataset` fails, an extra history row for the other datasetId.
- **corTree** `corTreeCloneMarked`: the out-copy is corTree's own clone, a callback for a marked object -
  not a call across the library per node (`corTreeClone@plt`, `corTreeChildAdd@plt` per node).
- **Retrieve and query** copy under the read lock as a plain clone and put the times back after it: the
  lock holds the copy of the smaller entity, fewer nodes than with every time kept.
- The entity's `createdAt` was meant to be found in one hop at the store entity's front, but the id index
  puts `id` first (`idFirst`): the lookup had been falling back to a walk. Now the front is checked first
  and second.

Patch at 50 connections in RAM went from -13 % to +8.1 %. One run showed durable reads at -10 % to -14 %;
alternating builds afterwards showed -1 % to -7 %, which the value-skipping walk then took to within ±2 %.

## 2026-10-06 - marked objects (cmp16)

The out-copy checked every member's name to find a missing time - under the read lock, which a PATCH
takes twice (it retrieves the entity before and after the write). An object whose times are inherited is
now marked with a CorNode flag bit, and the copy looks at no member of an unmarked object. The append
filled the inherited times into the attributes it names before corNgsild's update and dropped them after,
in place. Queries and retrieve came even; patch at 50 connections stayed at -13 % (at one connection
even: the write lock), batch update with history at -21 % - the history copied the whole entity under
the write lock; copying only the changed attributes brought that to -1 %.

## 2026-10-06 - one time per created entity (cmp15)

The rule: an entity is created whole, with one time; below it a time is stored only where it differs
from the entity's `createdAt` - a modified attribute its `modifiedAt`, an added one both. Memory -21.7 %;
patch at 50 connections -19 % to -25 %, merge -9 % to -15 %, delete -11 %: the out-copy checked every
member's name, and the append copied each named attribute twice under the write lock.

## 2026-10-05 - each time inherited from its parent (cmp14)

An instance's `createdAt` left out where it was the entity's, a sub-attribute's where it was its
instance's; `modifiedAt` where it was the object's own `createdAt`. Memory -22 %; patch at 50 connections
-27.5 %, delete -19 %, merge -14 % in RAM: several lookups per object on every copy, in and out. Not
taken.

## 2026-10-05 - two integers beside the node

The first design kept `createdAt` and `modifiedAt` as two integers in a prefix beside every object.
Replaced by inheritance before it was measured.
