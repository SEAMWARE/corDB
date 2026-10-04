# corDB

The in-process database of the [coraine](https://github.com/SEAMWARE/coraine) NGSI-LD context broker:
entities, subscriptions and registrations in the broker's own memory, one tree per tenant, no database
server. Two plugins:

| Plugin | Selected with | What it is |
|---|---|---|
| `corDB.so` | `--database corDB` | the current-state store - and with `--troe corDB` the temporal history (TRoE), in the same store |
| `troe/corDB.so` | - | the former TRoE ring buffer: no longer loaded (`--troe corDB` takes its functions from `corDB.so`) |

What it measures against MongoDB is in coraine's
[performance and footprint](https://github.com/SEAMWARE/coraine/blob/main/doc/performance.md).

- **License:** [Apache License 2.0](LICENSE) - Copyright 2026 Seamware

## Building

A Cor-Lib like the others: a checkout beside its siblings (`../corTree`, `../corNgsild`, ...), and beside
`../coraine`, whose plugin interface (`src/lib/db/DbDriver.h`, `src/lib/troe/TroeDriver.h`) it
implements. The [corLibs](https://github.com/SEAMWARE/corLibs) umbrella builds it with the rest.

```console
make                    # debug (traces in), as coraine's debug broker
make BUILD=release      # release
make install            # into /opt/seamware/plugins/db/currentState and .../troe/temporal
```

A plugin is built with the broker's feature switches: `COR_FEATURE_SUBSCRIPTIONS=0` and
`COR_FEATURE_REGISTRATIONS=0` for a broker built without them. `OBJDIR` and `OUT` build a variant
elsewhere. Needs `libgeos_c` (geo-queries).

The NGSI-LD and Cor-Lib functions the plugins call are resolved from the broker at `dlopen` (it is
linked `-rdynamic`), so a plugin links none of them - and is the same for both of coraine's HTTP
servers.

## Tested

Through the broker: coraine's functional suite run with `-db corDB` (`corTest`), and in coraine's CI.

## Persistence

In RAM by default. With `--dbDir <directory>` the store survives a restart: every write appends its
effect to a log (per tenant), synced every 100 ms, with a snapshot as the log grows - taken in
slices, so a writer waits for it a few milliseconds at most. A clean stop loses nothing; `kill -9` at
most the last 100 ms (`--dbSync request`: nothing acknowledged). 100 000 entities are back in 0.2 s.

```console
coraine --database corDB --dbDir /var/lib/coraine
```

| Option | Default | |
|---|---|---|
| `--dbDir` | none: in RAM only | one subdirectory per tenant: `snap-N.cor`, `log-N.cor` |
| `--dbSync` | `interval` | `request`: a write answers once its record is synced; `none`: never synced |
| `--dbSyncInterval` | 100 | ms |
| `--dbSnapshotEvery` | 64 | MiB of log (and at least as much as the last snapshot) |

[The design](doc/persistence.md), [how it got here](doc/history/persistence.md); what it costs: coraine's
[performance](https://github.com/SEAMWARE/coraine/blob/main/doc/performance.md), "corDB on disk".

## History (TRoE)

`--database corDB --troe corDB`: every write that reaches an attribute appends an instance, captured
where current state changes; on disk with `--dbDir` (`hist-N.cor`). [The design](doc/troe.md),
[how it got here](doc/history/troe.md).

## Next

corsh, the maintenance tool ([the design](doc/persistence.md) § 10), and a PATCH that logs only the
attributes it touched. Further ideas: coraine's [Ideas](https://github.com/SEAMWARE/coraine/blob/main/doc/ideas.md).
