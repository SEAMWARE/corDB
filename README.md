# corDB

The in-process database of the [coraine](https://github.com/SEAMWARE/coraine) NGSI-LD context broker:
entities, subscriptions and registrations in the broker's own memory, one tree per tenant, no database
server. Two plugins:

| Plugin | Selected with | What it is |
|---|---|---|
| `corDB.so` | `--database corDB` | the current-state store |
| `troe/corDB.so` | `--troe corDB` | temporal history (TRoE) in the same process |

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

## Next

Persistence - a log and snapshots, so the store survives a restart - and corsh, the maintenance
tool: coraine's `doc/cordb-persistence.md`, [Ideas](https://github.com/SEAMWARE/coraine/blob/main/doc/ideas.md).
