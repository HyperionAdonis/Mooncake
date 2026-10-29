# KVCacheStore Low Level backend

Mooncake can explicitly opt into the `kvcs-lowlevel` distributed-storage
adapter. Mooncake Master is the only authority for logical object existence,
size, replica identity, and commit state. A client selects a COMPLETE DFS
replica from Master metadata and accesses KVCS directly; KVCS is not queried
to build or prevalidate object metadata. EFC is deployed separately. Its socket
and shared-memory region must be visible to every Mooncake client using KVCS.
Size that region for the configured maximum value, batch size and SDK worker
counts according to the KVCacheStore deployment guide.
The presence of a socket never enables the adapter automatically.

## Initial scope

This first change supports **one KVCacheStore target and one value per object**.
The default target is the official G3.5 mountpoint at index 1. An explicit
single KVCacheStore target can instead be supplied through the EFC deployment
variables or `MOONCAKE_KVCS_EFC_CONFIG` YAML. Multiple mountpoints are rejected
at initialization rather than silently selecting one. Mooncake does not
inspect backend composition behind that target. The deployment's index must
match the configured EFC.

The value must be nonempty and fit the configured
`MOONCAKE_KVCS_MAX_VALUE_SIZE` (default 4 MiB). A larger value fails
validation **before a provider write**; there is no truncation or partial
large-object write. Chunking, manifests, and multi-target routing are separate
follow-up changes.

Set `MOONCAKE_DFS_FS_ADAPTER=kvcs-lowlevel` on the cooperating Mooncake
processes. The initial distributed-storage configuration is single-tenant;
each logical key is mapped to a printable, injective provider key.
KVCS Low Level Put is insert-only, so Upsert reuses Mooncake's existing
Master-owned Upsert state machine and performs a provider Delete followed by
Put while the DFS replica is PROCESSING. A failed replacement is revoked after
one best-effort provider cleanup confirms the value is absent; it does not
restore the previous KVCS value.
Single-key `Remove` and `BatchRemove` use a Master-owned reservation: Master
first checks the lease and replica state and hides the exact replica generation,
the client then deletes the KVCS value, and Master either commits the metadata
removal or restores the reserved replicas on provider failure. A missing
provider value is idempotent. The reservation blocks a same-key Put while
physical deletion is in flight. `RemoveByRegex` and `RemoveAll` return
`NOT_SUPPORTED` in this initial KVCS mode because provider-wide conditional
deletion is not defined.

The KVCS SDK is optional at build time. With no SDK, other Mooncake backends
build normally and explicit KVCS initialization returns `NOT_SUPPORTED`.
With the SDK installed, CMake discovers `kvcs` using `pkg-config`. Deploy
EFC using the official KVCacheStore tooling; Mooncake does not start EFC,
configure storage capacity, or modify `/dev/shm`.

The Low Level client reads these settings through Mooncake's shared environment
configuration path:

| Setting | Default |
| --- | --- |
| `MOONCAKE_KVCS_EFC_SOCKET` | `/var/run/kvcs/efc-grpc.sock` |
| `MOONCAKE_KVCS_MAX_VALUE_SIZE` | 4 MiB |
| `MOONCAKE_KVCS_MAX_KEY_SIZE` | 256 bytes |
| `MOONCAKE_KVCS_MAX_KEYS_PER_BATCH` | 8 |
| `MOONCAKE_KVCS_GET_WORKERS` | 1 |
| `MOONCAKE_KVCS_SET_WORKERS` | 1 |
| `MOONCAKE_KVCS_SIMPLE_WORKERS` | 1 |
| `MOONCAKE_KVCS_OPERATION_TIMEOUT_MS` | 30000 ms |

Reads use the Master descriptor's size and reject a shorter or longer KVCS
value. If Get explicitly returns `OBJECT_NOT_FOUND`, Mooncake conditionally
removes only the DFS replica observed by that read, matching its `ReplicaID`
under the Master metadata lock. A timeout, transport failure, or other KVCS
error keeps metadata unchanged. Master finalization follows the existing client
path without KVCS-specific retries. If a KVCS Put reports failure after it may
have stored the value, Mooncake makes one best-effort delete and reuses the
existing `PutRevoke` path only after the value is confirmed absent; it does not
maintain a client-local uncertain-key index.

## Temporary capacity source

The public KVCS Low Level API does not yet expose target capacity. As a
temporary workaround, Mooncake Master reads the EFC Prometheus endpoint and
maps the result directly to the existing DFS usage snapshot
(`used_bytes`/`capacity_bytes`). DFS metrics, watermark configuration, and the
eviction thread remain the only capacity-control path; the object-storage
adapter does not define a second capacity interface. The Prometheus names and
labels are isolated in `KvcsEfcMetricsCapacityProvider`. Replace only this
source when KVCS publishes a stable Low Level capacity API.

The provider selects exactly one G3.5 mountpoint using these samples:

```text
kvcs_efc_storage_capacity_bytes{tier="g35",state="total",mountpoint_id="..."}
kvcs_efc_storage_capacity_bytes{tier="g35",state="used",mountpoint_id="..."}
kvcs_efc_storage_ready{tier="g35",mountpoint_id="..."}
```

Missing, duplicate, malformed, unready, or inconsistent samples make capacity
unknown and never change metadata. Label order is irrelevant. The default
endpoint is `http://127.0.0.1:9093/metrics`; set
`MOONCAKE_KVCS_METRICS_ENDPOINT` when EFC is not reachable at that address.
The `mountpoint_id` label is taken from the existing single-target EFC
configuration; Mooncake does not maintain a second target configuration.

This change only feeds the existing DFS capacity snapshot. The follow-up
watermark path must reuse the existing DFS configuration and eviction thread.
Deleting a selected KVCS replica still requires deleting the KVCS value first
and then conditionally removing the same DFS replica metadata; it must not
introduce a client-local metadata index or a second decision loop.
