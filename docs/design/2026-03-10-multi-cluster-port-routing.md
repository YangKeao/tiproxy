# Proposal: Multi-Cluster PD Discovery and Port-Based Routing

- Author(s): [Yang Keao](https://github.com/YangKeao)
- Tracking Issue: N/A

## Abstract

This document describes the implemented design that allows one TiProxy instance to:
- discover TiDB instances from multiple PD clusters,
- route by TiProxy listening port using TiDB label `tiproxy-port`,
- dynamically add or remove backend PD clusters through the existing config API,
- optionally route DNS lookups per backend cluster via `ns-servers`.

The design keeps backward compatibility with legacy `proxy.pd-addrs` and static single-port routing.

## Background

Some deployments require one TiProxy to serve multiple independent TiDB/PD clusters while keeping strict routing isolation.
A common use case is:
- one frontend port maps to one backend cluster group,
- the target backend is selected only from TiDB instances with matching `tiproxy-port` label,
- configuration must be changed online by API without restarting TiProxy.

## Goals

- Keep backward compatibility with existing configuration.
- Support multiple backend clusters discovered from multiple PD endpoints.
- Support port-based routing with multiple frontend listening ports.
- Ensure route and rebalance stay inside the same routing group.
- Support dynamic add/remove of backend clusters through config API.
- Support per-cluster DNS server configuration for backend network access.

## Non-Goals

- Cross-group migration between different `tiproxy-port` groups.
- Making `proxy.port-range` or `balance.routing-rule` reloadable online.
- Defining a JSON merge-patch API for cluster list delta operations.

## Configuration Model

### Backend Clusters

`proxy.backend-clusters` is introduced as a reloadable list:

```toml
[[proxy.backend-clusters]]
name = "cluster-a"
pd-addrs = "10.0.1.1:2379,10.0.1.2:2379"
ns-servers = "10.0.10.2,10.0.10.3:53"

[[proxy.backend-clusters]]
name = "cluster-b"
pd-addrs = "10.0.2.1:2379,10.0.2.2:2379"
ns-servers = "10.0.20.2,10.0.20.3:53"
```

Validation rules:
- `name` must be non-empty and unique.
- `pd-addrs` must be a non-empty host:port list.
- `ns-servers` is optional; each entry must be a valid host[:port], default port is 53.

### Port Routing

```toml
[proxy]
addr = "0.0.0.0:6000"
port-range = [10000, 10512]

[balance]
routing-rule = "port"
```

Behavior:
- `proxy.port-range` expands one host to multiple listeners.
- `balance.routing-rule = "port"` groups backends by TiDB label `tiproxy-port`.
- A client connected to TiProxy port `P` is routed only to backends whose `tiproxy-port == P`.

### Backward Compatibility

If `proxy.backend-clusters` is empty, TiProxy falls back to legacy `proxy.pd-addrs` as one implicit cluster named `default`.

## Architecture

### Multi-Cluster Discovery

`MultiClusterFetcher` manages a dynamic map of cluster clients:
- builds one etcd client per configured cluster,
- supports per-cluster DNS dialer when `ns-servers` is configured,
- watches config changes and performs add/update/remove of cluster clients.

When fetching TiDB topology:
- results from all clusters are merged,
- each backend is tagged with synthetic label `tiproxy-cluster=<cluster-name>`,
- duplicate backend address from different clusters is rejected with warning and first one is kept.

### Dynamic Config Reload

`ConfigManager.SetTOMLConfig` does partial TOML overlay on current config, then validates and publishes.
`proxy.backend-clusters` itself is a full list field; when provided in PUT payload, it replaces the previous list.

Operationally, dynamic add/remove should use this workflow:
1. GET `/api/admin/config/?format=json` (or TOML).
2. Modify the full `proxy.backend-clusters` list to desired state.
3. PUT `/api/admin/config/` with updated TOML.

If new config is invalid, update is rejected and old runtime config is kept.

### DNS Routing (`ns-servers`)

`ClusterDialerManager` maintains one dialer per backend cluster:
- if `ns-servers` is empty, system/default resolver is used,
- if configured, Go resolver is forced with custom DNS servers,
- DNS server selection is round-robin,
- resolved host IPs are cached in-process (TTL 30s).

Usage points:
- PD/etcd access in multi-cluster fetcher,
- TiDB status/metrics HTTP requests,
- backend MySQL connection dial in proxy backend manager.

## Routing and Rebalance Semantics

### Route Selection

ClientInfo includes local TiProxy listening port. Under `routing-rule = "port"`:
- backend groups are keyed by `tiproxy-port` label value,
- missing or empty label means backend is not assigned to a port group,
- no matching group returns `ErrNoBackend`.

### Rebalance Scope

Rebalance is executed per group (`Group.Balance`), so migration candidates are always chosen inside the same group.
Therefore, migrations cannot cross `tiproxy-port` groups by design.

### Redirection Precondition

Connection migration needs backend redirection support (session token signing cert available on TiDB).
Without it, routing still works for new connections, but connection migration/rebalance is skipped.

## API Examples

Get config:

```bash
curl http://127.0.0.1:13080/api/admin/config/?format=json
```

Set backend clusters (replace with desired full list):

```bash
cat <<'CFG_EOF' | curl -X PUT http://127.0.0.1:13080/api/admin/config/ --data-binary @-
[[proxy.backend-clusters]]
name = "cluster-a"
pd-addrs = "127.0.0.1:2379"

[[proxy.backend-clusters]]
name = "cluster-b"
pd-addrs = "127.0.0.1:22379"
CFG_EOF
```

Remove one cluster by submitting list without it:

```bash
cat <<'CFG_EOF' | curl -X PUT http://127.0.0.1:13080/api/admin/config/ --data-binary @-
[[proxy.backend-clusters]]
name = "cluster-a"
pd-addrs = "127.0.0.1:2379"
CFG_EOF
```

## Observability

Key logs:
- `added backend cluster` / `updated backend cluster` / `removed backend cluster`
- backend update logs with labels including `tiproxy-cluster` and `tiproxy-port`
- `updated supporting redirection` for migration capability switch

Key metrics:
- `tiproxy_balance_b_conn`
- `tiproxy_balance_migrate_total{from,to,reason,migrate_res}`
- `tiproxy_balance_pending_migrate{from,to,reason}`

## Compatibility and Limitations

- `proxy.backend-clusters` is reloadable; `proxy.port-range` and `balance.routing-rule` are not reloadable.
- Config API update for cluster list is replacement semantics, not append/delete patch semantics.
- If any backend in router scope does not support redirection, migration may be disabled at runtime.
- When `ns-servers` is omitted, default DNS behavior remains unchanged.

## Validation Summary

Implemented behavior was validated with:
- two independent PD/TiKV playground clusters,
- six manually started TiDB instances (3 per cluster) with distinct SQL/status ports,
- label-based port routing (`tiproxy-port=10000/10001`),
- dynamic cluster add/remove via config API,
- rebalance/migration verification with long-lived connections,
- migration metrics proving no cross-group migration.

Observed result:
- route isolation by port is stable,
- rebalance happens only within same `tiproxy-port` group,
- dynamic API add/remove takes effect online without TiProxy restart.
