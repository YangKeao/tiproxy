# Tech Spec: TiProxy Multi-PD Clusters Support (Final)

- Author(s): [Yang Keao](https://github.com/YangKeao)
- Status: Implemented
- Last Updated: 2026-03-13
- Supersedes: `docs/design/2026-03-10-multi-cluster-port-routing.md`

## 1. Background and Goals

This project enables one TiProxy instance to concurrently serve multiple independent PD/TiDB clusters, while keeping routing isolation and online configurability.

Primary goals:
- Keep backward compatibility with legacy single `proxy.pd-addrs`.
- Support dynamic add/remove/update of backend clusters via existing config API.
- Support port-based routing (`balance.routing-rule = "port"`) with multi-listener `proxy.port-range`.
- Ensure rebalance and connection migration stay within the same routing group.
- Support per-cluster DNS routing via `ns-servers` for PD/etcd, TiDB HTTP, and backend MySQL connection paths.
- Improve etcd DNS HA when `pd-addrs` uses domain names (including headless-service style multi-IP results).

## 2. Non-Goals

- Cross-port-group migration.
- Making `proxy.port-range` or `balance.routing-rule` reloadable.
- Introducing a patch-style config API for append/remove delta updates.

## 3. Configuration Model

### 3.1 Backend cluster list

```toml
[[proxy.backend-clusters]]
name = "cluster-a"
pd-addrs = "10.0.1.1:2379,10.0.1.2:2379"
ns-servers = "10.10.0.2,10.10.0.3:53"

[[proxy.backend-clusters]]
name = "cluster-b"
pd-addrs = "10.0.2.1:2379,10.0.2.2:2379"
ns-servers = "10.20.0.2,10.20.0.3:53"
```

Validation rules:
- `name`: non-empty, unique.
- `pd-addrs`: non-empty `host:port` list.
- `ns-servers`: optional `host[:port]` list, default port `53`.

### 3.2 Port-based routing

```toml
[proxy]
addr = "0.0.0.0:6000"
port-range = [10000, 10512]

[balance]
routing-rule = "port"
```

Behavior:
- `proxy.port-range` expands one frontend host into many listening ports.
- route group key is TiDB label `tiproxy-port`.
- connection arriving at TiProxy port `P` is routed only to TiDB(s) labeled `tiproxy-port=P`.

### 3.3 Backward compatibility and precedence

Resolution order:
1. If `proxy.backend-clusters` is non-empty, use it.
2. Else if `proxy.pd-addrs` is non-empty, synthesize one implicit cluster named `default`.
3. Else no backend cluster is active (startup allowed).

So when both are present, `backend-clusters` takes precedence.

## 4. Runtime Architecture

### 4.1 Multi-cluster topology fetcher

`MultiClusterFetcher` (`pkg/manager/infosync/multi_cluster.go`):
- owns one etcd client per backend cluster,
- watches config channel and supports add/update/remove,
- merges topology from all clusters,
- injects synthetic label `tiproxy-cluster=<cluster-name>`.

Duplicate TiDB address across clusters is logged and first-seen entry is kept.

### 4.2 Multi-cluster infosync

`MultiClusterInfoSyncer` (`pkg/manager/infosync/multi_cluster_infosync.go`):
- owns one `InfoSyncer` per backend cluster,
- each syncer writes TiProxy topology into that cluster's PD/etcd,
- dynamic add/remove/update is supported.

### 4.3 Cluster-scoped metrics owner election

`BackendReader` (`pkg/balance/metricsreader/backend_reader.go`):
- builds one election context per backend cluster,
- each cluster has independent etcd client and owner key space,
- owner key prefix is cluster-scoped (`/tiproxy/metric_reader/<cluster>/...`, default cluster keeps historical path compatibility).

### 4.4 Routing and rebalance

Router grouping under `routing-rule=port` ensures:
- route selection only within matching `tiproxy-port` group,
- rebalance/migration (`Group.Balance`) only inside same group,
- no cross-group movement by design.

### 4.5 VIP behavior

VIP is enabled only when backend cluster count is exactly 1.
For multi-cluster mode, VIP is disabled intentionally.

## 5. DNS and etcd HA Design

### 5.1 Per-cluster DNS dialer

`dns.Dialer` provides:
- custom resolver bound to configured `ns-servers`,
- round-robin DNS server selection,
- in-process host cache (default TTL 30s),
- direct IP bypass when target host is already IP.

When `ns-servers` is empty, system default DNS path is used.

### 5.2 DNS-expanded etcd endpoints (new)

Problem:
- etcd client v3 uses an internal manual resolver (`etcd-endpoints`) with `round_robin`.
- if only one domain endpoint is provided, gRPC sub-conn granularity remains one endpoint.

Implemented solution:
- Added `InitEtcdClientWithAddrsAndDNSDialer` (`pkg/util/etcd/etcd.go`).
- During etcd client initialization:
  1. parse `pd-addrs` list,
  2. for domain host entries, resolve via cluster-specific `dns.Dialer`,
  3. expand one domain endpoint into multiple resolved endpoints (one per IP),
  4. encode each as `tiproxy-resolved://<original-host>/<ip:port>`.
- `dns.Dialer` recognizes this encoded endpoint and dials `<ip:port>` directly.

Effect:
- etcd manual resolver now receives multiple explicit addresses,
- gRPC `round_robin` can maintain multiple sub-connections for better availability.

Fallback:
- if domain resolve fails during expansion, keep original endpoint and continue.

## 6. Dynamic Reconfiguration Semantics

Config updates reuse existing API (`/api/admin/config/`) with full-list replacement semantics for `proxy.backend-clusters`.

Rules:
- update payload containing `proxy.backend-clusters` replaces old list,
- runtime watchers apply add/update/remove incrementally,
- if a specific cluster update fails, previous runtime instance for that cluster is retained when possible,
- removed clusters are closed and cleaned up.

## 7. Operational Invariants

- Port isolation: traffic to port `P` never routes to `tiproxy-port != P`.
- Cluster isolation: owner election and infosync are cluster-scoped.
- Dynamic safety: partial invalid update does not globally break existing healthy cluster clients.
- Backward compatibility: legacy single `pd-addrs` behavior remains available.

## 8. Known Limitations

- `proxy.port-range` and `balance.routing-rule` remain non-reloadable.
- `backend-clusters` API is replacement, not delta patch.
- If `ns-servers` host itself is a domain, first-hop resolution of that DNS server may still rely on system resolver.
- DNS-expanded endpoints are applied when cluster client is (re)built; further endpoint evolution still depends on etcd auto-sync/member list behavior.

## 9. Observability

Expected key logs:
- `added/updated/removed backend cluster`
- `added/updated/removed infosync backend cluster`
- `added/updated/removed backend metrics owner cluster`
- DNS config and resolve failures with cluster name context.

Recommended metrics to watch:
- route/connection metrics per port group,
- migration counters and reasons,
- backend availability and health-check metrics.

---

# Test Plan

## A. Test Scope

Cover three layers:
- unit tests for parser/dialer/resolver behaviors,
- integration tests for multi-cluster runtime wiring,
- end-to-end and failure-injection tests for availability and corner cases.

## B. Existing Automated Coverage (Implemented)

### B.1 DNS and etcd

- `pkg/util/dns/dialer_test.go`
  - `TestResolvedAddressEncoding`
  - `TestDialContextResolvedAddress`
  - resolver cache and config update tests.

- `pkg/util/etcd/etcd_test.go`
  - `TestEtcdClientWithDNSDialerExpandedEndpoints`
    - fake DNS server,
    - domain `pd-addrs` resolved through configured DNS,
    - endpoint expansion and successful etcd put verification.

### B.2 Multi-cluster infosync/fetcher

- `pkg/manager/infosync/multi_cluster_test.go`
  - dynamic add/remove/update cluster fetch behavior.

- `pkg/manager/infosync/multi_cluster_infosync_test.go`
  - dynamic infosync add/remove.
  - `TestMultiClusterInfoSyncerResolvePDAddrsWithClusterNSServers`.

- `pkg/manager/infosync/multi_cluster_dns_test.go`
  - fake DNS split-routing assertions per cluster.

### B.3 Server and metrics owner

- `pkg/balance/metricsreader` tests validate cluster owner logic compile and behavior.
- `pkg/server` tests validate startup wiring and config compatibility paths.

## C. Functional Test Matrix

### C.1 Basic feature tests

1. Legacy compatibility
- Only `proxy.pd-addrs` configured.
- Expect one implicit `default` cluster and normal routing.

2. Pure multi-cluster startup
- `proxy.pd-addrs = ""`, only `backend-clusters` configured.
- Expect all cluster clients created and topology merged.

3. Empty startup then dynamic add
- start with empty `backend-clusters`.
- add clusters via API; expect traffic starts working without restart.

4. Dynamic remove
- remove one cluster via API.
- expect removed cluster no longer appears in topology/owner keys.

5. Port routing correctness
- multiple TiDB labeled `tiproxy-port=10000/10001/...`.
- connect each port repeatedly; expect only matching backend set.

6. Rebalance boundary
- force load imbalance inside one port group.
- verify migrations stay in same `tiproxy-port` group.

### C.2 DNS route tests

1. Per-cluster DNS split
- cluster-a and cluster-b use distinct fake DNS servers.
- verify PD and TiDB hostnames are queried only on owning DNS server.

2. ns-servers omitted
- leave `ns-servers` empty.
- verify default resolver path still works.

3. ns-servers invalid update
- push invalid ns server value.
- verify update rejected or old runtime instance retained.

## D. Availability and Resilience Tests

1. Headless-service multi-IP PD endpoint
- one `pd-addrs` domain resolves to N IPs.
- kill one target PD IP endpoint.
- expect client still healthy via remaining expanded endpoints/sub-conns.

2. DNS server partial outage
- one cluster DNS unavailable, others healthy.
- expect affected cluster degradation only; unrelated clusters unaffected.

3. PD cluster full outage and recovery
- stop one PD cluster then recover.
- verify TiProxy recovers without restart after PD available again.

4. Runtime cluster churn under load
- run sustained SQL traffic, repeatedly add/remove clusters via API.
- assert no panic, no cross-port routing, acceptable error envelope during transition.

## E. Corner Cases

1. Duplicate cluster names in config.
2. Empty cluster name.
3. Invalid `pd-addrs` entry format.
4. `backend-clusters` and `pd-addrs` both present (verify precedence).
5. Duplicate TiDB addr appears from different clusters (first-kept + warning).
6. TiDB missing `tiproxy-port` label under `routing-rule=port`.
7. Very large `port-range` with sparse backend labels.
8. Cluster update where PD addrs changed but DNS unchanged (and vice versa).
9. Simultaneous config updates racing with reader/syncer refresh loops.

## F. Performance and Scale (Recommended)

1. 100+ listening ports with `routing-rule=port`.
2. 10+ backend clusters with periodic config churn.
3. DNS query rate and cache hit ratio under high connect churn.
4. etcd connection count/sub-conn growth after endpoint expansion.

## G. Acceptance Criteria

- No regression in single-cluster legacy mode.
- Multi-cluster add/remove/update works online without process restart.
- Port routing and rebalance isolation are invariant.
- Cluster-scoped infosync and metrics owner election are correct.
- DNS split-routing is provable in automated tests.
- Lint and relevant package tests pass.
