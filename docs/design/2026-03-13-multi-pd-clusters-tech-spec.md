# Tech Spec: TiProxy Multi-PD Clusters Support

- Author(s): [Yang Keao](https://github.com/YangKeao)
- Status: Design Baseline
- Last Updated: 2026-03-13
- Supersedes: `docs/design/2026-03-10-multi-cluster-port-routing.md`

## 1. Background and Goals

This design allows one TiProxy instance to serve multiple independent PD/TiDB clusters while preserving strict routing isolation and online reconfiguration.

Goals:
- Keep backward compatibility with legacy single `proxy.pd-addrs`.
- Support multi-cluster discovery from `proxy.backend-clusters`.
- Support dynamic add/remove/update of backend clusters via existing config API.
- Support port-based routing (`balance.routing-rule = "port"`) with multi-listener `proxy.port-range`.
- Ensure rebalance and connection migration stay within the same routing group.
- Support per-cluster DNS routing via `ns-servers`.
- Improve etcd availability when `pd-addrs` uses DNS names (including headless-service style multi-IP responses).

## 2. Non-Goals

- Cross-port-group migration.
- Reloading `proxy.port-range` or `balance.routing-rule` at runtime.
- Patch-style (append/remove delta) config API for cluster list updates.

## 3. Configuration Model

### 3.1 Backend Clusters

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

Validation:
- `name` must be non-empty and unique.
- `pd-addrs` must be a non-empty list of `host:port`.
- `ns-servers` is optional; each item must be `host[:port]`, with default port `53`.

### 3.2 Port-Based Routing

```toml
[proxy]
addr = "0.0.0.0:6000"
port-range = [10000, 10512]

[balance]
routing-rule = "port"
```

Behavior:
- `port-range` expands one frontend host into multiple listening ports.
- route group key is TiDB label `tiproxy-port`.
- a connection arriving at TiProxy port `P` is routed only to TiDB nodes labeled `tiproxy-port=P`.

### 3.3 Compatibility and Precedence

Resolution order:
1. If `proxy.backend-clusters` is non-empty, use it.
2. Else if `proxy.pd-addrs` is non-empty, synthesize one implicit cluster named `default`.
3. Else run with zero backend clusters and wait for dynamic config updates.

If both `proxy.pd-addrs` and `proxy.backend-clusters` are present, `backend-clusters` takes precedence.

## 4. Runtime Architecture

### 4.1 Multi-Cluster Topology Fetcher

`MultiClusterFetcher`:
- owns one etcd client per backend cluster,
- watches config channel and supports add/update/remove,
- merges TiDB topology from all clusters,
- injects synthetic label `tiproxy-cluster=<cluster-name>`.

Duplicate TiDB address across clusters is logged and first-seen entry is kept.

### 4.2 Multi-Cluster InfoSync

`MultiClusterInfoSyncer`:
- owns one `InfoSyncer` per backend cluster,
- writes TiProxy topology into each cluster's PD/etcd,
- applies dynamic add/remove/update.

### 4.3 Cluster-Scoped Metrics Owner Election

`BackendReader`:
- creates one election context per backend cluster,
- uses independent etcd clients and key spaces per cluster,
- keeps historical key-path compatibility for the single-cluster default path.

### 4.4 Routing and Rebalance Scope

With `routing-rule=port`:
- route selection is constrained to the matching `tiproxy-port` group,
- rebalance/migration is constrained to the same group,
- cross-group migration is not allowed.

### 4.5 VIP Rule

VIP is enabled only when backend cluster count is exactly `1`.
For multi-cluster mode, VIP is disabled.

## 5. DNS and etcd HA Design

### 5.1 Per-Cluster DNS Dialer

`dns.Dialer` provides:
- cluster-specific resolver bound to `ns-servers`,
- round-robin selection among configured DNS servers,
- in-process host cache,
- direct-IP fast path when target host is already an IP.

If `ns-servers` is empty, default system DNS behavior is used.

### 5.2 DNS-Expanded etcd Endpoints

Problem:
- etcd client v3 uses a manual resolver + `round_robin` over configured endpoints.
- if only one DNS endpoint is configured, gRPC endpoint granularity may stay too coarse.

Design:
- for each `pd-addrs` DNS endpoint, resolve host via cluster-specific `dns.Dialer`.
- expand one endpoint into multiple resolved endpoints (one per IP).
- encode expanded endpoint as `tiproxy-resolved://<origin-host>/<ip:port>`.
- `dns.Dialer` recognizes this encoded form and dials `<ip:port>` directly.

Effect:
- gRPC receives multiple explicit endpoints.
- `round_robin` can maintain multiple sub-connections and improve availability.

Fallback:
- if host resolution fails during expansion, keep original endpoint and continue.

## 6. Dynamic Reconfiguration Semantics

Config updates reuse `/api/admin/config/` with full-list replacement for `proxy.backend-clusters`.

Rules:
- payload containing `proxy.backend-clusters` replaces previous list.
- runtime managers apply add/update/remove incrementally.
- update failures are isolated to impacted clusters when possible.
- removed clusters are closed and cleaned up.

## 7. Operational Invariants

- Port isolation: traffic to port `P` never routes to `tiproxy-port != P`.
- Cluster isolation: infosync and metrics-owner election are cluster-scoped.
- Dynamic safety: config changes should not force process restart.
- Compatibility: legacy single-cluster behavior is preserved.

## 8. Known Constraints

- `proxy.port-range` and `balance.routing-rule` are non-reloadable.
- `backend-clusters` API uses replacement semantics.
- if a DNS server entry itself is a domain name, first-hop resolution of that DNS server endpoint may depend on system resolver.

## 9. Observability Requirements

Logs should include cluster identifiers for:
- cluster add/update/remove,
- DNS resolve failures,
- etcd client creation failures,
- owner election transitions.

Metrics should expose:
- per-group routing/connection distribution,
- migration counts and reasons,
- backend health status.

---

# Test Plan (From Scratch)

## A. Test Strategy and Principles

- Build tests in three layers: unit, integration, end-to-end.
- Treat each cluster as an independent failure domain.
- Verify both functional correctness and failure behavior.
- Prefer deterministic tests first, then stress/fault tests.
- Every test should assert at least one hard invariant.

## B. Test Environment Matrix

1. Local deterministic environment
- embedded etcd servers,
- fake DNS servers,
- mock TiDB topology/health endpoints.

2. Real-process integration environment
- `tiup playground` for multi-PD/TiKV clusters,
- manually started TiDB instances with explicit labels.

3. Fault-injection environment
- controllable DNS failures (NXDOMAIN, timeout, partial answer),
- PD endpoint process kill/restart,
- network drop/delay via traffic control.

## C. Core Invariants to Assert

1. `routing-rule=port`: request on port `P` only reaches backends with `tiproxy-port=P`.
2. Rebalance never migrates connection across different `tiproxy-port` groups.
3. Cluster add/remove/update via config API takes effect online.
4. Cluster-scoped infosync and owner-election keys are written only to the intended PD cluster.
5. Per-cluster `ns-servers` drives PD/TiDB hostname resolution path.
6. With DNS-expanded PD endpoints, single-endpoint domain can survive partial IP failure.

## D. Unit Test Design

### D.1 Config and Validation

- valid/invalid `backend-clusters` parsing.
- duplicate cluster name rejection.
- empty cluster name rejection.
- invalid `pd-addrs` / invalid `ns-servers` formats.
- precedence when both legacy and new configs are present.

### D.2 DNS Dialer

- parse and normalize `ns-servers`.
- host resolve cache behavior and TTL refresh.
- round-robin DNS server selection.
- encoded resolved-address parsing and direct dial path.
- fallback behavior when resolve fails.

### D.3 Endpoint Expansion Logic

- split and normalize `pd-addrs` list.
- domain endpoint expansion to multiple resolved addresses.
- dedup behavior for repeated IPs.
- mixed input (IP endpoints + DNS endpoints).
- fallback to original endpoint when expansion fails.

## E. Integration Test Design

### E.1 Multi-Cluster Fetch and InfoSync

- startup with empty backend list.
- dynamic add single cluster, then multiple clusters.
- dynamic remove one cluster, then all clusters.
- dynamic update of `pd-addrs` and `ns-servers`.
- verify topology merge labels (`tiproxy-cluster`) and isolation.

### E.2 Metrics Owner Isolation

- owner election keys are cluster-scoped.
- owner transitions per cluster are independent.
- one cluster failure does not block owner activity in another cluster.

### E.3 DNS Routing by Cluster

- two fake DNS servers with disjoint records.
- `cluster-a` and `cluster-b` use different `ns-servers`.
- verify PD/TiDB hostname queries hit only assigned DNS server.

### E.4 etcd DNS HA

- one PD domain returns multiple A records.
- establish client with expanded endpoints.
- kill one resolved target and verify operations continue.
- restore target and verify no regression.

## F. End-to-End Test Design

### F.1 Port Routing E2E

- run two independent TiDB clusters.
- run multiple TiDB instances per cluster, each with explicit `tiproxy-port` labels.
- open long-running client sessions on multiple TiProxy ports.
- assert each session stays within its port group.

### F.2 Dynamic API E2E

- start TiProxy with no backend clusters.
- add clusters via API while traffic is running.
- remove/update clusters via API while traffic is running.
- verify expected success/failure envelope and no crash.

### F.3 Rebalance E2E

- induce load skew within one port group.
- verify migration is triggered only within that group.
- verify migration is blocked or skipped cleanly when redirect preconditions are absent.

## G. Resilience and Fault Tests

1. DNS server timeout for one cluster.
2. DNS NXDOMAIN for PD host.
3. PD quorum loss in one cluster.
4. Intermittent network failures to subset of PD IPs.
5. Rapid repeated config updates (churn) under active traffic.
6. simultaneous failures in one cluster while another remains healthy.

Expected outcome: degradation is localized; unaffected clusters continue serving.

## H. Corner Cases

1. `backend-clusters` empty at boot, later populated.
2. duplicate TiDB address across clusters.
3. TiDB without `tiproxy-port` label under port-routing mode.
4. very large `port-range` with sparse backend labels.
5. cluster update that changes only DNS settings.
6. cluster update that changes only PD endpoints.
7. invalid config push followed by valid recovery push.
8. mixed IPv4/IPv6 DNS answers.
9. DNS answer reordering across repeated queries.

## I. Performance and Scale Plan

1. high number of frontend listening ports.
2. high number of backend clusters.
3. high connect churn with DNS-heavy workloads.
4. long-duration soak test with periodic cluster add/remove.

Measure:
- connection success rate,
- latency percentiles,
- migration rates,
- DNS lookup volume and cache hit ratio,
- etcd client/sub-connection stability.

## J. Release Gates

1. All unit and integration suites pass.
2. End-to-end routing and dynamic update scenarios pass.
3. Fault-injection suite meets availability expectations.
4. No cross-port routing violations in any run.
5. Lint and CI checks pass.
