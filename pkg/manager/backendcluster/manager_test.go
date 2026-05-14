// Copyright 2026 PingCAP, Inc.
// SPDX-License-Identifier: Apache-2.0

package backendcluster

import (
	"context"
	"crypto/tls"
	"encoding/json"
	"net"
	"path"
	"sync"
	"testing"
	"time"

	"github.com/pingcap/tiproxy/lib/config"
	"github.com/pingcap/tiproxy/lib/util/logger"
	"github.com/pingcap/tiproxy/pkg/manager/infosync"
	"github.com/pingcap/tiproxy/pkg/testkit"
	"github.com/pingcap/tiproxy/pkg/util/etcd"
	"github.com/stretchr/testify/require"
	clientv3 "go.etcd.io/etcd/client/v3"
	"go.etcd.io/etcd/server/v3/embed"
	"go.uber.org/zap"
)

const (
	testTiDBTopologyPath = "/topology/tidb"
	testInfoSuffix       = "info"
	testTTLSuffix        = "ttl"
)

func nilClusterTLS() *tls.Config {
	return nil
}

func TestManagerFetchesAllClusters(t *testing.T) {
	clusterA := newManagerTestEtcdCluster(t)
	clusterB := newManagerTestEtcdCluster(t)
	t.Cleanup(func() { clusterA.close(t) })
	t.Cleanup(func() { clusterB.close(t) })

	clusterA.putTopology(t, "10.0.0.1:4000", &infosync.TiDBTopologyInfo{IP: "10.0.0.1", StatusPort: 10080})
	clusterB.putTopology(t, "10.0.0.2:4000", &infosync.TiDBTopologyInfo{IP: "10.0.0.2", StatusPort: 10080})

	cfg := newManagerTestConfig()
	cfg.Proxy.BackendClusters = []config.BackendCluster{
		{Name: "cluster-a", PDAddrs: clusterA.addr},
		{Name: "cluster-b", PDAddrs: clusterB.addr},
	}
	cfgGetter := newManagerTestConfigGetter(cfg)
	cfgCh := make(chan *config.Config, 1)

	mgr := NewManager(zapLoggerForTest(t), nilClusterTLS)
	require.NoError(t, mgr.Start(context.Background(), cfgGetter, cfgCh))
	t.Cleanup(func() {
		close(cfgCh)
		require.NoError(t, mgr.Close())
	})

	require.Eventually(t, func() bool {
		topology, err := mgr.GetTiDBTopology(context.Background())
		if err != nil || len(topology) != 2 {
			return false
		}
		return topology[backendID("cluster-a", "10.0.0.1:4000")].ClusterName == "cluster-a" &&
			topology[backendID("cluster-b", "10.0.0.2:4000")].ClusterName == "cluster-b"
	}, 5*time.Second, 100*time.Millisecond)
}

func TestManagerDynamicClusterUpdate(t *testing.T) {
	clusterA := newManagerTestEtcdCluster(t)
	clusterB := newManagerTestEtcdCluster(t)
	t.Cleanup(func() { clusterA.close(t) })
	t.Cleanup(func() { clusterB.close(t) })

	clusterA.putTopology(t, "10.0.0.1:4000", &infosync.TiDBTopologyInfo{IP: "10.0.0.1", StatusPort: 10080})
	clusterB.putTopology(t, "10.0.0.2:4000", &infosync.TiDBTopologyInfo{IP: "10.0.0.2", StatusPort: 10080})

	cfg := newManagerTestConfig()
	cfg.Proxy.PDAddrs = ""
	cfg.Proxy.BackendClusters = nil
	cfgGetter := newManagerTestConfigGetter(cfg)
	cfgCh := make(chan *config.Config, 4)

	mgr := NewManager(zapLoggerForTest(t), nilClusterTLS)
	require.NoError(t, mgr.Start(context.Background(), cfgGetter, cfgCh))
	t.Cleanup(func() {
		require.NoError(t, mgr.Close())
	})

	topology, err := mgr.GetTiDBTopology(context.Background())
	require.NoError(t, err)
	require.Empty(t, topology)

	nextCfg := cfg.Clone()
	nextCfg.Proxy.BackendClusters = []config.BackendCluster{
		{Name: "cluster-a", PDAddrs: clusterA.addr},
	}
	cfgGetter.setConfig(nextCfg)
	cfgCh <- nextCfg.Clone()
	require.Eventually(t, func() bool {
		topology, err := mgr.GetTiDBTopology(context.Background())
		if err != nil || len(topology) != 1 {
			return false
		}
		info, ok := topology[backendID("cluster-a", "10.0.0.1:4000")]
		return ok && info.ClusterName == "cluster-a"
	}, 5*time.Second, 100*time.Millisecond)

	nextCfg = cfg.Clone()
	nextCfg.Proxy.BackendClusters = []config.BackendCluster{
		{Name: "cluster-b", PDAddrs: clusterB.addr},
	}
	cfgGetter.setConfig(nextCfg)
	cfgCh <- nextCfg.Clone()
	require.Eventually(t, func() bool {
		topology, err := mgr.GetTiDBTopology(context.Background())
		if err != nil || len(topology) != 1 {
			return false
		}
		info, ok := topology[backendID("cluster-b", "10.0.0.2:4000")]
		return ok && info.ClusterName == "cluster-b"
	}, 5*time.Second, 100*time.Millisecond)
}

func TestManagerDynamicAddRemoveAllClusters(t *testing.T) {
	clusterA := newManagerTestEtcdCluster(t)
	clusterB := newManagerTestEtcdCluster(t)
	t.Cleanup(func() { clusterA.close(t) })
	t.Cleanup(func() { clusterB.close(t) })

	clusterA.putTopology(t, "10.0.0.1:4000", &infosync.TiDBTopologyInfo{IP: "10.0.0.1", StatusPort: 10080})
	clusterB.putTopology(t, "10.0.0.2:4000", &infosync.TiDBTopologyInfo{IP: "10.0.0.2", StatusPort: 10080})

	cfg := newManagerTestConfig()
	cfgGetter := newManagerTestConfigGetter(cfg)
	cfgCh := make(chan *config.Config, 4)

	mgr := NewManager(zapLoggerForTest(t), nilClusterTLS)
	require.NoError(t, mgr.Start(context.Background(), cfgGetter, cfgCh))
	t.Cleanup(func() {
		require.NoError(t, mgr.Close())
	})

	topology, err := mgr.GetTiDBTopology(context.Background())
	require.NoError(t, err)
	require.Empty(t, topology)
	require.False(t, mgr.HasBackendClusters())

	updateClusters := func(clusters []config.BackendCluster) {
		nextCfg := cfg.Clone()
		nextCfg.Proxy.BackendClusters = clusters
		cfgGetter.setConfig(nextCfg)
		cfgCh <- nextCfg.Clone()
	}

	updateClusters([]config.BackendCluster{{Name: "cluster-a", PDAddrs: clusterA.addr}})
	require.Eventually(t, func() bool {
		topology, err := mgr.GetTiDBTopology(context.Background())
		if err != nil || len(topology) != 1 {
			return false
		}
		_, ok := topology[backendID("cluster-a", "10.0.0.1:4000")]
		return ok && mgr.HasBackendClusters()
	}, 5*time.Second, 100*time.Millisecond)

	updateClusters([]config.BackendCluster{
		{Name: "cluster-a", PDAddrs: clusterA.addr},
		{Name: "cluster-b", PDAddrs: clusterB.addr},
	})
	require.Eventually(t, func() bool {
		topology, err := mgr.GetTiDBTopology(context.Background())
		if err != nil || len(topology) != 2 {
			return false
		}
		_, okA := topology[backendID("cluster-a", "10.0.0.1:4000")]
		_, okB := topology[backendID("cluster-b", "10.0.0.2:4000")]
		return okA && okB
	}, 5*time.Second, 100*time.Millisecond)

	updateClusters([]config.BackendCluster{{Name: "cluster-b", PDAddrs: clusterB.addr}})
	require.Eventually(t, func() bool {
		topology, err := mgr.GetTiDBTopology(context.Background())
		if err != nil || len(topology) != 1 {
			return false
		}
		_, okA := topology[backendID("cluster-a", "10.0.0.1:4000")]
		_, okB := topology[backendID("cluster-b", "10.0.0.2:4000")]
		return !okA && okB
	}, 5*time.Second, 100*time.Millisecond)

	updateClusters(nil)
	require.Eventually(t, func() bool {
		topology, err := mgr.GetTiDBTopology(context.Background())
		return err == nil && len(topology) == 0 && !mgr.HasBackendClusters()
	}, 5*time.Second, 100*time.Millisecond)
}

func TestManagerUsesClusterNameServersForPD(t *testing.T) {
	clusterA := newManagerTestEtcdCluster(t)
	clusterB := newManagerTestEtcdCluster(t)
	t.Cleanup(func() { clusterA.close(t) })
	t.Cleanup(func() { clusterB.close(t) })

	clusterA.putTopology(t, "10.0.0.1:4000", &infosync.TiDBTopologyInfo{IP: "10.0.0.1", StatusPort: 10080})
	clusterB.putTopology(t, "10.0.0.2:4000", &infosync.TiDBTopologyInfo{IP: "10.0.0.2", StatusPort: 10080})

	dnsA := testkit.StartDNSServer(t, map[string][]string{"pd-a.test": {"127.0.0.1"}})
	dnsB := testkit.StartDNSServer(t, map[string][]string{"pd-b.test": {"127.0.0.1"}})
	_, portA, err := net.SplitHostPort(clusterA.addr)
	require.NoError(t, err)
	_, portB, err := net.SplitHostPort(clusterB.addr)
	require.NoError(t, err)

	cfg := newManagerTestConfig()
	cfg.Proxy.BackendClusters = []config.BackendCluster{
		{Name: "cluster-a", PDAddrs: net.JoinHostPort("pd-a.test", portA), NSServers: []string{dnsA.Addr()}},
		{Name: "cluster-b", PDAddrs: net.JoinHostPort("pd-b.test", portB), NSServers: []string{dnsB.Addr()}},
	}
	cfgGetter := newManagerTestConfigGetter(cfg)
	cfgCh := make(chan *config.Config, 1)

	mgr := NewManager(zapLoggerForTest(t), nilClusterTLS)
	require.NoError(t, mgr.Start(context.Background(), cfgGetter, cfgCh))
	t.Cleanup(func() {
		close(cfgCh)
		require.NoError(t, mgr.Close())
	})

	require.Eventually(t, func() bool {
		topology, err := mgr.GetTiDBTopology(context.Background())
		if err != nil || len(topology) != 2 {
			return false
		}
		return topology[backendID("cluster-a", "10.0.0.1:4000")].ClusterName == "cluster-a" &&
			topology[backendID("cluster-b", "10.0.0.2:4000")].ClusterName == "cluster-b"
	}, 5*time.Second, 100*time.Millisecond)
	require.Greater(t, dnsA.QueryCount("pd-a.test"), 0)
	require.Greater(t, dnsB.QueryCount("pd-b.test"), 0)
}

func TestManagerUsesReachableResolvedPDIP(t *testing.T) {
	cluster := newManagerTestEtcdCluster(t)
	t.Cleanup(func() { cluster.close(t) })

	cluster.putTopology(t, "10.0.0.1:4000", &infosync.TiDBTopologyInfo{IP: "10.0.0.1", StatusPort: 10080})

	dns := testkit.StartDNSServer(t, map[string][]string{
		"pd-ha.test": {"127.0.0.2", "127.0.0.1"},
	})
	_, port, err := net.SplitHostPort(cluster.addr)
	require.NoError(t, err)

	cfg := newManagerTestConfig()
	cfg.Proxy.BackendClusters = []config.BackendCluster{
		{Name: "cluster-a", PDAddrs: net.JoinHostPort("pd-ha.test", port), NSServers: []string{dns.Addr()}},
	}
	cfgGetter := newManagerTestConfigGetter(cfg)
	cfgCh := make(chan *config.Config, 1)

	mgr := NewManager(zapLoggerForTest(t), nilClusterTLS)
	require.NoError(t, mgr.Start(context.Background(), cfgGetter, cfgCh))
	t.Cleanup(func() {
		close(cfgCh)
		require.NoError(t, mgr.Close())
	})

	require.Eventually(t, func() bool {
		topology, err := mgr.GetTiDBTopology(context.Background())
		if err != nil || len(topology) != 1 {
			return false
		}
		_, ok := topology[backendID("cluster-a", "10.0.0.1:4000")]
		return ok
	}, 5*time.Second, 100*time.Millisecond)
	require.Greater(t, dns.QueryCount("pd-ha.test"), 0)
}

func TestManagerNetworkRouterUsesClusterNameServersForTiDB(t *testing.T) {
	clusterA := newManagerTestEtcdCluster(t)
	clusterB := newManagerTestEtcdCluster(t)
	t.Cleanup(func() { clusterA.close(t) })
	t.Cleanup(func() { clusterB.close(t) })

	dnsA := testkit.StartDNSServer(t, map[string][]string{"tidb-a.test": {"127.0.0.1"}})
	dnsB := testkit.StartDNSServer(t, map[string][]string{"tidb-b.test": {"127.0.0.1"}})
	listenerA, addrA := testkit.StartListener(t, "127.0.0.1:0")
	listenerB, addrB := testkit.StartListener(t, "127.0.0.1:0")
	t.Cleanup(func() { require.NoError(t, listenerA.Close()) })
	t.Cleanup(func() { require.NoError(t, listenerB.Close()) })
	_, portA, err := net.SplitHostPort(addrA)
	require.NoError(t, err)
	_, portB, err := net.SplitHostPort(addrB)
	require.NoError(t, err)

	acceptOne := func(listener net.Listener) <-chan error {
		accepted := make(chan error, 1)
		go func() {
			conn, err := listener.Accept()
			if err == nil {
				err = conn.Close()
			}
			accepted <- err
		}()
		return accepted
	}
	acceptedA := acceptOne(listenerA)
	acceptedB := acceptOne(listenerB)

	cfg := newManagerTestConfig()
	cfg.Proxy.BackendClusters = []config.BackendCluster{
		{Name: "cluster-a", PDAddrs: clusterA.addr, NSServers: []string{dnsA.Addr()}},
		{Name: "cluster-b", PDAddrs: clusterB.addr, NSServers: []string{dnsB.Addr()}},
	}
	cfgGetter := newManagerTestConfigGetter(cfg)
	cfgCh := make(chan *config.Config, 1)

	mgr := NewManager(zapLoggerForTest(t), nilClusterTLS)
	require.NoError(t, mgr.Start(context.Background(), cfgGetter, cfgCh))
	t.Cleanup(func() {
		close(cfgCh)
		require.NoError(t, mgr.Close())
	})
	require.Eventually(t, func() bool {
		return len(mgr.Snapshot()) == 2
	}, 5*time.Second, 100*time.Millisecond)

	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	conn, err := mgr.NetworkRouter().DialContext(ctx, "tcp", net.JoinHostPort("tidb-a.test", portA), "cluster-a")
	require.NoError(t, err)
	require.NoError(t, conn.Close())
	require.NoError(t, <-acceptedA)

	conn, err = mgr.NetworkRouter().DialContext(ctx, "tcp", net.JoinHostPort("tidb-b.test", portB), "cluster-b")
	require.NoError(t, err)
	require.NoError(t, conn.Close())
	require.NoError(t, <-acceptedB)

	require.Greater(t, dnsA.QueryCount("tidb-a.test"), 0)
	require.Equal(t, 0, dnsA.QueryCount("tidb-b.test"))
	require.Greater(t, dnsB.QueryCount("tidb-b.test"), 0)
	require.Equal(t, 0, dnsB.QueryCount("tidb-a.test"))
}

func TestManagerKeepsOldClusterWhenUpdateFails(t *testing.T) {
	clusterA := newManagerTestEtcdCluster(t)
	clusterB := newManagerTestEtcdCluster(t)
	t.Cleanup(func() { clusterA.close(t) })
	t.Cleanup(func() { clusterB.close(t) })

	clusterA.putTopology(t, "10.0.0.1:4000", &infosync.TiDBTopologyInfo{IP: "10.0.0.1", StatusPort: 10080})
	clusterB.putTopology(t, "10.0.0.2:4000", &infosync.TiDBTopologyInfo{IP: "10.0.0.2", StatusPort: 10080})

	cfg := newManagerTestConfig()
	cfg.Proxy.BackendClusters = []config.BackendCluster{
		{Name: "cluster-a", PDAddrs: clusterA.addr},
	}
	cfgGetter := newManagerTestConfigGetter(cfg)
	cfgCh := make(chan *config.Config, 1)

	mgr := NewManager(zapLoggerForTest(t), nilClusterTLS)
	require.NoError(t, mgr.Start(context.Background(), cfgGetter, cfgCh))
	t.Cleanup(func() {
		close(cfgCh)
		require.NoError(t, mgr.Close())
	})

	require.Eventually(t, func() bool {
		topology, err := mgr.GetTiDBTopology(context.Background())
		if err != nil || len(topology) != 1 {
			return false
		}
		_, ok := topology[backendID("cluster-a", "10.0.0.1:4000")]
		return ok
	}, 5*time.Second, 100*time.Millisecond)

	originalCluster := mgr.Snapshot()["cluster-a"]
	require.NotNil(t, originalCluster)

	nextCfg := cfg.Clone()
	nextCfg.Proxy.Addr = "invalid"
	nextCfg.Proxy.BackendClusters = []config.BackendCluster{
		{Name: "cluster-a", PDAddrs: clusterB.addr},
	}
	require.NoError(t, mgr.syncClusters(context.Background(), nextCfg))

	currentCluster := mgr.Snapshot()["cluster-a"]
	require.Same(t, originalCluster, currentCluster)

	topology, err := mgr.GetTiDBTopology(context.Background())
	require.NoError(t, err)
	require.Contains(t, topology, backendID("cluster-a", "10.0.0.1:4000"))
	require.NotContains(t, topology, backendID("cluster-a", "10.0.0.2:4000"))
}

func TestManagerUpdatesClusterPDAddrs(t *testing.T) {
	clusterA := newManagerTestEtcdCluster(t)
	clusterB := newManagerTestEtcdCluster(t)
	t.Cleanup(func() { clusterA.close(t) })
	t.Cleanup(func() { clusterB.close(t) })

	clusterA.putTopology(t, "10.0.0.1:4000", &infosync.TiDBTopologyInfo{IP: "10.0.0.1", StatusPort: 10080})
	clusterB.putTopology(t, "10.0.0.2:4000", &infosync.TiDBTopologyInfo{IP: "10.0.0.2", StatusPort: 10080})

	cfg := newManagerTestConfig()
	cfg.Proxy.BackendClusters = []config.BackendCluster{
		{Name: "cluster-a", PDAddrs: clusterA.addr},
	}
	cfgGetter := newManagerTestConfigGetter(cfg)
	cfgCh := make(chan *config.Config, 1)

	mgr := NewManager(zapLoggerForTest(t), nilClusterTLS)
	require.NoError(t, mgr.Start(context.Background(), cfgGetter, cfgCh))
	t.Cleanup(func() {
		close(cfgCh)
		require.NoError(t, mgr.Close())
	})

	require.Eventually(t, func() bool {
		topology, err := mgr.GetTiDBTopology(context.Background())
		if err != nil || len(topology) != 1 {
			return false
		}
		_, ok := topology[backendID("cluster-a", "10.0.0.1:4000")]
		return ok
	}, 5*time.Second, 100*time.Millisecond)
	originalCluster := mgr.Snapshot()["cluster-a"]
	require.NotNil(t, originalCluster)

	nextCfg := cfg.Clone()
	nextCfg.Proxy.BackendClusters = []config.BackendCluster{
		{Name: "cluster-a", PDAddrs: clusterB.addr},
	}
	cfgGetter.setConfig(nextCfg)
	cfgCh <- nextCfg.Clone()

	require.Eventually(t, func() bool {
		currentCluster := mgr.Snapshot()["cluster-a"]
		if currentCluster == nil || currentCluster == originalCluster {
			return false
		}
		topology, err := mgr.GetTiDBTopology(context.Background())
		if err != nil || len(topology) != 1 {
			return false
		}
		_, oldOK := topology[backendID("cluster-a", "10.0.0.1:4000")]
		_, newOK := topology[backendID("cluster-a", "10.0.0.2:4000")]
		return !oldOK && newOK
	}, 5*time.Second, 100*time.Millisecond)
}

func TestManagerUpdatesClusterNameServersForPD(t *testing.T) {
	cluster := newManagerTestEtcdCluster(t)
	t.Cleanup(func() { cluster.close(t) })

	cluster.putTopology(t, "10.0.0.1:4000", &infosync.TiDBTopologyInfo{IP: "10.0.0.1", StatusPort: 10080})

	dnsA := testkit.StartDNSServer(t, map[string][]string{"pd.test": {"127.0.0.1"}})
	dnsB := testkit.StartDNSServer(t, map[string][]string{"pd.test": {"127.0.0.1"}})
	_, port, err := net.SplitHostPort(cluster.addr)
	require.NoError(t, err)

	cfg := newManagerTestConfig()
	cfg.Proxy.BackendClusters = []config.BackendCluster{
		{Name: "cluster-a", PDAddrs: net.JoinHostPort("pd.test", port), NSServers: []string{dnsA.Addr()}},
	}
	cfgGetter := newManagerTestConfigGetter(cfg)
	cfgCh := make(chan *config.Config, 1)

	mgr := NewManager(zapLoggerForTest(t), nilClusterTLS)
	require.NoError(t, mgr.Start(context.Background(), cfgGetter, cfgCh))
	t.Cleanup(func() {
		require.NoError(t, mgr.Close())
	})

	require.Eventually(t, func() bool {
		return dnsA.QueryCount("pd.test") > 0
	}, 5*time.Second, 100*time.Millisecond)

	originalCluster := mgr.Snapshot()["cluster-a"]
	require.NotNil(t, originalCluster)

	nextCfg := cfg.Clone()
	nextCfg.Proxy.BackendClusters = []config.BackendCluster{
		{Name: "cluster-a", PDAddrs: net.JoinHostPort("pd.test", port), NSServers: []string{dnsB.Addr()}},
	}
	cfgGetter.setConfig(nextCfg)
	cfgCh <- nextCfg.Clone()

	require.Eventually(t, func() bool {
		currentCluster := mgr.Snapshot()["cluster-a"]
		return currentCluster != nil &&
			currentCluster != originalCluster &&
			dnsB.QueryCount("pd.test") > 0
	}, 5*time.Second, 100*time.Millisecond)
}

func TestManagerKeepsDuplicateBackendAddrsAcrossClusters(t *testing.T) {
	clusterA := newManagerTestEtcdCluster(t)
	clusterB := newManagerTestEtcdCluster(t)
	t.Cleanup(func() { clusterA.close(t) })
	t.Cleanup(func() { clusterB.close(t) })

	clusterA.putTopology(t, "shared.tidb:4000", &infosync.TiDBTopologyInfo{IP: "10.0.0.1", StatusPort: 10080})
	clusterB.putTopology(t, "shared.tidb:4000", &infosync.TiDBTopologyInfo{IP: "10.0.0.2", StatusPort: 10080})

	cfg := newManagerTestConfig()
	cfg.Proxy.BackendClusters = []config.BackendCluster{
		{Name: "cluster-a", PDAddrs: clusterA.addr},
		{Name: "cluster-b", PDAddrs: clusterB.addr},
	}
	cfgGetter := newManagerTestConfigGetter(cfg)
	cfgCh := make(chan *config.Config, 1)

	mgr := NewManager(zapLoggerForTest(t), nilClusterTLS)
	require.NoError(t, mgr.Start(context.Background(), cfgGetter, cfgCh))
	t.Cleanup(func() {
		close(cfgCh)
		require.NoError(t, mgr.Close())
	})

	require.Eventually(t, func() bool {
		topology, err := mgr.GetTiDBTopology(context.Background())
		if err != nil || len(topology) != 2 {
			return false
		}
		infoA, okA := topology[backendID("cluster-a", "shared.tidb:4000")]
		infoB, okB := topology[backendID("cluster-b", "shared.tidb:4000")]
		return okA && okB && infoA.Addr == "shared.tidb:4000" && infoB.Addr == "shared.tidb:4000"
	}, 5*time.Second, 100*time.Millisecond)
}

func TestClusterReusableIgnoresPDAddrOrder(t *testing.T) {
	cluster := &Cluster{
		cfg: config.BackendCluster{
			Name:    "cluster-a",
			PDAddrs: "pd1:2379, pd2:2379",
		},
	}

	reusable := clusterReusable(cluster, config.BackendCluster{
		Name:    " cluster-a ",
		PDAddrs: "pd2:2379,pd1:2379",
	})

	require.True(t, reusable)
}

func TestClusterReusableIgnoresNSServerOrder(t *testing.T) {
	cluster := &Cluster{
		cfg: config.BackendCluster{
			Name:      "cluster-a",
			PDAddrs:   "pd1:2379",
			NSServers: []string{"10.0.0.2", "10.0.0.3"},
		},
	}

	reusable := clusterReusable(cluster, config.BackendCluster{
		Name:      "cluster-a",
		PDAddrs:   "pd1:2379",
		NSServers: []string{"10.0.0.3", "10.0.0.2"},
	})

	require.True(t, reusable)
}

type managerTestConfigGetter struct {
	mu  sync.RWMutex
	cfg *config.Config
}

func newManagerTestConfigGetter(cfg *config.Config) *managerTestConfigGetter {
	return &managerTestConfigGetter{cfg: cfg}
}

func (g *managerTestConfigGetter) GetConfig() *config.Config {
	g.mu.RLock()
	defer g.mu.RUnlock()
	return g.cfg
}

func (g *managerTestConfigGetter) setConfig(cfg *config.Config) {
	g.mu.Lock()
	g.cfg = cfg
	g.mu.Unlock()
}

type managerTestEtcdCluster struct {
	etcd   *embed.Etcd
	client *clientv3.Client
	kv     clientv3.KV
	addr   string
}

func newManagerTestEtcdCluster(t *testing.T) *managerTestEtcdCluster {
	lg, _ := logger.CreateLoggerForTest(t)
	etcdSrv, err := etcd.CreateEtcdServer("127.0.0.1:0", t.TempDir(), lg)
	require.NoError(t, err)
	addr := etcdSrv.Clients[0].Addr().String()
	cli, err := etcd.InitEtcdClientWithAddrs(lg, addr, nil)
	require.NoError(t, err)
	return &managerTestEtcdCluster{
		etcd:   etcdSrv,
		client: cli,
		kv:     clientv3.NewKV(cli),
		addr:   addr,
	}
}

func (tec *managerTestEtcdCluster) close(t *testing.T) {
	require.NoError(t, tec.client.Close())
	tec.etcd.Close()
}

func (tec *managerTestEtcdCluster) putTopology(t *testing.T, sqlAddr string, info *infosync.TiDBTopologyInfo) {
	data, err := json.Marshal(info)
	require.NoError(t, err)
	_, err = tec.kv.Put(context.Background(), path.Join(testTiDBTopologyPath, sqlAddr, testInfoSuffix), string(data))
	require.NoError(t, err)
	_, err = tec.kv.Put(context.Background(), path.Join(testTiDBTopologyPath, sqlAddr, testTTLSuffix), "1")
	require.NoError(t, err)
}

func newManagerTestConfig() *config.Config {
	cfg := config.NewConfig()
	cfg.Proxy.Addr = "127.0.0.1:6000"
	cfg.API.Addr = "127.0.0.1:3080"
	cfg.Proxy.PDAddrs = ""
	cfg.Proxy.BackendClusters = nil
	return cfg
}

func zapLoggerForTest(t *testing.T) *zap.Logger {
	lg, _ := logger.CreateLoggerForTest(t)
	return lg
}
