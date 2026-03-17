// Copyright 2026 PingCAP, Inc.
// SPDX-License-Identifier: Apache-2.0

package backendcluster

import (
	"context"
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

	mgr := NewManager(zapLoggerForTest(t), nil)
	require.NoError(t, mgr.Start(context.Background(), cfgGetter, nil))
	t.Cleanup(func() {
		require.NoError(t, mgr.Close())
	})

	require.Eventually(t, func() bool {
		topology, err := mgr.GetTiDBTopology(context.Background())
		if err != nil || len(topology) != 2 {
			return false
		}
		return topology["10.0.0.1:4000"].Labels[config.ClusterLabelName] == "cluster-a" &&
			topology["10.0.0.2:4000"].Labels[config.ClusterLabelName] == "cluster-b"
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

	mgr := NewManager(zapLoggerForTest(t), nil)
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
		info, ok := topology["10.0.0.1:4000"]
		return ok && info.Labels[config.ClusterLabelName] == "cluster-a"
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
		info, ok := topology["10.0.0.2:4000"]
		return ok && info.Labels[config.ClusterLabelName] == "cluster-b"
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
		{Name: "cluster-a", PDAddrs: net.JoinHostPort("pd-a.test", portA), NSServers: dnsA.Addr()},
		{Name: "cluster-b", PDAddrs: net.JoinHostPort("pd-b.test", portB), NSServers: dnsB.Addr()},
	}
	cfgGetter := newManagerTestConfigGetter(cfg)

	mgr := NewManager(zapLoggerForTest(t), nil)
	require.NoError(t, mgr.Start(context.Background(), cfgGetter, nil))
	t.Cleanup(func() {
		require.NoError(t, mgr.Close())
	})

	require.Eventually(t, func() bool {
		topology, err := mgr.GetTiDBTopology(context.Background())
		if err != nil || len(topology) != 2 {
			return false
		}
		return topology["10.0.0.1:4000"].Labels[config.ClusterLabelName] == "cluster-a" &&
			topology["10.0.0.2:4000"].Labels[config.ClusterLabelName] == "cluster-b"
	}, 5*time.Second, 100*time.Millisecond)
	require.Greater(t, dnsA.QueryCount("pd-a.test"), 0)
	require.Greater(t, dnsB.QueryCount("pd-b.test"), 0)
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
