// Copyright 2023 PingCAP, Inc.
// SPDX-License-Identifier: Apache-2.0

package infosync

import (
	"context"
	"crypto/tls"
	"encoding/json"
	"path"
	"sync"
	"testing"
	"time"

	"github.com/pingcap/tiproxy/lib/config"
	"github.com/pingcap/tiproxy/lib/util/logger"
	"github.com/pingcap/tiproxy/pkg/manager/cert"
	"github.com/pingcap/tiproxy/pkg/util/etcd"
	"github.com/stretchr/testify/require"
	clientv3 "go.etcd.io/etcd/client/v3"
	"go.etcd.io/etcd/server/v3/embed"
)

type mockConfigGetter struct {
	mu  sync.RWMutex
	cfg *config.Config
}

func (mcg *mockConfigGetter) GetConfig() *config.Config {
	mcg.mu.RLock()
	cfg := mcg.cfg.Clone()
	mcg.mu.RUnlock()
	return cfg
}

func (mcg *mockConfigGetter) SetConfig(cfg *config.Config) {
	mcg.mu.Lock()
	mcg.cfg = cfg
	mcg.mu.Unlock()
}

type testEtcdCluster struct {
	etcd   *embed.Etcd
	client *clientv3.Client
	kv     clientv3.KV
	addr   string
}

func newTestEtcdCluster(t *testing.T) *testEtcdCluster {
	lg, _ := logger.CreateLoggerForTest(t)
	etcdSrv, err := etcd.CreateEtcdServer("0.0.0.0:0", t.TempDir(), lg)
	require.NoError(t, err)
	addr := etcdSrv.Clients[0].Addr().String()
	cfg := etcd.ConfigForEtcdTest(addr)
	certMgr := cert.NewCertManager()
	require.NoError(t, certMgr.Init(cfg, lg, nil))
	cli, err := etcd.InitEtcdClient(lg, cfg, certMgr)
	require.NoError(t, err)
	return &testEtcdCluster{
		etcd:   etcdSrv,
		client: cli,
		kv:     clientv3.NewKV(cli),
		addr:   addr,
	}
}

func (tec *testEtcdCluster) close(t *testing.T) {
	require.NoError(t, tec.client.Close())
	tec.etcd.Close()
}

func (tec *testEtcdCluster) putTopology(t *testing.T, sqlAddr string, info *TiDBTopologyInfo) {
	data, err := json.Marshal(info)
	require.NoError(t, err)
	_, err = tec.kv.Put(context.Background(), path.Join(tidbTopologyInformationPath, sqlAddr, infoSuffix), string(data))
	require.NoError(t, err)
	_, err = tec.kv.Put(context.Background(), path.Join(tidbTopologyInformationPath, sqlAddr, ttlSuffix), "1")
	require.NoError(t, err)
}

func (tec *testEtcdCluster) putPromInfo(t *testing.T, info *PrometheusInfo) {
	data, err := json.Marshal(info)
	require.NoError(t, err)
	_, err = tec.kv.Put(context.Background(), promTopologyPath, string(data))
	require.NoError(t, err)
}

func TestMultiClusterFetcherDynamicUpdate(t *testing.T) {
	clusterA := newTestEtcdCluster(t)
	clusterB := newTestEtcdCluster(t)
	clusterC := newTestEtcdCluster(t)
	t.Cleanup(func() { clusterA.close(t) })
	t.Cleanup(func() { clusterB.close(t) })
	t.Cleanup(func() { clusterC.close(t) })

	clusterA.putTopology(t, "1.1.1.1:4000", &TiDBTopologyInfo{
		IP:         "1.1.1.1",
		StatusPort: 10080,
	})
	clusterB.putTopology(t, "2.2.2.2:4000", &TiDBTopologyInfo{
		IP:         "2.2.2.2",
		StatusPort: 10080,
	})
	clusterC.putTopology(t, "3.3.3.3:4000", &TiDBTopologyInfo{
		IP:         "3.3.3.3",
		StatusPort: 10080,
	})
	clusterB.putPromInfo(t, &PrometheusInfo{
		IP:   "2.2.2.2",
		Port: 9090,
	})

	initialCfg := config.NewConfig()
	initialCfg.Proxy.PDAddrs = ""
	initialCfg.Proxy.BackendClusters = []config.BackendCluster{
		{
			Name:    "cluster-a",
			PDAddrs: clusterA.addr,
		},
		{
			Name:    "cluster-b",
			PDAddrs: clusterB.addr,
		},
	}
	cfgGetter := &mockConfigGetter{
		cfg: initialCfg,
	}
	cfgCh := make(chan *config.Config, 8)

	lg, _ := logger.CreateLoggerForTest(t)
	ctx, cancel := context.WithCancel(context.Background())
	t.Cleanup(cancel)
	fetcher := NewMultiClusterFetcher(lg, func() *tls.Config { return nil }, cfgGetter, cfgCh)
	require.NoError(t, fetcher.Start(ctx))
	t.Cleanup(func() {
		require.NoError(t, fetcher.Close())
	})

	require.Eventually(t, func() bool {
		topology, err := fetcher.GetTiDBTopology(context.Background())
		if err != nil {
			return false
		}
		if len(topology) != 2 {
			return false
		}
		return topology["1.1.1.1:4000"].Labels[config.ClusterLabelName] == "cluster-a" &&
			topology["2.2.2.2:4000"].Labels[config.ClusterLabelName] == "cluster-b"
	}, 5*time.Second, 100*time.Millisecond)

	promInfo, err := fetcher.GetPromInfo(context.Background())
	require.NoError(t, err)
	require.Equal(t, "2.2.2.2", promInfo.IP)
	require.Equal(t, 9090, promInfo.Port)

	updatedCfg := initialCfg.Clone()
	updatedCfg.Proxy.BackendClusters = []config.BackendCluster{
		{
			Name:    "cluster-c",
			PDAddrs: clusterC.addr,
		},
	}
	cfgGetter.SetConfig(updatedCfg)
	cfgCh <- updatedCfg.Clone()

	require.Eventually(t, func() bool {
		topology, err := fetcher.GetTiDBTopology(context.Background())
		if err != nil {
			return false
		}
		if len(topology) != 1 {
			return false
		}
		backend, ok := topology["3.3.3.3:4000"]
		return ok && backend.Labels[config.ClusterLabelName] == "cluster-c"
	}, 5*time.Second, 100*time.Millisecond)

	_, err = fetcher.GetPromInfo(context.Background())
	require.ErrorIs(t, err, ErrNoProm)
}
