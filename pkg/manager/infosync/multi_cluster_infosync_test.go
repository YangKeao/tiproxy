// Copyright 2026 PingCAP, Inc.
// SPDX-License-Identifier: Apache-2.0

package infosync

import (
	"context"
	"crypto/tls"
	"net"
	"testing"
	"time"

	"github.com/pingcap/tiproxy/lib/config"
	"github.com/pingcap/tiproxy/lib/util/logger"
	"github.com/stretchr/testify/require"
	clientv3 "go.etcd.io/etcd/client/v3"
)

func TestMultiClusterInfoSyncerDynamicUpdate(t *testing.T) {
	clusterA := newTestEtcdCluster(t)
	clusterB := newTestEtcdCluster(t)
	t.Cleanup(func() { clusterA.close(t) })
	t.Cleanup(func() { clusterB.close(t) })

	initialCfg := config.NewConfig()
	initialCfg.Proxy.PDAddrs = ""
	initialCfg.Proxy.BackendClusters = nil
	cfgGetter := &mockConfigGetter{cfg: initialCfg}
	cfgCh := make(chan *config.Config, 8)

	lg, _ := logger.CreateLoggerForTest(t)
	ctx, cancel := context.WithCancel(context.Background())
	t.Cleanup(cancel)
	syncer := NewMultiClusterInfoSyncer(lg, func() *tls.Config { return nil }, cfgGetter, cfgCh)
	require.NoError(t, syncer.Start(ctx))
	t.Cleanup(func() {
		require.NoError(t, syncer.Close())
	})

	hasTopology := func(cluster *testEtcdCluster) bool {
		resp, err := cluster.client.Get(context.Background(), tiproxyTopologyPath, clientv3.WithPrefix())
		require.NoError(t, err)
		return len(resp.Kvs) > 0
	}

	require.False(t, hasTopology(clusterA))
	require.False(t, hasTopology(clusterB))

	updatedA := initialCfg.Clone()
	updatedA.Proxy.BackendClusters = []config.BackendCluster{
		{
			Name:    "cluster-a",
			PDAddrs: clusterA.addr,
		},
	}
	cfgGetter.SetConfig(updatedA)
	cfgCh <- updatedA.Clone()
	require.Eventually(t, func() bool {
		return hasTopology(clusterA)
	}, 5*time.Second, 100*time.Millisecond)
	require.False(t, hasTopology(clusterB))

	updatedAB := updatedA.Clone()
	updatedAB.Proxy.BackendClusters = append(updatedAB.Proxy.BackendClusters, config.BackendCluster{
		Name:    "cluster-b",
		PDAddrs: clusterB.addr,
	})
	cfgGetter.SetConfig(updatedAB)
	cfgCh <- updatedAB.Clone()
	require.Eventually(t, func() bool {
		return hasTopology(clusterA) && hasTopology(clusterB)
	}, 5*time.Second, 100*time.Millisecond)

	updatedB := updatedAB.Clone()
	updatedB.Proxy.BackendClusters = []config.BackendCluster{
		{
			Name:    "cluster-b",
			PDAddrs: clusterB.addr,
		},
	}
	cfgGetter.SetConfig(updatedB)
	cfgCh <- updatedB.Clone()
	require.Eventually(t, func() bool {
		return !hasTopology(clusterA) && hasTopology(clusterB)
	}, 5*time.Second, 100*time.Millisecond)
}

func TestMultiClusterInfoSyncerResolvePDAddrsWithClusterNSServers(t *testing.T) {
	clusterA := newTestEtcdCluster(t)
	clusterB := newTestEtcdCluster(t)
	t.Cleanup(func() { clusterA.close(t) })
	t.Cleanup(func() { clusterB.close(t) })

	dnsA := newFakeDNSServer(t, map[string]string{
		"pd-a.test.": "127.0.0.1",
	})
	dnsB := newFakeDNSServer(t, map[string]string{
		"pd-b.test.": "127.0.0.1",
	})

	_, portA, err := net.SplitHostPort(clusterA.addr)
	require.NoError(t, err)
	_, portB, err := net.SplitHostPort(clusterB.addr)
	require.NoError(t, err)

	cfg := config.NewConfig()
	cfg.Proxy.PDAddrs = ""
	cfg.Proxy.BackendClusters = []config.BackendCluster{
		{
			Name:      "cluster-a",
			PDAddrs:   net.JoinHostPort("pd-a.test.", portA),
			NSServers: dnsA.addr(),
		},
		{
			Name:      "cluster-b",
			PDAddrs:   net.JoinHostPort("pd-b.test.", portB),
			NSServers: dnsB.addr(),
		},
	}
	cfgGetter := &mockConfigGetter{cfg: cfg}
	cfgCh := make(chan *config.Config, 8)

	lg, _ := logger.CreateLoggerForTest(t)
	ctx, cancel := context.WithCancel(context.Background())
	t.Cleanup(cancel)
	syncer := NewMultiClusterInfoSyncer(lg, func() *tls.Config { return nil }, cfgGetter, cfgCh)
	require.NoError(t, syncer.Start(ctx))
	t.Cleanup(func() {
		require.NoError(t, syncer.Close())
	})

	hasTopology := func(cluster *testEtcdCluster) bool {
		resp, err := cluster.client.Get(context.Background(), tiproxyTopologyPath, clientv3.WithPrefix())
		require.NoError(t, err)
		return len(resp.Kvs) > 0
	}

	require.Eventually(t, func() bool {
		return hasTopology(clusterA) && hasTopology(clusterB)
	}, 5*time.Second, 100*time.Millisecond)

	require.Eventually(t, func() bool {
		return dnsA.queryCount("pd-a.test") > 0 && dnsB.queryCount("pd-b.test") > 0
	}, 5*time.Second, 100*time.Millisecond)

	require.Equal(t, 0, dnsA.queryCount("pd-b.test"))
	require.Equal(t, 0, dnsB.queryCount("pd-a.test"))
}
