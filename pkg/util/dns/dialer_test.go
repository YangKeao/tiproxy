// Copyright 2026 PingCAP, Inc.
// SPDX-License-Identifier: Apache-2.0

package dns

import (
	"context"
	"net"
	"sync/atomic"
	"testing"
	"time"

	"github.com/pingcap/tiproxy/lib/config"
	"github.com/stretchr/testify/require"
	"go.uber.org/zap"
)

func TestParseNSServers(t *testing.T) {
	servers, err := ParseNSServers("10.0.0.1,10.0.0.2:1053")
	require.NoError(t, err)
	require.Equal(t, []string{"10.0.0.1:53", "10.0.0.2:1053"}, servers)

	_, err = ParseNSServers("10.0.0.1:abc")
	require.Error(t, err)
}

func TestDialerResolveCache(t *testing.T) {
	d, err := NewDialer(zap.NewNop(), "10.0.0.1")
	require.NoError(t, err)
	d.cacheTTL = time.Minute
	called := int32(0)
	d.lookupHost = func(context.Context, string) ([]net.IPAddr, error) {
		atomic.AddInt32(&called, 1)
		return []net.IPAddr{{IP: net.ParseIP("127.0.0.1")}}, nil
	}

	addrs, err := d.resolveHost(context.Background(), "tidb.service")
	require.NoError(t, err)
	require.Equal(t, []string{"127.0.0.1"}, addrs)
	addrs, err = d.resolveHost(context.Background(), "tidb.service")
	require.NoError(t, err)
	require.Equal(t, []string{"127.0.0.1"}, addrs)
	require.Equal(t, int32(1), atomic.LoadInt32(&called))
}

func TestClusterDialerManagerUpdateConfig(t *testing.T) {
	cfg := config.NewConfig()
	cfg.Proxy.PDAddrs = ""
	cfg.Proxy.BackendClusters = []config.BackendCluster{
		{
			Name:      "cluster-a",
			PDAddrs:   "127.0.0.1:2379",
			NSServers: "10.0.0.1,10.0.0.2:1053",
		},
	}
	mgr := NewClusterDialerManager(zap.NewNop())
	require.NoError(t, mgr.UpdateConfig(cfg))

	mgr.mu.RLock()
	dialer := mgr.mu.dialers["cluster-a"]
	mgr.mu.RUnlock()
	require.NotNil(t, dialer)
	require.Equal(t, []string{"10.0.0.1:53", "10.0.0.2:1053"}, dialer.nsServers)
}

func TestResolvedAddressEncoding(t *testing.T) {
	encoded := EncodeResolvedAddress("pd-a.test", "127.0.0.1:2379")
	require.Equal(t, "tiproxy-resolved://pd-a.test/127.0.0.1:2379", encoded)

	target, ok := ParseResolvedAddress(encoded)
	require.True(t, ok)
	require.Equal(t, "127.0.0.1:2379", target)

	_, ok = ParseResolvedAddress("127.0.0.1:2379")
	require.False(t, ok)
}

func TestDialContextResolvedAddress(t *testing.T) {
	d, err := NewDialer(zap.NewNop(), "")
	require.NoError(t, err)

	dialed := ""
	d.dialCtx = func(ctx context.Context, network, address string) (net.Conn, error) {
		dialed = address
		return nil, context.DeadlineExceeded
	}

	encoded := EncodeResolvedAddress("pd-a.test", "127.0.0.1:2379")
	_, err = d.DialContext(context.Background(), "tcp", encoded, 0)
	require.Error(t, err)
	require.Equal(t, "127.0.0.1:2379", dialed)
}
