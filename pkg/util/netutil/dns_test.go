// Copyright 2026 PingCAP, Inc.
// SPDX-License-Identifier: Apache-2.0

package netutil

import (
	"context"
	"net"
	"strconv"
	"testing"
	"time"

	"github.com/pingcap/tiproxy/pkg/testkit"
	"github.com/stretchr/testify/require"
)

func TestDNSDialerUsesConfiguredNameServerAndCache(t *testing.T) {
	listener, addr := testkit.StartListener(t, "127.0.0.1:0")
	t.Cleanup(func() { require.NoError(t, listener.Close()) })
	_, port := testkit.ParseHostPort(t, addr)
	dns := testkit.StartDNSServer(t, map[string][]string{
		"tidb.test": {"127.0.0.1"},
	})

	for range 2 {
		go func() {
			conn, err := listener.Accept()
			require.NoError(t, err)
			require.NoError(t, conn.Close())
		}()
	}

	dialer := NewDNSDialer([]string{dns.Addr()})
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()

	conn, err := dialer.DialContext(ctx, "tcp", net.JoinHostPort("tidb.test", strconv.Itoa(int(port))))
	require.NoError(t, err)
	require.NoError(t, conn.Close())
	queryCount := dns.QueryCount("tidb.test")
	require.Greater(t, queryCount, 0)

	conn, err = dialer.DialContext(ctx, "tcp", net.JoinHostPort("tidb.test", strconv.Itoa(int(port))))
	require.NoError(t, err)
	require.NoError(t, conn.Close())
	require.Equal(t, queryCount, dns.QueryCount("tidb.test"))
}

func TestDNSDialerFallbackToSystemResolver(t *testing.T) {
	listener, addr := testkit.StartListener(t, "127.0.0.1:0")
	t.Cleanup(func() { require.NoError(t, listener.Close()) })
	_, port := testkit.ParseHostPort(t, addr)
	go func() {
		conn, err := listener.Accept()
		require.NoError(t, err)
		require.NoError(t, conn.Close())
	}()

	dialer := NewDNSDialer(nil)
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	conn, err := dialer.DialContext(ctx, "tcp", net.JoinHostPort("localhost", strconv.Itoa(int(port))))
	require.NoError(t, err)
	require.NoError(t, conn.Close())
}
