// Copyright 2026 PingCAP, Inc.
// SPDX-License-Identifier: Apache-2.0

package backendcluster

import (
	"context"
	"crypto/tls"
	"net"

	httputil "github.com/pingcap/tiproxy/pkg/util/http"
	"github.com/pingcap/tiproxy/pkg/util/netutil"
)

// NetworkRouter is a thin dispatch view over cluster-scoped dialers and HTTP clients.
// It does not own any cluster lifecycle by itself.
type NetworkRouter struct {
	manager     *Manager
	defaultDial *netutil.DNSDialer
	defaultHTTP *httputil.Client
}

func NewNetworkRouter(manager *Manager, clusterTLS func() *tls.Config) *NetworkRouter {
	if clusterTLS == nil {
		clusterTLS = func() *tls.Config { return nil }
	}
	return &NetworkRouter{
		manager:     manager,
		defaultDial: netutil.NewDNSDialer(nil),
		defaultHTTP: httputil.NewHTTPClientWithDialContext(clusterTLS, nil),
	}
}

func (nr *NetworkRouter) HTTPClient(clusterName string) *httputil.Client {
	if nr == nil {
		return httputil.NewHTTPClientWithDialContext(func() *tls.Config { return nil }, nil)
	}
	if nr.manager == nil {
		return nr.defaultHTTP
	}
	if clusterName != "" {
		if cluster := nr.manager.Snapshot()[clusterName]; cluster != nil && cluster.HTTPClient() != nil {
			return cluster.HTTPClient()
		}
	}
	return nr.defaultHTTP
}

func (nr *NetworkRouter) DialContext(ctx context.Context, network, addr, clusterName string) (net.Conn, error) {
	if nr == nil {
		var dialer net.Dialer
		return dialer.DialContext(ctx, network, addr)
	}
	if nr.manager != nil && clusterName != "" {
		if cluster := nr.manager.Snapshot()[clusterName]; cluster != nil {
			return cluster.DialContext(ctx, network, addr)
		}
	}
	return nr.defaultDial.DialContext(ctx, network, addr)
}
