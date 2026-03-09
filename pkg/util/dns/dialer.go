// Copyright 2026 PingCAP, Inc.
// SPDX-License-Identifier: Apache-2.0

package dns

import (
	"context"
	"net"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/pingcap/tiproxy/lib/config"
	"github.com/pingcap/tiproxy/lib/util/errors"
	"go.uber.org/zap"
)

const (
	defaultDNSPort  = "53"
	defaultCacheTTL = 30 * time.Second
)

type cacheEntry struct {
	addrs    []string
	expireAt time.Time
}

// Dialer dials backend addresses with a dedicated DNS server list.
// It keeps a small in-process DNS cache to reduce repeated lookups.
type Dialer struct {
	lg         *zap.Logger
	nsServers  []string
	cacheTTL   time.Duration
	dialCtx    func(context.Context, string, string) (net.Conn, error)
	lookupHost func(context.Context, string) ([]net.IPAddr, error)

	mu    sync.Mutex
	cache map[string]cacheEntry
}

func NewDialer(lg *zap.Logger, nsServers string) (*Dialer, error) {
	if lg == nil {
		lg = zap.NewNop()
	}
	servers, err := ParseNSServers(nsServers)
	if err != nil {
		return nil, err
	}

	d := &Dialer{
		lg:        lg,
		nsServers: servers,
		cacheTTL:  defaultCacheTTL,
		dialCtx:   (&net.Dialer{}).DialContext,
		cache:     make(map[string]cacheEntry),
	}

	if len(servers) > 0 {
		var seq uint64
		resolver := &net.Resolver{
			PreferGo: true,
			Dial: func(ctx context.Context, network, _ string) (net.Conn, error) {
				// Round-robin among configured DNS servers.
				idx := int(seq % uint64(len(servers)))
				seq++
				return (&net.Dialer{}).DialContext(ctx, network, servers[idx])
			},
		}
		d.lookupHost = resolver.LookupIPAddr
	}
	return d, nil
}

// GRPCDialContext matches grpc.WithContextDialer signature.
func (d *Dialer) GRPCDialContext(ctx context.Context, address string) (net.Conn, error) {
	return d.DialContext(ctx, "tcp", address, 0)
}

func (d *Dialer) DialContext(ctx context.Context, network, address string, timeout time.Duration) (net.Conn, error) {
	host, port, err := net.SplitHostPort(address)
	if err != nil {
		return nil, errors.WithStack(err)
	}
	if len(d.nsServers) == 0 || net.ParseIP(host) != nil {
		return d.directDial(ctx, network, address, timeout)
	}

	addrs, err := d.resolveHost(ctx, host)
	if err != nil {
		return nil, err
	}
	var lastErr error
	for _, ip := range addrs {
		target := net.JoinHostPort(ip, port)
		conn, dialErr := d.directDial(ctx, network, target, timeout)
		if dialErr == nil {
			return conn, nil
		}
		lastErr = dialErr
	}
	if lastErr != nil {
		return nil, lastErr
	}
	return nil, errors.New("failed to dial backend")
}

func (d *Dialer) directDial(ctx context.Context, network, address string, timeout time.Duration) (net.Conn, error) {
	if timeout <= 0 {
		conn, err := d.dialCtx(ctx, network, address)
		return conn, errors.WithStack(err)
	}
	childCtx, cancel := context.WithTimeout(ctx, timeout)
	defer cancel()
	conn, err := d.dialCtx(childCtx, network, address)
	return conn, errors.WithStack(err)
}

func (d *Dialer) resolveHost(ctx context.Context, host string) ([]string, error) {
	now := time.Now()
	d.mu.Lock()
	if entry, ok := d.cache[host]; ok && now.Before(entry.expireAt) {
		addrs := append([]string(nil), entry.addrs...)
		d.mu.Unlock()
		return addrs, nil
	}
	d.mu.Unlock()

	if d.lookupHost == nil {
		return nil, errors.Errorf("resolver for host %s is unavailable", host)
	}
	ips, err := d.lookupHost(ctx, host)
	if err != nil {
		return nil, errors.Wrapf(err, "resolve host %s failed", host)
	}
	addrs := make([]string, 0, len(ips))
	for _, ip := range ips {
		if len(ip.IP) == 0 {
			continue
		}
		addrs = append(addrs, ip.IP.String())
	}
	if len(addrs) == 0 {
		return nil, errors.Errorf("resolve host %s returned empty result", host)
	}
	d.mu.Lock()
	d.cache[host] = cacheEntry{
		addrs:    append([]string(nil), addrs...),
		expireAt: now.Add(d.cacheTTL),
	}
	d.mu.Unlock()
	return addrs, nil
}

func ParseNSServers(nsServers string) ([]string, error) {
	parts := strings.Split(nsServers, ",")
	servers := make([]string, 0, len(parts))
	for _, part := range parts {
		server := strings.TrimSpace(part)
		if len(server) == 0 {
			continue
		}
		addr, err := normalizeNSServer(server)
		if err != nil {
			return nil, err
		}
		servers = append(servers, addr)
	}
	return servers, nil
}

func normalizeNSServer(server string) (string, error) {
	host, port, err := net.SplitHostPort(server)
	if err != nil {
		trimmed := strings.TrimSpace(server)
		host = strings.TrimSuffix(strings.TrimPrefix(trimmed, "["), "]")
		port = defaultDNSPort
	}
	host = strings.TrimSpace(strings.TrimSuffix(strings.TrimPrefix(host, "["), "]"))
	if len(host) == 0 {
		return "", errors.New("ns server host is empty")
	}
	p, err := strconv.Atoi(port)
	if err != nil || p < 1 || p > 65535 {
		return "", errors.New("ns server port is invalid")
	}
	return net.JoinHostPort(host, strconv.Itoa(p)), nil
}

type ClusterDialerManager struct {
	lg *zap.Logger
	mu struct {
		sync.RWMutex
		dialers map[string]*Dialer
	}
}

func NewClusterDialerManager(lg *zap.Logger) *ClusterDialerManager {
	if lg == nil {
		lg = zap.NewNop()
	}
	mgr := &ClusterDialerManager{
		lg: lg,
	}
	mgr.mu.dialers = make(map[string]*Dialer)
	return mgr
}

func (mgr *ClusterDialerManager) UpdateConfig(cfg *config.Config) error {
	if cfg == nil {
		return nil
	}
	clusters := cfg.GetBackendClusters()
	newDialers := make(map[string]*Dialer, len(clusters))
	for _, cluster := range clusters {
		d, err := NewDialer(mgr.lg.With(zap.String("cluster", cluster.Name)), cluster.NSServers)
		if err != nil {
			return errors.Wrapf(err, "invalid ns-servers for cluster %s", cluster.Name)
		}
		newDialers[cluster.Name] = d
	}
	mgr.mu.Lock()
	mgr.mu.dialers = newDialers
	mgr.mu.Unlock()
	return nil
}

func (mgr *ClusterDialerManager) DialContext(ctx context.Context, network, address, cluster string, timeout time.Duration) (net.Conn, error) {
	mgr.mu.RLock()
	dialer := mgr.mu.dialers[cluster]
	mgr.mu.RUnlock()
	if dialer == nil {
		base := &Dialer{
			lg:      mgr.lg,
			dialCtx: (&net.Dialer{}).DialContext,
		}
		return base.directDial(ctx, network, address, timeout)
	}
	return dialer.DialContext(ctx, network, address, timeout)
}
