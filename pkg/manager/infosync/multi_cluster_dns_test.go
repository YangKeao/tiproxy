// Copyright 2026 PingCAP, Inc.
// SPDX-License-Identifier: Apache-2.0

package infosync

import (
	"context"
	"crypto/tls"
	"encoding/binary"
	"fmt"
	"io"
	"net"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/pingcap/tiproxy/lib/config"
	"github.com/pingcap/tiproxy/lib/util/logger"
	"github.com/stretchr/testify/require"
	"golang.org/x/net/dns/dnsmessage"
)

type fakeDNSServer struct {
	records map[string]net.IP
	queries map[string]int

	udpConn net.PacketConn
	tcpLn   net.Listener

	mu     sync.Mutex
	cancel context.CancelFunc
	wg     sync.WaitGroup
}

func newFakeDNSServer(t *testing.T, records map[string]string) *fakeDNSServer {
	t.Helper()

	normalizedRecords := make(map[string]net.IP, len(records))
	for host, ipStr := range records {
		ip := net.ParseIP(ipStr)
		require.NotNilf(t, ip, "invalid IP %s", ipStr)
		normalizedRecords[normalizeDNSHost(host)] = ip
	}

	udpConn, err := net.ListenPacket("udp", "127.0.0.1:0")
	require.NoError(t, err)
	port := udpConn.LocalAddr().(*net.UDPAddr).Port

	tcpLn, err := net.Listen("tcp", fmt.Sprintf("127.0.0.1:%d", port))
	require.NoError(t, err)

	ctx, cancel := context.WithCancel(context.Background())
	srv := &fakeDNSServer{
		records: normalizedRecords,
		queries: make(map[string]int),
		udpConn: udpConn,
		tcpLn:   tcpLn,
		cancel:  cancel,
	}
	srv.wg.Add(2)
	go srv.serveUDP(ctx)
	go srv.serveTCP(ctx)
	t.Cleanup(func() {
		srv.close()
	})
	return srv
}

func (srv *fakeDNSServer) close() {
	if srv.cancel != nil {
		srv.cancel()
	}
	if srv.udpConn != nil {
		_ = srv.udpConn.Close()
	}
	if srv.tcpLn != nil {
		_ = srv.tcpLn.Close()
	}
	srv.wg.Wait()
}

func (srv *fakeDNSServer) addr() string {
	return srv.udpConn.LocalAddr().String()
}

func (srv *fakeDNSServer) queryCount(host string) int {
	host = normalizeDNSHost(host)
	srv.mu.Lock()
	defer srv.mu.Unlock()
	return srv.queries[host]
}

func (srv *fakeDNSServer) serveUDP(ctx context.Context) {
	defer srv.wg.Done()
	buf := make([]byte, 4096)
	for {
		n, addr, err := srv.udpConn.ReadFrom(buf)
		if err != nil {
			if ctx.Err() != nil {
				return
			}
			return
		}
		resp, err := srv.buildResponse(buf[:n])
		if err != nil {
			continue
		}
		_, _ = srv.udpConn.WriteTo(resp, addr)
	}
}

func (srv *fakeDNSServer) serveTCP(ctx context.Context) {
	defer srv.wg.Done()
	for {
		conn, err := srv.tcpLn.Accept()
		if err != nil {
			if ctx.Err() != nil {
				return
			}
			return
		}
		srv.wg.Add(1)
		go func() {
			defer srv.wg.Done()
			defer func() {
				_ = conn.Close()
			}()
			srv.handleTCPConn(conn)
		}()
	}
}

func (srv *fakeDNSServer) handleTCPConn(conn net.Conn) {
	var lenBuf [2]byte
	for {
		if _, err := io.ReadFull(conn, lenBuf[:]); err != nil {
			return
		}
		msgLen := int(binary.BigEndian.Uint16(lenBuf[:]))
		if msgLen == 0 {
			return
		}
		msg := make([]byte, msgLen)
		if _, err := io.ReadFull(conn, msg); err != nil {
			return
		}

		resp, err := srv.buildResponse(msg)
		if err != nil {
			return
		}
		binary.BigEndian.PutUint16(lenBuf[:], uint16(len(resp)))
		if _, err := conn.Write(lenBuf[:]); err != nil {
			return
		}
		if _, err := conn.Write(resp); err != nil {
			return
		}
	}
}

func (srv *fakeDNSServer) buildResponse(req []byte) ([]byte, error) {
	var msg dnsmessage.Message
	if err := msg.Unpack(req); err != nil {
		return nil, err
	}

	resp := dnsmessage.Message{
		Header: dnsmessage.Header{
			ID:                 msg.Header.ID,
			Response:           true,
			Authoritative:      true,
			RecursionAvailable: true,
		},
		Questions: msg.Questions,
	}
	knownHost := false
	for _, question := range msg.Questions {
		host := normalizeDNSHost(question.Name.String())
		srv.mu.Lock()
		srv.queries[host]++
		ip, ok := srv.records[host]
		srv.mu.Unlock()
		if !ok {
			continue
		}
		knownHost = true
		if question.Type != dnsmessage.TypeA {
			continue
		}
		ip4 := ip.To4()
		if ip4 == nil {
			continue
		}
		var a [4]byte
		copy(a[:], ip4)
		resp.Answers = append(resp.Answers, dnsmessage.Resource{
			Header: dnsmessage.ResourceHeader{
				Name:  question.Name,
				Type:  dnsmessage.TypeA,
				Class: dnsmessage.ClassINET,
				TTL:   1,
			},
			Body: &dnsmessage.AResource{A: a},
		})
	}
	if !knownHost {
		resp.Header.RCode = dnsmessage.RCodeNameError
	}
	return resp.Pack()
}

func normalizeDNSHost(host string) string {
	host = strings.TrimSpace(host)
	host = strings.TrimSuffix(host, ".")
	return strings.ToLower(host)
}

func TestMultiClusterFetcherClusterSpecificNSServers(t *testing.T) {
	clusterA := newTestEtcdCluster(t)
	clusterB := newTestEtcdCluster(t)
	t.Cleanup(func() { clusterA.close(t) })
	t.Cleanup(func() { clusterB.close(t) })

	clusterA.putTopology(t, "10.1.1.1:4000", &TiDBTopologyInfo{IP: "10.1.1.1", StatusPort: 10080})
	clusterB.putTopology(t, "10.2.2.2:4000", &TiDBTopologyInfo{IP: "10.2.2.2", StatusPort: 10080})

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
		backendA, okA := topology["10.1.1.1:4000"]
		backendB, okB := topology["10.2.2.2:4000"]
		if !okA || !okB {
			return false
		}
		return backendA.Labels[config.ClusterLabelName] == "cluster-a" &&
			backendB.Labels[config.ClusterLabelName] == "cluster-b"
	}, 5*time.Second, 100*time.Millisecond)

	require.Eventually(t, func() bool {
		return dnsA.queryCount("pd-a.test") > 0 && dnsB.queryCount("pd-b.test") > 0
	}, 5*time.Second, 100*time.Millisecond)

	require.Equal(t, 0, dnsA.queryCount("pd-b.test"))
	require.Equal(t, 0, dnsB.queryCount("pd-a.test"))
}
