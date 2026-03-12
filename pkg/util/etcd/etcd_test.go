// Copyright 2024 PingCAP, Inc.
// SPDX-License-Identifier: Apache-2.0

package etcd

import (
	"context"
	"encoding/binary"
	"fmt"
	"io"
	"net"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/pingcap/tiproxy/lib/util/logger"
	"github.com/pingcap/tiproxy/pkg/manager/cert"
	"github.com/pingcap/tiproxy/pkg/util/dns"
	"github.com/stretchr/testify/require"
	"go.uber.org/zap"
	"golang.org/x/net/dns/dnsmessage"
)

func TestEtcdClient(t *testing.T) {
	lg, _ := logger.CreateLoggerForTest(t)
	server, err := CreateEtcdServer("0.0.0.0:0", t.TempDir(), lg)
	require.NoError(t, err)
	endpoint := server.Clients[0].Addr().String()

	cfg := ConfigForEtcdTest(endpoint)
	certMgr := cert.NewCertManager()
	err = certMgr.Init(cfg, lg, nil)
	require.NoError(t, err)
	client, err := InitEtcdClient(lg, cfg, certMgr)
	require.NoError(t, err)

	_, err = client.Put(context.Background(), "key", "value")
	require.NoError(t, err)
	kvs, err := GetKVs(context.Background(), client, "key", nil, 3*time.Second, 10*time.Millisecond, 3)
	require.NoError(t, err)
	require.Equal(t, "value", string(kvs[0].Value))

	require.NoError(t, client.Close())
	server.Close()
}

func TestEtcdClientWithCustomDialer(t *testing.T) {
	lg, _ := logger.CreateLoggerForTest(t)
	server, err := CreateEtcdServer("0.0.0.0:0", t.TempDir(), lg)
	require.NoError(t, err)
	endpoint := server.Clients[0].Addr().String()

	called := int32(0)
	client, err := InitEtcdClientWithAddrsAndDialer(lg, endpoint, nil, func(ctx context.Context, address string) (net.Conn, error) {
		atomic.AddInt32(&called, 1)
		return (&net.Dialer{}).DialContext(ctx, "tcp", address)
	})
	require.NoError(t, err)

	_, err = client.Put(context.Background(), "key", "value")
	require.NoError(t, err)
	require.Greater(t, atomic.LoadInt32(&called), int32(0))

	require.NoError(t, client.Close())
	server.Close()
}

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
	t.Cleanup(func() { srv.close() })
	return srv
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
			defer func() { _ = conn.Close() }()
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

func TestEtcdClientWithDNSDialerExpandedEndpoints(t *testing.T) {
	lg, _ := logger.CreateLoggerForTest(t)
	server, err := CreateEtcdServer("0.0.0.0:0", t.TempDir(), lg)
	require.NoError(t, err)
	t.Cleanup(server.Close)

	_, port, err := net.SplitHostPort(server.Clients[0].Addr().String())
	require.NoError(t, err)

	dnsSrv := newFakeDNSServer(t, map[string]string{
		"pd-a.test.": "127.0.0.1",
	})
	d, err := dns.NewDialer(zap.NewNop(), dnsSrv.addr())
	require.NoError(t, err)

	pdAddrs := net.JoinHostPort("pd-a.test.", port)
	client, err := InitEtcdClientWithAddrsAndDNSDialer(lg, pdAddrs, nil, d)
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, client.Close()) })

	_, err = client.Put(context.Background(), "dns/key", "value")
	require.NoError(t, err)

	endpoints := client.Endpoints()
	require.Len(t, endpoints, 1)
	require.True(t, strings.HasPrefix(endpoints[0], "tiproxy-resolved://pd-a.test./"))
	require.Eventually(t, func() bool {
		return dnsSrv.queryCount("pd-a.test") > 0
	}, 3*time.Second, 50*time.Millisecond)
}
