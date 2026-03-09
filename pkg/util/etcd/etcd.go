// Copyright 2023 PingCAP, Inc.
// SPDX-License-Identifier: Apache-2.0

package etcd

import (
	"context"
	"crypto/tls"
	"fmt"
	"net"
	"net/url"
	"strings"
	"time"

	"github.com/pingcap/tiproxy/lib/config"
	"github.com/pingcap/tiproxy/lib/util/errors"
	"github.com/pingcap/tiproxy/lib/util/retry"
	"github.com/pingcap/tiproxy/pkg/manager/cert"
	"go.etcd.io/etcd/api/v3/mvccpb"
	"go.etcd.io/etcd/client/pkg/v3/transport"
	clientv3 "go.etcd.io/etcd/client/v3"
	"go.etcd.io/etcd/server/v3/embed"
	"go.uber.org/zap"
	"google.golang.org/grpc"
	"google.golang.org/grpc/backoff"
	"google.golang.org/grpc/keepalive"
)

// InitEtcdClient initializes an etcd client that connects to PD ETCD server.
func InitEtcdClient(logger *zap.Logger, cfg *config.Config, certMgr *cert.CertManager) (*clientv3.Client, error) {
	return InitEtcdClientWithAddrs(logger, cfg.Proxy.PDAddrs, certMgr.ClusterTLS())
}

func InitEtcdClientWithAddrs(logger *zap.Logger, pdAddrs string, tlsConfig *tls.Config) (*clientv3.Client, error) {
	return InitEtcdClientWithAddrsAndDialer(logger, pdAddrs, tlsConfig, nil)
}

func InitEtcdClientWithAddrsAndDialer(logger *zap.Logger, pdAddrs string, tlsConfig *tls.Config, dialContext func(context.Context, string) (net.Conn, error)) (*clientv3.Client, error) {
	if len(strings.TrimSpace(pdAddrs)) == 0 {
		// use tidb server addresses directly
		return nil, nil
	}
	pdEndpoints := strings.Split(pdAddrs, ",")
	for i := range pdEndpoints {
		pdEndpoints[i] = strings.TrimSpace(pdEndpoints[i])
	}
	logger.Info("connect ETCD servers", zap.Strings("addrs", pdEndpoints))
	dialOpts := []grpc.DialOption{
		grpc.WithKeepaliveParams(keepalive.ClientParameters{
			Time:    10 * time.Second,
			Timeout: 3 * time.Second,
		}),
		grpc.WithConnectParams(grpc.ConnectParams{
			Backoff: backoff.Config{
				BaseDelay:  time.Second,
				Multiplier: 1.1,
				Jitter:     0.1,
				MaxDelay:   3 * time.Second,
			},
			MinConnectTimeout: 3 * time.Second,
		}),
	}
	if dialContext != nil {
		dialOpts = append(dialOpts, grpc.WithContextDialer(dialContext))
	}
	etcdClient, err := clientv3.New(clientv3.Config{
		Endpoints:        pdEndpoints,
		TLS:              tlsConfig,
		Logger:           logger.Named("etcdcli"),
		AutoSyncInterval: 30 * time.Second,
		DialTimeout:      5 * time.Second,
		DialOptions:      dialOpts,
	})
	return etcdClient, errors.Wrapf(err, "init etcd client failed")
}

func GetKVs(ctx context.Context, etcdCli *clientv3.Client, key string, opts []clientv3.OpOption, timeout, retryIntvl time.Duration, retryCnt uint64) ([]*mvccpb.KeyValue, error) {
	var resp *clientv3.GetResponse
	err := retry.Retry(func() error {
		childCtx, cancel := context.WithTimeout(ctx, timeout)
		var err error
		resp, err = etcdCli.Get(childCtx, key, opts...)
		cancel()
		return errors.WithStack(err)
	}, ctx, retryIntvl, retryCnt)
	if err != nil {
		return nil, err
	}
	return resp.Kvs, nil
}

// CreateEtcdServer creates an etcd server and is only used for testing.
func CreateEtcdServer(addr, dir string, lg *zap.Logger) (*embed.Etcd, error) {
	serverURL, err := url.Parse(fmt.Sprintf("http://%s", addr))
	if err != nil {
		return nil, err
	}
	cfg := embed.NewConfig()
	cfg.Dir = dir
	cfg.ListenClientUrls = []url.URL{*serverURL}
	cfg.ListenPeerUrls = []url.URL{*serverURL}
	cfg.ZapLoggerBuilder = embed.NewZapLoggerBuilder(lg)
	cfg.LogLevel = "fatal"
	// Reuse port so that it can reboot with the same port immediately.
	cfg.SocketOpts = transport.SocketOpts{
		ReuseAddress: true,
		ReusePort:    true,
	}
	etcd, err := embed.StartEtcd(cfg)
	if err != nil {
		return nil, err
	}
	<-etcd.Server.ReadyNotify()
	return etcd, err
}

func ConfigForEtcdTest(endpoint string) *config.Config {
	return &config.Config{
		Proxy: config.ProxyServer{
			Addr:    "0.0.0.0:6000",
			PDAddrs: endpoint,
		},
		API: config.API{
			Addr: "0.0.0.0:3080",
		},
	}
}
