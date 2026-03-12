// Copyright 2026 PingCAP, Inc.
// SPDX-License-Identifier: Apache-2.0

package infosync

import (
	"context"
	"crypto/tls"
	"strings"
	"sync"

	"github.com/pingcap/tiproxy/lib/config"
	"github.com/pingcap/tiproxy/lib/util/errors"
	"github.com/pingcap/tiproxy/pkg/util/dns"
	"github.com/pingcap/tiproxy/pkg/util/etcd"
	"github.com/pingcap/tiproxy/pkg/util/waitgroup"
	clientv3 "go.etcd.io/etcd/client/v3"
	"go.uber.org/zap"
)

type infoSyncClusterClient struct {
	name      string
	pdAddrs   string
	nsServers string
	etcdCli   *clientv3.Client
	syncer    *InfoSyncer
}

// MultiClusterInfoSyncer syncs TiProxy topology into all configured backend clusters.
// It updates cluster syncers dynamically when configs change.
type MultiClusterInfoSyncer struct {
	lg         *zap.Logger
	clusterTLS func() *tls.Config
	cfgGetter  config.ConfigGetter
	cfgCh      <-chan *config.Config

	mu       sync.RWMutex
	clusters map[string]*infoSyncClusterClient

	wg     waitgroup.WaitGroup
	cancel context.CancelFunc
}

func NewMultiClusterInfoSyncer(lg *zap.Logger, clusterTLS func() *tls.Config, cfgGetter config.ConfigGetter, cfgCh <-chan *config.Config) *MultiClusterInfoSyncer {
	if clusterTLS == nil {
		clusterTLS = func() *tls.Config { return nil }
	}
	if lg == nil {
		lg = zap.NewNop()
	}
	return &MultiClusterInfoSyncer{
		lg:         lg,
		clusterTLS: clusterTLS,
		cfgGetter:  cfgGetter,
		cfgCh:      cfgCh,
		clusters:   make(map[string]*infoSyncClusterClient),
	}
}

func (ms *MultiClusterInfoSyncer) Start(ctx context.Context) error {
	if ms.cfgGetter == nil {
		return nil
	}
	if err := ms.syncClusters(ctx, ms.cfgGetter.GetConfig()); err != nil {
		return err
	}
	childCtx, cancel := context.WithCancel(ctx)
	ms.cancel = cancel
	ms.wg.Run(func() {
		ms.watchConfig(childCtx)
	}, ms.lg)
	return nil
}

func (ms *MultiClusterInfoSyncer) watchConfig(ctx context.Context) {
	for {
		select {
		case <-ctx.Done():
			return
		case cfg := <-ms.cfgCh:
			if cfg == nil {
				ms.lg.Warn("config channel is closed, stop watching backend clusters for infosync")
				return
			}
			if err := ms.syncClusters(ctx, cfg); err != nil {
				ms.lg.Error("failed to sync infosync backend clusters", zap.Error(err))
			}
		}
	}
}

func (ms *MultiClusterInfoSyncer) syncClusters(ctx context.Context, cfg *config.Config) error {
	if cfg == nil {
		return nil
	}
	desiredClusters := cfg.GetBackendClusters()
	desiredMap := make(map[string]config.BackendCluster, len(desiredClusters))
	for _, cluster := range desiredClusters {
		desiredMap[cluster.Name] = cluster
	}

	ms.mu.Lock()
	oldClusters := ms.clusters
	newClusters := make(map[string]*infoSyncClusterClient, len(desiredClusters))
	closeList := make([]*infoSyncClusterClient, 0, len(oldClusters))

	for _, cluster := range desiredClusters {
		oldClient, ok := oldClusters[cluster.Name]
		if ok && strings.TrimSpace(oldClient.pdAddrs) == strings.TrimSpace(cluster.PDAddrs) &&
			strings.TrimSpace(oldClient.nsServers) == strings.TrimSpace(cluster.NSServers) {
			newClusters[cluster.Name] = oldClient
			delete(oldClusters, cluster.Name)
			continue
		}

		d, err := dns.NewDialer(ms.lg.With(zap.String("cluster", cluster.Name)), cluster.NSServers)
		if err != nil {
			if ok {
				ms.lg.Warn("failed to update infosync backend cluster DNS config, keep the old one", zap.String("cluster", cluster.Name), zap.Error(err))
				newClusters[cluster.Name] = oldClient
				delete(oldClusters, cluster.Name)
				continue
			}
			ms.lg.Error("failed to add infosync backend cluster DNS config", zap.String("cluster", cluster.Name), zap.Error(err))
			continue
		}

		etcdCli, err := etcd.InitEtcdClientWithAddrsAndDNSDialer(
			ms.lg.With(zap.String("cluster", cluster.Name)),
			cluster.PDAddrs,
			ms.clusterTLS(),
			d,
		)
		if err != nil {
			if ok {
				ms.lg.Warn("failed to update infosync backend cluster etcd client, keep the old one", zap.String("cluster", cluster.Name), zap.Error(err))
				newClusters[cluster.Name] = oldClient
				delete(oldClusters, cluster.Name)
				continue
			}
			ms.lg.Error("failed to add infosync backend cluster etcd client", zap.String("cluster", cluster.Name), zap.Error(err))
			continue
		}

		syncer := NewInfoSyncer(ms.lg.With(zap.String("cluster", cluster.Name)), etcdCli)
		if err := syncer.Init(ctx, cfg); err != nil {
			if ok {
				ms.lg.Warn("failed to update infosync backend cluster, keep the old one", zap.String("cluster", cluster.Name), zap.Error(err))
				newClusters[cluster.Name] = oldClient
				delete(oldClusters, cluster.Name)
			} else {
				ms.lg.Error("failed to add infosync backend cluster", zap.String("cluster", cluster.Name), zap.Error(err))
			}
			if closeErr := etcdCli.Close(); closeErr != nil {
				ms.lg.Warn("failed to close newly created infosync etcd client", zap.String("cluster", cluster.Name), zap.Error(closeErr))
			}
			continue
		}

		newClient := &infoSyncClusterClient{
			name:      cluster.Name,
			pdAddrs:   cluster.PDAddrs,
			nsServers: cluster.NSServers,
			etcdCli:   etcdCli,
			syncer:    syncer,
		}
		newClusters[cluster.Name] = newClient
		if ok {
			closeList = append(closeList, oldClient)
			delete(oldClusters, cluster.Name)
			ms.lg.Info("updated infosync backend cluster", zap.String("cluster", cluster.Name), zap.String("pd_addrs", cluster.PDAddrs))
		} else {
			ms.lg.Info("added infosync backend cluster", zap.String("cluster", cluster.Name), zap.String("pd_addrs", cluster.PDAddrs))
		}
	}

	for name, client := range oldClusters {
		if _, ok := desiredMap[name]; ok {
			continue
		}
		closeList = append(closeList, client)
		ms.lg.Info("removed infosync backend cluster", zap.String("cluster", name), zap.String("pd_addrs", client.pdAddrs))
	}

	ms.clusters = newClusters
	ms.mu.Unlock()

	for _, client := range closeList {
		if err := ms.closeCluster(client); err != nil {
			ms.lg.Warn("failed to close infosync backend cluster", zap.String("cluster", client.name), zap.Error(err))
		}
	}
	return nil
}

func (ms *MultiClusterInfoSyncer) clusterSnapshot() map[string]*infoSyncClusterClient {
	ms.mu.RLock()
	snapshot := make(map[string]*infoSyncClusterClient, len(ms.clusters))
	for name, cluster := range ms.clusters {
		snapshot[name] = cluster
	}
	ms.mu.RUnlock()
	return snapshot
}

func (ms *MultiClusterInfoSyncer) closeCluster(cluster *infoSyncClusterClient) error {
	if cluster == nil {
		return nil
	}
	errs := make([]error, 0, 2)
	if cluster.syncer != nil {
		errs = append(errs, cluster.syncer.Close())
	}
	if cluster.etcdCli != nil {
		errs = append(errs, cluster.etcdCli.Close())
	}
	return errors.Collect(errors.New("close infosync backend cluster"), errs...)
}

func (ms *MultiClusterInfoSyncer) Close() error {
	if ms.cancel != nil {
		ms.cancel()
	}
	ms.wg.Wait()

	clusters := ms.clusterSnapshot()
	ms.mu.Lock()
	ms.clusters = make(map[string]*infoSyncClusterClient)
	ms.mu.Unlock()

	errs := make([]error, 0, len(clusters))
	for _, cluster := range clusters {
		if err := ms.closeCluster(cluster); err != nil {
			errs = append(errs, err)
		}
	}
	if len(errs) == 0 {
		return nil
	}
	return errors.Collect(errors.New("close infosync backend clusters"), errs...)
}
