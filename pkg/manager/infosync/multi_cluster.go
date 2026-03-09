// Copyright 2023 PingCAP, Inc.
// SPDX-License-Identifier: Apache-2.0

package infosync

import (
	"context"
	"crypto/tls"
	"maps"
	"slices"
	"strings"
	"sync"

	"github.com/pingcap/tiproxy/lib/config"
	"github.com/pingcap/tiproxy/lib/util/errors"
	"github.com/pingcap/tiproxy/pkg/util/etcd"
	"github.com/pingcap/tiproxy/pkg/util/waitgroup"
	clientv3 "go.etcd.io/etcd/client/v3"
	"go.uber.org/zap"
)

type clusterClient struct {
	name    string
	pdAddrs string
	etcdCli *clientv3.Client
}

// MultiClusterFetcher fetches topology and Prometheus info from multiple PD clusters.
// It updates cluster clients dynamically when configs change.
type MultiClusterFetcher struct {
	lg         *zap.Logger
	clusterTLS func() *tls.Config
	cfgGetter  config.ConfigGetter
	cfgCh      <-chan *config.Config
	syncConfig syncConfig

	mu       sync.RWMutex
	clusters map[string]*clusterClient

	wg     waitgroup.WaitGroup
	cancel context.CancelFunc
}

func NewMultiClusterFetcher(lg *zap.Logger, clusterTLS func() *tls.Config, cfgGetter config.ConfigGetter, cfgCh <-chan *config.Config) *MultiClusterFetcher {
	if clusterTLS == nil {
		clusterTLS = func() *tls.Config { return nil }
	}
	return &MultiClusterFetcher{
		lg:         lg,
		clusterTLS: clusterTLS,
		cfgGetter:  cfgGetter,
		cfgCh:      cfgCh,
		syncConfig: syncConfig{
			getPromTimeout:    getPromTimeout,
			getPromRetryIntvl: getPromRetryIntvl,
			getPromRetryCnt:   getPromRetryCnt,
		},
		clusters: make(map[string]*clusterClient),
	}
}

func (mcf *MultiClusterFetcher) Start(ctx context.Context) error {
	if err := mcf.syncClusters(mcf.cfgGetter.GetConfig()); err != nil {
		return err
	}
	childCtx, cancel := context.WithCancel(ctx)
	mcf.cancel = cancel
	mcf.wg.Run(func() {
		mcf.watchConfig(childCtx)
	}, mcf.lg)
	return nil
}

func (mcf *MultiClusterFetcher) watchConfig(ctx context.Context) {
	for {
		select {
		case <-ctx.Done():
			return
		case cfg := <-mcf.cfgCh:
			if cfg == nil {
				mcf.lg.Warn("config channel is closed, stop watching backend clusters")
				return
			}
			if err := mcf.syncClusters(cfg); err != nil {
				mcf.lg.Error("failed to sync backend clusters", zap.Error(err))
			}
		}
	}
}

func (mcf *MultiClusterFetcher) syncClusters(cfg *config.Config) error {
	if cfg == nil {
		return nil
	}
	desiredClusters := cfg.GetBackendClusters()
	desiredMap := make(map[string]config.BackendCluster, len(desiredClusters))
	for _, cluster := range desiredClusters {
		desiredMap[cluster.Name] = cluster
	}

	mcf.mu.Lock()
	oldClusters := mcf.clusters
	newClusters := make(map[string]*clusterClient, len(desiredClusters))
	closeList := make([]*clusterClient, 0, len(oldClusters))

	for _, cluster := range desiredClusters {
		oldClient, ok := oldClusters[cluster.Name]
		if ok && strings.TrimSpace(oldClient.pdAddrs) == strings.TrimSpace(cluster.PDAddrs) {
			newClusters[cluster.Name] = oldClient
			delete(oldClusters, cluster.Name)
			continue
		}

		etcdCli, err := etcd.InitEtcdClientWithAddrs(mcf.lg.With(zap.String("cluster", cluster.Name)), cluster.PDAddrs, mcf.clusterTLS())
		if err != nil {
			if ok {
				mcf.lg.Warn("failed to update backend cluster client, keep the old one", zap.String("cluster", cluster.Name), zap.Error(err))
				newClusters[cluster.Name] = oldClient
				delete(oldClusters, cluster.Name)
				continue
			}
			mcf.lg.Error("failed to add backend cluster client", zap.String("cluster", cluster.Name), zap.Error(err))
			continue
		}
		newClusters[cluster.Name] = &clusterClient{
			name:    cluster.Name,
			pdAddrs: cluster.PDAddrs,
			etcdCli: etcdCli,
		}
		if ok {
			closeList = append(closeList, oldClient)
			delete(oldClusters, cluster.Name)
			mcf.lg.Info("updated backend cluster", zap.String("cluster", cluster.Name), zap.String("pd_addrs", cluster.PDAddrs))
		} else {
			mcf.lg.Info("added backend cluster", zap.String("cluster", cluster.Name), zap.String("pd_addrs", cluster.PDAddrs))
		}
	}

	for name, client := range oldClusters {
		if _, ok := desiredMap[name]; ok {
			continue
		}
		closeList = append(closeList, client)
		mcf.lg.Info("removed backend cluster", zap.String("cluster", name), zap.String("pd_addrs", client.pdAddrs))
	}

	mcf.clusters = newClusters
	mcf.mu.Unlock()

	for _, client := range closeList {
		if client == nil || client.etcdCli == nil {
			continue
		}
		if err := client.etcdCli.Close(); err != nil {
			mcf.lg.Warn("failed to close backend cluster client", zap.String("cluster", client.name), zap.Error(err))
		}
	}
	return nil
}

func (mcf *MultiClusterFetcher) clusterSnapshot() map[string]*clusterClient {
	mcf.mu.RLock()
	snapshot := make(map[string]*clusterClient, len(mcf.clusters))
	maps.Copy(snapshot, mcf.clusters)
	mcf.mu.RUnlock()
	return snapshot
}

func (mcf *MultiClusterFetcher) GetTiDBTopology(ctx context.Context) (map[string]*TiDBTopologyInfo, error) {
	clusters := mcf.clusterSnapshot()
	merged := make(map[string]*TiDBTopologyInfo, 128)
	errs := make([]error, 0, len(clusters))
	for clusterName, cluster := range clusters {
		infos, err := getTiDBTopology(ctx, mcf.lg.With(zap.String("cluster", clusterName)), cluster.etcdCli)
		if err != nil {
			errs = append(errs, err)
			continue
		}
		for addr, info := range infos {
			if oldInfo, ok := merged[addr]; ok {
				mcf.lg.Warn("duplicate backend from different clusters, keep the first one", zap.String("addr", addr),
					zap.String("cluster", clusterName), zap.String("first_cluster", oldInfo.Labels[config.ClusterLabelName]))
				continue
			}
			cloned := *info
			cloned.Labels = maps.Clone(info.Labels)
			if cloned.Labels == nil {
				cloned.Labels = make(map[string]string, 1)
			}
			cloned.Labels[config.ClusterLabelName] = clusterName
			merged[addr] = &cloned
		}
	}
	if len(merged) == 0 && len(errs) > 0 {
		return nil, errors.Collect(errors.New("fetch from backend clusters"), errs...)
	}
	return merged, nil
}

func (mcf *MultiClusterFetcher) GetPromInfo(ctx context.Context) (*PrometheusInfo, error) {
	clusters := mcf.clusterSnapshot()
	if len(clusters) == 0 {
		return nil, ErrNoProm
	}
	clusterNames := make([]string, 0, len(clusters))
	for clusterName := range clusters {
		clusterNames = append(clusterNames, clusterName)
	}
	slices.Sort(clusterNames)
	var firstErr error
	for _, clusterName := range clusterNames {
		info, err := getPromInfo(ctx, clusters[clusterName].etcdCli, mcf.syncConfig)
		if err == nil {
			return info, nil
		}
		if firstErr == nil {
			firstErr = err
		}
	}
	if firstErr != nil {
		return nil, firstErr
	}
	return nil, ErrNoProm
}

func (mcf *MultiClusterFetcher) Close() error {
	if mcf.cancel != nil {
		mcf.cancel()
	}
	mcf.wg.Wait()

	clusters := mcf.clusterSnapshot()
	mcf.mu.Lock()
	mcf.clusters = make(map[string]*clusterClient)
	mcf.mu.Unlock()

	errs := make([]error, 0, len(clusters))
	for _, cluster := range clusters {
		if cluster == nil || cluster.etcdCli == nil {
			continue
		}
		if err := cluster.etcdCli.Close(); err != nil {
			errs = append(errs, err)
		}
	}
	if len(errs) == 0 {
		return nil
	}
	return errors.Collect(errors.New("close backend cluster clients"), errs...)
}
