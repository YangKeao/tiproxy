// Copyright 2024 PingCAP, Inc.
// SPDX-License-Identifier: Apache-2.0

package metricsreader

import (
	"context"
	"crypto/tls"
	"encoding/json"
	"fmt"
	"math"
	"net"
	"slices"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/cenkalti/backoff/v4"
	"github.com/pingcap/tiproxy/lib/config"
	"github.com/pingcap/tiproxy/lib/util/errors"
	"github.com/pingcap/tiproxy/pkg/manager/elect"
	"github.com/pingcap/tiproxy/pkg/metrics"
	"github.com/pingcap/tiproxy/pkg/util/dns"
	"github.com/pingcap/tiproxy/pkg/util/etcd"
	"github.com/pingcap/tiproxy/pkg/util/http"
	"github.com/pingcap/tiproxy/pkg/util/waitgroup"
	dto "github.com/prometheus/client_model/go"
	"github.com/prometheus/common/expfmt"
	"github.com/prometheus/common/model"
	"github.com/siddontang/go/hack"
	clientv3 "go.etcd.io/etcd/client/v3"
	"go.uber.org/zap"
)

const (
	// readerOwnerKeyPrefix is the key prefix in etcd for backend reader owner election.
	// For global owner, the key is "/tiproxy/metric_reader/owner".
	// For zonal owner, the key is "/tiproxy/metric_reader/{zone}/owner".
	readerOwnerKeyPrefix = "/tiproxy/metric_reader"
	readerOwnerKeySuffix = "owner"
	// sessionTTL is the session's TTL in seconds for backend reader owner election.
	sessionTTL = 15
	// backendMetricPath is the path of backend HTTP API to read metrics.
	backendMetricPath = "/metrics"
	// ownerMetricPath is the path of reading backend metrics from the backend reader owner.
	ownerMetricPath = "/api/backend/metrics"
	goPoolSize      = 100
	goMaxIdle       = time.Minute
)

var (
	errReadMetrics = errors.New("read backend metrics failed")
)

type backendAddr struct {
	statusAddr string
	label      string
	cluster    string
}

type backendHistory struct {
	Step1History []model.SamplePair
	Step2History []model.SamplePair
}

type ownerGroup struct {
	zones  []string
	owners []string
}

type clusterOwner struct {
	name      string
	pdAddrs   string
	nsServers string
	zone      string
	etcdCli   *clientv3.Client
	election  elect.Election
	isOwner   atomic.Bool
}

type clusterOwnerMember struct {
	onElected func()
	onRetired func()
}

func (m *clusterOwnerMember) OnElected() {
	if m.onElected != nil {
		m.onElected()
	}
}

func (m *clusterOwnerMember) OnRetired() {
	if m.onRetired != nil {
		m.onRetired()
	}
}

type BackendReader struct {
	sync.Mutex
	// rule key: QueryRule
	queryRules map[string]QueryRule
	// rule key: QueryResult
	queryResults map[string]QueryResult
	// the owner generates the history from querying backends and other members query the history from the owner
	// rule key: {backend name: backendHistory}
	history map[string]map[string]backendHistory
	// the owner marshalles history to share it to other members
	// cache the marshalled history to avoid duplicated marshalling
	marshalledHistory []byte
	cfgGetter         config.ConfigGetter
	backendFetcher    TopologyFetcher
	ownerID           string
	clusterZone       string
	clusterMu         sync.RWMutex
	clusterOwners     map[string]*clusterOwner
	electionCfg       elect.ElectionConfig
	// isOwner is kept for compatibility with existing tests and indicates whether this member
	// is owner for at least one backend cluster.
	isOwner atomic.Bool
	wgp     *waitgroup.WaitGroupPool
	// etcdCli is a legacy fallback used by tests that construct BackendReader without config.
	etcdCli    *clientv3.Client
	clusterTLS func() *tls.Config
	httpCli    *http.Client
	lg         *zap.Logger
	cfg        *config.HealthCheck
}

func NewBackendReader(lg *zap.Logger, cfgGetter config.ConfigGetter, httpCli *http.Client, etcdCli *clientv3.Client,
	clusterTLS func() *tls.Config,
	backendFetcher TopologyFetcher, cfg *config.HealthCheck) *BackendReader {
	if clusterTLS == nil {
		clusterTLS = func() *tls.Config { return nil }
	}
	return &BackendReader{
		queryRules:        make(map[string]QueryRule),
		queryResults:      make(map[string]QueryResult),
		history:           make(map[string]map[string]backendHistory),
		lg:                lg,
		cfgGetter:         cfgGetter,
		backendFetcher:    backendFetcher,
		cfg:               cfg,
		wgp:               waitgroup.NewWaitGroupPool(goPoolSize, goMaxIdle),
		electionCfg:       elect.DefaultElectionConfig(sessionTTL),
		clusterOwners:     make(map[string]*clusterOwner),
		etcdCli:           etcdCli,
		clusterTLS:        clusterTLS,
		httpCli:           httpCli,
		marshalledHistory: []byte{},
	}
}

func (br *BackendReader) Start(ctx context.Context) error {
	if br.cfgGetter == nil {
		return nil
	}
	cfg := br.cfgGetter.GetConfig()
	if cfg == nil {
		return nil
	}
	if err := br.ensureOwnerID(cfg); err != nil {
		return err
	}
	return br.syncClusterOwners(ctx, cfg)
}

func (br *BackendReader) ensureOwnerID(cfg *config.Config) error {
	if cfg == nil || br.ownerID != "" {
		return nil
	}
	ip, _, statusPort, err := cfg.GetIPPort()
	if err != nil {
		return err
	}
	// Use the status address as the key so that it can read metrics from the address.
	br.ownerID = net.JoinHostPort(ip, statusPort)
	return nil
}

func ownerKeyPrefixForCluster(clusterName string) string {
	if clusterName == "" || clusterName == "default" {
		return readerOwnerKeyPrefix
	}
	return fmt.Sprintf("%s/%s", readerOwnerKeyPrefix, clusterName)
}

func ownerKeyForCluster(clusterName, zone string) string {
	keyPrefix := ownerKeyPrefixForCluster(clusterName)
	if zone != "" {
		return fmt.Sprintf("%s/%s/%s", keyPrefix, zone, readerOwnerKeySuffix)
	}
	return fmt.Sprintf("%s/%s", keyPrefix, readerOwnerKeySuffix)
}

func (br *BackendReader) newClusterOwner(ctx context.Context, cluster config.BackendCluster, zone string) (*clusterOwner, error) {
	d, err := dns.NewDialer(br.lg.With(zap.String("cluster", cluster.Name)), cluster.NSServers)
	if err != nil {
		return nil, err
	}
	etcdCli, err := etcd.InitEtcdClientWithAddrsAndDialer(
		br.lg.With(zap.String("cluster", cluster.Name)),
		cluster.PDAddrs,
		br.clusterTLS(),
		d.GRPCDialContext,
	)
	if err != nil {
		return nil, err
	}
	newOwner := &clusterOwner{
		name:      cluster.Name,
		pdAddrs:   cluster.PDAddrs,
		nsServers: cluster.NSServers,
		zone:      zone,
		etcdCli:   etcdCli,
	}
	member := &clusterOwnerMember{
		onElected: func() {
			newOwner.isOwner.Store(true)
			br.refreshOwnerState()
		},
		onRetired: func() {
			newOwner.isOwner.Store(false)
			br.refreshOwnerState()
		},
	}
	newOwner.election = elect.NewElection(
		br.lg.Named("elect").With(zap.String("cluster", cluster.Name)),
		etcdCli,
		br.electionCfg,
		br.ownerID,
		ownerKeyForCluster(cluster.Name, zone),
		member,
	)
	newOwner.election.Start(ctx)
	return newOwner, nil
}

func (br *BackendReader) syncClusterOwners(ctx context.Context, cfg *config.Config) error {
	if cfg == nil {
		return nil
	}
	if err := br.ensureOwnerID(cfg); err != nil {
		return err
	}
	zone := cfg.GetLocation()
	desiredClusters := cfg.GetBackendClusters()
	desiredMap := make(map[string]config.BackendCluster, len(desiredClusters))
	for _, cluster := range desiredClusters {
		desiredMap[cluster.Name] = cluster
	}

	br.clusterMu.Lock()
	oldClusters := br.clusterOwners
	newClusters := make(map[string]*clusterOwner, len(desiredClusters))
	closeList := make([]*clusterOwner, 0, len(oldClusters))

	for _, cluster := range desiredClusters {
		oldOwner, ok := oldClusters[cluster.Name]
		if ok && strings.TrimSpace(oldOwner.pdAddrs) == strings.TrimSpace(cluster.PDAddrs) &&
			strings.TrimSpace(oldOwner.nsServers) == strings.TrimSpace(cluster.NSServers) &&
			strings.TrimSpace(oldOwner.zone) == strings.TrimSpace(zone) {
			newClusters[cluster.Name] = oldOwner
			delete(oldClusters, cluster.Name)
			continue
		}

		newOwner, err := br.newClusterOwner(ctx, cluster, zone)
		if err != nil {
			if ok {
				br.lg.Warn("failed to update backend metrics owner cluster, keep old one", zap.String("cluster", cluster.Name), zap.Error(err))
				newClusters[cluster.Name] = oldOwner
				delete(oldClusters, cluster.Name)
				continue
			}
			br.lg.Error("failed to add backend metrics owner cluster", zap.String("cluster", cluster.Name), zap.Error(err))
			continue
		}
		newClusters[cluster.Name] = newOwner
		if ok {
			closeList = append(closeList, oldOwner)
			delete(oldClusters, cluster.Name)
			br.lg.Info("updated backend metrics owner cluster", zap.String("cluster", cluster.Name), zap.String("pd_addrs", cluster.PDAddrs))
		} else {
			br.lg.Info("added backend metrics owner cluster", zap.String("cluster", cluster.Name), zap.String("pd_addrs", cluster.PDAddrs))
		}
	}

	for name, owner := range oldClusters {
		if _, ok := desiredMap[name]; ok {
			continue
		}
		closeList = append(closeList, owner)
		br.lg.Info("removed backend metrics owner cluster", zap.String("cluster", name), zap.String("pd_addrs", owner.pdAddrs))
	}

	br.clusterOwners = newClusters
	br.clusterZone = zone
	br.clusterMu.Unlock()

	for _, owner := range closeList {
		br.closeClusterOwner(owner, true)
	}
	br.refreshOwnerState()
	return nil
}

func (br *BackendReader) closeClusterOwner(owner *clusterOwner, closeEtcd bool) {
	if owner == nil {
		return
	}
	if owner.election != nil {
		owner.election.Close()
	}
	if closeEtcd && owner.etcdCli != nil {
		if err := owner.etcdCli.Close(); err != nil {
			br.lg.Warn("close backend metrics owner cluster client failed", zap.String("cluster", owner.name), zap.Error(err))
		}
	}
}

func (br *BackendReader) snapshotClusterOwners() map[string]*clusterOwner {
	br.clusterMu.RLock()
	snapshot := make(map[string]*clusterOwner, len(br.clusterOwners))
	for name, owner := range br.clusterOwners {
		snapshot[name] = owner
	}
	br.clusterMu.RUnlock()
	return snapshot
}

func (br *BackendReader) refreshOwnerState() {
	snapshot := br.snapshotClusterOwners()
	for _, owner := range snapshot {
		if owner != nil && owner.isOwner.Load() {
			br.isOwner.Store(true)
			return
		}
	}
	br.isOwner.Store(false)
}

func (br *BackendReader) clusterOwnerSnapshot() map[string]string {
	snapshot := br.snapshotClusterOwners()
	owned := make(map[string]string, len(snapshot))
	for clusterName, owner := range snapshot {
		if owner != nil && owner.isOwner.Load() {
			owned[clusterName] = owner.zone
		}
	}
	return owned
}

func (br *BackendReader) AddQueryRule(key string, rule QueryRule) {
	br.Lock()
	defer br.Unlock()
	br.queryRules[key] = rule
}

func (br *BackendReader) RemoveQueryRule(key string) {
	br.Lock()
	defer br.Unlock()
	delete(br.queryRules, key)
}

func (br *BackendReader) GetQueryResult(key string) QueryResult {
	br.Lock()
	defer br.Unlock()
	// Return an empty QueryResult if it's not found.
	return br.queryResults[key]
}

func (br *BackendReader) ReadMetrics(ctx context.Context) error {
	if br.cfgGetter != nil {
		cfg := br.cfgGetter.GetConfig()
		if cfg != nil {
			if err := br.syncClusterOwners(ctx, cfg); err != nil {
				return err
			}
		}
	}

	// Read from all owners, regardless of whether the owner is a zone owner or global owner.
	clusterOwners, err := br.queryClusterOwners(ctx)
	if err != nil {
		return err
	}

	var errs []error
	backendLabels := make([]string, 0)
	ownedClusters := br.clusterOwnerSnapshot()
	if len(ownedClusters) > 0 {
		for clusterName, zone := range ownedClusters {
			var excludeZones []string
			if ownerGroup, ok := clusterOwners[clusterName]; ok {
				excludeZones = append(excludeZones, ownerGroup.zones...)
			}
			if zone != "" {
				if idx := slices.Index(excludeZones, zone); idx >= 0 {
					excludeZones = slices.Delete(excludeZones, idx, idx+1)
				}
			}
			clusterLabels, readErr := br.readFromBackendsByCluster(ctx, clusterName, excludeZones)
			if readErr != nil {
				errs = append(errs, readErr)
				continue
			}
			backendLabels = append(backendLabels, clusterLabels...)
		}
	} else {
		// No elected owner yet. Fall back to reading backends directly to avoid empty metrics.
		hasOwner := false
		for _, ownerGroup := range clusterOwners {
			if len(ownerGroup.owners) > 0 {
				hasOwner = true
				break
			}
		}
		if !hasOwner {
			backendLabels, err = br.readFromBackends(ctx, nil)
			if err != nil {
				errs = append(errs, err)
			}
		}
	}

	ownerSet := make(map[string]struct{}, len(clusterOwners))
	for clusterName, ownerGroup := range clusterOwners {
		if _, owned := ownedClusters[clusterName]; owned {
			continue
		}
		for _, owner := range ownerGroup.owners {
			if owner == br.ownerID {
				continue
			}
			ownerSet[owner] = struct{}{}
		}
	}
	for owner := range ownerSet {
		if err = br.readFromOwner(ctx, owner); err != nil {
			errs = append(errs, err)
		}
	}

	// Purge expired history.
	br.purgeHistory()
	// Marshal backend history for other members to query.
	if err := br.marshalHistory(backendLabels); err != nil {
		br.lg.Error("marshal backend history failed", zap.Any("addrs", backendLabels), zap.Error(err))
	}
	// Generate query result for all backends.
	br.history2QueryResult()
	if len(errs) > 0 {
		return errors.Collect(errReadMetrics, errs...)
	}
	return nil
}

func (br *BackendReader) queryClusterOwners(ctx context.Context) (map[string]ownerGroup, error) {
	snapshot := br.snapshotClusterOwners()
	if len(snapshot) == 0 {
		if br.etcdCli == nil {
			return map[string]ownerGroup{}, nil
		}
		owners, err := br.queryOwnerGroupByPrefix(ctx, br.etcdCli, ownerKeyPrefixForCluster("default"))
		if err != nil {
			return nil, err
		}
		return map[string]ownerGroup{
			"default": owners,
		}, nil
	}
	ownerGroups := make(map[string]ownerGroup, len(snapshot))
	errs := make([]error, 0, len(snapshot))
	for clusterName, owner := range snapshot {
		group, err := br.queryOwnerGroupByPrefix(ctx, owner.etcdCli, ownerKeyPrefixForCluster(clusterName))
		if err != nil {
			errs = append(errs, errors.Wrapf(err, "query owner of cluster %s failed", clusterName))
			continue
		}
		ownerGroups[clusterName] = group
	}
	if len(ownerGroups) == 0 && len(errs) > 0 {
		return nil, errors.Collect(errors.New("query owner failed"), errs...)
	}
	return ownerGroups, nil
}

func (br *BackendReader) queryOwnerGroupByPrefix(ctx context.Context, etcdCli *clientv3.Client, keyPrefix string) (ownerGroup, error) {
	if etcdCli == nil {
		return ownerGroup{}, nil
	}
	opts := []clientv3.OpOption{clientv3.WithPrefix()}
	kvs, err := etcd.GetKVs(ctx, etcdCli, keyPrefix, opts, br.electionCfg.Timeout, br.electionCfg.RetryIntvl, br.electionCfg.RetryCnt)
	if err != nil {
		return ownerGroup{}, err
	}

	type ownerInfo struct {
		addr     string
		revision int64
	}
	ownerMap := make(map[string]ownerInfo)
	for _, kv := range kvs {
		key := strings.TrimPrefix(hack.String(kv.Key), keyPrefix)
		if len(key) == 0 || key[0] != '/' {
			continue
		}
		key = key[1:]

		var zone string
		if strings.HasPrefix(key, readerOwnerKeySuffix) {
			// global owner key, such as "/tiproxy/metric_reader/owner/leaseID"
			// or "/tiproxy/metric_reader/{cluster}/owner/leaseID"
		} else if endIdx := strings.Index(key, "/"); endIdx > 0 && strings.HasPrefix(key[endIdx+1:], readerOwnerKeySuffix) {
			// zonal owner key, such as "/tiproxy/metric_reader/east/owner/leaseID"
			// or "/tiproxy/metric_reader/{cluster}/east/owner/leaseID"
			zone = key[:endIdx]
		} else {
			continue
		}

		if info, ok := ownerMap[zone]; !ok || info.revision > kv.CreateRevision {
			ownerMap[zone] = ownerInfo{
				addr:     hack.String(kv.Value),
				revision: kv.CreateRevision,
			}
		}
	}

	group := ownerGroup{
		owners: make([]string, 0, len(ownerMap)),
		zones:  make([]string, 0, len(ownerMap)),
	}
	for zone, info := range ownerMap {
		if len(zone) > 0 && !slices.Contains(group.zones, zone) {
			group.zones = append(group.zones, zone)
		}
		if !slices.Contains(group.owners, info.addr) {
			group.owners = append(group.owners, info.addr)
		}
	}
	return group, nil
}

// Query all owners, including zone owner and global owner.
func (br *BackendReader) queryAllOwners(ctx context.Context) (zones, owners []string, err error) {
	ownerGroups, err := br.queryClusterOwners(ctx)
	if err != nil {
		return nil, nil, err
	}
	for _, ownerGroup := range ownerGroups {
		for _, zone := range ownerGroup.zones {
			if !slices.Contains(zones, zone) {
				zones = append(zones, zone)
			}
		}
		for _, owner := range ownerGroup.owners {
			if !slices.Contains(owners, owner) {
				owners = append(owners, owner)
			}
		}
	}
	return zones, owners, nil
}

// If self is a owner, read backends except excludeZones. The backends in those zones are read by other zonal owners.
//
// If the zone is not set, there is only one global owner, who queries all backends.
// If the zone is set, there are several zonal owners, who query the backends in the same zone.
// There are some exceptions:
// 1. In k8s, the zone is not set at startup and then is set by HTTP API, so there may temporarily exist both global and zonal owners.
// 2. Some backends may not be in the same zone with any owner. E.g. there are only 2 TiProxy in a 3-AZ cluster.
// In any way, the owner queries the backends that are not queried by other owners.
func (br *BackendReader) readFromBackends(ctx context.Context, excludeZones []string) ([]string, error) {
	return br.readFromBackendsByCluster(ctx, "", excludeZones)
}

func (br *BackendReader) readFromBackendsByCluster(ctx context.Context, clusterName string, excludeZones []string) ([]string, error) {
	addrs, err := br.getBackendAddrsByCluster(ctx, clusterName, excludeZones)
	if err != nil {
		return nil, err
	}
	if len(addrs) == 0 {
		return nil, nil
	}
	allNames := br.collectAllNames()
	if len(allNames) == 0 {
		return nil, nil
	}

	backendLabels := make([]string, 0, len(addrs))
	for _, addr := range addrs {
		backendLabels = append(backendLabels, addr.label)
	}
	for i := range addrs {
		func(addr backendAddr, label string) {
			br.wgp.RunWithRecover(func() {
				if ctx.Err() != nil {
					return
				}
				resp, err := br.readBackendMetric(ctx, addr.statusAddr, addr.cluster)
				if err != nil {
					br.lg.Debug("read metrics from backend failed", zap.String("addr", addr.statusAddr), zap.String("cluster", addr.cluster), zap.Error(err))
					return
				}
				text := filterMetrics(hack.String(resp), allNames)
				mf, err := parseMetrics(text)
				if err != nil {
					br.lg.Warn("parse metrics failed", zap.String("addr", addr.statusAddr), zap.String("cluster", addr.cluster), zap.Error(err))
					return
				}
				br.metric2History(mf, label)
			}, nil, br.lg)
		}(addrs[i], backendLabels[i])
	}
	br.wgp.Wait()
	return backendLabels, nil
}

func (br *BackendReader) collectAllNames() []string {
	br.Lock()
	defer br.Unlock()
	names := make([]string, 0, len(br.queryRules)*3)
	for _, rule := range br.queryRules {
		for _, name := range rule.Names {
			if slices.Index(names, name) < 0 {
				names = append(names, name)
			}
		}
	}
	return names
}

func (br *BackendReader) readBackendMetric(ctx context.Context, addr, cluster string) ([]byte, error) {
	b := backoff.WithContext(backoff.WithMaxRetries(backoff.NewConstantBackOff(br.cfg.RetryInterval), uint64(br.cfg.MaxRetries)), ctx)
	return br.httpCli.GetWithCluster(addr, backendMetricPath, cluster, b, br.cfg.DialTimeout)
}

// metric2History appends the metrics to history for each rule of one backend.
func (br *BackendReader) metric2History(mfs map[string]*dto.MetricFamily, backend string) {
	now := model.TimeFromUnixNano(time.Now().UnixNano())
	br.Lock()
	defer br.Unlock()

	for ruleKey, rule := range br.queryRules {
		// If the metric doesn't exist, skip it.
		metricExists := true
		for _, name := range rule.Names {
			if _, ok := mfs[name]; !ok {
				metricExists = false
				break
			}
		}
		if !metricExists {
			continue
		}

		// step 1: get the latest pair (at a timepoint) and add it to step1History
		// E.g. calculate process_cpu_seconds_total/tidb_server_maxprocs
		sampleValue := rule.Metric2Value(mfs)
		if math.IsNaN(float64(sampleValue)) {
			continue
		}
		pair := model.SamplePair{Timestamp: now, Value: sampleValue}
		ruleHistory, ok := br.history[ruleKey]
		if !ok {
			ruleHistory = make(map[string]backendHistory)
			br.history[ruleKey] = ruleHistory
		}
		beHistory := ruleHistory[backend]
		beHistory.Step1History = append(beHistory.Step1History, pair)

		// step 2: get the latest pair by the history and add it to step2History
		// E.g. calculate irate(process_cpu_seconds_total/tidb_server_maxprocs[30s])
		sampleValue = rule.Range2Value(beHistory.Step1History)
		if !math.IsNaN(float64(sampleValue)) {
			beHistory.Step2History = append(beHistory.Step2History, model.SamplePair{Timestamp: now, Value: sampleValue})
		}
		ruleHistory[backend] = beHistory
	}
}

// history2QueryResult generates new query results from the history.
func (br *BackendReader) history2QueryResult() {
	now := time.Now()
	br.Lock()
	defer br.Unlock()

	queryResults := make(map[string]QueryResult, len(br.queryRules))
	for ruleKey, rule := range br.queryRules {
		ruleHistory := br.history[ruleKey]
		if len(ruleHistory) == 0 {
			continue
		}

		var value model.Value
		switch rule.ResultType {
		case model.ValVector:
			results := make([]*model.Sample, 0, len(ruleHistory))
			for backend, beHistory := range ruleHistory {
				if len(beHistory.Step2History) == 0 {
					continue
				}
				labels := map[model.LabelName]model.LabelValue{LabelNameInstance: model.LabelValue(backend)}
				// vector indicates returning the latest pair
				lastPair := beHistory.Step2History[len(beHistory.Step2History)-1]
				results = append(results, &model.Sample{Value: lastPair.Value, Timestamp: lastPair.Timestamp, Metric: labels})
			}
			value = model.Vector(results)
		case model.ValMatrix:
			results := make([]*model.SampleStream, 0, len(ruleHistory))
			for backend, beHistory := range ruleHistory {
				if len(beHistory.Step2History) == 0 {
					continue
				}
				labels := map[model.LabelName]model.LabelValue{LabelNameInstance: model.LabelValue(backend)}
				// matrix indicates returning the history
				// copy a slice to avoid data race
				pairs := make([]model.SamplePair, len(beHistory.Step2History))
				copy(pairs, beHistory.Step2History)
				results = append(results, &model.SampleStream{Values: pairs, Metric: labels})
			}
			value = model.Matrix(results)
		default:
			br.lg.Error("unsupported value type", zap.String("value type", rule.ResultType.String()))
		}

		queryResults[ruleKey] = QueryResult{
			Value:      value,
			UpdateTime: now,
		}
	}

	br.queryResults = queryResults
}

// purgeHistory purges the expired or useless history values, otherwise the memory grows infinitely.
func (br *BackendReader) purgeHistory() {
	now := time.Now()
	br.Lock()
	defer br.Unlock()
	for id, ruleHistory := range br.history {
		rule, ok := br.queryRules[id]
		// the rule is removed
		if !ok {
			delete(br.history, id)
			continue
		}
		for backend, backendHistory := range ruleHistory {
			backendHistory.Step1History = purgeHistory(backendHistory.Step1History, rule.Retention, now)
			backendHistory.Step2History = purgeHistory(backendHistory.Step2History, rule.Retention, now)
			// the history is expired, maybe the backend is down
			if len(backendHistory.Step1History) == 0 && len(backendHistory.Step2History) == 0 {
				delete(ruleHistory, backend)
			} else {
				ruleHistory[backend] = backendHistory
			}
		}
	}
}

func (br *BackendReader) GetBackendMetrics() []byte {
	br.Lock()
	defer br.Unlock()
	return br.marshalledHistory
}

// readFromOwner queries metric history from the owner.
// If every member queries directly from backends, the backends may suffer from too much pressure.
func (br *BackendReader) readFromOwner(ctx context.Context, ownerAddr string) error {
	b := backoff.WithContext(backoff.WithMaxRetries(backoff.NewConstantBackOff(br.cfg.RetryInterval), uint64(br.cfg.MaxRetries)), ctx)
	resp, err := br.httpCli.Get(ownerAddr, ownerMetricPath, b, br.cfg.DialTimeout)
	if err != nil {
		return err
	}
	if len(resp) == 0 {
		return nil
	}

	var newHistory map[string]map[string]backendHistory
	if err := json.Unmarshal(resp, &newHistory); err != nil {
		return err
	}

	// If this instance becomes the owner in the next round, it can reuse the history.
	br.mergeHistory(newHistory)
	return nil
}

// If the history of one backend already exists, choose the latest one.
func (br *BackendReader) mergeHistory(newHistory map[string]map[string]backendHistory) {
	br.Lock()
	defer br.Unlock()
	for ruleKey, newRuleHistory := range newHistory {
		ruleHistory, ok := br.history[ruleKey]
		if !ok {
			br.history[ruleKey] = newRuleHistory
			continue
		}
		for backend, newBackendHistory := range newRuleHistory {
			backendHistory, ok := ruleHistory[backend]
			if !ok {
				ruleHistory[backend] = newBackendHistory
				continue
			}
			if len(backendHistory.Step1History) == 0 || (len(newBackendHistory.Step1History) > 0 &&
				newBackendHistory.Step1History[len(newBackendHistory.Step1History)-1].Timestamp.After(backendHistory.Step1History[len(backendHistory.Step1History)-1].Timestamp)) {
				backendHistory.Step1History = newBackendHistory.Step1History
			}
			if len(backendHistory.Step2History) == 0 || (len(newBackendHistory.Step2History) > 0 &&
				newBackendHistory.Step2History[len(newBackendHistory.Step2History)-1].Timestamp.After(backendHistory.Step2History[len(backendHistory.Step2History)-1].Timestamp)) {
				backendHistory.Step2History = newBackendHistory.Step2History
			}
			ruleHistory[backend] = backendHistory
		}
	}
}

// marshalHistory marshals the backends that are read by this owner. The marshaled data will be returned to other members.
func (br *BackendReader) marshalHistory(backends []string) error {
	br.Lock()
	defer br.Unlock()

	filteredHistory := make(map[string]map[string]backendHistory, len(br.queryRules))
	if len(backends) > 0 {
		for ruleKey, ruleHistory := range br.history {
			filteredRuleHistory := make(map[string]backendHistory, len(backends))
			filteredHistory[ruleKey] = filteredRuleHistory
			for backend, backendHistory := range ruleHistory {
				if slices.Contains(backends, backend) {
					filteredRuleHistory[backend] = backendHistory
				}
			}
		}
	}

	marshalled, err := json.Marshal(filteredHistory)
	if err != nil {
		return errors.WithStack(err)
	}
	br.marshalledHistory = marshalled
	return nil
}

func (br *BackendReader) getBackendAddrs(ctx context.Context, excludeZones []string) ([]backendAddr, error) {
	return br.getBackendAddrsByCluster(ctx, "", excludeZones)
}

func (br *BackendReader) getBackendAddrsByCluster(ctx context.Context, clusterName string, excludeZones []string) ([]backendAddr, error) {
	backends, err := br.backendFetcher.GetTiDBTopology(ctx)
	if err != nil {
		br.lg.Error("failed to get backend addresses, stop reading metrics", zap.Error(err))
		metrics.ServerErrCounter.WithLabelValues("backend_metrics").Inc()
		return nil, err
	}
	addrs := make([]backendAddr, 0, len(backends))
	for _, backend := range backends {
		backendCluster := backend.Labels[config.ClusterLabelName]
		if backendCluster == "" {
			backendCluster = "default"
		}
		if clusterName != "" && backendCluster != clusterName {
			continue
		}
		if len(excludeZones) > 0 {
			if slices.Contains(excludeZones, backend.Labels[config.LocationLabelName]) {
				continue
			}
		}
		statusAddr := net.JoinHostPort(backend.IP, strconv.Itoa(int(backend.StatusPort)))
		addrs = append(addrs, backendAddr{
			statusAddr: statusAddr,
			label:      getLabel4Addr(statusAddr),
			cluster:    backendCluster,
		})
	}
	return addrs, nil
}

func (br *BackendReader) closeClusterOwners(closeEtcd bool) {
	br.clusterMu.Lock()
	owners := make([]*clusterOwner, 0, len(br.clusterOwners))
	for _, owner := range br.clusterOwners {
		owners = append(owners, owner)
	}
	if closeEtcd {
		br.clusterOwners = make(map[string]*clusterOwner)
	}
	br.clusterMu.Unlock()
	for _, owner := range owners {
		br.closeClusterOwner(owner, closeEtcd)
	}
	br.refreshOwnerState()
}

func (br *BackendReader) PreClose() {
	br.closeClusterOwners(false)
}

func (br *BackendReader) Close() {
	br.closeClusterOwners(true)
}

func purgeHistory(history []model.SamplePair, retention time.Duration, now time.Time) []model.SamplePair {
	idx := -1
	for i := range history {
		if time.UnixMilli(int64(history[i].Timestamp)).Add(retention).After(now) {
			idx = i
			break
		}
	}
	if idx > 0 {
		copy(history[:], history[idx:])
		return history[:len(history)-idx]
	} else if idx < 0 {
		history = history[:0]
	}
	return history
}

// filterMetrics filters the necessary metrics so that it's faster to parse.
func filterMetrics(all string, names []string) string {
	var buffer strings.Builder
	buffer.Grow(4096)
	for {
		idx := strings.Index(all, "\n")
		var line string
		if idx < 0 {
			line = all
		} else {
			line = all[:idx+1]
			all = all[idx+1:]
		}
		for i := range names {
			// strings.Contains() includes the metric type in the result but it's slower.
			// Note that the result is always in `Metric.Untyped` because the metric type is ignored.
			if strings.HasPrefix(line, names[i]) {
				buffer.WriteString(line)
				break
			}
		}
		if idx < 0 {
			break
		}
	}
	return buffer.String()
}

func parseMetrics(text string) (map[string]*dto.MetricFamily, error) {
	var parser expfmt.TextParser
	return parser.TextToMetricFamilies(strings.NewReader(text))
}
