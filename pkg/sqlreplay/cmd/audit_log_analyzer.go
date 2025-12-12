// Copyright 2025 PingCAP, Inc.
// SPDX-License-Identifier: Apache-2.0

package cmd

import (
	"strconv"
	"strings"
	"time"

	"github.com/pingcap/tidb/pkg/parser"
	"github.com/pingcap/tiproxy/lib/util/errors"
	pnet "github.com/pingcap/tiproxy/pkg/proxy/net"
	"github.com/siddontang/go/hack"
)

type stmtTypesSet map[string]struct{}

func (s stmtTypesSet) String() string {
	var types []string
	for stmtType := range s {
		types = append(types, stmtType)
	}
	return strings.Join(types, ",")
}

// AuditLogGroup is the analysis result for a group of similar audit log entries.
type AuditLogGroup struct {
	ExecutionCount        int
	TotalCostTime         time.Duration
	TotalAffectedRows     int64
	StmtTypes             stmtTypesSet
	CommandAfterStartTime []time.Duration
	ConnectionLifeTime    []connectionLifeTimePair
}

type connectionLifeTimePair struct {
	LifeTime     time.Duration
	CommandCount int
}

const metaGroup = "META"

// AuditLogAnalyzeResult is the result of analyzing audit logs.
type AuditLogAnalyzeResult map[string]AuditLogGroup

func (r AuditLogAnalyzeResult) Merge(other AuditLogAnalyzeResult) {
	for sql, group := range other {
		finalGroup := r[sql]
		finalGroup.ExecutionCount += group.ExecutionCount
		finalGroup.TotalCostTime += group.TotalCostTime
		finalGroup.TotalAffectedRows += group.TotalAffectedRows
		for stmtType := range group.StmtTypes {
			if finalGroup.StmtTypes == nil {
				finalGroup.StmtTypes = make(map[string]struct{})
			}
			finalGroup.StmtTypes[stmtType] = struct{}{}
		}
		finalGroup.CommandAfterStartTime = append(finalGroup.CommandAfterStartTime, group.CommandAfterStartTime...)
		finalGroup.ConnectionLifeTime = append(finalGroup.ConnectionLifeTime, group.ConnectionLifeTime...)
		r[sql] = finalGroup
	}
}

// AnalyzeConfig is the configuration for audit log analysis.
type AnalyzeConfig struct {
	Input                        string
	Start                        time.Time
	End                          time.Time
	DB                           string
	FilterCommandWithRetry       bool
	OnlyAnalyzeConnDuringAnalyze bool
}

type analyzeConnCtx struct {
	auditLogPluginConnCtx

	connectionCreateTime time.Time
	startDuringAnalyze   bool
	commandCount         int
}

type auditLogAnalyzer struct {
	reader LineReader

	cfg      AnalyzeConfig
	connInfo map[uint64]analyzeConnCtx
}

// NewAuditLogAnalyzer creates a new audit log analyzer.
func NewAuditLogAnalyzer(reader LineReader, cfg AnalyzeConfig) *auditLogAnalyzer {
	return &auditLogAnalyzer{
		reader:   reader,
		cfg:      cfg,
		connInfo: make(map[uint64]analyzeConnCtx),
	}
}

// Analyze analyzes the audit log and returns the analysis result.
func (a *auditLogAnalyzer) Analyze() (AuditLogAnalyzeResult, error) {
	result := make(AuditLogAnalyzeResult)
	defer func() {
		for _, conn := range a.connInfo {
			if a.cfg.OnlyAnalyzeConnDuringAnalyze && !conn.startDuringAnalyze {
				continue
			}
			connLifeTime := a.cfg.End.Sub(conn.connectionCreateTime)
			metaGroupInfo := result[metaGroup]
			metaGroupInfo.ConnectionLifeTime = append(metaGroupInfo.ConnectionLifeTime, connectionLifeTimePair{
				LifeTime:     connLifeTime,
				CommandCount: conn.commandCount,
			})
			result[metaGroup] = metaGroupInfo
		}
	}()

	kvs := make(map[string]string, 25)
	for {
		line, filename, lineIdx, err := a.reader.ReadLine()
		if err != nil {
			return result, err
		}
		clear(kvs)
		err = parseLog(kvs, hack.String(line))
		if err != nil {
			return result, errors.Errorf("%s, line %d: %s", filename, lineIdx, err.Error())
		}
		// Only analyze the COMPLETED event
		event, ok := kvs[auditPluginKeyEvent]
		if !ok || event != auditPluginEventEnd {
			if kvs[auditPluginKeyClass] == auditPluginClassConnect {
				switch kvs[auditPluginKeySubClass] {
				case auditPluginSubClassDisconnect:
					connID, err := strconv.ParseUint(kvs[auditPluginKeyConnID], 10, 64)
					if err != nil {
						return result, errors.Wrapf(err, "%s, line %d: parse conn id failed: %s", filename, lineIdx, kvs[auditPluginKeyConnID])
					}
					connInfo, ok := a.connInfo[connID]
					if ok {
						if a.cfg.OnlyAnalyzeConnDuringAnalyze && !connInfo.startDuringAnalyze {
							continue
						}
						_, endTs, err := parseStartAndEndTs(kvs)
						if err != nil {
							return result, err
						}
						connLifeTime := endTs.Sub(connInfo.connectionCreateTime)
						metaGroupInfo := result[metaGroup]
						metaGroupInfo.ConnectionLifeTime = append(metaGroupInfo.ConnectionLifeTime, connectionLifeTimePair{
							LifeTime:     connLifeTime,
							CommandCount: connInfo.commandCount,
						})
						result[metaGroup] = metaGroupInfo
						delete(a.connInfo, connID)
					}
				case auditPluginSubClassConnected:
					connID, err := strconv.ParseUint(kvs[auditPluginKeyConnID], 10, 64)
					if err != nil {
						return result, errors.Wrapf(err, "%s, line %d: parse conn id failed: %s", filename, lineIdx, kvs[auditPluginKeyConnID])
					}
					connInfo, ok := a.connInfo[connID]
					if ok {
						return result, errors.Errorf("%s, line %d: found duplicated connection create for conn id: %d", filename, lineIdx, connID)
					}
					startTs, _, err := parseStartAndEndTs(kvs)
					if err != nil {
						return result, err
					}
					connInfo.connectionCreateTime = startTs
					connInfo.startDuringAnalyze = true
					a.connInfo[connID] = connInfo
				}
			}
			continue
		}

		// Only analyze the event within the time range
		startTs, endTs, err := parseStartAndEndTs(kvs)
		if err != nil {
			return nil, errors.Wrapf(err, "%s, line %d", filename, lineIdx)
		}
		if endTs.Before(a.cfg.Start) {
			continue
		}
		if endTs.After(a.cfg.End) {
			// Reach the end time, stop analyzing.
			return result, nil
		}

		// Only analyze the `Query` and `Execute` commands
		cmdStr := parseCommand(kvs[auditPluginKeyCommand])
		if cmdStr != "Query" && cmdStr != "Execute" {
			continue
		}

		// Only analyze the SQL in given database
		if len(a.cfg.DB) != 0 {
			databases, ok := kvs[auditPluginKeyDatabases]
			if !ok {
				continue
			}

			includeTargetDB := false
			for _, db := range strings.Split(databases, ",") {
				if db == a.cfg.DB {
					includeTargetDB = true
				}
			}
			if !includeTargetDB {
				continue
			}
		}

		// Try to filter out retried commands
		connID, err := strconv.ParseUint(kvs[auditPluginKeyConnID], 10, 64)
		if err != nil {
			return result, errors.Wrapf(err, "%s, line %d: parse conn id failed: %s", filename, lineIdx, kvs[auditPluginKeyConnID])
		}
		connInfo, ok := a.connInfo[connID]
		if !ok {
			// This connection has started before analysis start time.
			connInfo.connectionCreateTime = a.cfg.Start
		}
		if a.cfg.FilterCommandWithRetry {
			if retryStr, ok := kvs[auditPluginKeyRetry]; ok {
				// If it's a retry command, just skip it.
				if retryStr == "true" {
					continue
				}
			}
		} else {
			sql, err := parseSQL(kvs[auditPluginKeySQL])
			if err != nil {
				return result, errors.Wrapf(err, "%s, line %d: unquote sql failed: %s", filename, lineIdx, kvs[auditPluginKeySQL])
			}
			if isDuplicatedWrite(connInfo.lastCmd, kvs, cmdStr, sql, startTs, endTs) {
				continue
			}
		}

		sql, err := parseSQL(kvs[auditPluginKeySQL])
		if err != nil {
			return result, errors.Wrapf(err, "unquote sql failed: %s", kvs[auditPluginKeySQL])
		}
		normalizedSQL := parser.Normalize(sql, "ON")
		group := result[normalizedSQL]

		var costTime time.Duration
		costTimeStr := kvs[auditPluginKeyCostTime]
		if len(costTimeStr) != 0 {
			millis, err := strconv.ParseFloat(costTimeStr, 32)
			if err != nil {
				return result, errors.Errorf("parsing cost time failed: %s", costTimeStr)
			}
			costTime = time.Duration(millis) * (time.Millisecond)
		}

		var affectedRows int64
		affectedRowsStr := kvs[auditPluginKeyRows]
		if len(affectedRowsStr) != 0 {
			affectedRows, err = strconv.ParseInt(affectedRowsStr, 10, 64)
			if err != nil {
				return result, errors.Errorf("parsing affected rows failed: %s", affectedRowsStr)
			}
		}

		// Record the last command info for deduplication. We only recorded the needed fields here.
		connInfo.lastCmd = &Command{
			StartTs: startTs,
			EndTs:   endTs,
			ConnID:  connID,
		}
		switch cmdStr {
		case "Query":
			connInfo.lastCmd.Type = pnet.ComQuery
			connInfo.lastCmd.Payload = append([]byte{pnet.ComQuery.Byte()}, hack.Slice(sql)...)
		case "Execute":
			connInfo.lastCmd.Type = pnet.ComStmtExecute
			connInfo.lastCmd.PreparedStmt = sql
		}
		connInfo.lastCmd.StmtType = kvs[auditPluginKeyStmtType]
		connInfo.lastCmd.kvs = kvs
		connInfo.commandCount++
		a.connInfo[connID] = connInfo

		group.ExecutionCount++
		group.TotalCostTime += costTime
		group.TotalAffectedRows += affectedRows
		if len(kvs[auditPluginKeyStmtType]) != 0 {
			if group.StmtTypes == nil {
				group.StmtTypes = make(map[string]struct{})
			}
			group.StmtTypes[kvs[auditPluginKeyStmtType]] = struct{}{}
		}

		if !(a.cfg.OnlyAnalyzeConnDuringAnalyze && !connInfo.startDuringAnalyze) {
			group.CommandAfterStartTime = append(group.CommandAfterStartTime, startTs.Sub(connInfo.connectionCreateTime))
		}
		result[normalizedSQL] = group
	}
}
