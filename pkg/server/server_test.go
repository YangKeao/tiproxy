// Copyright 2024 PingCAP, Inc.
// SPDX-License-Identifier: Apache-2.0

package server

import (
	"context"
	"os"
	"testing"

	"github.com/pingcap/tiproxy/pkg/sctx"
	"github.com/stretchr/testify/require"
)

func TestServer(t *testing.T) {
	dir := t.TempDir()
	configFile := dir + "/config.toml"
	require.NoError(t, os.WriteFile(configFile, []byte(`proxy.pd-addrs = ""`), 0o644))

	server, err := NewServer(context.Background(), &sctx.Context{
		ConfigFile: configFile,
	})
	require.NoError(t, err)
	require.NoError(t, server.Close())
}
