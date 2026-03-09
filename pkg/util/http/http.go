// Copyright 2024 PingCAP, Inc.
// SPDX-License-Identifier: Apache-2.0

package http

import (
	"context"
	"crypto/tls"
	"fmt"
	"io"
	"net"
	"net/http"
	"time"

	"github.com/cenkalti/backoff/v4"
	"github.com/pingcap/tiproxy/lib/util/errors"
	pnet "github.com/pingcap/tiproxy/pkg/proxy/net"
)

type Client struct {
	cli          *http.Client
	getTLSConfig func() *tls.Config
	dialBackend  func(ctx context.Context, network, address, cluster string, timeout time.Duration) (net.Conn, error)
}

func NewHTTPClient(getTLSConfig func() *tls.Config) *Client {
	return NewHTTPClientWithDialer(getTLSConfig, nil)
}

func NewHTTPClientWithDialer(getTLSConfig func() *tls.Config, dialBackend func(ctx context.Context, network, address, cluster string, timeout time.Duration) (net.Conn, error)) *Client {
	client := &Client{
		getTLSConfig: getTLSConfig,
		dialBackend:  dialBackend,
	}
	// Since TLS config will hot reload, `TLSClientConfig` need update by `getTLSConfig()`
	// to obtain the latest TLS config.
	tr := &http.Transport{TLSClientConfig: getTLSConfig()}
	if dialBackend != nil {
		tr.DialContext = func(ctx context.Context, network, address string) (net.Conn, error) {
			cluster, _ := ctx.Value(clusterContextKey{}).(string)
			return dialBackend(ctx, network, address, cluster, 0)
		}
	}
	client.cli = &http.Client{Transport: tr}
	return client
}

func (client *Client) Get(addr, path string, b backoff.BackOff, timeout time.Duration) ([]byte, error) {
	return client.GetWithCluster(addr, path, "", b, timeout)
}

func (client *Client) GetWithCluster(addr, path, cluster string, b backoff.BackOff, timeout time.Duration) ([]byte, error) {
	// http cli is shared in the server, copy cli is to avoid concurrently setting http request timeout
	cli := *client.cli
	cli.Timeout = timeout
	schema := "http"
	// If the `http.Client.Transport.TLSClientConfig` is nil, it will be set to not nil when first time
	// calling `http.Client.Get()`. So the url schema decided by `client.getTLSConfig()`, which indicate
	// TiProxy server contain TLS config or not.
	// See https://github.com/pingcap/tiproxy/issues/602
	if tlsConfig := client.getTLSConfig(); tlsConfig != nil {
		schema = "https"
	}
	url := fmt.Sprintf("%s://%s%s", schema, addr, path)
	var body []byte
	err := ConnectWithRetry(func() error {
		req, err := http.NewRequest(http.MethodGet, url, nil)
		if err != nil {
			return err
		}
		if len(cluster) > 0 {
			req = req.WithContext(context.WithValue(req.Context(), clusterContextKey{}, cluster))
		}
		resp, err := cli.Do(req)
		if err != nil {
			return err
		}
		defer resp.Body.Close()

		if resp.StatusCode != http.StatusOK {
			return backoff.Permanent(errors.Errorf("http status %d", resp.StatusCode))
		}
		body, err = io.ReadAll(resp.Body)
		if err != nil {
			return errors.Errorf("read response body failed, url: %s, err: %s", url, err.Error())
		}
		return err
	}, b)
	return body, err
}

func (client *Client) DialContext(ctx context.Context, network, address, cluster string, timeout time.Duration) (net.Conn, error) {
	if client != nil && client.dialBackend != nil {
		return client.dialBackend(ctx, network, address, cluster, timeout)
	}
	if timeout > 0 {
		childCtx, cancel := context.WithTimeout(ctx, timeout)
		defer cancel()
		return (&net.Dialer{}).DialContext(childCtx, network, address)
	}
	return (&net.Dialer{}).DialContext(ctx, network, address)
}

func ConnectWithRetry(connect func() error, b backoff.BackOff) error {
	err := backoff.Retry(func() error {
		err := connect()
		if !pnet.IsRetryableError(err) {
			return backoff.Permanent(err)
		}
		return err
	}, b)
	return err
}

type clusterContextKey struct{}
