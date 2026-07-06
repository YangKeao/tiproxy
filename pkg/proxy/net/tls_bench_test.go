// Copyright 2026 PingCAP, Inc.
// SPDX-License-Identifier: Apache-2.0

package net

import (
	"crypto/ecdsa"
	"crypto/ed25519"
	"crypto/elliptic"
	"crypto/rand"
	"crypto/rsa"
	"crypto/tls"
	"crypto/x509"
	"io"
	"math/big"
	stdnet "net"
	"testing"
	"time"
)

type benchmarkCertKind string

const (
	benchmarkCertRSA     benchmarkCertKind = "rsa"
	benchmarkCertECDSA   benchmarkCertKind = "ecdsa"
	benchmarkCertEd25519 benchmarkCertKind = "ed25519"
)

func TestBenchmarkTLSBackendSessionResumption(t *testing.T) {
	cert := mustCreateBenchmarkCertificate(t, benchmarkCertRSA)
	serverConfig := benchmarkServerTLSConfig(cert)
	clientConfig := benchmarkClientTLSConfig(true)

	state, err := benchmarkTLSHandshake(clientConfig, serverConfig)
	if err != nil {
		t.Fatal(err)
	}
	if state.DidResume {
		t.Fatal("first handshake unexpectedly resumed")
	}

	state, err = benchmarkTLSHandshake(clientConfig, serverConfig)
	if err != nil {
		t.Fatal(err)
	}
	if !state.DidResume {
		t.Fatal("second handshake did not resume")
	}
}

func BenchmarkConnectionEstablishTLSMatrix(b *testing.B) {
	certs := map[benchmarkCertKind]tls.Certificate{
		benchmarkCertRSA:     mustCreateBenchmarkCertificate(b, benchmarkCertRSA),
		benchmarkCertECDSA:   mustCreateBenchmarkCertificate(b, benchmarkCertECDSA),
		benchmarkCertEd25519: mustCreateBenchmarkCertificate(b, benchmarkCertEd25519),
	}
	frontendServerConfig := benchmarkServerTLSConfig(certs[benchmarkCertRSA])
	frontendClientConfig := benchmarkClientTLSConfig(false)

	type benchmarkCase struct {
		name              string
		clientTLS         bool
		backendTLS        bool
		backendResumption bool
		backendCert       benchmarkCertKind
	}

	cases := []benchmarkCase{
		{name: "client_tls_off/backend_tls_off"},
		{name: "client_tls_rsa/backend_tls_off", clientTLS: true},
	}
	for _, clientTLS := range []bool{false, true} {
		clientTLSName := "client_tls_off"
		if clientTLS {
			clientTLSName = "client_tls_rsa"
		}
		for _, certKind := range []benchmarkCertKind{benchmarkCertRSA, benchmarkCertECDSA, benchmarkCertEd25519} {
			for _, resume := range []bool{false, true} {
				resumeName := "resume_off"
				if resume {
					resumeName = "resume_on"
				}
				cases = append(cases, benchmarkCase{
					name:              clientTLSName + "/backend_tls_" + string(certKind) + "/" + resumeName,
					clientTLS:         clientTLS,
					backendTLS:        true,
					backendResumption: resume,
					backendCert:       certKind,
				})
			}
		}
	}

	for _, bc := range cases {
		b.Run(bc.name, func(b *testing.B) {
			backendServerConfig := benchmarkServerTLSConfig(certs[bc.backendCert])
			backendClientConfig := benchmarkClientTLSConfig(bc.backendResumption)
			if bc.backendTLS && bc.backendResumption {
				warmBenchmarkTLSClientSession(b, backendClientConfig, backendServerConfig)
			}

			b.ReportAllocs()
			b.ResetTimer()
			for range b.N {
				if bc.clientTLS {
					if _, err := benchmarkTLSHandshake(frontendClientConfig, frontendServerConfig); err != nil {
						b.Fatal(err)
					}
				} else if err := benchmarkPlainHandshake(); err != nil {
					b.Fatal(err)
				}

				if bc.backendTLS {
					state, err := benchmarkTLSHandshake(backendClientConfig, backendServerConfig)
					if err != nil {
						b.Fatal(err)
					}
					if bc.backendResumption && !state.DidResume {
						b.Fatal("backend TLS handshake did not resume")
					}
				} else if err := benchmarkPlainHandshake(); err != nil {
					b.Fatal(err)
				}
			}
		})
	}
}

func benchmarkServerTLSConfig(cert tls.Certificate) *tls.Config {
	return &tls.Config{
		Certificates: []tls.Certificate{cert},
		MinVersion:   tls.VersionTLS12,
		MaxVersion:   tls.VersionTLS12,
	}
}

func benchmarkClientTLSConfig(resume bool) *tls.Config {
	tcfg := &tls.Config{
		InsecureSkipVerify: true,
		MinVersion:         tls.VersionTLS12,
		MaxVersion:         tls.VersionTLS12,
	}
	if resume {
		tcfg.ClientSessionCache = tls.NewLRUClientSessionCache(64)
	}
	return tcfg
}

func warmBenchmarkTLSClientSession(tb testing.TB, clientConfig, serverConfig *tls.Config) {
	tb.Helper()
	if _, err := benchmarkTLSHandshake(clientConfig, serverConfig); err != nil {
		tb.Fatal(err)
	}
	state, err := benchmarkTLSHandshake(clientConfig, serverConfig)
	if err != nil {
		tb.Fatal(err)
	}
	if !state.DidResume {
		tb.Fatal("failed to warm TLS session cache")
	}
}

func benchmarkTLSHandshake(clientConfig, serverConfig *tls.Config) (tls.ConnectionState, error) {
	clientPipe, serverPipe := stdnet.Pipe()
	clientConn := tls.Client(clientPipe, clientConfig.Clone())
	serverConn := tls.Server(serverPipe, serverConfig)

	serverErrCh := make(chan error, 1)
	go func() {
		err := serverConn.Handshake()
		_ = serverPipe.Close()
		serverErrCh <- err
	}()

	err := clientConn.Handshake()
	state := clientConn.ConnectionState()
	_ = clientPipe.Close()
	if serverErr := <-serverErrCh; err == nil {
		err = serverErr
	}
	return state, err
}

func benchmarkPlainHandshake() error {
	clientPipe, serverPipe := stdnet.Pipe()
	serverErrCh := make(chan error, 1)
	go func() {
		var buf [1]byte
		_, err := io.ReadFull(serverPipe, buf[:])
		if err == nil {
			_, err = serverPipe.Write(buf[:])
		}
		_ = serverPipe.Close()
		serverErrCh <- err
	}()

	_, err := clientPipe.Write([]byte{1})
	if err == nil {
		var buf [1]byte
		_, err = io.ReadFull(clientPipe, buf[:])
	}
	_ = clientPipe.Close()
	if serverErr := <-serverErrCh; err == nil {
		err = serverErr
	}
	return err
}

func mustCreateBenchmarkCertificate(tb testing.TB, kind benchmarkCertKind) tls.Certificate {
	tb.Helper()
	template := &x509.Certificate{
		SerialNumber: big.NewInt(int64(time.Now().UnixNano())),
		NotBefore:    time.Now().Add(-time.Hour),
		NotAfter:     time.Now().Add(time.Hour),
		KeyUsage:     x509.KeyUsageDigitalSignature | x509.KeyUsageKeyEncipherment,
		ExtKeyUsage:  []x509.ExtKeyUsage{x509.ExtKeyUsageServerAuth},
		DNSNames:     []string{"localhost"},
	}

	var publicKey any
	var privateKey any
	var err error
	switch kind {
	case benchmarkCertRSA:
		key, genErr := rsa.GenerateKey(rand.Reader, 2048)
		if genErr != nil {
			tb.Fatal(genErr)
		}
		publicKey = &key.PublicKey
		privateKey = key
	case benchmarkCertECDSA:
		key, genErr := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
		if genErr != nil {
			tb.Fatal(genErr)
		}
		publicKey = &key.PublicKey
		privateKey = key
	case benchmarkCertEd25519:
		publicKey, privateKey, err = ed25519.GenerateKey(rand.Reader)
		if err != nil {
			tb.Fatal(err)
		}
	default:
		tb.Fatalf("unsupported benchmark cert kind %q", kind)
	}

	certDER, err := x509.CreateCertificate(rand.Reader, template, template, publicKey, privateKey)
	if err != nil {
		tb.Fatal(err)
	}
	return tls.Certificate{
		Certificate: [][]byte{certDER},
		PrivateKey:  privateKey,
	}
}
