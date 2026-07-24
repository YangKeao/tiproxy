// Copyright 2026 PingCAP, Inc.
// SPDX-License-Identifier: Apache-2.0

package backend

import (
	"bytes"
	"context"
	"crypto/tls"
	"io"
	"net"
	"testing"
	"time"

	"github.com/go-mysql-org/go-mysql/mysql"
	"github.com/pingcap/tiproxy/lib/config"
	pnet "github.com/pingcap/tiproxy/pkg/proxy/net"
	"github.com/pingcap/tiproxy/pkg/proxy/proxyprotocol"
	"github.com/stretchr/testify/require"
	"go.uber.org/zap"
)

type handshakeFuzzContext struct {
	values map[any]any
}

func newHandshakeFuzzContext() *handshakeFuzzContext {
	return &handshakeFuzzContext{values: make(map[any]any)}
}

func (*handshakeFuzzContext) ClientAddr() string               { return "" }
func (*handshakeFuzzContext) ServerAddr() string               { return "" }
func (*handshakeFuzzContext) ClientInBytes() uint64            { return 0 }
func (*handshakeFuzzContext) ClientOutBytes() uint64           { return 0 }
func (*handshakeFuzzContext) UpdateLogger(fields ...zap.Field) {}
func (ctx *handshakeFuzzContext) SetValue(key, value any)      { ctx.values[key] = value }
func (ctx *handshakeFuzzContext) Value(key any) any            { return ctx.values[key] }

type handshakeFuzzConn struct {
	reader *bytes.Reader
}

func newHandshakeFuzzConn(data []byte) *handshakeFuzzConn {
	return &handshakeFuzzConn{reader: bytes.NewReader(append([]byte(nil), data...))}
}

func (conn *handshakeFuzzConn) Read(data []byte) (int, error) {
	return conn.reader.Read(data)
}

func (*handshakeFuzzConn) Write(data []byte) (int, error) {
	return len(data), nil
}

func (*handshakeFuzzConn) Close() error                     { return nil }
func (*handshakeFuzzConn) LocalAddr() net.Addr              { return &net.TCPAddr{} }
func (*handshakeFuzzConn) RemoteAddr() net.Addr             { return &net.TCPAddr{} }
func (*handshakeFuzzConn) SetDeadline(time.Time) error      { return nil }
func (*handshakeFuzzConn) SetReadDeadline(time.Time) error  { return nil }
func (*handshakeFuzzConn) SetWriteDeadline(time.Time) error { return nil }

type handshakeFuzzPacketIO struct {
	packets       [][]byte
	packetIndex   int
	sequence      uint8
	inBytes       uint64
	outBytes      uint64
	inPackets     uint64
	outPackets    uint64
	lastKeepAlive config.KeepAlive
}

func newHandshakeFuzzPacketIO(packets ...[]byte) *handshakeFuzzPacketIO {
	packetCopies := make([][]byte, len(packets))
	for i := range packets {
		packetCopies[i] = append([]byte(nil), packets[i]...)
	}
	return &handshakeFuzzPacketIO{packets: packetCopies}
}

func (*handshakeFuzzPacketIO) ApplyOpts(opts ...pnet.PacketIOption) {}
func (*handshakeFuzzPacketIO) LocalAddr() net.Addr {
	return &net.TCPAddr{IP: net.IPv4zero, Port: 4000}
}
func (*handshakeFuzzPacketIO) RemoteAddr() net.Addr {
	return &net.TCPAddr{IP: net.IPv4zero, Port: 4000}
}
func (packetIO *handshakeFuzzPacketIO) ResetSequence()     { packetIO.sequence = 0 }
func (packetIO *handshakeFuzzPacketIO) GetSequence() uint8 { return packetIO.sequence }

func (packetIO *handshakeFuzzPacketIO) ReadPacket() ([]byte, error) {
	if packetIO.packetIndex >= len(packetIO.packets) {
		return nil, io.EOF
	}
	packet := packetIO.packets[packetIO.packetIndex]
	packetIO.packetIndex++
	packetIO.sequence++
	packetIO.inBytes += uint64(len(packet))
	packetIO.inPackets++
	return packet, nil
}

func (packetIO *handshakeFuzzPacketIO) WritePacket(data []byte, _ bool) error {
	packetIO.sequence++
	packetIO.outBytes += uint64(len(data))
	packetIO.outPackets++
	return nil
}

func (packetIO *handshakeFuzzPacketIO) PeekPacketFirstByte() (byte, int, error) {
	if packetIO.packetIndex >= len(packetIO.packets) {
		return 0, 0, io.EOF
	}
	packet := packetIO.packets[packetIO.packetIndex]
	if len(packet) == 0 {
		return 0, 0, nil
	}
	return packet[0], len(packet), nil
}

func (packetIO *handshakeFuzzPacketIO) ForwardPacketTo(destIO pnet.PacketIO, captureLimit int) ([]byte, error) {
	packet, err := packetIO.ReadPacket()
	if err != nil {
		return nil, err
	}
	if err = destIO.WritePacket(packet, true); err != nil {
		return nil, err
	}
	if captureLimit <= 0 {
		return nil, nil
	}
	return packet[:min(captureLimit, len(packet))], nil
}

func (*handshakeFuzzPacketIO) ForwardUntil(pnet.PacketIO, func(byte, int) (bool, bool), func([]byte) error) error {
	return io.EOF
}

func (packetIO *handshakeFuzzPacketIO) InBytes() uint64    { return packetIO.inBytes }
func (packetIO *handshakeFuzzPacketIO) OutBytes() uint64   { return packetIO.outBytes }
func (packetIO *handshakeFuzzPacketIO) InPackets() uint64  { return packetIO.inPackets }
func (packetIO *handshakeFuzzPacketIO) OutPackets() uint64 { return packetIO.outPackets }
func (*handshakeFuzzPacketIO) Flush() error                { return nil }
func (*handshakeFuzzPacketIO) IsPeerActive() bool          { return true }
func (packetIO *handshakeFuzzPacketIO) SetKeepalive(cfg config.KeepAlive) error {
	packetIO.lastKeepAlive = cfg
	return nil
}
func (packetIO *handshakeFuzzPacketIO) LastKeepAlive() config.KeepAlive {
	return packetIO.lastKeepAlive
}
func (*handshakeFuzzPacketIO) GracefulClose() error                   { return nil }
func (*handshakeFuzzPacketIO) Close() error                           { return nil }
func (*handshakeFuzzPacketIO) EnableProxyClient(*proxyprotocol.Proxy) {}
func (*handshakeFuzzPacketIO) EnableProxyServer()                     {}
func (*handshakeFuzzPacketIO) Proxy() *proxyprotocol.Proxy            { return nil }
func (*handshakeFuzzPacketIO) ProxyAddr() net.Addr                    { return nil }
func (*handshakeFuzzPacketIO) ServerTLSHandshake(tlsConfig *tls.Config) (tls.ConnectionState, error) {
	if tlsConfig == nil {
		return tls.ConnectionState{}, io.ErrUnexpectedEOF
	}
	return tls.ConnectionState{}, nil
}
func (*handshakeFuzzPacketIO) ClientTLSHandshake(tlsConfig *tls.Config) error {
	if tlsConfig == nil {
		return io.ErrUnexpectedEOF
	}
	return nil
}
func (*handshakeFuzzPacketIO) TLSConnectionState() tls.ConnectionState { return tls.ConnectionState{} }
func (*handshakeFuzzPacketIO) SetCompressionAlgorithm(pnet.CompressAlgorithm, int) error {
	return nil
}

func runFuzzHandshakeFirstTime(
	clientHandshake, clientTLSHandshake, clientAuth,
	backendHandshake, backendAuth1, backendAuth2 []byte,
	enableFrontendTLS, enableProxyProtocol bool,
) error {
	clientPackets := [][]byte{clientHandshake}
	if enableFrontendTLS {
		clientPackets = append(clientPackets, clientTLSHandshake)
	}
	clientPackets = append(clientPackets, clientAuth, clientAuth)
	clientIO := newHandshakeFuzzPacketIO(clientPackets...)
	// Keep a valid OK packet at the end so every mutated prefix that remains
	// protocol-valid can advance all the way to successful authentication.
	backendIO := newHandshakeFuzzPacketIO(
		backendHandshake,
		backendAuth1,
		backendAuth2,
		pnet.MakeOKPacket(0, pnet.OKHeader),
	)

	var frontendTLSConfig *tls.Config
	if enableFrontendTLS {
		frontendTLSConfig = &tls.Config{MinVersion: tls.VersionTLS12}
	}
	auth := NewAuthenticator(&BCConfig{ProxyProtocol: enableProxyProtocol})
	handler := &CustomHandshakeHandler{}
	return auth.handshakeFirstTime(
		context.Background(),
		zap.NewNop(),
		newHandshakeFuzzContext(),
		clientIO,
		handler,
		func(context.Context, ConnContext, *pnet.HandshakeResp) (pnet.PacketIO, error) {
			return backendIO, nil
		},
		frontendTLSConfig,
		nil,
	)
}

func runFuzzHandshakeWithBackend(backendHandshake, backendAuth1, backendAuth2 []byte) error {
	backendIO := newHandshakeFuzzPacketIO(
		backendHandshake,
		backendAuth1,
		backendAuth2,
		pnet.MakeOKPacket(0, pnet.OKHeader),
	)
	auth := NewAuthenticator(&BCConfig{})
	return auth.handshakeWithBackend(
		context.Background(),
		zap.NewNop(),
		newHandshakeFuzzContext(),
		&CustomHandshakeHandler{},
		"user",
		"password",
		"db",
		func(context.Context, ConnContext, *pnet.HandshakeResp) (pnet.PacketIO, error) {
			return backendIO, nil
		},
		nil,
	)
}

func runFuzzHandshakeReconnect(
	clientHandshake, clientAuth,
	backendHandshake, backendAuth1, backendAuth2 []byte,
) error {
	clientIO := newHandshakeFuzzPacketIO(clientHandshake, clientAuth, clientAuth, clientAuth)
	backendIOs := []pnet.PacketIO{
		newHandshakeFuzzPacketIO(
			backendHandshake,
			backendAuth1,
			pnet.MakeOKPacket(0, pnet.OKHeader),
		),
		newHandshakeFuzzPacketIO(
			backendHandshake,
			backendAuth2,
			pnet.MakeOKPacket(0, pnet.OKHeader),
		),
	}
	backendIndex := 0
	reconnected := false
	handler := &CustomHandshakeHandler{
		handleHandshakeErr: func(ConnContext, *mysql.MyError) bool {
			if reconnected {
				return false
			}
			reconnected = true
			return true
		},
	}

	auth := NewAuthenticator(&BCConfig{})
	return auth.handshakeFirstTime(
		context.Background(),
		zap.NewNop(),
		newHandshakeFuzzContext(),
		clientIO,
		handler,
		func(context.Context, ConnContext, *pnet.HandshakeResp) (pnet.PacketIO, error) {
			if backendIndex >= len(backendIOs) {
				return nil, io.EOF
			}
			backendIO := backendIOs[backendIndex]
			backendIndex++
			return backendIO, nil
		},
		nil,
		nil,
	)
}

func runFuzzHandshakeSecondTime(backendHandshake, backendAuth []byte, enableProxyProtocol bool) error {
	clientIO := newHandshakeFuzzPacketIO()
	backendIO := newHandshakeFuzzPacketIO(
		backendHandshake,
		backendAuth,
		pnet.MakeOKPacket(0, pnet.OKHeader),
	)
	auth := NewAuthenticator(&BCConfig{ProxyProtocol: enableProxyProtocol})
	auth.capability = defaultTestClientCapability
	return auth.handshakeSecondTime(zap.NewNop(), clientIO, backendIO, nil, "session-token")
}

func appendHandshakeWirePacket(wire []byte, sequence byte, payload []byte) []byte {
	length := len(payload)
	wire = append(wire, byte(length), byte(length>>8), byte(length>>16), sequence)
	return append(wire, payload...)
}

func validHandshakeWirePackets() (clientWire, backendWire []byte) {
	clientResp, _, backendInitial, authSwitch, okPacket := validHandshakeFuzzPackets()
	clientWire = appendHandshakeWirePacket(clientWire, 1, clientResp)
	clientWire = appendHandshakeWirePacket(clientWire, 3, mockAuthData)
	backendWire = appendHandshakeWirePacket(backendWire, 0, backendInitial)
	backendWire = appendHandshakeWirePacket(backendWire, 2, authSwitch)
	backendWire = appendHandshakeWirePacket(backendWire, 4, okPacket)
	return
}

func runFuzzHandshakeWire(clientWire, backendWire []byte) error {
	logger := zap.NewNop()
	clientIO := pnet.NewPacketIO(newHandshakeFuzzConn(clientWire), logger, 0)
	backendIO := pnet.NewPacketIO(newHandshakeFuzzConn(backendWire), logger, 0)
	defer func() {
		_ = clientIO.Close()
		_ = backendIO.Close()
	}()

	auth := NewAuthenticator(&BCConfig{})
	return auth.handshakeFirstTime(
		context.Background(),
		logger,
		newHandshakeFuzzContext(),
		clientIO,
		&CustomHandshakeHandler{},
		func(context.Context, ConnContext, *pnet.HandshakeResp) (pnet.PacketIO, error) {
			return backendIO, nil
		},
		&tls.Config{MinVersion: tls.VersionTLS12},
		nil,
	)
}

func validHandshakeFuzzPackets() (clientResp, tlsRequest, backendInitial, authSwitch, okPacket []byte) {
	clientResp = pnet.MakeHandshakeResponse(&pnet.HandshakeResp{
		User:       mockUsername,
		DB:         mockDBName,
		AuthPlugin: pnet.AuthNativePassword,
		AuthData:   mockAuthData,
		Capability: defaultTestClientCapability &^ pnet.ClientSSL,
		Collation:  pnet.Collation,
	})
	tlsResp := pnet.MakeHandshakeResponse(&pnet.HandshakeResp{
		User:       mockUsername,
		DB:         mockDBName,
		AuthPlugin: pnet.AuthNativePassword,
		AuthData:   mockAuthData,
		Capability: defaultTestClientCapability | pnet.ClientSSL,
		Collation:  pnet.Collation,
	})
	tlsRequest = tlsResp[:32]
	backendInitial = pnet.MakeInitialHandshake(
		defaultTestBackendCapability,
		mockSalt,
		pnet.AuthNativePassword,
		pnet.ServerVersion,
		1,
	)
	authSwitch = pnet.MakeSwitchRequest(pnet.AuthNativePassword, mockSalt)
	okPacket = pnet.MakeOKPacket(0, pnet.OKHeader)
	return
}

func TestHandshakeFuzzHarnessReachesAuthSuccess(t *testing.T) {
	clientResp, tlsRequest, backendInitial, authSwitch, okPacket := validHandshakeFuzzPackets()
	require.NoError(t, runFuzzHandshakeFirstTime(
		clientResp, clientResp, mockAuthData,
		backendInitial, authSwitch, okPacket,
		false, false,
	))

	tlsResp := append([]byte(nil), clientResp...)
	tlsCapability := defaultTestClientCapability | pnet.ClientSSL
	copy(tlsResp[:4], pnet.Uint32ToBytes(tlsCapability.Uint32()))
	require.NoError(t, runFuzzHandshakeFirstTime(
		tlsRequest, tlsResp, mockAuthData,
		backendInitial, authSwitch, okPacket,
		true, true,
	))

	require.NoError(t, runFuzzHandshakeWithBackend(backendInitial, authSwitch, okPacket))
	require.NoError(t, runFuzzHandshakeSecondTime(backendInitial, okPacket, false))
	require.NoError(t, runFuzzHandshakeSecondTime(backendInitial, okPacket, true))

	cachingSHA2Switch := pnet.MakeSwitchRequest(pnet.AuthCachingSha2Password, mockSalt)
	require.NoError(t, runFuzzHandshakeFirstTime(
		clientResp, clientResp, mockAuthData,
		backendInitial, cachingSHA2Switch, []byte{pnet.ShaCommand, 3},
		false, true,
	))
	require.NoError(t, runFuzzHandshakeReconnect(
		clientResp,
		mockAuthData,
		backendInitial,
		pnet.MakeErrPacket(mysql.NewDefaultError(mysql.ER_UNKNOWN_ERROR)),
		authSwitch,
	))

	clientWire, backendWire := validHandshakeWirePackets()
	require.NoError(t, runFuzzHandshakeWire(clientWire, backendWire))
}

func FuzzHandshakeUntilAuthSuccess(f *testing.F) {
	clientResp, tlsRequest, backendInitial, authSwitch, okPacket := validHandshakeFuzzPackets()
	cachingSHA2Switch := pnet.MakeSwitchRequest(pnet.AuthCachingSha2Password, mockSalt)
	cachingSHA2FastAuth := []byte{pnet.ShaCommand, 3}
	backendErr := pnet.MakeErrPacket(mysql.NewDefaultError(mysql.ER_UNKNOWN_ERROR))
	f.Add(clientResp, clientResp, mockAuthData)

	tlsResp := append([]byte(nil), clientResp...)
	tlsCapability := defaultTestClientCapability | pnet.ClientSSL
	copy(tlsResp[:4], pnet.Uint32ToBytes(tlsCapability.Uint32()))
	f.Add(tlsRequest, tlsResp, mockAuthData)
	f.Add(clientResp, clientResp, []byte{})
	f.Add([]byte{}, []byte{}, []byte{})

	f.Fuzz(func(
		t *testing.T,
		clientHandshake, clientTLSHandshake, clientAuth []byte,
	) {
		_ = runFuzzHandshakeFirstTime(
			clientHandshake, clientTLSHandshake, clientAuth,
			backendInitial, authSwitch, okPacket,
			false, false,
		)
		_ = runFuzzHandshakeFirstTime(
			clientHandshake, clientTLSHandshake, clientAuth,
			backendInitial, authSwitch, okPacket,
			true, true,
		)
		_ = runFuzzHandshakeFirstTime(
			clientHandshake, clientTLSHandshake, clientAuth,
			backendInitial,
			cachingSHA2Switch,
			cachingSHA2FastAuth,
			false, true,
		)
		_ = runFuzzHandshakeReconnect(
			clientHandshake,
			clientAuth,
			backendInitial,
			backendErr,
			authSwitch,
		)
	})
}

func FuzzHandshakeWirePackets(f *testing.F) {
	clientWire, backendWire := validHandshakeWirePackets()
	_, tlsRequest, _, _, _ := validHandshakeFuzzPackets()
	clientTLSWire := appendHandshakeWirePacket(nil, 1, tlsRequest)
	clientTLSWire = append(clientTLSWire, 0x16, 0x03, 0x03, 0x00, 0x01, 0x00)
	f.Add(clientWire)
	f.Add(clientTLSWire)
	f.Add([]byte{})
	f.Add([]byte{1, 0, 0x10, 1, 0})

	f.Fuzz(func(t *testing.T, fuzzClientWire []byte) {
		_ = runFuzzHandshakeWire(fuzzClientWire, backendWire)
	})
}
