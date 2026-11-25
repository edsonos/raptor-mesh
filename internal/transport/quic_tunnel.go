package transport

import (
	"bytes"
	"context"
	"crypto/tls"
	"errors"
	"fmt"
	"io"
	"net"
	"net/http"
	"strconv"
	"sync"
	"sync/atomic"
	"time"

	"github.com/quic-go/quic-go"

	"raptor-mesh/internal/protocol"
)

// TunnelStats exposes RMESH/1 QUIC 0-RTT client tunnel performance counters.
type TunnelStats struct {
	ServerAddr     string `json:"server_addr"`
	PoolConns      int    `json:"pool_conns"`
	StreamsOpened  int64  `json:"streams_opened"`
	ZeroRTTConns   int64  `json:"zero_rtt_conns"`
	Reconnects     int64  `json:"reconnects"`
}

// TunnelClient manages a multiplexed pool of persistent RMESH/1 QUIC 0-RTT connections to a raptor-server node.
type TunnelClient struct {
	serverAddr   string
	poolSize     int
	tlsCfg       *tls.Config
	quicCfg      *quic.Config
	mu           sync.RWMutex
	conns        []*quic.Conn
	rrIdx        atomic.Uint64
	streamsTotal atomic.Int64
	zeroRTTCount atomic.Int64
	reconnects   atomic.Int64
}

// NewTunnelClient creates a multiplexed RMESH/1 QUIC 0-RTT client pool targeting serverAddr.
func NewTunnelClient(serverAddr string, poolSize int, insecureSkipVerify bool) *TunnelClient {
	if poolSize <= 0 {
		poolSize = 4
	}
	return &TunnelClient{
		serverAddr: serverAddr,
		poolSize:   poolSize,
		conns:      make([]*quic.Conn, poolSize),
		tlsCfg: &tls.Config{
			ServerName:         "raptor-mesh.local",
			InsecureSkipVerify: insecureSkipVerify,
			NextProtos:         []string{protocol.ALPN},
			MinVersion:         tls.VersionTLS13,
			ClientSessionCache: tls.NewLRUClientSessionCache(64),
		},
		quicCfg: &quic.Config{
			Allow0RTT:                      true,
			KeepAlivePeriod:                8 * time.Second,
			MaxIdleTimeout:                 120 * time.Second,
			HandshakeIdleTimeout:           3 * time.Second,
			MaxIncomingStreams:             4096,
			InitialStreamReceiveWindow:     4 << 20,
			MaxStreamReceiveWindow:         16 << 20,
			InitialConnectionReceiveWindow: 8 << 20,
			MaxConnectionReceiveWindow:     32 << 20,
		},
	}
}

// Warmup pre-establishes tunnel 0 first to populate the TLS 1.3 0-RTT session ticket cache,
// then opens remaining tunnels in parallel using genuine QUIC 0-RTT Early Data!
func (tc *TunnelClient) Warmup(ctx context.Context) error {
	// Step 1: Establish tunnel 0 and run a tiny CmdPing so raptor-server delivers its TLS 1.3 NewSessionTicket.
	c0, err := tc.ensureConn(ctx, 0)
	if err != nil {
		return err
	}
	if s, serr := c0.OpenStreamSync(ctx); serr == nil {
		_ = protocol.WriteHeader(s, protocol.FrameHeader{
			Version: protocol.Version1,
			Flags:   protocol.FlagKeepAlive,
			Cmd:     protocol.CmdPing,
		})
		_ = protocol.ReadStatus(s)
		_ = s.Close()
	}
	// Wait briefly for the post-handshake NewSessionTicket frame to land in ClientSessionCache.
	select {
	case <-time.After(25 * time.Millisecond):
	case <-ctx.Done():
		return ctx.Err()
	}

	// Step 2: Open remaining pool tunnels in parallel with 0-RTT session resumption!
	var firstErr error
	var wg sync.WaitGroup
	var errMu sync.Mutex

	for i := 1; i < tc.poolSize; i++ {
		idx := i
		wg.Add(1)
		go func() {
			defer wg.Done()
			if _, err := tc.ensureConn(ctx, idx); err != nil {
				errMu.Lock()
				if firstErr == nil {
					firstErr = err
				}
				errMu.Unlock()
			}
		}()
	}
	wg.Wait()
	return firstErr
}

// Close closes all active QUIC tunnel connections.
func (tc *TunnelClient) Close() {
	tc.mu.Lock()
	defer tc.mu.Unlock()
	for i, c := range tc.conns {
		if c != nil {
			_ = c.CloseWithError(0, "client shutdown")
			tc.conns[i] = nil
		}
	}
}

// OpenConnectStream opens a bidirectional RMESH/1 stream for CONNECT tunneling to host:port.
func (tc *TunnelClient) OpenConnectStream(ctx context.Context, host string, port uint16) (io.ReadWriteCloser, error) {
	stream, err := tc.openStream(ctx)
	if err != nil {
		return nil, err
	}
	// Prioritize interactive tunnel setup frames.
	stream.SetPriority(1, true)

	hdr := protocol.FrameHeader{
		Version: protocol.Version1,
		Flags:   protocol.Flag0RTT | protocol.FlagConnect | protocol.FlagPriority,
		Cmd:     protocol.CmdConnect,
		Port:    port,
		Host:    host,
	}
	if err := protocol.WriteHeader(stream, hdr); err != nil {
		_ = stream.Close()
		return nil, err
	}
	if err := protocol.ReadStatus(stream); err != nil {
		_ = stream.Close()
		return nil, err
	}
	return stream, nil
}

// RoundTripFast forwards an HTTP request in binary RMESH/1 framing over a 0-RTT QUIC stream
// so raptor-server executes it over its hot H3/H2 multiplexed edge pool.
func (tc *TunnelClient) RoundTripFast(ctx context.Context, req *http.Request) (*http.Response, error) {
	stream, err := tc.openStream(ctx)
	if err != nil {
		return nil, err
	}
	defer stream.Close()

	stream.SetPriority(0, false)

	var bodyBytes []byte
	if req.Body != nil && req.Body != http.NoBody {
		bodyBytes, err = io.ReadAll(req.Body)
		if err != nil {
			return nil, err
		}
		_ = req.Body.Close()
	}

	port := uint16(443)
	if req.URL.Scheme == "http" {
		port = 80
	}
	if p := req.URL.Port(); p != "" {
		if pn, perr := strconv.Atoi(p); perr == nil {
			port = uint16(pn)
		}
	}

	hdr := protocol.FrameHeader{
		Version: protocol.Version1,
		Flags:   protocol.Flag0RTT | protocol.FlagHTTPFastPath | protocol.FlagPriority,
		Cmd:     protocol.CmdHTTPFast,
		Port:    port,
		Host:    req.URL.Hostname(),
	}
	if err := protocol.WriteHeader(stream, hdr); err != nil {
		return nil, err
	}

	fastReq := protocol.FastHTTPRequest{
		Method:  req.Method,
		URL:     req.URL.String(),
		Headers: req.Header,
		Body:    bodyBytes,
	}
	if err := protocol.WriteFastHTTPRequest(stream, fastReq); err != nil {
		return nil, err
	}

	if err := protocol.ReadStatus(stream); err != nil {
		return nil, err
	}

	fastResp, err := protocol.ReadFastHTTPResponse(stream)
	if err != nil {
		return nil, err
	}

	httpResp := &http.Response{
		StatusCode:    fastResp.StatusCode,
		Status:        fmt.Sprintf("%d %s", fastResp.StatusCode, http.StatusText(fastResp.StatusCode)),
		Proto:         "HTTP/2.0",
		ProtoMajor:    2,
		ProtoMinor:    0,
		Header:        fastResp.Headers,
		Body:          io.NopCloser(bytes.NewReader(fastResp.Body)),
		ContentLength: int64(len(fastResp.Body)),
		Request:       req,
	}
	if httpResp.Header == nil {
		httpResp.Header = make(http.Header)
	}
	if fastResp.UpstreamProto != "" {
		httpResp.Header.Set("X-Raptor-Upstream-Proto", fastResp.UpstreamProto)
	}
	httpResp.Header.Set("X-Raptor-Tunnel", "RMESH/1-QUIC-0RTT")
	return httpResp, nil
}

// GetStats returns live RMESH/1 tunnel telemetry.
func (tc *TunnelClient) GetStats() TunnelStats {
	tc.mu.RLock()
	active := 0
	for _, c := range tc.conns {
		if c != nil && c.Context().Err() == nil {
			active++
		}
	}
	tc.mu.RUnlock()
	return TunnelStats{
		ServerAddr:    tc.serverAddr,
		PoolConns:     active,
		StreamsOpened: tc.streamsTotal.Load(),
		ZeroRTTConns:  tc.zeroRTTCount.Load(),
		Reconnects:    tc.reconnects.Load(),
	}
}

func (tc *TunnelClient) openStream(ctx context.Context) (*quic.Stream, error) {
	idx := int(tc.rrIdx.Add(1)-1) % tc.poolSize
	conn, err := tc.ensureConn(ctx, idx)
	if err != nil {
		return nil, err
	}
	stream, err := conn.OpenStreamSync(ctx)
	if err != nil {
		// Connection may have just dropped; transparently reconnect once and retry.
		tc.invalidateConn(idx, conn)
		conn, err = tc.ensureConn(ctx, idx)
		if err != nil {
			return nil, err
		}
		stream, err = conn.OpenStreamSync(ctx)
		if err != nil {
			return nil, err
		}
	}
	tc.streamsTotal.Add(1)
	return stream, nil
}

func (tc *TunnelClient) ensureConn(ctx context.Context, idx int) (*quic.Conn, error) {
	tc.mu.RLock()
	c := tc.conns[idx]
	tc.mu.RUnlock()
	if c != nil && c.Context().Err() == nil {
		return c, nil
	}

	tc.mu.Lock()
	defer tc.mu.Unlock()
	if c = tc.conns[idx]; c != nil && c.Context().Err() == nil {
		return c, nil
	}

	dialCtx, cancel := context.WithTimeout(ctx, 4*time.Second)
	defer cancel()
	newConn, err := quic.DialAddrEarly(dialCtx, tc.serverAddr, tc.tlsCfg.Clone(), tc.quicCfg)
	if err != nil {
		return nil, err
	}
	go func(c *quic.Conn) {
		select {
		case <-c.HandshakeComplete():
			if c.ConnectionState().Used0RTT {
				tc.zeroRTTCount.Add(1)
			}
		case <-c.Context().Done():
		}
	}(newConn)
	tc.reconnects.Add(1)
	tc.conns[idx] = newConn
	return newConn, nil
}

func (tc *TunnelClient) invalidateConn(idx int, bad *quic.Conn) {
	tc.mu.Lock()
	if tc.conns[idx] == bad {
		tc.conns[idx] = nil
	}
	tc.mu.Unlock()
	_ = bad.CloseWithError(0, "reconnect")
}

// TunnelServer accepts RMESH/1 QUIC 0-RTT connections and dispatches streams over HyperTransport + SpeculativeTCPPool.
type TunnelServer struct {
	listenAddr string
	hyper      *HyperTransport
	tcpPool    *SpeculativeTCPPool
	listener   *quic.EarlyListener

	acceptedConns   atomic.Int64
	acceptedStreams atomic.Int64
}

// NewTunnelServer constructs an RMESH/1 QUIC 0-RTT egress node.
func NewTunnelServer(listenAddr string, hyper *HyperTransport, tcpPool *SpeculativeTCPPool) *TunnelServer {
	return &TunnelServer{
		listenAddr: listenAddr,
		hyper:      hyper,
		tcpPool:    tcpPool,
	}
}

// Start binds UDP listenAddr with QUIC 0-RTT enabled and serves until ctx is cancelled.
func (ts *TunnelServer) Start(ctx context.Context) error {
	tlsCfg, err := protocol.GenerateSelfSignedTLSConfig()
	if err != nil {
		return err
	}
	udpAddr, err := net.ResolveUDPAddr("udp", ts.listenAddr)
	if err != nil {
		return err
	}
	udpConn, err := net.ListenUDP("udp", udpAddr)
	if err != nil {
		return err
	}
	_ = udpConn.SetReadBuffer(4 << 20)
	_ = udpConn.SetWriteBuffer(4 << 20)

	quicCfg := &quic.Config{
		Allow0RTT:                      true,
		KeepAlivePeriod:                10 * time.Second,
		MaxIdleTimeout:                 120 * time.Second,
		MaxIncomingStreams:             4096,
		InitialStreamReceiveWindow:     4 << 20,
		MaxStreamReceiveWindow:         16 << 20,
		InitialConnectionReceiveWindow: 8 << 20,
		MaxConnectionReceiveWindow:     32 << 20,
	}

	ln, err := quic.ListenEarly(udpConn, tlsCfg, quicCfg)
	if err != nil {
		_ = udpConn.Close()
		return err
	}
	ts.listener = ln

	go func() {
		<-ctx.Done()
		_ = ln.Close()
		_ = udpConn.Close()
	}()

	for {
		conn, err := ln.Accept(ctx)
		if err != nil {
			if ctx.Err() != nil || errors.Is(err, net.ErrClosed) {
				return nil
			}
			continue
		}
		ts.acceptedConns.Add(1)
		go ts.handleConn(ctx, conn)
	}
}

// Stats returns live server-side accepted connection & stream counts.
func (ts *TunnelServer) Stats() (conns int64, streams int64) {
	return ts.acceptedConns.Load(), ts.acceptedStreams.Load()
}

func (ts *TunnelServer) handleConn(ctx context.Context, conn *quic.Conn) {
	for {
		stream, err := conn.AcceptStream(ctx)
		if err != nil {
			return
		}
		ts.acceptedStreams.Add(1)
		go ts.handleStream(ctx, stream)
	}
}

func (ts *TunnelServer) handleStream(ctx context.Context, stream *quic.Stream) {
	defer stream.Close()

	hdr, err := protocol.ReadHeader(stream)
	if err != nil {
		return
	}

	switch hdr.Cmd {
	case protocol.CmdPing:
		_ = protocol.WriteStatus(stream, protocol.StatusOK, "")

	case protocol.CmdConnect:
		target := net.JoinHostPort(hdr.Host, strconv.Itoa(int(hdr.Port)))
		dialCtx, cancel := context.WithTimeout(ctx, 5*time.Second)
		upstream, _, err := ts.tcpPool.Acquire(dialCtx, target)
		cancel()
		if err != nil {
			_ = protocol.WriteStatus(stream, protocol.StatusErr, err.Error())
			return
		}
		if err := protocol.WriteStatus(stream, protocol.StatusOK, ""); err != nil {
			_ = upstream.Close()
			return
		}
		protocol.CopyBidirectionalZeroAlloc(stream, upstream)

	case protocol.CmdHTTPFast:
		fastReq, err := protocol.ReadFastHTTPRequest(stream)
		if err != nil {
			_ = protocol.WriteStatus(stream, protocol.StatusErr, err.Error())
			return
		}
		reqCtx, cancel := context.WithTimeout(ctx, 15*time.Second)
		defer cancel()
		var bodyReader io.Reader
		if len(fastReq.Body) > 0 {
			bodyReader = bytes.NewReader(fastReq.Body)
		}
		outReq, err := http.NewRequestWithContext(reqCtx, fastReq.Method, fastReq.URL, bodyReader)
		if err != nil {
			_ = protocol.WriteStatus(stream, protocol.StatusErr, err.Error())
			return
		}
		outReq.Header = fastReq.Headers.Clone()

		resp, err := ts.hyper.RoundTrip(outReq)
		if err != nil {
			_ = protocol.WriteStatus(stream, protocol.StatusErr, err.Error())
			return
		}
		respBody, readErr := io.ReadAll(resp.Body)
		_ = resp.Body.Close()
		if readErr != nil {
			_ = protocol.WriteStatus(stream, protocol.StatusErr, readErr.Error())
			return
		}
		if err := protocol.WriteStatus(stream, protocol.StatusOK, ""); err != nil {
			return
		}
		_ = protocol.WriteFastHTTPResponse(stream, protocol.FastHTTPResponse{
			StatusCode:    resp.StatusCode,
			UpstreamProto: resp.Proto,
			Headers:       resp.Header,
			Body:          respBody,
		})
	}
}
