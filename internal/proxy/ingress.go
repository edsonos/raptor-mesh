package proxy

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net"
	"net/http"
	"net/url"
	"strconv"
	"strings"
	"sync/atomic"
	"time"

	"raptor-mesh/internal/config"
	rdns "raptor-mesh/internal/dns"
	"raptor-mesh/internal/prewarmer"
	"raptor-mesh/internal/protocol"
	"raptor-mesh/internal/transport"
)

var hopByHopHeaders = []string{
	"Connection",
	"Keep-Alive",
	"Proxy-Authenticate",
	"Proxy-Authorization",
	"Proxy-Connection",
	"Te",
	"Trailers",
	"Transfer-Encoding",
	"Upgrade",
	"X-Raptor-Target",
}

// IngressStats is returned by GET http://127.0.0.1:17908/raptor/stats.
type IngressStats struct {
	ListenAddr       string                        `json:"listen_addr"`
	Mode             string                        `json:"mode"`
	UptimeSec        float64                       `json:"uptime_sec"`
	TotalRequests    int64                         `json:"total_requests"`
	FastPathRequests int64                         `json:"fast_path_requests"`
	ConnectTunnels   int64                         `json:"connect_tunnels"`
	ActiveConns      int64                         `json:"active_conns"`
	PrewarmerCycles  int64                         `json:"prewarmer_cycles"`
	DNS              rdns.Stats                    `json:"dns"`
	Sockets          transport.SocketStats         `json:"sockets"`
	HyperTransport   transport.HyperStats          `json:"hyper_transport"`
	TCPPool          transport.TCPPoolStats        `json:"tcp_pool"`
	Routes           []transport.HostRouteSnapshot `json:"routes"`
	Tunnel           *transport.TunnelStats        `json:"tunnel,omitempty"`
}

// IngressServer is the local edge proxy listening on 127.0.0.1:17908.
// It supports:
//  1. Accelerated HTTP/HTTPS Fast-Path (multiplexed H3 QUIC 0-RTT + H2 or RMESH/1 QUIC tunnel)
//  2. Standard HTTPS CONNECT tunneling (with Speculative TCP Pre-Connect Pool + TFO or RMESH/1 tunnel)
//  3. Live telemetry endpoints (/raptor/stats and /raptor/health)
type IngressServer struct {
	cfg       *config.Config
	dialer    *transport.SmartDialer
	hyper     *transport.HyperTransport
	tcpPool   *transport.SpeculativeTCPPool
	prewarmer *prewarmer.Predictor
	tunnel    *transport.TunnelClient // optional: nil in direct-edge mode, non-nil in mesh-relay mode

	httpServer *http.Server
	startedAt  time.Time

	totalReqs    atomic.Int64
	fastPathReqs atomic.Int64
	connectReqs  atomic.Int64
	activeConns  atomic.Int64
}

// NewIngressServer wires up the Raptor-Mesh local listener.
func NewIngressServer(
	cfg *config.Config,
	dialer *transport.SmartDialer,
	hyper *transport.HyperTransport,
	tcpPool *transport.SpeculativeTCPPool,
	pw *prewarmer.Predictor,
	tunnel *transport.TunnelClient,
) *IngressServer {
	srv := &IngressServer{
		cfg:       cfg,
		dialer:    dialer,
		hyper:     hyper,
		tcpPool:   tcpPool,
		prewarmer: pw,
		tunnel:    tunnel,
		startedAt: time.Now(),
	}
	srv.httpServer = &http.Server{
		Addr:              cfg.ListenAddr,
		Handler:           srv,
		ReadHeaderTimeout: 10 * time.Second,
		IdleTimeout:       120 * time.Second,
		ConnState: func(c net.Conn, s http.ConnState) {
			switch s {
			case http.StateNew:
				transport.TuneAcceptedConn(c)
				srv.activeConns.Add(1)
			case http.StateClosed, http.StateHijacked:
				srv.activeConns.Add(-1)
			}
		},
	}
	return srv
}

// Start binds 127.0.0.1:17908 with Winsock2 Loopback Fast Path + TCP_FASTOPEN and serves until ctx is cancelled.
func (s *IngressServer) Start(ctx context.Context) error {
	lc := net.ListenConfig{
		Control:   transport.ControlListener,
		KeepAlive: 30 * time.Second,
	}
	ln, err := lc.Listen(ctx, "tcp", s.cfg.ListenAddr)
	if err != nil {
		return err
	}

	go func() {
		<-ctx.Done()
		shutCtx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
		defer cancel()
		_ = s.httpServer.Shutdown(shutCtx)
	}()

	err = s.httpServer.Serve(ln)
	if errors.Is(err, http.ErrServerClosed) {
		return nil
	}
	return err
}

// ServeHTTP dispatches CONNECT tunnels, accelerated HTTP/HTTPS fast-path requests, and /raptor/* control endpoints.
func (s *IngressServer) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	s.totalReqs.Add(1)

	if r.Method == http.MethodConnect {
		s.handleConnect(w, r)
		return
	}

	// Control / telemetry endpoints when accessed directly on 127.0.0.1:17908 without a proxy target.
	if r.URL.Scheme == "" && r.Header.Get("X-Raptor-Target") == "" {
		switch r.URL.Path {
		case "/raptor/health":
			w.Header().Set("Content-Type", "application/json")
			w.WriteHeader(http.StatusOK)
			_, _ = w.Write([]byte(`{"status":"ok","protocol":"RMESH/1"}`))
			return
		case "/raptor/proxy.pac":
			s.handleProxyPAC(w)
			return
		case "/raptor/stats":
			s.handleStats(w)
			return
		case "/raptor/fetch":
			if target := r.URL.Query().Get("url"); target != "" {
				s.handleFastPath(w, r, target)
				return
			}
		}
	}

	// Determine target URL for Accelerated Fast-Path:
	// 1. Explicit X-Raptor-Target header (allows local HTTP/1.1 clients to hit HTTPS origins via H3/H2 mux pool)
	// 2. Standard proxy absolute URL (http://... or https://...)
	targetURL := r.Header.Get("X-Raptor-Target")
	if targetURL == "" {
		if r.URL.IsAbs() {
			targetURL = r.URL.String()
		} else if r.Host != "" && !strings.HasPrefix(r.Host, "127.0.0.1:") && !strings.HasPrefix(r.Host, "localhost:") {
			targetURL = "http://" + r.Host + r.URL.RequestURI()
		}
	}
	if targetURL == "" {
		http.Error(w, "Raptor-Mesh proxy: provide an absolute URL, CONNECT host:port, X-Raptor-Target header, or /raptor/stats", http.StatusBadRequest)
		return
	}

	s.handleFastPath(w, r, targetURL)
}

func (s *IngressServer) handleFastPath(w http.ResponseWriter, r *http.Request, rawTarget string) {
	s.fastPathReqs.Add(1)
	start := time.Now()

	parsed, err := url.Parse(rawTarget)
	if err != nil || parsed.Host == "" {
		http.Error(w, "Raptor-Mesh: invalid target URL: "+rawTarget, http.StatusBadRequest)
		return
	}
	if parsed.Scheme == "" {
		parsed.Scheme = "https"
	}

	if s.prewarmer != nil {
		s.prewarmer.RecordAccess(parsed.Hostname(), true)
	}

	outReq := r.Clone(r.Context())
	outReq.RequestURI = ""
	outReq.URL = parsed
	outReq.Host = parsed.Host
	outReq.Header = r.Header.Clone()
	for _, h := range hopByHopHeaders {
		outReq.Header.Del(h)
	}
	if outReq.Header.Get("User-Agent") == "" {
		outReq.Header.Set("User-Agent", "Raptor-Mesh/1.0")
	}

	var resp *http.Response
	if s.tunnel != nil {
		resp, err = s.tunnel.RoundTripFast(r.Context(), outReq)
	} else {
		resp, err = s.hyper.RoundTrip(outReq)
	}
	if err != nil {
		http.Error(w, "Raptor-Mesh upstream error: "+err.Error(), http.StatusBadGateway)
		return
	}
	defer resp.Body.Close()

	dstHdr := w.Header()
	for k, vals := range resp.Header {
		for _, v := range vals {
			dstHdr.Add(k, v)
		}
	}
	for _, h := range hopByHopHeaders {
		dstHdr.Del(h)
	}
	protoTag := resp.Proto
	if up := resp.Header.Get("X-Raptor-Upstream-Proto"); up != "" {
		protoTag = "RMESH/1->" + up
	}
	dstHdr.Set("X-Raptor-Proto", protoTag)
	dstHdr.Set("X-Raptor-Latency-Ms", fmt.Sprintf("%.2f", float64(time.Since(start).Microseconds())/1000.0))

	w.WriteHeader(resp.StatusCode)
	bp := protocol.GetBuffer()
	defer protocol.PutBuffer(bp)

	// Stream incrementally with Flush() for SSE / chunked AI responses (OpenAI / Claude streaming).
	ct := strings.ToLower(resp.Header.Get("Content-Type"))
	if flusher, ok := w.(http.Flusher); ok && (strings.Contains(ct, "text/event-stream") || resp.ContentLength == -1) {
		buf := *bp
		for {
			n, rerr := resp.Body.Read(buf)
			if n > 0 {
				if _, werr := w.Write(buf[:n]); werr != nil {
					return
				}
				flusher.Flush()
			}
			if rerr != nil {
				return
			}
		}
	}
	_, _ = io.CopyBuffer(w, resp.Body, *bp)
}

func (s *IngressServer) handleConnect(w http.ResponseWriter, r *http.Request) {
	s.connectReqs.Add(1)
	targetAddr := r.Host
	if !strings.Contains(targetAddr, ":") {
		targetAddr = net.JoinHostPort(targetAddr, "443")
	}
	host, portStr, err := net.SplitHostPort(targetAddr)
	if err != nil {
		http.Error(w, "invalid CONNECT target", http.StatusBadRequest)
		return
	}
	portNum, err := strconv.Atoi(portStr)
	if err != nil || portNum <= 0 || portNum > 65535 {
		http.Error(w, "invalid CONNECT port", http.StatusBadRequest)
		return
	}

	if s.prewarmer != nil {
		s.prewarmer.RecordAccess(host, false)
	}

	dialCtx, cancel := context.WithTimeout(r.Context(), 8*time.Second)
	var upstream io.ReadWriteCloser
	if s.tunnel != nil {
		upstream, err = s.tunnel.OpenConnectStream(dialCtx, host, uint16(portNum))
	} else {
		upstream, _, err = s.tcpPool.Acquire(dialCtx, targetAddr)
	}
	cancel()
	if err != nil {
		http.Error(w, "Raptor-Mesh CONNECT failed: "+err.Error(), http.StatusBadGateway)
		return
	}

	hijacker, ok := w.(http.Hijacker)
	if !ok {
		_ = upstream.Close()
		http.Error(w, "hijacking not supported", http.StatusInternalServerError)
		return
	}
	clientConn, clientBuf, err := hijacker.Hijack()
	if err != nil {
		_ = upstream.Close()
		return
	}
	// Explicitly clear any residual HTTP server deadlines so long-lived WebSockets/streams never drop.
	_ = clientConn.SetDeadline(time.Time{})
	transport.TuneAcceptedConn(clientConn)

	if _, err := clientConn.Write([]byte("HTTP/1.1 200 Connection Established\r\n\r\n")); err != nil {
		_ = clientConn.Close()
		_ = upstream.Close()
		return
	}

	// Flush any already-buffered bytes (e.g. TLS ClientHello pipelined right behind CONNECT).
	if n := clientBuf.Reader.Buffered(); n > 0 {
		if peeked, perr := clientBuf.Reader.Peek(n); perr == nil && len(peeked) > 0 {
			_, _ = upstream.Write(peeked)
			_, _ = clientBuf.Reader.Discard(n)
		}
	}

	protocol.CopyBidirectionalZeroAlloc(clientConn, upstream)
}

func (s *IngressServer) handleProxyPAC(w http.ResponseWriter) {
	pac := fmt.Sprintf(`function FindProxyForURL(url, host) {
  if (
    isPlainHostName(host) ||
    host === "localhost" ||
    shExpMatch(host, "*.localhost") ||
    host === "127.0.0.1" ||
    host === "::1" ||
    host === "[::1]" ||
    shExpMatch(host, "fe80:*") ||
    shExpMatch(host, "[fe80:*") ||
    shExpMatch(host, "*.local") ||
    shExpMatch(host, "*.lan") ||
    shExpMatch(host, "*.home") ||
    shExpMatch(host, "*.internal") ||
    shExpMatch(host, "*.corp") ||
    shExpMatch(host, "*.arpa") ||
    shExpMatch(host, "10.*") ||
    shExpMatch(host, "169.254.*") ||
    shExpMatch(host, "192.168.*") ||
    shExpMatch(host, "172.16.*") ||
    shExpMatch(host, "172.17.*") ||
    shExpMatch(host, "172.18.*") ||
    shExpMatch(host, "172.19.*") ||
    shExpMatch(host, "172.2?.*") ||
    shExpMatch(host, "172.30.*") ||
    shExpMatch(host, "172.31.*") ||
    shExpMatch(host, "*.riotgames.com") ||
    host === "riotgames.com" ||
    shExpMatch(host, "*.leagueoflegends.com") ||
    host === "leagueoflegends.com" ||
    shExpMatch(host, "*.pvp.net") ||
    shExpMatch(host, "*.riotcdn.net") ||
    shExpMatch(host, "*.rgpub.io")
  ) {
    return "DIRECT";
  }
  return "PROXY %s; DIRECT";
}
`, s.cfg.ListenAddr)
	w.Header().Set("Content-Type", "application/x-ns-proxy-autoconfig")
	w.Header().Set("Cache-Control", "no-store, max-age=0")
	w.WriteHeader(http.StatusOK)
	_, _ = w.Write([]byte(pac))
}

func (s *IngressServer) handleStats(w http.ResponseWriter) {
	mode := "direct-edge-h3h2"
	var tStats *transport.TunnelStats
	if s.tunnel != nil {
		mode = "mesh-relay-rmesh1-quic-0rtt"
		ts := s.tunnel.GetStats()
		tStats = &ts
	}
	var cycles int64
	if s.prewarmer != nil {
		cycles = s.prewarmer.Cycles()
	}
	stats := IngressStats{
		ListenAddr:       s.cfg.ListenAddr,
		Mode:             mode,
		UptimeSec:        time.Since(s.startedAt).Seconds(),
		TotalRequests:    s.totalReqs.Load(),
		FastPathRequests: s.fastPathReqs.Load(),
		ConnectTunnels:   s.connectReqs.Load(),
		ActiveConns:      s.activeConns.Load(),
		PrewarmerCycles:  cycles,
		DNS:              s.dialer.Resolver().GetStats(),
		Sockets:          transport.GetSocketStats(),
		HyperTransport:   s.hyper.GetStats(),
		TCPPool:          s.tcpPool.GetStats(),
		Routes:           s.dialer.SnapshotRoutes(),
		Tunnel:           tStats,
	}
	w.Header().Set("Content-Type", "application/json")
	enc := json.NewEncoder(w)
	enc.SetIndent("", "  ")
	_ = enc.Encode(stats)
}
