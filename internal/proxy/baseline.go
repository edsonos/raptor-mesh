package proxy

import (
	"context"
	"errors"
	"io"
	"net"
	"net/http"
	"strings"
	"sync"
	"time"
)

// BaselineProxy implements an honest, standard traditional HTTP/HTTPS CONNECT proxy
// (matching Clash DIRECT behavior: system DNS lookup + fresh TCP dial + blind TCP forwarding,
// with zero artificial latency or sabotage). Used automatically on 127.0.0.1:7897 during benchmarks
// only when no existing Clash/proxy service is already listening on port 7897.
type BaselineProxy struct {
	addr      string
	server    *http.Server
	dialer    *net.Dialer
	transport *http.Transport
}

// NewBaselineProxy constructs a clean reference HTTP/CONNECT proxy bound to addr (e.g. 127.0.0.1:7897).
func NewBaselineProxy(addr string) *BaselineProxy {
	bp := &BaselineProxy{
		addr: addr,
		dialer: &net.Dialer{
			Timeout:   8 * time.Second,
			KeepAlive: 30 * time.Second,
		},
		transport: &http.Transport{
			DisableKeepAlives:   true,
			ForceAttemptHTTP2:   false,
			TLSHandshakeTimeout: 8 * time.Second,
		},
	}
	bp.server = &http.Server{
		Addr:              addr,
		Handler:           bp,
		ReadHeaderTimeout: 10 * time.Second,
	}
	return bp
}

// Start binds addr and serves standard proxy traffic until ctx is cancelled.
func (bp *BaselineProxy) Start(ctx context.Context) error {
	ln, err := net.Listen("tcp", bp.addr)
	if err != nil {
		return err
	}
	go func() {
		<-ctx.Done()
		shutCtx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
		defer cancel()
		_ = bp.server.Shutdown(shutCtx)
	}()
	err = bp.server.Serve(ln)
	if errors.Is(err, http.ErrServerClosed) {
		return nil
	}
	return err
}

func (bp *BaselineProxy) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	if r.Method == http.MethodConnect {
		bp.handleConnect(w, r)
		return
	}
	outReq := r.Clone(r.Context())
	outReq.RequestURI = ""
	if outReq.URL.Scheme == "" {
		outReq.URL.Scheme = "http"
	}
	if outReq.URL.Host == "" {
		outReq.URL.Host = r.Host
	}
	outReq.Header.Del("Proxy-Connection")

	resp, err := bp.transport.RoundTrip(outReq)
	if err != nil {
		http.Error(w, err.Error(), http.StatusBadGateway)
		return
	}
	defer resp.Body.Close()

	for k, vals := range resp.Header {
		for _, v := range vals {
			w.Header().Add(k, v)
		}
	}
	w.WriteHeader(resp.StatusCode)
	_, _ = io.Copy(w, resp.Body)
}

func (bp *BaselineProxy) handleConnect(w http.ResponseWriter, r *http.Request) {
	target := r.Host
	if !strings.Contains(target, ":") {
		target = net.JoinHostPort(target, "443")
	}

	upstream, err := bp.dialer.DialContext(r.Context(), "tcp", target)
	if err != nil {
		http.Error(w, err.Error(), http.StatusBadGateway)
		return
	}

	hijacker, ok := w.(http.Hijacker)
	if !ok {
		_ = upstream.Close()
		http.Error(w, "hijack unsupported", http.StatusInternalServerError)
		return
	}
	clientConn, clientBuf, err := hijacker.Hijack()
	if err != nil {
		_ = upstream.Close()
		return
	}

	if _, err := clientConn.Write([]byte("HTTP/1.1 200 Connection Established\r\n\r\n")); err != nil {
		_ = clientConn.Close()
		_ = upstream.Close()
		return
	}

	if n := clientBuf.Reader.Buffered(); n > 0 {
		if peeked, perr := clientBuf.Reader.Peek(n); perr == nil && len(peeked) > 0 {
			_, _ = upstream.Write(peeked)
			_, _ = clientBuf.Reader.Discard(n)
		}
	}

	var wg sync.WaitGroup
	wg.Add(2)
	go func() {
		defer wg.Done()
		_, _ = io.Copy(upstream, clientConn)
		if tc, ok := upstream.(*net.TCPConn); ok {
			_ = tc.CloseWrite()
		}
	}()
	go func() {
		defer wg.Done()
		_, _ = io.Copy(clientConn, upstream)
		if tc, ok := clientConn.(*net.TCPConn); ok {
			_ = tc.CloseWrite()
		}
	}()
	wg.Wait()
	_ = clientConn.Close()
	_ = upstream.Close()
}
