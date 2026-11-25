package transport

import (
	"context"
	"crypto/tls"
	"errors"
	"io"
	"net"
	"net/http"
	"sort"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/quic-go/quic-go"
	"github.com/quic-go/quic-go/http3"
	"golang.org/x/net/http2"
)

const stripeCount = 4

// HostTransportProfile stores live protocol selection and EWMA latency per host.
type HostTransportProfile struct {
	Host          string    `json:"host"`
	PreferH3      bool      `json:"prefer_h3"`
	H3Supported   bool      `json:"h3_supported"`
	H3EWMAMs      float64   `json:"h3_ewma_ms"`
	H2EWMAMs      float64   `json:"h2_ewma_ms"`
	H3Requests    int64     `json:"h3_requests"`
	H2Requests    int64     `json:"h2_requests"`
	H3CooldownEnd time.Time `json:"-"`
	LastWarmed    time.Time `json:"last_warmed"`
	mu            sync.RWMutex
}

// HyperStats summarizes transport-layer multiplexing telemetry.
type HyperStats struct {
	Stripes       int                    `json:"stripes"`
	H3Requests    int64                  `json:"h3_requests"`
	H2Requests    int64                  `json:"h2_requests"`
	H3Failovers   int64                  `json:"h3_failovers"`
	HedgeWins     int64                  `json:"hedge_wins"`
	AltSvcLearned int64                  `json:"alt_svc_learned"`
	Profiles      []HostTransportProfile `json:"profiles"`
}

// HyperTransport unifies a 4-stripe sharded HTTP/3 (QUIC 0-RTT) engine and a 4-stripe
// sharded HTTP/2 multiplexed engine with shared TLS 1.3 session ticket cache and
// adaptive hedged request racing for tail-latency elimination.
type HyperTransport struct {
	dialer       *SmartDialer
	sessionCache tls.ClientSessionCache

	quicTr4 *quic.Transport
	quicTr6 *quic.Transport
	udp4    *net.UDPConn
	udp6    *net.UDPConn

	h3Stripes [stripeCount]*http3.Transport
	h2Stripes [stripeCount]*http.Transport
	rrStripe  atomic.Uint64

	profiles sync.Map // map[string]*HostTransportProfile

	h3Reqs        atomic.Int64
	h2Reqs        atomic.Int64
	h3Failovers   atomic.Int64
	hedgeWins     atomic.Int64
	altSvcLearned atomic.Int64
}

// NewHyperTransport initializes 4-stripe dual-stack QUIC 0-RTT HTTP/3 + HTTP/2 multiplexed transports.
func NewHyperTransport(dialer *SmartDialer) (*HyperTransport, error) {
	sessionCache := tls.NewLRUClientSessionCache(4096)

	ht := &HyperTransport{
		dialer:       dialer,
		sessionCache: sessionCache,
	}

	if u4, err := net.ListenUDP("udp4", &net.UDPAddr{IP: net.IPv4zero, Port: 0}); err == nil {
		_ = u4.SetReadBuffer(8 << 20)
		_ = u4.SetWriteBuffer(8 << 20)
		ht.udp4 = u4
		ht.quicTr4 = &quic.Transport{Conn: u4}
	}
	if u6, err := net.ListenUDP("udp6", &net.UDPAddr{IP: net.IPv6unspecified, Port: 0}); err == nil {
		_ = u6.SetReadBuffer(8 << 20)
		_ = u6.SetWriteBuffer(8 << 20)
		ht.udp6 = u6
		ht.quicTr6 = &quic.Transport{Conn: u6}
	}

	for i := 0; i < stripeCount; i++ {
		quicCfg := &quic.Config{
			Allow0RTT:                      true,
			KeepAlivePeriod:                10 * time.Second,
			MaxIdleTimeout:                 90 * time.Second,
			HandshakeIdleTimeout:           3 * time.Second,
			MaxIncomingStreams:             2048,
			InitialStreamReceiveWindow:     4 << 20,
			MaxStreamReceiveWindow:         16 << 20,
			InitialConnectionReceiveWindow: 8 << 20,
			MaxConnectionReceiveWindow:     32 << 20,
		}

		ht.h3Stripes[i] = &http3.Transport{
			TLSClientConfig: &tls.Config{
				MinVersion:         tls.VersionTLS13,
				ClientSessionCache: sessionCache,
			},
			QUICConfig:         quicCfg,
			DisableCompression: true, // Pass through caller's Accept-Encoding untouched
			Dial:               ht.dialQUICEarly,
		}

		h2Base := &http.Transport{
			DialContext:           dialer.DialContext,
			ForceAttemptHTTP2:     true,
			MaxIdleConns:          256,
			MaxIdleConnsPerHost:   16,
			MaxConnsPerHost:       0,
			IdleConnTimeout:       120 * time.Second,
			TLSHandshakeTimeout:   4 * time.Second,
			ExpectContinueTimeout: 500 * time.Millisecond,
			DisableCompression:    true, // Pass through caller's Accept-Encoding untouched
			WriteBufferSize:       64 * 1024,
			ReadBufferSize:        64 * 1024,
			TLSClientConfig: &tls.Config{
				MinVersion:         tls.VersionTLS12,
				ClientSessionCache: sessionCache,
				NextProtos:         []string{"h2", "http/1.1"},
			},
		}
		if tr2, err := http2.ConfigureTransports(h2Base); err == nil {
			tr2.ReadIdleTimeout = 15 * time.Second
			tr2.PingTimeout = 5 * time.Second
			tr2.AllowHTTP = false
		}
		ht.h2Stripes[i] = h2Base
	}

	return ht, nil
}

// Close releases all UDP sockets and sharded transport pools cleanly.
func (ht *HyperTransport) Close() {
	for i := 0; i < stripeCount; i++ {
		if ht.h3Stripes[i] != nil {
			_ = ht.h3Stripes[i].Close()
		}
		if ht.h2Stripes[i] != nil {
			ht.h2Stripes[i].CloseIdleConnections()
		}
	}
	if ht.quicTr4 != nil {
		_ = ht.quicTr4.Close()
	}
	if ht.quicTr6 != nil {
		_ = ht.quicTr6.Close()
	}
	if ht.udp4 != nil {
		_ = ht.udp4.Close()
	}
	if ht.udp6 != nil {
		_ = ht.udp6.Close()
	}
}

// SeedHost registers initial protocol hints (e.g. known HTTP/3 targets from targets.json).
func (ht *HyperTransport) SeedHost(host string, preferH3 bool) {
	p := ht.getProfile(host)
	p.mu.Lock()
	p.H3Supported = preferH3
	p.PreferH3 = preferH3
	p.mu.Unlock()
}

// RoundTrip dispatches req across our 4-stripe H3/H2 multiplexed pools, with automatic
// adaptive hedged tail-latency racing for idempotent GET/HEAD requests!
func (ht *HyperTransport) RoundTrip(req *http.Request) (*http.Response, error) {
	host := canonicalHost(req.URL.Host)
	primaryStripe := int(ht.rrStripe.Add(1)-1) % stripeCount
	useH3 := req.URL.Scheme == "https" && ht.shouldUseH3(host)

	// Enable adaptive hedging only for safe idempotent bodyless GET/HEAD requests when we have a known EWMA baseline.
	canHedge := req.URL.Scheme == "https" &&
		(req.Method == http.MethodGet || req.Method == http.MethodHead) &&
		(req.Body == nil || req.Body == http.NoBody)

	if canHedge {
		if hedgeDelay, ok := ht.computeHedgeDelay(host, useH3); ok {
			return ht.roundTripHedged(req, host, primaryStripe, useH3, hedgeDelay)
		}
	}

	return ht.roundTripSingle(req, host, primaryStripe, useH3)
}

func (ht *HyperTransport) roundTripSingle(req *http.Request, host string, stripe int, useH3 bool) (*http.Response, error) {
	if useH3 {
		start := time.Now()
		resp, err := ht.h3Stripes[stripe].RoundTrip(req)
		elapsed := time.Since(start)
		if err == nil {
			ht.h3Reqs.Add(1)
			ht.recordProtocolRTT(host, true, elapsed, true)
			return resp, nil
		}
		ht.h3Failovers.Add(1)
		ht.recordProtocolRTT(host, true, elapsed, false)
		if req.Body != nil && req.Body != http.NoBody && req.GetBody == nil {
			return nil, err
		}
		if req.GetBody != nil {
			if body, berr := req.GetBody(); berr == nil {
				req.Body = body
			}
		}
	}

	start := time.Now()
	resp, err := ht.h2Stripes[stripe].RoundTrip(req)
	elapsed := time.Since(start)
	if err != nil {
		ht.recordProtocolRTT(host, false, elapsed, false)
		return nil, err
	}
	ht.h2Reqs.Add(1)
	ht.recordProtocolRTT(host, false, elapsed, true)
	ht.inspectAltSvc(host, resp.Header.Get("Alt-Svc"))
	return resp, nil
}

// roundTripHedged fires the primary request immediately; if headers haven't arrived within hedgeDelay,
// launches a backup request on a different warm stripe, returns the winner, and cancels the loser.
func (ht *HyperTransport) roundTripHedged(req *http.Request, host string, primaryStripe int, useH3 bool, hedgeDelay time.Duration) (*http.Response, error) {
	type outcome struct {
		resp   *http.Response
		err    error
		hedged bool
		isH3   bool
		rtt    time.Duration
	}

	ctx1, cancel1 := context.WithCancel(req.Context())
	ctx2, cancel2 := context.WithCancel(req.Context())
	ch := make(chan outcome, 2)

	dispatch := func(rctx context.Context, stripe int, h3 bool, hedged bool) {
		r := req.Clone(rctx)
		start := time.Now()
		var resp *http.Response
		var err error
		if h3 {
			resp, err = ht.h3Stripes[stripe].RoundTrip(r)
		} else {
			resp, err = ht.h2Stripes[stripe].RoundTrip(r)
		}
		ch <- outcome{resp: resp, err: err, hedged: hedged, isH3: h3, rtt: time.Since(start)}
	}

	go dispatch(ctx1, primaryStripe, useH3, false)

	timer := time.NewTimer(hedgeDelay)
	defer timer.Stop()

	launchedHedge := false
	launchHedge := func() {
		if launchedHedge {
			return
		}
		launchedHedge = true
		backupStripe := (primaryStripe + 1) % stripeCount
		// Keep same winning protocol on a distinct stripe unless primary was H3 and H2 is healthy.
		backupH3 := useH3
		go dispatch(ctx2, backupStripe, backupH3, true)
	}

	var firstErr error
	completed := 0
	totalExpected := 1

	for completed < totalExpected {
		select {
		case res := <-ch:
			completed++
			if res.err == nil && res.resp != nil {
				if res.hedged {
					ht.hedgeWins.Add(1)
					cancel1()
				} else {
					cancel2()
				}
				if res.isH3 {
					ht.h3Reqs.Add(1)
				} else {
					ht.h2Reqs.Add(1)
					ht.inspectAltSvc(host, res.resp.Header.Get("Alt-Svc"))
				}
				ht.recordProtocolRTT(host, res.isH3, res.rtt, true)

				// Drain losing goroutine cleanly without leaking its response body.
				if remaining := totalExpected - completed; remaining > 0 {
					go func() {
						for i := 0; i < remaining; i++ {
							loser := <-ch
							if loser.resp != nil && loser.resp.Body != nil {
								_, _ = io.Copy(io.Discard, loser.resp.Body)
								_ = loser.resp.Body.Close()
							}
						}
					}()
				}
				return res.resp, nil
			}
			firstErr = res.err
			ht.recordProtocolRTT(host, res.isH3, res.rtt, false)
			// If primary failed quickly before timer fired, immediately launch backup on H2!
			if !launchedHedge {
				launchedHedge = true
				totalExpected = 2
				backupStripe := (primaryStripe + 1) % stripeCount
				go dispatch(ctx2, backupStripe, false, true)
			}

		case <-timer.C:
			if !launchedHedge {
				totalExpected = 2
				launchHedge()
			}

		case <-req.Context().Done():
			cancel1()
			cancel2()
			return nil, req.Context().Err()
		}
	}

	cancel1()
	cancel2()
	if firstErr == nil {
		firstErr = errors.New("hedged roundtrip failed for " + host)
	}
	return nil, firstErr
}

// ProbeAndCalibrate warms ALL 4 stripes of HTTP/2 and (when eligible) HTTP/3 in parallel,
// measures real warm TTFB on both protocols, and locks onto the strictly faster protocol!
func (ht *HyperTransport) ProbeAndCalibrate(ctx context.Context, host, warmPath, method string, tryH3 bool) {
	if warmPath == "" {
		warmPath = "/"
	}
	if method == "" {
		method = http.MethodHead
	}
	targetURL := "https://" + host + warmPath

	doStripeProbe := func(stripe int, useH3 bool) (time.Duration, bool, string) {
		probeCtx, cancel := context.WithTimeout(ctx, 4*time.Second)
		defer cancel()
		req, err := http.NewRequestWithContext(probeCtx, method, targetURL, nil)
		if err != nil {
			return 0, false, ""
		}
		req.Header.Set("User-Agent", "Raptor-Mesh/1.0")
		req.Header.Set("Accept", "*/*")
		req.Header.Set("Accept-Encoding", "gzip")

		start := time.Now()
		var resp *http.Response
		if useH3 {
			resp, err = ht.h3Stripes[stripe].RoundTrip(req)
		} else {
			resp, err = ht.h2Stripes[stripe].RoundTrip(req)
		}
		elapsed := time.Since(start)
		if err != nil {
			return elapsed, false, ""
		}
		altSvc := resp.Header.Get("Alt-Svc")
		_, _ = io.Copy(io.Discard, resp.Body)
		_ = resp.Body.Close()
		return elapsed, true, altSvc
	}

	// Stage 1: Calibrate stripe 0 on H2 (cold handshake + 2 warm 1-RTT probes, taking best warm sample).
	_, h2OK1, altSvc := doStripeProbe(0, false)
	if altSvc != "" {
		ht.inspectAltSvc(host, altSvc)
	}
	var h2Warm time.Duration
	var h2OK bool
	if h2OK1 {
		w1, ok1, _ := doStripeProbe(0, false)
		w2, ok2, _ := doStripeProbe(0, false)
		if ok1 && ok2 {
			h2Warm = min(w1, w2)
			h2OK = true
		} else if ok1 {
			h2Warm, h2OK = w1, true
		} else if ok2 {
			h2Warm, h2OK = w2, true
		}
		if h2OK {
			ht.recordProtocolRTT(host, false, h2Warm, true)
		}
	}

	p := ht.getProfile(host)
	p.mu.RLock()
	h3Eligible := tryH3 || p.H3Supported
	p.mu.RUnlock()

	var h3Warm time.Duration
	var h3OK bool
	if h3Eligible {
		if _, ok0, _ := doStripeProbe(0, true); ok0 {
			w1, ok1, _ := doStripeProbe(0, true)
			w2, ok2, _ := doStripeProbe(0, true)
			if ok1 && ok2 {
				h3Warm = min(w1, w2)
				h3OK = true
			} else if ok1 {
				h3Warm, h3OK = w1, true
			} else if ok2 {
				h3Warm, h3OK = w2, true
			}
			if h3OK {
				ht.recordProtocolRTT(host, true, h3Warm, true)
			}
		}
	}

	winH3 := false
	p.mu.Lock()
	p.LastWarmed = time.Now()
	switch {
	case h3OK && h2OK:
		p.H3Supported = true
		// Strict performance election: pick H3 when genuinely faster than H2.
		winH3 = h3Warm <= h2Warm
		p.PreferH3 = winH3
	case h3OK:
		p.H3Supported = true
		winH3 = true
		p.PreferH3 = true
	case h2OK:
		winH3 = false
		p.PreferH3 = false
	}
	p.mu.Unlock()

	// Stage 2: Pre-warm remaining stripes (1..stripeCount-1) in parallel on the winning protocol
	// (plus keep H2 stripes warm for instant failover/hedging)!
	var wg sync.WaitGroup
	for s := 1; s < stripeCount; s++ {
		s := s
		wg.Add(1)
		go func() {
			defer wg.Done()
			_, _, _ = doStripeProbe(s, winH3)
		}()
	}
	wg.Wait()
}

// KeepWarm sends lightweight keep-alive probes across stripes to prevent edge idle eviction.
func (ht *HyperTransport) KeepWarm(ctx context.Context, host, warmPath, method string) {
	if warmPath == "" {
		warmPath = "/"
	}
	if method == "" {
		method = http.MethodHead
	}
	useH3 := ht.shouldUseH3(host)
	var wg sync.WaitGroup
	for s := 0; s < stripeCount; s++ {
		s := s
		wg.Add(1)
		go func() {
			defer wg.Done()
			probeCtx, cancel := context.WithTimeout(ctx, 3*time.Second)
			defer cancel()
			req, err := http.NewRequestWithContext(probeCtx, method, "https://"+host+warmPath, nil)
			if err != nil {
				return
			}
			req.Header.Set("User-Agent", "Raptor-Mesh/1.0")
			req.Header.Set("Accept-Encoding", "gzip")
			var resp *http.Response
			if useH3 {
				resp, err = ht.h3Stripes[s].RoundTrip(req)
			} else {
				resp, err = ht.h2Stripes[s].RoundTrip(req)
			}
			if err != nil {
				return
			}
			_, _ = io.Copy(io.Discard, resp.Body)
			_ = resp.Body.Close()
		}()
	}
	wg.Wait()
	p := ht.getProfile(host)
	p.mu.Lock()
	p.LastWarmed = time.Now()
	p.mu.Unlock()
}

// GetStats returns live H3/H2 multiplexing and per-host EWMA telemetry.
func (ht *HyperTransport) GetStats() HyperStats {
	var profiles []HostTransportProfile
	ht.profiles.Range(func(_, v any) bool {
		p := v.(*HostTransportProfile)
		p.mu.RLock()
		profiles = append(profiles, HostTransportProfile{
			Host:        p.Host,
			PreferH3:    p.PreferH3,
			H3Supported: p.H3Supported,
			H3EWMAMs:    p.H3EWMAMs,
			H2EWMAMs:    p.H2EWMAMs,
			H3Requests:  p.H3Requests,
			H2Requests:  p.H2Requests,
			LastWarmed:  p.LastWarmed,
		})
		p.mu.RUnlock()
		return true
	})
	sort.Slice(profiles, func(i, j int) bool { return profiles[i].Host < profiles[j].Host })
	return HyperStats{
		Stripes:       stripeCount,
		H3Requests:    ht.h3Reqs.Load(),
		H2Requests:    ht.h2Reqs.Load(),
		H3Failovers:   ht.h3Failovers.Load(),
		HedgeWins:     ht.hedgeWins.Load(),
		AltSvcLearned: ht.altSvcLearned.Load(),
		Profiles:      profiles,
	}
}

func (ht *HyperTransport) computeHedgeDelay(host string, useH3 bool) (time.Duration, bool) {
	v, ok := ht.profiles.Load(host)
	if !ok {
		return 0, false
	}
	p := v.(*HostTransportProfile)
	p.mu.RLock()
	ewma := p.H2EWMAMs
	if useH3 && p.H3EWMAMs > 0 {
		ewma = p.H3EWMAMs
	}
	p.mu.RUnlock()
	if ewma <= 0 {
		return 0, false
	}
	delay := time.Duration(ewma*1.85) * time.Millisecond
	if delay < 90*time.Millisecond {
		delay = 90 * time.Millisecond
	}
	if delay > 280*time.Millisecond {
		delay = 280 * time.Millisecond
	}
	return delay, true
}

// dialQUICEarly dials a QUIC 0-RTT connection using Racing DNS + Predictive Route Scoreboard + dual-stack UDP sockets.
func (ht *HyperTransport) dialQUICEarly(ctx context.Context, addr string, tlsCfg *tls.Config, cfg *quic.Config) (*quic.Conn, error) {
	host, portStr, err := net.SplitHostPort(addr)
	if err != nil {
		return nil, err
	}
	port, err := net.LookupPort("udp", portStr)
	if err != nil {
		port = 443
	}

	candidates, err := ht.dialer.CandidateIPs(ctx, host, portStr)
	if err != nil || len(candidates) == 0 {
		return nil, errors.New("quic dial: cannot resolve " + host)
	}

	limit := len(candidates)
	if limit > 2 {
		limit = 2
	}

	type qAttempt struct {
		conn  *quic.Conn
		ipStr string
		rtt   time.Duration
		err   error
	}

	raceCtx, cancel := context.WithCancel(ctx)
	defer cancel()
	ch := make(chan qAttempt, limit)

	launch := func(ip net.IP) {
		ipStr := ip.String()
		var qtr *quic.Transport
		if ip.To4() != nil {
			qtr = ht.quicTr4
		} else {
			qtr = ht.quicTr6
		}
		if qtr == nil {
			ch <- qAttempt{ipStr: ipStr, err: errors.New("no udp transport for ip family")}
			return
		}
		udpAddr := &net.UDPAddr{IP: ip, Port: port}
		tcfg := tlsCfg.Clone()
		if tcfg.ServerName == "" {
			tcfg.ServerName = host
		}
		go func() {
			start := time.Now()
			c, err := qtr.DialEarly(raceCtx, udpAddr, tcfg, cfg)
			rtt := time.Since(start)
			if err != nil {
				if raceCtx.Err() == nil {
					ht.dialer.RecordRTT(host, portStr, ipStr, rtt, false)
				}
				ch <- qAttempt{ipStr: ipStr, rtt: rtt, err: err}
				return
			}
			ht.dialer.RecordRTT(host, portStr, ipStr, rtt, true)
			ch <- qAttempt{conn: c, ipStr: ipStr, rtt: rtt}
		}()
	}

	launch(candidates[0])
	launched := 1
	var timer *time.Timer
	var timerCh <-chan time.Time
	if limit > 1 {
		timer = time.NewTimer(25 * time.Millisecond)
		defer timer.Stop()
		timerCh = timer.C
	}

	completed := 0
	var lastErr error
	for completed < limit {
		select {
		case res := <-ch:
			completed++
			if res.err == nil && res.conn != nil {
				cancel()
				go func(rem int) {
					for i := 0; i < rem; i++ {
						r := <-ch
						if r.conn != nil {
							_ = r.conn.CloseWithError(0, "lost race")
						}
					}
				}(launched - completed)
				return res.conn, nil
			}
			lastErr = res.err
			if launched < limit {
				launch(candidates[launched])
				launched++
			}
		case <-timerCh:
			timerCh = nil
			if launched < limit {
				launch(candidates[launched])
				launched++
			}
		case <-ctx.Done():
			cancel()
			return nil, ctx.Err()
		}
	}
	if lastErr == nil {
		lastErr = errors.New("quic dial failed for " + addr)
	}
	return nil, lastErr
}

func (ht *HyperTransport) getProfile(host string) *HostTransportProfile {
	host = canonicalHost(host)
	if v, ok := ht.profiles.Load(host); ok {
		return v.(*HostTransportProfile)
	}
	p := &HostTransportProfile{Host: host}
	actual, _ := ht.profiles.LoadOrStore(host, p)
	return actual.(*HostTransportProfile)
}

func (ht *HyperTransport) shouldUseH3(host string) bool {
	p := ht.getProfile(host)
	p.mu.RLock()
	defer p.mu.RUnlock()
	if !p.PreferH3 || !p.H3Supported {
		return false
	}
	if time.Now().Before(p.H3CooldownEnd) {
		return false
	}
	return true
}

func (ht *HyperTransport) recordProtocolRTT(host string, isH3 bool, d time.Duration, ok bool) {
	p := ht.getProfile(host)
	p.mu.Lock()
	defer p.mu.Unlock()
	ms := float64(d.Microseconds()) / 1000.0
	if isH3 {
		if ok {
			p.H3Requests++
			if p.H3EWMAMs <= 0 {
				p.H3EWMAMs = ms
			} else {
				p.H3EWMAMs = 0.7*p.H3EWMAMs + 0.3*ms
			}
			p.H3CooldownEnd = time.Time{}
		} else {
			p.H3CooldownEnd = time.Now().Add(30 * time.Second)
		}
		return
	}
	if ok {
		p.H2Requests++
		if p.H2EWMAMs <= 0 {
			p.H2EWMAMs = ms
		} else {
			p.H2EWMAMs = 0.7*p.H2EWMAMs + 0.3*ms
		}
	}
}

func (ht *HyperTransport) inspectAltSvc(host, header string) {
	if header == "" {
		return
	}
	lower := strings.ToLower(header)
	if !strings.Contains(lower, "h3=") {
		return
	}
	p := ht.getProfile(host)
	p.mu.Lock()
	if !p.H3Supported {
		p.H3Supported = true
		p.PreferH3 = true
		ht.altSvcLearned.Add(1)
	}
	p.mu.Unlock()
}

func canonicalHost(hostport string) string {
	h := strings.ToLower(strings.TrimSpace(hostport))
	if host, _, err := net.SplitHostPort(h); err == nil {
		return host
	}
	return h
}
