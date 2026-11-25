package transport

import (
	"context"
	"errors"
	"net"
	"sort"
	"sync"
	"sync/atomic"
	"time"

	rdns "raptor-mesh/internal/dns"
)

// RouteMetric records the EWMA connection latency and health of a specific resolved IP.
type RouteMetric struct {
	IP          string    `json:"ip"`
	EWMAMs      float64   `json:"ewma_ms"`
	Successes   int64     `json:"successes"`
	Failures    int64     `json:"failures"`
	LastUsed    time.Time `json:"last_used"`
	CooldownEnd time.Time `json:"-"`
}

// HostRouteSnapshot is an exported telemetry view of a destination's winning route.
type HostRouteSnapshot struct {
	Host       string  `json:"host"`
	ChampionIP string  `json:"champion_ip"`
	EWMAMs     float64 `json:"ewma_ms"`
	Candidates int     `json:"candidates"`
}

type routeBucket struct {
	mu      sync.RWMutex
	metrics map[string]*RouteMetric
}

// SmartDialer implements Aggressive Happy Eyeballs v2 (15ms stagger), Hedged Champion Dialing,
// EWMA Predictive Route Caching, Racing DNS, and kernel TCP_FASTOPEN + TCP_NODELAY tuning.
type SmartDialer struct {
	resolver *rdns.RacingResolver
	routes   sync.Map // map[string]*routeBucket (keyed by "host:port")

	baseDialer net.Dialer

	hedgedWins atomic.Int64
	raceDials  atomic.Int64
	fastDials  atomic.Int64
}

// NewSmartDialer builds an ultra-low-latency TCP dialer wired to the RacingResolver.
func NewSmartDialer(resolver *rdns.RacingResolver) *SmartDialer {
	return &SmartDialer{
		resolver: resolver,
		baseDialer: net.Dialer{
			Timeout:   4 * time.Second,
			KeepAlive: 15 * time.Second,
			Control:   ControlDialer,
		},
	}
}

// Resolver exposes the backing SWR RacingResolver.
func (d *SmartDialer) Resolver() *rdns.RacingResolver {
	return d.resolver
}

// BestIP returns the current champion IP for a host:port (resolving via SWR DNS if needed).
// Used by QUIC/HTTP3 transports to lock directly onto the lowest-RTT edge POP IP.
func (d *SmartDialer) BestIP(ctx context.Context, host, port string) (net.IP, error) {
	if ip := net.ParseIP(host); ip != nil {
		return ip, nil
	}
	res, err := d.resolver.Resolve(ctx, host)
	if err != nil {
		return nil, err
	}
	candidates := d.rankCandidates(net.JoinHostPort(host, port), res.AllInterleaved())
	if len(candidates) == 0 {
		return nil, errors.New("no candidate IPs for " + host)
	}
	return candidates[0], nil
}

// CandidateIPs returns all resolved IPs ordered by EWMA predictive route score.
func (d *SmartDialer) CandidateIPs(ctx context.Context, host, port string) ([]net.IP, error) {
	if ip := net.ParseIP(host); ip != nil {
		return []net.IP{ip}, nil
	}
	res, err := d.resolver.Resolve(ctx, host)
	if err != nil {
		return nil, err
	}
	return d.rankCandidates(net.JoinHostPort(host, port), res.AllInterleaved()), nil
}

// RecordRTT lets external transports (like QUIC/HTTP3) feed real RTT observations into the route scoreboard.
func (d *SmartDialer) RecordRTT(host, port, ipStr string, rtt time.Duration, ok bool) {
	key := net.JoinHostPort(host, port)
	bucket := d.getBucket(key)
	bucket.mu.Lock()
	defer bucket.mu.Unlock()

	m, exists := bucket.metrics[ipStr]
	if !exists {
		m = &RouteMetric{IP: ipStr}
		bucket.metrics[ipStr] = m
	}
	now := time.Now()
	m.LastUsed = now
	if ok {
		ms := float64(rtt.Microseconds()) / 1000.0
		if m.Successes == 0 || m.EWMAMs <= 0 {
			m.EWMAMs = ms
		} else {
			// Alpha = 0.35 responds quickly to POP routing shifts while smoothing jitter.
			m.EWMAMs = 0.65*m.EWMAMs + 0.35*ms
		}
		m.Successes++
		m.CooldownEnd = time.Time{}
	} else {
		m.Failures++
		m.CooldownEnd = now.Add(45 * time.Second)
	}
}

// DialContext connects to address using predictive champion hedging or Happy Eyeballs v2 parallel racing.
func (d *SmartDialer) DialContext(ctx context.Context, network, address string) (net.Conn, error) {
	host, port, err := net.SplitHostPort(address)
	if err != nil {
		return d.baseDialer.DialContext(ctx, network, address)
	}
	if ip := net.ParseIP(host); ip != nil {
		conn, err := d.baseDialer.DialContext(ctx, network, address)
		if err == nil {
			TuneAcceptedConn(conn)
		}
		return conn, err
	}

	res, err := d.resolver.Resolve(ctx, host)
	if err != nil {
		return nil, err
	}
	candidates := d.rankCandidates(address, res.AllInterleaved())
	if len(candidates) == 0 {
		return nil, errors.New("smartdialer: no IPs for " + host)
	}

	// Single IP fast path.
	if len(candidates) == 1 {
		d.fastDials.Add(1)
		start := time.Now()
		ipStr := candidates[0].String()
		conn, err := d.baseDialer.DialContext(ctx, network, net.JoinHostPort(ipStr, port))
		d.RecordRTT(host, port, ipStr, time.Since(start), err == nil)
		if err == nil {
			TuneAcceptedConn(conn)
		}
		return conn, err
	}

	// Check if we already have a proven fast champion IP -> use Hedged Champion Dialing.
	if champ, ok := d.getProvenChampion(address, candidates); ok {
		d.fastDials.Add(1)
		hedgeDelay := time.Duration(champ.EWMAMs*1.6) * time.Millisecond
		if hedgeDelay < 25*time.Millisecond {
			hedgeDelay = 25 * time.Millisecond
		}
		if hedgeDelay > 80*time.Millisecond {
			hedgeDelay = 80 * time.Millisecond
		}
		// Race top 2 candidates with adaptive hedge delay so healthy paths only open 1 socket,
		// while stalled paths immediately failover!
		top := candidates
		if len(top) > 3 {
			top = top[:3]
		}
		return d.raceConnect(ctx, network, host, port, top, hedgeDelay)
	}

	// Exploration path: Aggressive Happy Eyeballs v2 with 15ms stagger across up to 4 IPs.
	d.raceDials.Add(1)
	top := candidates
	if len(top) > 4 {
		top = top[:4]
	}
	return d.raceConnect(ctx, network, host, port, top, 15*time.Millisecond)
}

type dialAttempt struct {
	conn  net.Conn
	ipStr string
	rtt   time.Duration
	idx   int
	err   error
}

func (d *SmartDialer) raceConnect(
	ctx context.Context,
	network, host, port string,
	candidates []net.IP,
	stagger time.Duration,
) (net.Conn, error) {
	raceCtx, cancel := context.WithCancel(ctx)
	defer cancel()

	ch := make(chan dialAttempt, len(candidates))
	launched := 0

	launch := func(idx int) {
		ipStr := candidates[idx].String()
		target := net.JoinHostPort(ipStr, port)
		launched++
		go func() {
			start := time.Now()
			conn, err := d.baseDialer.DialContext(raceCtx, network, target)
			rtt := time.Since(start)
			if err != nil {
				// Don't penalize IPs that merely lost the race due to cancellation.
				if raceCtx.Err() == nil {
					d.RecordRTT(host, port, ipStr, rtt, false)
				}
				ch <- dialAttempt{ipStr: ipStr, rtt: rtt, idx: idx, err: err}
				return
			}
			d.RecordRTT(host, port, ipStr, rtt, true)
			ch <- dialAttempt{conn: conn, ipStr: ipStr, rtt: rtt, idx: idx}
		}()
	}

	// Launch candidate #0 immediately at t=0.
	launch(0)
	nextIdx := 1
	timer := time.NewTimer(stagger)
	defer timer.Stop()

	completed := 0
	var lastErr error

	for completed < len(candidates) {
		select {
		case res := <-ch:
			completed++
			if res.err == nil && res.conn != nil {
				if res.idx > 0 {
					d.hedgedWins.Add(1)
				}
				cancel()
				TuneAcceptedConn(res.conn)
				// Drain any losing sockets that may arrive right after cancel.
				go func(remaining int) {
					for i := 0; i < remaining; i++ {
						r := <-ch
						if r.conn != nil {
							_ = r.conn.Close()
						}
					}
				}(launched - completed)
				return res.conn, nil
			}
			lastErr = res.err
			// If candidate failed fast, launch the next candidate immediately instead of waiting for stagger!
			if nextIdx < len(candidates) {
				launch(nextIdx)
				nextIdx++
				if !timer.Stop() {
					select {
					case <-timer.C:
					default:
					}
				}
				timer.Reset(stagger)
			}

		case <-timer.C:
			if nextIdx < len(candidates) {
				launch(nextIdx)
				nextIdx++
				if nextIdx < len(candidates) {
					timer.Reset(stagger)
				}
			}

		case <-ctx.Done():
			cancel()
			return nil, ctx.Err()
		}
	}

	if lastErr == nil {
		lastErr = errors.New("all dial candidates failed for " + host)
	}
	return nil, lastErr
}

// SnapshotRoutes returns live predictive routing table telemetry for /raptor/stats.
func (d *SmartDialer) SnapshotRoutes() []HostRouteSnapshot {
	var out []HostRouteSnapshot
	d.routes.Range(func(k, v any) bool {
		host := k.(string)
		b := v.(*routeBucket)
		b.mu.RLock()
		var best *RouteMetric
		for _, m := range b.metrics {
			if m.Successes > 0 && (best == nil || m.EWMAMs < best.EWMAMs) {
				best = m
			}
		}
		if best != nil {
			out = append(out, HostRouteSnapshot{
				Host:       host,
				ChampionIP: best.IP,
				EWMAMs:     best.EWMAMs,
				Candidates: len(b.metrics),
			})
		}
		b.mu.RUnlock()
		return true
	})
	sort.Slice(out, func(i, j int) bool { return out[i].Host < out[j].Host })
	return out
}

// PruneStaleRoutes evicts route metrics that haven't been used within maxIdle to keep 24/7 RAM bounded.
func (d *SmartDialer) PruneStaleRoutes(maxIdle time.Duration) {
	cutoff := time.Now().Add(-maxIdle)
	d.routes.Range(func(k, v any) bool {
		b := v.(*routeBucket)
		b.mu.Lock()
		for ip, m := range b.metrics {
			if m.LastUsed.Before(cutoff) {
				delete(b.metrics, ip)
			}
		}
		empty := len(b.metrics) == 0
		b.mu.Unlock()
		if empty {
			d.routes.Delete(k)
		}
		return true
	})
}

func (d *SmartDialer) getBucket(key string) *routeBucket {
	if v, ok := d.routes.Load(key); ok {
		return v.(*routeBucket)
	}
	b := &routeBucket{metrics: make(map[string]*RouteMetric)}
	actual, _ := d.routes.LoadOrStore(key, b)
	return actual.(*routeBucket)
}

func (d *SmartDialer) getProvenChampion(key string, candidates []net.IP) (*RouteMetric, bool) {
	v, ok := d.routes.Load(key)
	if !ok || len(candidates) == 0 {
		return nil, false
	}
	b := v.(*routeBucket)
	b.mu.RLock()
	defer b.mu.RUnlock()
	m, exists := b.metrics[candidates[0].String()]
	if !exists || m.Successes < 1 || time.Now().Before(m.CooldownEnd) {
		return nil, false
	}
	return m, true
}

func (d *SmartDialer) rankCandidates(key string, ips []net.IP) []net.IP {
	if len(ips) <= 1 {
		return ips
	}
	v, ok := d.routes.Load(key)
	if !ok {
		return ips
	}
	b := v.(*routeBucket)
	b.mu.RLock()
	defer b.mu.RUnlock()

	now := time.Now()
	ranked := make([]net.IP, len(ips))
	copy(ranked, ips)

	score := func(ip net.IP) float64 {
		m, exists := b.metrics[ip.String()]
		if !exists {
			return 500.0 // Unknown IP: explore ahead of cooled-down bad IPs, behind proven fast IPs.
		}
		if now.Before(m.CooldownEnd) {
			return 10000.0 + float64(m.Failures)*1000.0
		}
		if m.Successes > 0 {
			return m.EWMAMs
		}
		return 600.0
	}

	sort.SliceStable(ranked, func(i, j int) bool {
		return score(ranked[i]) < score(ranked[j])
	})
	return ranked
}
