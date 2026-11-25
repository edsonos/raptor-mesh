package prewarmer

import (
	"context"
	"log"
	"net"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"raptor-mesh/internal/config"
	"raptor-mesh/internal/transport"
)

type learnedTarget struct {
	host       string
	isFastPath bool
	hits       atomic.Int64
	lastSeen   atomic.Int64
}

// Predictor implements Predictive Routing & Pre-Warming:
// 1. Preloads Geo/ECS-aware SWR DNS records for all seed hosts in parallel
// 2. Calibrates HTTP/3 (QUIC 0-RTT) vs HTTP/2 multiplexed pools for Fast-Path targets
// 3. Keeps verified-alive Speculative TCP sockets hot for browser CONNECT bursts
// 4. Periodically prunes expired DNS/route/socket entries for bounded 24/7 memory usage
// 5. Never fires unsolicited HTTP HEAD probes at random websites visited via browser CONNECT
type Predictor struct {
	cfg     *config.Config
	dialer  *transport.SmartDialer
	hyper   *transport.HyperTransport
	tcpPool *transport.SpeculativeTCPPool
	seeds   map[string]struct{}
	learned sync.Map // map[string]*learnedTarget
	verbose bool

	cycles atomic.Int64
}

// New creates a Predictive Pre-Warmer bound to the active transport stack.
func New(cfg *config.Config, dialer *transport.SmartDialer, hyper *transport.HyperTransport, tcpPool *transport.SpeculativeTCPPool, verbose bool) *Predictor {
	seeds := make(map[string]struct{}, len(cfg.Targets))
	for _, t := range cfg.Targets {
		seeds[strings.ToLower(strings.TrimSpace(t.Host))] = struct{}{}
	}
	return &Predictor{
		cfg:     cfg,
		dialer:  dialer,
		hyper:   hyper,
		tcpPool: tcpPool,
		seeds:   seeds,
		verbose: verbose,
	}
}

// RecordAccess feeds real traffic into the predictive hot-domain detector.
// When isFastPath is false (normal browser CONNECT traffic), Raptor-Mesh strictly avoids
// sending unrequested HTTP HEAD probes to third-party sites (preventing anti-bot / rate-limit issues).
func (p *Predictor) RecordAccess(host string, isFastPath bool) {
	host = strings.ToLower(strings.TrimSpace(host))
	if host == "" {
		return
	}
	if h, _, err := net.SplitHostPort(host); err == nil {
		host = h
	}
	if net.ParseIP(host) != nil {
		return
	}
	if _, isSeed := p.seeds[host]; isSeed {
		return
	}
	var lt *learnedTarget
	if v, ok := p.learned.Load(host); ok {
		lt = v.(*learnedTarget)
	} else {
		nlt := &learnedTarget{host: host, isFastPath: isFastPath}
		actual, _ := p.learned.LoadOrStore(host, nlt)
		lt = actual.(*learnedTarget)
	}
	hits := lt.hits.Add(1)
	lt.lastSeen.Store(time.Now().Unix())

	// Only run H3/H2 HTTP calibration probes for hosts actually used via Fast-Path APIs.
	if isFastPath && hits == 2 {
		lt.isFastPath = true
		go func() {
			ctx, cancel := context.WithTimeout(context.Background(), 4*time.Second)
			defer cancel()
			p.hyper.ProbeAndCalibrate(ctx, host, "/", "HEAD", true)
		}()
	}
}

// WarmupAll synchronously resolves DNS, calibrates H3 vs H2 (for non-tcp_only targets),
// and fills the speculative TCP pool for all configured priority targets in parallel.
func (p *Predictor) WarmupAll(ctx context.Context) {
	start := time.Now()
	hosts := make([]string, 0, len(p.cfg.Targets))
	for _, t := range p.cfg.Targets {
		hosts = append(hosts, t.Host)
		if !t.TCPOnly {
			p.hyper.SeedHost(t.Host, t.PreferH3)
		}
	}

	// Stage 1: Parallel Geo/ECS-aware Racing DNS resolution for every priority host.
	dnsCtx, dnsCancel := context.WithTimeout(ctx, 3*time.Second)
	p.dialer.Resolver().Preload(dnsCtx, hosts)
	dnsCancel()

	// Stage 2: Parallel H3/H2 calibration (when not tcp_only) + Speculative TCP pool.
	var wg sync.WaitGroup
	for _, t := range p.cfg.Targets {
		t := t
		wg.Add(1)
		go func() {
			defer wg.Done()
			if !t.TCPOnly {
				p.hyper.ProbeAndCalibrate(ctx, t.Host, t.WarmPath, t.WarmMethod, t.PreferH3)
			}
			port := t.Port
			if port <= 0 {
				port = 443
			}
			poolCount := 2
			if t.TCPOnly {
				poolCount = 1
			}
			p.tcpPool.Prewarm(ctx, net.JoinHostPort(t.Host, strconv.Itoa(port)), poolCount)
		}()
	}
	wg.Wait()
	p.cycles.Add(1)

	if p.verbose {
		log.Printf("[prewarmer] Initial predictive warmup complete for %d targets in %v", len(p.cfg.Targets), time.Since(start).Round(time.Millisecond))
	}
}

// StartLoop launches background maintenance: fresh speculative TCP pool, Fast-Path keep-warm,
// and 24/7 cache/route pruning so memory and socket count stay tightly bounded.
func (p *Predictor) StartLoop(ctx context.Context) {
	interval := time.Duration(p.cfg.PrewarmIntervalSec) * time.Second
	if interval < 5*time.Second {
		interval = 5 * time.Second
	}

	// Prune & refresh hot speculative TCP sockets every 3.5s (aligned with 4s max socket age).
	tcpTicker := time.NewTicker(3500 * time.Millisecond)
	h3Ticker := time.NewTicker(interval)
	gcTicker := time.NewTicker(2 * time.Minute)

	go func() {
		defer tcpTicker.Stop()
		defer h3Ticker.Stop()
		defer gcTicker.Stop()
		for {
			select {
			case <-ctx.Done():
				return
			case <-tcpTicker.C:
				p.tcpPool.PruneAll()
				for _, t := range p.cfg.Targets {
					t := t
					port := t.Port
					if port <= 0 {
						port = 443
					}
					poolCount := 2
					if t.TCPOnly {
						poolCount = 1
					}
					go p.tcpPool.Prewarm(ctx, net.JoinHostPort(t.Host, strconv.Itoa(port)), poolCount)
				}
			case <-h3Ticker.C:
				p.keepWarmCycle(ctx)
			case <-gcTicker.C:
				p.pruneAll()
			}
		}
	}()
}

// Cycles returns how many predictive warmup cycles have completed.
func (p *Predictor) Cycles() int64 {
	return p.cycles.Load()
}

func (p *Predictor) keepWarmCycle(ctx context.Context) {
	seen := make(map[string]struct{}, len(p.cfg.Targets))
	for _, t := range p.cfg.Targets {
		t := t
		seen[t.Host] = struct{}{}
		if !t.TCPOnly {
			go p.hyper.KeepWarm(ctx, t.Host, t.WarmPath, t.WarmMethod)
		} else {
			// For tcp_only browser targets, keep DNS warm without sending unsolicited HTTP HEAD probes!
			go func(h string) {
				dctx, cancel := context.WithTimeout(ctx, 2*time.Second)
				defer cancel()
				_, _ = p.dialer.Resolver().Resolve(dctx, h)
			}(t.Host)
		}
	}

	// Keep recently learned Fast-Path domains alive for 5 minutes after last access.
	cutoff := time.Now().Add(-5 * time.Minute).Unix()
	p.learned.Range(func(k, v any) bool {
		lt := v.(*learnedTarget)
		if _, isSeed := seen[lt.host]; isSeed {
			return true
		}
		if lt.isFastPath && lt.hits.Load() >= 2 && lt.lastSeen.Load() >= cutoff {
			go p.hyper.KeepWarm(ctx, lt.host, "/", "HEAD")
		}
		return true
	})
	p.cycles.Add(1)
}

func (p *Predictor) pruneAll() {
	p.dialer.Resolver().Prune()
	p.dialer.PruneStaleRoutes(30 * time.Minute)
	p.tcpPool.PruneAll()

	cutoff := time.Now().Add(-15 * time.Minute).Unix()
	p.learned.Range(func(k, v any) bool {
		lt := v.(*learnedTarget)
		if lt.lastSeen.Load() < cutoff {
			p.learned.Delete(k)
		}
		return true
	})
}
