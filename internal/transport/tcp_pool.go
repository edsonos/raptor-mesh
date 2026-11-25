package transport

import (
	"context"
	"net"
	"sync"
	"sync/atomic"
	"time"
)

const (
	// Strict 4s max age + live MSG_PEEK socket liveness check guarantees zero half-closed
	// or peer-FIN sockets are ever handed to browsers during HTTPS CONNECT.
	maxSpeculativeSocketAge = 4 * time.Second
)

type pooledTCPConn struct {
	conn      net.Conn
	createdAt time.Time
}

// TCPPoolStats reports speculative pre-connect hit/miss ratios for HTTPS CONNECT mode.
type TCPPoolStats struct {
	Hits       int64 `json:"hits"`
	Misses     int64 `json:"misses"`
	Expired    int64 `json:"expired"`
	DeadReaped int64 `json:"dead_reaped"`
	ActiveKeys int   `json:"active_keys"`
}

// SpeculativeTCPPool maintains fresh, verified-alive pre-connected TCP sockets for
// explicitly registered hot targets (avoiding zombie socket leaks on random 1-hit hosts).
type SpeculativeTCPPool struct {
	dialer   *SmartDialer
	poolSize int
	buckets  sync.Map // map[string]chan pooledTCPConn
	hotSet   sync.Map // map[string]struct{}

	hits       atomic.Int64
	misses     atomic.Int64
	expired    atomic.Int64
	deadReaped atomic.Int64
}

// NewSpeculativeTCPPool creates a speculative pre-connect pool sized per hot destination.
func NewSpeculativeTCPPool(dialer *SmartDialer, poolSize int) *SpeculativeTCPPool {
	if poolSize <= 0 {
		poolSize = 2
	}
	if poolSize > 6 {
		poolSize = 6
	}
	return &SpeculativeTCPPool{
		dialer:   dialer,
		poolSize: poolSize,
	}
}

// RegisterHot marks addr (host:port) as eligible for speculative pre-connection & refill.
func (p *SpeculativeTCPPool) RegisterHot(addr string) {
	p.hotSet.Store(addr, struct{}{})
}

// Acquire returns a fresh, verified-alive pre-connected socket to addr if available,
// otherwise dials immediately via SmartDialer (cached Geo/Racing DNS + Happy Eyeballs v2).
func (p *SpeculativeTCPPool) Acquire(ctx context.Context, addr string) (net.Conn, bool, error) {
	if v, ok := p.buckets.Load(addr); ok {
		ch := v.(chan pooledTCPConn)
		now := time.Now()
		for {
			select {
			case item := <-ch:
				if now.Sub(item.createdAt) > maxSpeculativeSocketAge {
					p.expired.Add(1)
					_ = item.conn.Close()
					continue
				}
				if !IsTCPConnAlive(item.conn) {
					p.deadReaped.Add(1)
					_ = item.conn.Close()
					continue
				}
				p.hits.Add(1)
				if _, isHot := p.hotSet.Load(addr); isHot {
					go p.refillOne(addr)
				}
				return item.conn, true, nil
			default:
				goto miss
			}
		}
	}

miss:
	p.misses.Add(1)
	conn, err := p.dialer.DialContext(ctx, "tcp", addr)
	if err == nil {
		// Only replenish asynchronously for explicitly registered hot targets so random 1-hit
		// domains never leave zombie idle sockets open!
		if _, isHot := p.hotSet.Load(addr); isHot {
			go p.refillOne(addr)
		}
	}
	return conn, false, err
}

// Prewarm fills the speculative pool for a hot addr up to targetCount fresh sockets.
func (p *SpeculativeTCPPool) Prewarm(ctx context.Context, addr string, targetCount int) {
	p.RegisterHot(addr)
	if targetCount <= 0 {
		targetCount = 1
	}
	if targetCount > p.poolSize {
		targetCount = p.poolSize
	}
	ch := p.getBucket(addr)
	p.pruneBucket(ch)

	need := targetCount - len(ch)
	if need <= 0 {
		return
	}
	var wg sync.WaitGroup
	for i := 0; i < need; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			dialCtx, cancel := context.WithTimeout(ctx, 2500*time.Millisecond)
			defer cancel()
			conn, err := p.dialer.DialContext(dialCtx, "tcp", addr)
			if err != nil {
				return
			}
			select {
			case ch <- pooledTCPConn{conn: conn, createdAt: time.Now()}:
			default:
				_ = conn.Close()
			}
		}()
	}
	wg.Wait()
}

// PruneAll closes all expired or peer-closed sockets across every bucket.
func (p *SpeculativeTCPPool) PruneAll() {
	p.buckets.Range(func(k, v any) bool {
		ch := v.(chan pooledTCPConn)
		p.pruneBucket(ch)
		if len(ch) == 0 {
			if _, isHot := p.hotSet.Load(k); !isHot {
				p.buckets.Delete(k)
			}
		}
		return true
	})
}

// GetStats returns speculative TCP pool hit/miss metrics.
func (p *SpeculativeTCPPool) GetStats() TCPPoolStats {
	keys := 0
	p.buckets.Range(func(_, _ any) bool {
		keys++
		return true
	})
	return TCPPoolStats{
		Hits:       p.hits.Load(),
		Misses:     p.misses.Load(),
		Expired:    p.expired.Load(),
		DeadReaped: p.deadReaped.Load(),
		ActiveKeys: keys,
	}
}

func (p *SpeculativeTCPPool) refillOne(addr string) {
	ch := p.getBucket(addr)
	if len(ch) >= p.poolSize {
		return
	}
	ctx, cancel := context.WithTimeout(context.Background(), 2500*time.Millisecond)
	defer cancel()
	conn, err := p.dialer.DialContext(ctx, "tcp", addr)
	if err != nil {
		return
	}
	select {
	case ch <- pooledTCPConn{conn: conn, createdAt: time.Now()}:
	default:
		_ = conn.Close()
	}
}

func (p *SpeculativeTCPPool) pruneBucket(ch chan pooledTCPConn) {
	now := time.Now()
	n := len(ch)
	for i := 0; i < n; i++ {
		select {
		case item := <-ch:
			if now.Sub(item.createdAt) > maxSpeculativeSocketAge {
				p.expired.Add(1)
				_ = item.conn.Close()
				continue
			}
			if !IsTCPConnAlive(item.conn) {
				p.deadReaped.Add(1)
				_ = item.conn.Close()
				continue
			}
			select {
			case ch <- item:
			default:
				_ = item.conn.Close()
			}
		default:
			return
		}
	}
}

func (p *SpeculativeTCPPool) getBucket(addr string) chan pooledTCPConn {
	if v, ok := p.buckets.Load(addr); ok {
		return v.(chan pooledTCPConn)
	}
	ch := make(chan pooledTCPConn, p.poolSize)
	actual, _ := p.buckets.LoadOrStore(addr, ch)
	return actual.(chan pooledTCPConn)
}
