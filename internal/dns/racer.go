package dns

import (
	"context"
	"errors"
	"net"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	mdns "github.com/miekg/dns"
)

// geoSensitiveSuffixes lists CDN and media/streaming domains that rely on ISP DNS or
// EDNS Client Subnet (ECS) to steer users to local ISP caches (Meta FNA, Google GGC,
// Netflix OCA, Akamai, Twitch, Globo, etc.) instead of distant US/EU POPs.
var geoSensitiveSuffixes = []string{
	"fbcdn.net",
	"cdninstagram.com",
	"instagram.com",
	"facebook.com",
	"whatsapp.net",
	"whatsapp.com",
	"googlevideo.com",
	"ytimg.com",
	"youtube.com",
	"ggpht.com",
	"nflxvideo.net",
	"nflximg.net",
	"netflix.com",
	"ttvnw.net",
	"twitch.tv",
	"tiktokcdn.com",
	"tiktokv.com",
	"akamaized.net",
	"akamaihd.net",
	"edgesuite.net",
	"edgekey.net",
	"glbimg.com",
	"globo.com",
	"scdn.co",
	"spotifycdn.com",
	"steamstatic.com",
	"steamcontent.com",
	"mzstatic.com",
	"cdn-apple.com",
	"cloudfront.net",
	"mlstatic.com",
	"mercadolivre.com.br",
	"mercadolibre.com",
	"primevideo.com",
	"aiv-cdn.net",
	"disneyplus.com",
	"bamgrid.com",
	"riotgames.com",
	"riotcdn.net",
	"leagueoflegends.com",
	"pvp.net",
	"rgpub.io",
}

// ResolveResult holds cached IPv4 and IPv6 addresses for a domain.
type ResolveResult struct {
	IPv4      []net.IP
	IPv6      []net.IP
	Resolved  time.Time
	RefreshAt time.Time
	ExpiresAt time.Time
	TTL       time.Duration
	Source    string
	GeoAware  bool

	refreshing atomic.Bool
}

// AllInterleaved returns IPv4 and IPv6 addresses interleaved for Happy Eyeballs v2 racing.
func (r *ResolveResult) AllInterleaved() []net.IP {
	out := make([]net.IP, 0, len(r.IPv4)+len(r.IPv6))
	maxLen := len(r.IPv4)
	if len(r.IPv6) > maxLen {
		maxLen = len(r.IPv6)
	}
	for i := 0; i < maxLen; i++ {
		if i < len(r.IPv4) {
			out = append(out, r.IPv4[i])
		}
		if i < len(r.IPv6) {
			out = append(out, r.IPv6[i])
		}
	}
	return out
}

// Stats reports live DNS cache and racing performance metrics.
type Stats struct {
	CacheHits        int64  `json:"cache_hits"`
	CacheMisses      int64  `json:"cache_misses"`
	SWRRefreshes     int64  `json:"swr_refreshes"`
	GeoCDNLookups    int64  `json:"geo_cdn_lookups"`
	CachedHosts      int    `json:"cached_hosts"`
	AvgLookupMicros  int64  `json:"avg_lookup_us"`
	LastWinnerServer string `json:"last_winner_server"`
}

// RacingResolver queries multiple upstream DNS resolvers concurrently and serves
// cached records with Stale-While-Revalidate (SWR) in sub-microsecond latency.
// For Geo/ECS-sensitive media CDNs (Instagram/Meta, YouTube, Netflix, Akamai, etc.),
// it automatically uses ISP/System DNS + ECS-capable resolvers (8.8.8.8 / 8.8.4.4)
// so users always hit local ISP edge caches instead of distant POPs.
type RacingResolver struct {
	resolvers    []string
	ecsResolvers []string
	cache        sync.Map // map[string]*ResolveResult
	client       *mdns.Client

	hits        atomic.Int64
	misses      atomic.Int64
	swrCount    atomic.Int64
	geoLookups  atomic.Int64
	totalNs     atomic.Int64
	lookupCount atomic.Int64
	lastWinner  atomic.Value
}

// NewRacingResolver constructs a concurrent Geo/ECS-aware multi-upstream DNS resolver with SWR cache.
func NewRacingResolver(resolvers []string) *RacingResolver {
	if len(resolvers) == 0 {
		resolvers = []string{"1.1.1.1:53", "8.8.8.8:53", "9.9.9.9:53", "1.0.0.1:53"}
	}
	rr := &RacingResolver{
		resolvers:    resolvers,
		ecsResolvers: []string{"8.8.8.8:53", "8.8.4.4:53"},
		client: &mdns.Client{
			Net:     "udp",
			Timeout: 1500 * time.Millisecond,
			UDPSize: 1232,
		},
	}
	rr.lastWinner.Store("none")
	return rr
}

// IsGeoSensitiveCDN reports whether host belongs to a CDN/streaming network that requires
// ISP DNS or EDNS Client Subnet (ECS) for optimal local POP selection.
func IsGeoSensitiveCDN(host string) bool {
	host = strings.TrimSuffix(strings.ToLower(strings.TrimSpace(host)), ".")
	for _, suf := range geoSensitiveSuffixes {
		if host == suf || strings.HasSuffix(host, "."+suf) {
			return true
		}
	}
	return false
}

// Resolve returns IPv4 and IPv6 addresses for host, hitting the lock-free SWR cache when warm.
func (r *RacingResolver) Resolve(ctx context.Context, host string) (*ResolveResult, error) {
	host = strings.TrimSuffix(strings.ToLower(strings.TrimSpace(host)), ".")
	if ip := net.ParseIP(host); ip != nil {
		res := &ResolveResult{Resolved: time.Now(), ExpiresAt: time.Now().Add(24 * time.Hour), Source: "literal"}
		if ip.To4() != nil {
			res.IPv4 = []net.IP{ip}
		} else {
			res.IPv6 = []net.IP{ip}
		}
		return res, nil
	}

	now := time.Now()
	if val, ok := r.cache.Load(host); ok {
		entry := val.(*ResolveResult)
		if now.Before(entry.ExpiresAt) {
			r.hits.Add(1)
			if now.After(entry.RefreshAt) && entry.refreshing.CompareAndSwap(false, true) {
				r.swrCount.Add(1)
				go func() {
					defer entry.refreshing.Store(false)
					bgCtx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
					defer cancel()
					if fresh, err := r.lookup(bgCtx, host); err == nil && (len(fresh.IPv4) > 0 || len(fresh.IPv6) > 0) {
						r.cache.Store(host, fresh)
					}
				}()
			}
			return entry, nil
		}
	}

	r.misses.Add(1)
	start := time.Now()
	fresh, err := r.lookup(ctx, host)
	elapsed := time.Since(start)
	r.totalNs.Add(elapsed.Nanoseconds())
	r.lookupCount.Add(1)
	if err != nil {
		if val, ok := r.cache.Load(host); ok {
			return val.(*ResolveResult), nil
		}
		return nil, err
	}
	r.cache.Store(host, fresh)
	return fresh, nil
}

// Preload resolves a batch of hosts in parallel to populate the SWR cache before traffic arrives.
func (r *RacingResolver) Preload(ctx context.Context, hosts []string) {
	var wg sync.WaitGroup
	for _, h := range hosts {
		h := h
		wg.Add(1)
		go func() {
			defer wg.Done()
			_, _ = r.Resolve(ctx, h)
		}()
	}
	wg.Wait()
}

// Prune removes expired entries from the DNS cache to keep 24/7 memory bounded.
func (r *RacingResolver) Prune() {
	cutoff := time.Now().Add(-5 * time.Minute)
	r.cache.Range(func(k, v any) bool {
		entry := v.(*ResolveResult)
		if entry.ExpiresAt.Before(cutoff) {
			r.cache.Delete(k)
		}
		return true
	})
}

// GetStats returns live resolver telemetry.
func (r *RacingResolver) GetStats() Stats {
	count := 0
	r.cache.Range(func(_, _ any) bool {
		count++
		return true
	})
	var avgUs int64
	if lc := r.lookupCount.Load(); lc > 0 {
		avgUs = (r.totalNs.Load() / lc) / 1000
	}
	winner, _ := r.lastWinner.Load().(string)
	return Stats{
		CacheHits:        r.hits.Load(),
		CacheMisses:      r.misses.Load(),
		SWRRefreshes:     r.swrCount.Load(),
		GeoCDNLookups:    r.geoLookups.Load(),
		CachedHosts:      count,
		AvgLookupMicros:  avgUs,
		LastWinnerServer: winner,
	}
}

func (r *RacingResolver) lookup(ctx context.Context, host string) (*ResolveResult, error) {
	if IsGeoSensitiveCDN(host) {
		r.geoLookups.Add(1)
		return r.geoAwareLookup(ctx, host)
	}
	return r.raceLookup(ctx, host, r.resolvers, false)
}

// geoAwareLookup prioritizes the active ISP/System DNS (which automatically locates in-ISP
// caches like Meta FNA, Google GGC, and Netflix OCA) with a fast parallel fallback to
// ECS-enabled resolvers (8.8.8.8 / 8.8.4.4). Never lets non-ECS resolvers (1.1.1.1/9.9.9.9)
// hijack local media CDN routes to Miami/US POPs.
func (r *RacingResolver) geoAwareLookup(ctx context.Context, host string) (*ResolveResult, error) {
	geoCtx, cancel := context.WithTimeout(ctx, 2*time.Second)
	defer cancel()

	type sysRes struct {
		ipv4 []net.IP
		ipv6 []net.IP
		err  error
	}
	sysCh := make(chan sysRes, 1)
	go func() {
		ips, err := net.DefaultResolver.LookupIPAddr(geoCtx, host)
		if err != nil {
			sysCh <- sysRes{err: err}
			return
		}
		var v4, v6 []net.IP
		for _, ipAddr := range ips {
			if ipAddr.IP.To4() != nil {
				v4 = append(v4, ipAddr.IP)
			} else {
				v6 = append(v6, ipAddr.IP)
			}
		}
		sysCh <- sysRes{ipv4: v4, ipv6: v6}
	}()

	// Give ISP/System DNS a 65ms priority head-start so in-ISP FNA/GGC caches ALWAYS win when healthy.
	timer := time.NewTimer(65 * time.Millisecond)
	defer timer.Stop()

	select {
	case res := <-sysCh:
		if res.err == nil && (len(res.ipv4) > 0 || len(res.ipv6) > 0) {
			r.lastWinner.Store("isp-geo")
			now := time.Now()
			ttl := 2 * time.Minute
			return &ResolveResult{
				IPv4:      res.ipv4,
				IPv6:      res.ipv6,
				Resolved:  now,
				RefreshAt: now.Add(ttl * 7 / 10),
				ExpiresAt: now.Add(ttl),
				TTL:       ttl,
				Source:    "isp-geo",
				GeoAware:  true,
			}, nil
		}
		// ISP resolver failed quickly -> fallback immediately to ECS-capable Google DNS (8.8.8.8/8.8.4.4).
		return r.raceLookup(geoCtx, host, r.ecsResolvers, true)

	case <-timer.C:
		// ISP DNS is slower than 65ms -> race ECS resolvers alongside the in-flight ISP lookup!
		ecsCh := make(chan *ResolveResult, 1)
		go func() {
			if res, err := r.raceLookup(geoCtx, host, r.ecsResolvers, true); err == nil {
				ecsCh <- res
			}
		}()
		select {
		case res := <-sysCh:
			if res.err == nil && (len(res.ipv4) > 0 || len(res.ipv6) > 0) {
				r.lastWinner.Store("isp-geo")
				now := time.Now()
				ttl := 2 * time.Minute
				return &ResolveResult{
					IPv4:      res.ipv4,
					IPv6:      res.ipv6,
					Resolved:  now,
					RefreshAt: now.Add(ttl * 7 / 10),
					ExpiresAt: now.Add(ttl),
					TTL:       ttl,
					Source:    "isp-geo",
					GeoAware:  true,
				}, nil
			}
			select {
			case eres := <-ecsCh:
				return eres, nil
			case <-geoCtx.Done():
				return nil, geoCtx.Err()
			}
		case eres := <-ecsCh:
			return eres, nil
		case <-geoCtx.Done():
			return nil, geoCtx.Err()
		}

	case <-geoCtx.Done():
		return nil, geoCtx.Err()
	}
}

type familyResp struct {
	qtype     uint16
	ips       []net.IP
	ttl       time.Duration
	server    string
	noRecords bool
	err       error
}

// raceLookup queries A and AAAA across serverList in parallel.
// The fastest valid response for each family wins immediately; once one family arrives,
// we give the sibling family a tiny 12ms merge window so Happy Eyeballs receives dual-stack IPs.
func (r *RacingResolver) raceLookup(ctx context.Context, host string, serverList []string, geoAware bool) (*ResolveResult, error) {
	raceCtx, cancel := context.WithTimeout(ctx, 2*time.Second)
	defer cancel()

	fqdn := mdns.Fqdn(host)
	aCh := make(chan familyResp, 1)
	aaaaCh := make(chan familyResp, 1)

	go func() { aCh <- r.raceFamily(raceCtx, fqdn, mdns.TypeA, serverList) }()
	go func() { aaaaCh <- r.raceFamily(raceCtx, fqdn, mdns.TypeAAAA, serverList) }()

	var ipv4, ipv6 []net.IP
	var minTTL = 5 * time.Minute
	var winner string

	merge := func(resp familyResp) {
		if resp.err != nil || len(resp.ips) == 0 {
			return
		}
		if resp.qtype == mdns.TypeA {
			ipv4 = resp.ips
		} else {
			ipv6 = resp.ips
		}
		if resp.ttl > 0 && resp.ttl < minTTL {
			minTTL = resp.ttl
		}
		if winner == "" && resp.server != "" {
			winner = resp.server
		}
	}

	var secondCh <-chan familyResp
	select {
	case resp := <-aCh:
		merge(resp)
		secondCh = aaaaCh
	case resp := <-aaaaCh:
		merge(resp)
		secondCh = aCh
	case <-raceCtx.Done():
	}

	if len(ipv4) > 0 || len(ipv6) > 0 {
		timer := time.NewTimer(12 * time.Millisecond)
		select {
		case resp := <-secondCh:
			merge(resp)
		case <-timer.C:
		case <-raceCtx.Done():
		}
		timer.Stop()
	} else if secondCh != nil {
		select {
		case resp := <-secondCh:
			merge(resp)
		case <-raceCtx.Done():
		}
	}

	// Ultimate fallback: OS resolver (handles /etc/hosts, intranet, or corporate DNS).
	if len(ipv4) == 0 && len(ipv6) == 0 {
		ips, err := net.DefaultResolver.LookupIPAddr(raceCtx, host)
		if err != nil {
			return nil, err
		}
		for _, ipAddr := range ips {
			if ipAddr.IP.To4() != nil {
				ipv4 = append(ipv4, ipAddr.IP)
			} else {
				ipv6 = append(ipv6, ipAddr.IP)
			}
		}
		winner = "system"
		minTTL = 60 * time.Second
	}

	if len(ipv4) == 0 && len(ipv6) == 0 {
		return nil, errors.New("dns: no A or AAAA records found for " + host)
	}

	if minTTL < 45*time.Second {
		minTTL = 45 * time.Second
	}
	if minTTL > 10*time.Minute {
		minTTL = 10 * time.Minute
	}
	if winner != "" {
		r.lastWinner.Store(winner)
	}

	now := time.Now()
	return &ResolveResult{
		IPv4:      ipv4,
		IPv6:      ipv6,
		Resolved:  now,
		RefreshAt: now.Add(minTTL * 7 / 10),
		ExpiresAt: now.Add(minTTL),
		TTL:       minTTL,
		Source:    winner,
		GeoAware:  geoAware,
	}, nil
}

func (r *RacingResolver) raceFamily(ctx context.Context, fqdn string, qtype uint16, serverList []string) familyResp {
	subCtx, cancel := context.WithCancel(ctx)
	defer cancel()

	resCh := make(chan familyResp, len(serverList))
	for _, srv := range serverList {
		srv := srv
		go func() {
			msg := new(mdns.Msg)
			msg.SetQuestion(fqdn, qtype)
			msg.RecursionDesired = true
			msg.SetEdns0(1232, false)

			resp, _, err := r.client.ExchangeContext(subCtx, msg, srv)
			if err != nil || resp == nil || resp.Rcode != mdns.RcodeSuccess {
				resCh <- familyResp{qtype: qtype, err: errors.New("query failed")}
				return
			}
			var ips []net.IP
			var ttl uint32 = 300
			for _, ans := range resp.Answer {
				switch rr := ans.(type) {
				case *mdns.A:
					if qtype == mdns.TypeA {
						ips = append(ips, rr.A)
						if rr.Hdr.Ttl > 0 && rr.Hdr.Ttl < ttl {
							ttl = rr.Hdr.Ttl
						}
					}
				case *mdns.AAAA:
					if qtype == mdns.TypeAAAA {
						ips = append(ips, rr.AAAA)
						if rr.Hdr.Ttl > 0 && rr.Hdr.Ttl < ttl {
							ttl = rr.Hdr.Ttl
						}
					}
				}
			}
			if len(ips) == 0 {
				// Clean authoritative NOERROR with 0 records (e.g. IPv4-only host queried for AAAA).
				resCh <- familyResp{qtype: qtype, noRecords: true, server: srv}
				return
			}
			resCh <- familyResp{
				qtype:  qtype,
				ips:    ips,
				ttl:    time.Duration(ttl) * time.Second,
				server: srv,
			}
		}()
	}

	var lastErr error
	for i := 0; i < len(serverList); i++ {
		select {
		case res := <-resCh:
			if res.err == nil && len(res.ips) > 0 {
				cancel()
				return res
			}
			if res.noRecords {
				cancel()
				return res
			}
			lastErr = res.err
		case <-subCtx.Done():
			return familyResp{qtype: qtype, err: subCtx.Err()}
		}
	}
	return familyResp{qtype: qtype, err: lastErr}
}
