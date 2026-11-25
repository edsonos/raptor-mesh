package main

import (
	"context"
	"flag"
	"log"
	"os"
	"os/signal"
	"path/filepath"
	"syscall"
	"time"

	"raptor-mesh/internal/config"
	rdns "raptor-mesh/internal/dns"
	"raptor-mesh/internal/prewarmer"
	"raptor-mesh/internal/proxy"
	"raptor-mesh/internal/transport"
)

func main() {
	cfgPath := flag.String("config", "config/targets.json", "Path to Raptor-Mesh configuration JSON")
	listenAddr := flag.String("listen", "", "Local proxy listen address (default 127.0.0.1:17908)")
	upstreamAddr := flag.String("upstream", "", "Optional raptor-server UDP address (e.g. 127.0.0.1:17909) to relay traffic via RMESH/1 QUIC 0-RTT")
	tunnelConns := flag.Int("tunnel-conns", 4, "Number of multiplexed QUIC 0-RTT connections to upstream raptor-server")
	noPrewarm := flag.Bool("no-prewarm", false, "Disable startup predictive pre-warming")
	logFile := flag.String("logfile", "", "Optional log file path (ideal for background/Task Scheduler mode)")
	flag.Parse()

	if *logFile != "" {
		_ = os.MkdirAll(filepath.Dir(*logFile), 0o755)
		if f, ferr := os.OpenFile(*logFile, os.O_CREATE|os.O_WRONLY|os.O_TRUNC, 0o644); ferr == nil {
			defer f.Close()
			log.SetOutput(f)
		}
	}

	cfg, err := config.Load(*cfgPath)
	if err != nil {
		log.Fatalf("[raptor-client] failed to load config: %v", err)
	}
	if *listenAddr != "" {
		cfg.ListenAddr = *listenAddr
	}

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	resolver := rdns.NewRacingResolver(cfg.DNSResolvers)
	dialer := transport.NewSmartDialer(resolver)
	hyper, err := transport.NewHyperTransport(dialer)
	if err != nil {
		log.Fatalf("[raptor-client] failed to initialize HyperTransport: %v", err)
	}
	defer hyper.Close()

	tcpPool := transport.NewSpeculativeTCPPool(dialer, cfg.SpeculativeTCPPoolSize)
	pw := prewarmer.New(cfg, dialer, hyper, tcpPool, true)

	var tunnel *transport.TunnelClient
	modeLabel := "DIRECT EDGE ACCELERATOR (HTTP/3 QUIC 0-RTT + HTTP/2 MUX + TFO)"
	if *upstreamAddr != "" {
		modeLabel = "MESH RELAY CLIENT -> RMESH/1 QUIC 0-RTT (" + *upstreamAddr + ")"
		tunnel = transport.NewTunnelClient(*upstreamAddr, *tunnelConns, true)
		defer tunnel.Close()
		warmCtx, cancel := context.WithTimeout(ctx, 6*time.Second)
		if err := tunnel.Warmup(warmCtx); err != nil {
			log.Printf("[raptor-client] warning: initial QUIC tunnel warmup error (%v) - will retry on demand", err)
		} else {
			log.Printf("[raptor-client] RMESH/1 QUIC 0-RTT pool ready (%d multiplexed tunnels)", *tunnelConns)
		}
		cancel()
	}

	log.Printf("=================================================================")
	log.Printf("  RAPTOR-MESH LOCAL EDGE CLIENT")
	log.Printf("  Listen    : http://%s", cfg.ListenAddr)
	log.Printf("  Stats UI  : http://%s/raptor/stats", cfg.ListenAddr)
	log.Printf("  Mode      : %s", modeLabel)
	log.Printf("  DNS Racers: %v", cfg.DNSResolvers)
	log.Printf("=================================================================")

	if !*noPrewarm && tunnel == nil {
		warmCtx, cancel := context.WithTimeout(ctx, 12*time.Second)
		pw.WarmupAll(warmCtx)
		cancel()
		pw.StartLoop(ctx)
	}

	ingress := proxy.NewIngressServer(cfg, dialer, hyper, tcpPool, pw, tunnel)
	log.Printf("[raptor-client] Listening on %s (TCP_FASTOPEN + Loopback Fast Path active)", cfg.ListenAddr)
	if err := ingress.Start(ctx); err != nil {
		log.Fatalf("[raptor-client] ingress exited with error: %v", err)
	}
	log.Printf("[raptor-client] shutdown cleanly")
}
