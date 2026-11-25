package main

import (
	"context"
	"flag"
	"log"
	"os"
	"os/signal"
	"syscall"
	"time"

	"raptor-mesh/internal/config"
	rdns "raptor-mesh/internal/dns"
	"raptor-mesh/internal/prewarmer"
	"raptor-mesh/internal/transport"
)

func main() {
	cfgPath := flag.String("config", "config/targets.json", "Path to Raptor-Mesh configuration JSON")
	listenAddr := flag.String("listen", "", "UDP listen address for RMESH/1 QUIC 0-RTT server (default from config: 127.0.0.1:17909)")
	noPrewarm := flag.Bool("no-prewarm", false, "Disable startup predictive pre-warming")
	flag.Parse()

	cfg, err := config.Load(*cfgPath)
	if err != nil {
		log.Fatalf("[raptor-server] failed to load config: %v", err)
	}
	if *listenAddr != "" {
		cfg.ServerAddr = *listenAddr
	}

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	resolver := rdns.NewRacingResolver(cfg.DNSResolvers)
	dialer := transport.NewSmartDialer(resolver)
	hyper, err := transport.NewHyperTransport(dialer)
	if err != nil {
		log.Fatalf("[raptor-server] failed to initialize HyperTransport: %v", err)
	}
	defer hyper.Close()

	tcpPool := transport.NewSpeculativeTCPPool(dialer, cfg.SpeculativeTCPPoolSize)
	pw := prewarmer.New(cfg, dialer, hyper, tcpPool, true)

	log.Printf("=================================================================")
	log.Printf("  RAPTOR-MESH SERVER (RMESH/1 over QUIC 0-RTT)")
	log.Printf("  UDP Listen : %s", cfg.ServerAddr)
	log.Printf("  DNS Racers : %v", cfg.DNSResolvers)
	log.Printf("=================================================================")

	if !*noPrewarm {
		warmCtx, cancel := context.WithTimeout(ctx, 12*time.Second)
		pw.WarmupAll(warmCtx)
		cancel()
		pw.StartLoop(ctx)
	}

	srv := transport.NewTunnelServer(cfg.ServerAddr, hyper, tcpPool)
	log.Printf("[raptor-server] Accepting RMESH/1 QUIC 0-RTT streams on udp://%s", cfg.ServerAddr)
	if err := srv.Start(ctx); err != nil {
		log.Fatalf("[raptor-server] server exited with error: %v", err)
	}
	log.Printf("[raptor-server] shutdown cleanly")
}
