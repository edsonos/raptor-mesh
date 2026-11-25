package config

import (
	"encoding/json"
	"os"
)

// TargetConfig describes a high-priority origin to pre-warm and keep hot.
type TargetConfig struct {
	Host       string `json:"host"`
	Port       int    `json:"port"`
	WarmPath   string `json:"warm_path"`
	WarmMethod string `json:"warm_method"`
	PreferH3   bool   `json:"prefer_h3"`
	TCPOnly    bool   `json:"tcp_only,omitempty"` // Browser-friendly mode: warm DNS + EWMA route + Speculative TCP without sending periodic HTTP probes
}

// Config holds runtime settings for Raptor-Mesh client and server nodes.
type Config struct {
	ListenAddr             string         `json:"listen_addr"`
	ServerAddr             string         `json:"server_addr"`
	DNSResolvers           []string       `json:"dns_resolvers"`
	PrewarmIntervalSec     int            `json:"prewarm_interval_sec"`
	SpeculativeTCPPoolSize int            `json:"speculative_tcp_pool_size"`
	Targets                []TargetConfig `json:"targets"`
}

// Default returns an optimized built-in configuration calibrated for low-latency targets.
func Default() *Config {
	return &Config{
		ListenAddr:             "127.0.0.1:17908",
		ServerAddr:             "127.0.0.1:17909",
		DNSResolvers:           []string{"1.1.1.1:53", "8.8.8.8:53", "9.9.9.9:53", "1.0.0.1:53"},
		PrewarmIntervalSec:     12,
		SpeculativeTCPPoolSize: 3,
		Targets: []TargetConfig{
			{Host: "ipinfo.io", Port: 443, WarmPath: "/json", WarmMethod: "HEAD", PreferH3: true},
			{Host: "www.google.com", Port: 443, WarmPath: "/generate_204", WarmMethod: "GET", PreferH3: true},
			{Host: "google.com", Port: 443, WarmPath: "/generate_204", WarmMethod: "GET", PreferH3: true},
			{Host: "github.com", Port: 443, WarmPath: "/", WarmMethod: "HEAD", PreferH3: false},
			{Host: "github.githubassets.com", Port: 443, WarmPath: "/favicons/favicon.svg", WarmMethod: "HEAD", PreferH3: true},
			{Host: "api.openai.com", Port: 443, WarmPath: "/", WarmMethod: "HEAD", PreferH3: true},
			{Host: "claude.ai", Port: 443, WarmPath: "/", WarmMethod: "HEAD", PreferH3: true},
		},
	}
}

// Load reads JSON configuration from path, falling back to Default() when empty or absent.
func Load(path string) (*Config, error) {
	cfg := Default()
	if path == "" {
		return cfg, nil
	}
	data, err := os.ReadFile(path)
	if err != nil {
		if os.IsNotExist(err) {
			return cfg, nil
		}
		return nil, err
	}
	if err := json.Unmarshal(data, cfg); err != nil {
		return nil, err
	}
	if cfg.ListenAddr == "" {
		cfg.ListenAddr = "127.0.0.1:17908"
	}
	if cfg.ServerAddr == "" {
		cfg.ServerAddr = "127.0.0.1:17909"
	}
	if len(cfg.DNSResolvers) == 0 {
		cfg.DNSResolvers = Default().DNSResolvers
	}
	if cfg.PrewarmIntervalSec <= 0 {
		cfg.PrewarmIntervalSec = 12
	}
	if cfg.SpeculativeTCPPoolSize <= 0 {
		cfg.SpeculativeTCPPoolSize = 3
	}
	return cfg, nil
}
