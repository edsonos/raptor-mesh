//go:build !windows

package transport

import (
	"net"
	"sync/atomic"
	"syscall"
)

const loopbackBufSize = 512 * 1024

// SocketStats exposes low-level kernel socket optimization counters.
type SocketStats struct {
	TFOEnabled          int64 `json:"tfo_enabled"`
	LoopbackFastEnabled int64 `json:"loopback_fast_path_enabled"`
	TunedSockets        int64 `json:"tuned_sockets"`
}

var tunedSocketCount atomic.Int64

// GetSocketStats returns a snapshot of kernel socket optimization counters.
func GetSocketStats() SocketStats {
	return SocketStats{
		TunedSockets: tunedSocketCount.Load(),
	}
}

// ControlDialer applies TCP_NODELAY on non-Windows hosts while preserving OS TCP auto-tuning on WAN sockets.
func ControlDialer(network, address string, c syscall.RawConn) error {
	_ = c.Control(func(fd uintptr) {
		tunedSocketCount.Add(1)
		_ = syscall.SetsockoptInt(int(fd), syscall.IPPROTO_TCP, syscall.TCP_NODELAY, 1)
	})
	return nil
}

// ControlListener applies tuning options to listening sockets.
func ControlListener(network, address string, c syscall.RawConn) error {
	_ = c.Control(func(fd uintptr) {
		_ = syscall.SetsockoptInt(int(fd), syscall.IPPROTO_TCP, syscall.TCP_NODELAY, 1)
	})
	return nil
}

// TuneAcceptedConn configures TCP_NODELAY on accepted connections.
func TuneAcceptedConn(conn net.Conn) {
	if tc, ok := conn.(*net.TCPConn); ok {
		_ = tc.SetNoDelay(true)
	}
}

// IsTCPConnAlive returns true on non-Windows builds (age bound still enforced by SpeculativeTCPPool).
func IsTCPConnAlive(conn net.Conn) bool {
	return conn != nil
}
