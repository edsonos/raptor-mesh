//go:build windows

package transport

import (
	"net"
	"sync/atomic"
	"syscall"
	"unsafe"
)

const (
	// Windows ws2ipdef.h: TCP_FASTOPEN option for IPPROTO_TCP (Windows 10 1607+).
	winTCPFastOpen = 15
	// Windows mstcpip.h: SIO_LOOPBACK_FAST_PATH bypasses TCP/IP loopback overhead on 127.0.0.1 / ::1.
	winSIOLoopbackFastPath = 0x98000010
	// Winsock2 WSAPoll event flags (winsock2.h).
	winPollErr    int16 = 0x0001
	winPollHup    int16 = 0x0002
	winPollNval   int16 = 0x0004
	winPollRdNorm int16 = 0x0100
	// Large fixed buffers are applied ONLY to loopback sockets; WAN sockets leave SO_RCVBUF/SO_SNDBUF
	// untouched so Windows TCP Receive Window Auto-Tuning scales dynamically up to 16MB on fiber links.
	loopbackBufSize = 512 * 1024
)

var (
	ws2_32      = syscall.NewLazyDLL("ws2_32.dll")
	procWSAPoll = ws2_32.NewProc("WSAPoll")
)

type wsaPollFD struct {
	fd      uintptr
	events  int16
	revents int16
}

// SocketStats exposes low-level kernel socket optimization counters.
type SocketStats struct {
	TFOEnabled          int64 `json:"tfo_enabled"`
	LoopbackFastEnabled int64 `json:"loopback_fast_path_enabled"`
	TunedSockets        int64 `json:"tuned_sockets"`
}

var (
	tfoEnabledCount          atomic.Int64
	loopbackFastEnabledCount atomic.Int64
	tunedSocketCount         atomic.Int64
)

// GetSocketStats returns a snapshot of kernel socket optimization counters.
func GetSocketStats() SocketStats {
	return SocketStats{
		TFOEnabled:          tfoEnabledCount.Load(),
		LoopbackFastEnabled: loopbackFastEnabledCount.Load(),
		TunedSockets:        tunedSocketCount.Load(),
	}
}

// ControlDialer applies TCP_NODELAY, TCP_FASTOPEN, and loopback fast path on outbound sockets,
// while preserving Windows TCP Receive Window Auto-Tuning on external WAN connections.
func ControlDialer(network, address string, c syscall.RawConn) error {
	isLoopback := isLoopbackAddress(address)
	_ = c.Control(func(fd uintptr) {
		h := syscall.Handle(fd)
		tunedSocketCount.Add(1)
		_ = syscall.SetsockoptInt(h, syscall.IPPROTO_TCP, syscall.TCP_NODELAY, 1)

		if err := syscall.SetsockoptInt(h, syscall.IPPROTO_TCP, winTCPFastOpen, 1); err == nil {
			tfoEnabledCount.Add(1)
		}
		if isLoopback {
			_ = syscall.SetsockoptInt(h, syscall.SOL_SOCKET, syscall.SO_SNDBUF, loopbackBufSize)
			_ = syscall.SetsockoptInt(h, syscall.SOL_SOCKET, syscall.SO_RCVBUF, loopbackBufSize)
			if enableLoopbackFastPath(h) {
				loopbackFastEnabledCount.Add(1)
			}
		}
	})
	return nil
}

// ControlListener applies TCP_FASTOPEN, large loopback buffers, and SIO_LOOPBACK_FAST_PATH on local listeners.
func ControlListener(network, address string, c syscall.RawConn) error {
	isLoopback := isLoopbackAddress(address)
	_ = c.Control(func(fd uintptr) {
		h := syscall.Handle(fd)
		_ = syscall.SetsockoptInt(h, syscall.IPPROTO_TCP, syscall.TCP_NODELAY, 1)
		if err := syscall.SetsockoptInt(h, syscall.IPPROTO_TCP, winTCPFastOpen, 1); err == nil {
			tfoEnabledCount.Add(1)
		}
		if isLoopback {
			_ = syscall.SetsockoptInt(h, syscall.SOL_SOCKET, syscall.SO_SNDBUF, loopbackBufSize)
			_ = syscall.SetsockoptInt(h, syscall.SOL_SOCKET, syscall.SO_RCVBUF, loopbackBufSize)
			if enableLoopbackFastPath(h) {
				loopbackFastEnabledCount.Add(1)
			}
		}
	})
	return nil
}

// TuneAcceptedConn configures TCP_NODELAY (and loopback buffers only on loopback peers).
func TuneAcceptedConn(conn net.Conn) {
	if tc, ok := conn.(*net.TCPConn); ok {
		_ = tc.SetNoDelay(true)
		if raddr, ok := tc.RemoteAddr().(*net.TCPAddr); ok && raddr.IP != nil && raddr.IP.IsLoopback() {
			_ = tc.SetReadBuffer(loopbackBufSize)
			_ = tc.SetWriteBuffer(loopbackBufSize)
		}
	}
}

// IsTCPConnAlive performs a non-blocking zero-copy socket health check via Windows WSAPoll(timeout=0)
// before reusing a speculative pre-connected socket, catching peer FIN/RST in ~1µs with zero IOCP interference.
func IsTCPConnAlive(conn net.Conn) bool {
	sc, ok := conn.(syscall.Conn)
	if !ok {
		return true
	}
	raw, err := sc.SyscallConn()
	if err != nil {
		return false
	}
	alive := true
	if cerr := raw.Control(func(fd uintptr) {
		pfd := wsaPollFD{
			fd:     fd,
			events: winPollRdNorm,
		}
		r1, _, _ := procWSAPoll.Call(
			uintptr(unsafe.Pointer(&pfd)),
			1,
			0, // timeout = 0ms (strictly non-blocking)
		)
		rc := int32(r1)
		if rc < 0 {
			alive = false
			return
		}
		if rc > 0 && (pfd.revents&(winPollHup|winPollErr|winPollNval|winPollRdNorm)) != 0 {
			// Either peer closed/reset (POLLHUP/POLLERR/POLLRDNORM EOF) or invalid handle.
			alive = false
			return
		}
		alive = true
	}); cerr != nil {
		return false
	}
	return alive
}

func enableLoopbackFastPath(h syscall.Handle) bool {
	var on uint32 = 1
	var bytesReturned uint32
	err := syscall.WSAIoctl(
		h,
		winSIOLoopbackFastPath,
		(*byte)(unsafe.Pointer(&on)),
		uint32(unsafe.Sizeof(on)),
		nil,
		0,
		&bytesReturned,
		nil,
		0,
	)
	return err == nil
}

func isLoopbackAddress(address string) bool {
	host, _, err := net.SplitHostPort(address)
	if err != nil {
		return false
	}
	if host == "localhost" {
		return true
	}
	ip := net.ParseIP(host)
	return ip != nil && ip.IsLoopback()
}
