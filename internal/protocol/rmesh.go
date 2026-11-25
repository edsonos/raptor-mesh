package protocol

import (
	"bufio"
	"crypto/ed25519"
	"crypto/rand"
	"crypto/tls"
	"crypto/x509"
	"crypto/x509/pkix"
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"math/big"
	"net"
	"net/http"
	"sync"
	"time"
)

const (
	// MagicByte identifies every RMESH/1 binary stream frame ('R').
	MagicByte byte = 0x52
	// Version1 is the wire protocol version.
	Version1 byte = 0x01
	// ALPN is negotiated over QUIC TLS 1.3 for RMESH/1 tunnels.
	ALPN = "rmesh/1"

	// Wire flags.
	Flag0RTT         byte = 1 << 0
	FlagHTTPFastPath byte = 1 << 1
	FlagConnect      byte = 1 << 2
	FlagKeepAlive    byte = 1 << 3
	FlagPriority     byte = 1 << 4

	// Command opcodes.
	CmdConnect  byte = 0x01
	CmdHTTPFast byte = 0x02
	CmdPing     byte = 0x03

	// Response status codes on tunnel stream.
	StatusOK  byte = 0x00
	StatusErr byte = 0x01
)

// FrameHeader is the compact 7-byte + hostname prefix for RMESH/1 streams.
type FrameHeader struct {
	Version byte
	Flags   byte
	Cmd     byte
	Port    uint16
	Host    string
}

// FastHTTPRequest is a compact binary representation of an HTTP request sent over a 0-RTT QUIC stream.
type FastHTTPRequest struct {
	Method  string
	URL     string
	Headers http.Header
	Body    []byte
}

// FastHTTPResponse is a compact binary response returned by raptor-server over a QUIC stream.
type FastHTTPResponse struct {
	StatusCode    int
	UpstreamProto string
	Headers       http.Header
	Body          []byte
}

var bufPool = sync.Pool{
	New: func() any {
		b := make([]byte, 64*1024)
		return &b
	},
}

// GetBuffer borrows a 64KB buffer from the zero-allocation pool.
func GetBuffer() *[]byte {
	return bufPool.Get().(*[]byte)
}

// PutBuffer returns a buffer to the pool.
func PutBuffer(b *[]byte) {
	if b != nil && cap(*b) >= 64*1024 {
		*b = (*b)[:64*1024]
		bufPool.Put(b)
	}
}

// CopyBidirectionalZeroAlloc relays data in both directions using pooled 64KB buffers.
// Preserves clean TCP half-close on EOF, and bounds orphan teardown on abrupt resets.
func CopyBidirectionalZeroAlloc(a, b io.ReadWriteCloser) {
	var wg sync.WaitGroup
	wg.Add(2)

	pipe := func(dst, src io.ReadWriteCloser) {
		defer wg.Done()
		bp := GetBuffer()
		defer PutBuffer(bp)
		_, err := io.CopyBuffer(dst, src, *bp)
		// Propagate half-close where supported so the peer sees EOF cleanly.
		if cw, ok := dst.(interface{ CloseWrite() error }); ok {
			_ = cw.CloseWrite()
		}
		// If this side errored abruptly (e.g. browser closed tab with RST), bound the reverse
		// direction's read deadline so orphaned half-open sockets cannot linger indefinitely.
		if err != nil {
			if dl, ok := dst.(interface{ SetReadDeadline(time.Time) error }); ok {
				_ = dl.SetReadDeadline(time.Now().Add(3 * time.Second))
			}
		}
	}

	go pipe(a, b)
	go pipe(b, a)
	wg.Wait()
	_ = a.Close()
	_ = b.Close()
}

// WriteHeader serializes an RMESH/1 frame header into w in a single allocation-free write.
func WriteHeader(w io.Writer, h FrameHeader) error {
	hostBytes := []byte(h.Host)
	if len(hostBytes) > 255 {
		return errors.New("rmesh: hostname exceeds 255 bytes")
	}
	var stack [264]byte
	stack[0] = MagicByte
	stack[1] = Version1
	stack[2] = h.Flags
	stack[3] = h.Cmd
	binary.BigEndian.PutUint16(stack[4:6], h.Port)
	stack[6] = byte(len(hostBytes))
	copy(stack[7:], hostBytes)
	_, err := w.Write(stack[:7+len(hostBytes)])
	return err
}

// ReadHeader reads and validates an RMESH/1 frame header from r.
func ReadHeader(r io.Reader) (FrameHeader, error) {
	var fixed [7]byte
	if _, err := io.ReadFull(r, fixed[:]); err != nil {
		return FrameHeader{}, err
	}
	if fixed[0] != MagicByte {
		return FrameHeader{}, fmt.Errorf("rmesh: invalid magic 0x%02x", fixed[0])
	}
	if fixed[1] != Version1 {
		return FrameHeader{}, fmt.Errorf("rmesh: unsupported version %d", fixed[1])
	}
	hostLen := int(fixed[6])
	var host string
	if hostLen > 0 {
		var hbuf [256]byte
		if _, err := io.ReadFull(r, hbuf[:hostLen]); err != nil {
			return FrameHeader{}, err
		}
		host = string(hbuf[:hostLen])
	}
	return FrameHeader{
		Version: fixed[1],
		Flags:   fixed[2],
		Cmd:     fixed[3],
		Port:    binary.BigEndian.Uint16(fixed[4:6]),
		Host:    host,
	}, nil
}

// WriteStatus writes a single-byte status + optional error message on a tunnel stream.
func WriteStatus(w io.Writer, code byte, msg string) error {
	if code == StatusOK {
		_, err := w.Write([]byte{StatusOK})
		return err
	}
	mb := []byte(msg)
	if len(mb) > 65535 {
		mb = mb[:65535]
	}
	buf := make([]byte, 3+len(mb))
	buf[0] = code
	binary.BigEndian.PutUint16(buf[1:3], uint16(len(mb)))
	copy(buf[3:], mb)
	_, err := w.Write(buf)
	return err
}

// ReadStatus reads the tunnel stream status reply.
func ReadStatus(r io.Reader) error {
	var b [1]byte
	if _, err := io.ReadFull(r, b[:]); err != nil {
		return err
	}
	if b[0] == StatusOK {
		return nil
	}
	var lb [2]byte
	if _, err := io.ReadFull(r, lb[:]); err != nil {
		return err
	}
	n := int(binary.BigEndian.Uint16(lb[:]))
	mb := make([]byte, n)
	if _, err := io.ReadFull(r, mb); err != nil {
		return err
	}
	return fmt.Errorf("rmesh server error: %s", string(mb))
}

// WriteFastHTTPRequest encodes an HTTP request into compact binary form over w.
func WriteFastHTTPRequest(w io.Writer, req FastHTTPRequest) error {
	bw := bufio.NewWriterSize(w, 8192)
	if err := writeStr16(bw, req.Method); err != nil {
		return err
	}
	if err := writeStr16(bw, req.URL); err != nil {
		return err
	}
	// Count total header pairs
	pairCount := 0
	for _, vals := range req.Headers {
		pairCount += len(vals)
	}
	if err := binary.Write(bw, binary.BigEndian, uint16(pairCount)); err != nil {
		return err
	}
	for k, vals := range req.Headers {
		for _, v := range vals {
			if err := writeStr16(bw, k); err != nil {
				return err
			}
			if err := writeStr16(bw, v); err != nil {
				return err
			}
		}
	}
	if err := binary.Write(bw, binary.BigEndian, uint32(len(req.Body))); err != nil {
		return err
	}
	if len(req.Body) > 0 {
		if _, err := bw.Write(req.Body); err != nil {
			return err
		}
	}
	return bw.Flush()
}

// ReadFastHTTPRequest decodes a compact binary HTTP request from r.
func ReadFastHTTPRequest(r io.Reader) (FastHTTPRequest, error) {
	br := bufio.NewReaderSize(r, 8192)
	method, err := readStr16(br)
	if err != nil {
		return FastHTTPRequest{}, err
	}
	rawURL, err := readStr16(br)
	if err != nil {
		return FastHTTPRequest{}, err
	}
	var pairCount uint16
	if err := binary.Read(br, binary.BigEndian, &pairCount); err != nil {
		return FastHTTPRequest{}, err
	}
	hdr := make(http.Header, pairCount)
	for i := 0; i < int(pairCount); i++ {
		k, err := readStr16(br)
		if err != nil {
			return FastHTTPRequest{}, err
		}
		v, err := readStr16(br)
		if err != nil {
			return FastHTTPRequest{}, err
		}
		hdr.Add(k, v)
	}
	var bodyLen uint32
	if err := binary.Read(br, binary.BigEndian, &bodyLen); err != nil {
		return FastHTTPRequest{}, err
	}
	if bodyLen > 32<<20 {
		return FastHTTPRequest{}, errors.New("rmesh: request body too large")
	}
	var body []byte
	if bodyLen > 0 {
		body = make([]byte, bodyLen)
		if _, err := io.ReadFull(br, body); err != nil {
			return FastHTTPRequest{}, err
		}
	}
	return FastHTTPRequest{
		Method:  method,
		URL:     rawURL,
		Headers: hdr,
		Body:    body,
	}, nil
}

// WriteFastHTTPResponse encodes a compact binary HTTP response over w.
func WriteFastHTTPResponse(w io.Writer, resp FastHTTPResponse) error {
	bw := bufio.NewWriterSize(w, 16384)
	if err := binary.Write(bw, binary.BigEndian, uint16(resp.StatusCode)); err != nil {
		return err
	}
	if err := writeStr16(bw, resp.UpstreamProto); err != nil {
		return err
	}
	pairCount := 0
	for _, vals := range resp.Headers {
		pairCount += len(vals)
	}
	if err := binary.Write(bw, binary.BigEndian, uint16(pairCount)); err != nil {
		return err
	}
	for k, vals := range resp.Headers {
		for _, v := range vals {
			if err := writeStr16(bw, k); err != nil {
				return err
			}
			if err := writeStr16(bw, v); err != nil {
				return err
			}
		}
	}
	if err := binary.Write(bw, binary.BigEndian, uint32(len(resp.Body))); err != nil {
		return err
	}
	if len(resp.Body) > 0 {
		if _, err := bw.Write(resp.Body); err != nil {
			return err
		}
	}
	return bw.Flush()
}

// ReadFastHTTPResponse decodes a compact binary HTTP response from r.
func ReadFastHTTPResponse(r io.Reader) (FastHTTPResponse, error) {
	br := bufio.NewReaderSize(r, 16384)
	var status uint16
	if err := binary.Read(br, binary.BigEndian, &status); err != nil {
		return FastHTTPResponse{}, err
	}
	proto, err := readStr16(br)
	if err != nil {
		return FastHTTPResponse{}, err
	}
	var pairCount uint16
	if err := binary.Read(br, binary.BigEndian, &pairCount); err != nil {
		return FastHTTPResponse{}, err
	}
	hdr := make(http.Header, pairCount)
	for i := 0; i < int(pairCount); i++ {
		k, err := readStr16(br)
		if err != nil {
			return FastHTTPResponse{}, err
		}
		v, err := readStr16(br)
		if err != nil {
			return FastHTTPResponse{}, err
		}
		hdr.Add(k, v)
	}
	var bodyLen uint32
	if err := binary.Read(br, binary.BigEndian, &bodyLen); err != nil {
		return FastHTTPResponse{}, err
	}
	if bodyLen > 64<<20 {
		return FastHTTPResponse{}, errors.New("rmesh: response body too large")
	}
	var body []byte
	if bodyLen > 0 {
		body = make([]byte, bodyLen)
		if _, err := io.ReadFull(br, body); err != nil {
			return FastHTTPResponse{}, err
		}
	}
	return FastHTTPResponse{
		StatusCode:    int(status),
		UpstreamProto: proto,
		Headers:       hdr,
		Body:          body,
	}, nil
}

func writeStr16(w io.Writer, s string) error {
	if len(s) > 65535 {
		s = s[:65535]
	}
	var lb [2]byte
	binary.BigEndian.PutUint16(lb[:], uint16(len(s)))
	if _, err := w.Write(lb[:]); err != nil {
		return err
	}
	if len(s) > 0 {
		_, err := io.WriteString(w, s)
		return err
	}
	return nil
}

func readStr16(r io.Reader) (string, error) {
	var lb [2]byte
	if _, err := io.ReadFull(r, lb[:]); err != nil {
		return "", err
	}
	n := int(binary.BigEndian.Uint16(lb[:]))
	if n == 0 {
		return "", nil
	}
	b := make([]byte, n)
	if _, err := io.ReadFull(r, b); err != nil {
		return "", err
	}
	return string(b), nil
}

// GenerateSelfSignedTLSConfig creates an ultra-fast Ed25519 TLS 1.3 certificate for RMESH/1 QUIC 0-RTT tunnels.
func GenerateSelfSignedTLSConfig() (*tls.Config, error) {
	pub, priv, err := ed25519.GenerateKey(rand.Reader)
	if err != nil {
		return nil, err
	}
	serial, err := rand.Int(rand.Reader, new(big.Int).Lsh(big.NewInt(1), 128))
	if err != nil {
		return nil, err
	}
	tmpl := x509.Certificate{
		SerialNumber: serial,
		Subject: pkix.Name{
			CommonName:   "raptor-mesh.local",
			Organization: []string{"Raptor-Mesh"},
		},
		NotBefore:             time.Now().Add(-time.Hour),
		NotAfter:              time.Now().Add(365 * 24 * time.Hour),
		KeyUsage:              x509.KeyUsageDigitalSignature,
		ExtKeyUsage:           []x509.ExtKeyUsage{x509.ExtKeyUsageServerAuth},
		BasicConstraintsValid: true,
		DNSNames:              []string{"localhost", "raptor-mesh.local"},
		IPAddresses:           []net.IP{net.ParseIP("127.0.0.1"), net.ParseIP("::1")},
	}
	derBytes, err := x509.CreateCertificate(rand.Reader, &tmpl, &tmpl, pub, priv)
	if err != nil {
		return nil, err
	}
	cert := tls.Certificate{
		Certificate: [][]byte{derBytes},
		PrivateKey:  priv,
	}
	return &tls.Config{
		Certificates: []tls.Certificate{cert},
		NextProtos:   []string{ALPN},
		MinVersion:   tls.VersionTLS13,
	}, nil
}
