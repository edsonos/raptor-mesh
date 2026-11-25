package protocol

import (
	"bytes"
	"net/http"
	"testing"
)

func TestFrameHeaderRoundTrip(t *testing.T) {
	in := FrameHeader{
		Version: Version1,
		Flags:   Flag0RTT | FlagHTTPFastPath | FlagPriority,
		Cmd:     CmdHTTPFast,
		Port:    443,
		Host:    "api.openai.com",
	}
	var buf bytes.Buffer
	if err := WriteHeader(&buf, in); err != nil {
		t.Fatalf("WriteHeader failed: %v", err)
	}
	out, err := ReadHeader(&buf)
	if err != nil {
		t.Fatalf("ReadHeader failed: %v", err)
	}
	if out != in {
		t.Fatalf("header mismatch: got %+v, want %+v", out, in)
	}
}

func TestFastHTTPRoundTrip(t *testing.T) {
	reqIn := FastHTTPRequest{
		Method: http.MethodGet,
		URL:    "https://claude.ai/",
		Headers: http.Header{
			"User-Agent":      {"Raptor-Mesh/1.0"},
			"Accept-Encoding": {"gzip"},
		},
		Body: []byte("payload-test"),
	}
	var reqBuf bytes.Buffer
	if err := WriteFastHTTPRequest(&reqBuf, reqIn); err != nil {
		t.Fatalf("WriteFastHTTPRequest failed: %v", err)
	}
	reqOut, err := ReadFastHTTPRequest(&reqBuf)
	if err != nil {
		t.Fatalf("ReadFastHTTPRequest failed: %v", err)
	}
	if reqOut.Method != reqIn.Method || reqOut.URL != reqIn.URL || !bytes.Equal(reqOut.Body, reqIn.Body) {
		t.Fatalf("request mismatch: got %+v, want %+v", reqOut, reqIn)
	}

	respIn := FastHTTPResponse{
		StatusCode:    http.StatusNoContent,
		UpstreamProto: "HTTP/3.0",
		Headers: http.Header{
			"Alt-Svc": {`h3=":443"; ma=86400`},
		},
		Body: []byte("ok"),
	}
	var respBuf bytes.Buffer
	if err := WriteFastHTTPResponse(&respBuf, respIn); err != nil {
		t.Fatalf("WriteFastHTTPResponse failed: %v", err)
	}
	respOut, err := ReadFastHTTPResponse(&respBuf)
	if err != nil {
		t.Fatalf("ReadFastHTTPResponse failed: %v", err)
	}
	if respOut.StatusCode != respIn.StatusCode || respOut.UpstreamProto != respIn.UpstreamProto || !bytes.Equal(respOut.Body, respIn.Body) {
		t.Fatalf("response mismatch: got %+v, want %+v", respOut, respIn)
	}
}

func TestSelfSignedTLSConfig(t *testing.T) {
	cfg, err := GenerateSelfSignedTLSConfig()
	if err != nil {
		t.Fatalf("GenerateSelfSignedTLSConfig failed: %v", err)
	}
	if len(cfg.Certificates) != 1 {
		t.Fatalf("expected 1 certificate, got %d", len(cfg.Certificates))
	}
	if len(cfg.NextProtos) != 1 || cfg.NextProtos[0] != ALPN {
		t.Fatalf("unexpected ALPN: %v", cfg.NextProtos)
	}
}
