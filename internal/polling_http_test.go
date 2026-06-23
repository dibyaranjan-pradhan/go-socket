// Copyright (c) 2026 Dibyaranjan Pradhan. All rights reserved.

package internal

import (
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/dibyaranjan-pradhan/go-socket/internal/engineio"
)

func TestPollHandlerHandshake(t *testing.T) {
	var attached bool
	h := NewPollHandler(time.Second, 2*time.Second, 1<<20, func(Transport, *http.Request) {
		attached = true
	}, nil)

	req := httptest.NewRequest(http.MethodGet, "/?EIO=4&transport=polling", nil)
	rec := httptest.NewRecorder()
	h.ServeHTTP(rec, req)

	if rec.Code != http.StatusOK {
		t.Fatalf("status = %d", rec.Code)
	}
	if !strings.HasPrefix(rec.Body.String(), "0") {
		t.Fatalf("body = %q", rec.Body.String())
	}
	time.Sleep(20 * time.Millisecond)
	if !attached {
		t.Fatal("attach callback not invoked")
	}
	if h.SessionCount() != 1 {
		t.Fatalf("sessions = %d", h.SessionCount())
	}
}

func TestPollHandlerRejectsBadEIO(t *testing.T) {
	h := NewPollHandler(time.Second, time.Second, 1000, nil, nil)
	req := httptest.NewRequest(http.MethodGet, "/?EIO=3&transport=polling", nil)
	rec := httptest.NewRecorder()
	h.ServeHTTP(rec, req)
	if rec.Code != http.StatusBadRequest {
		t.Fatalf("status = %d", rec.Code)
	}
}

func TestPollHandlerRejectsUnknownSID(t *testing.T) {
	h := NewPollHandler(time.Second, time.Second, 1000, nil, nil)
	req := httptest.NewRequest(http.MethodGet, "/?EIO=4&transport=polling&sid=missing", nil)
	rec := httptest.NewRecorder()
	h.ServeHTTP(rec, req)
	if rec.Code != http.StatusBadRequest {
		t.Fatalf("status = %d", rec.Code)
	}
}

func TestPollHandlerPostAndPoll(t *testing.T) {
	h := NewPollHandler(50*time.Millisecond, 200*time.Millisecond, 1<<20, nil, nil)

	// Handshake
	req := httptest.NewRequest(http.MethodGet, "/?EIO=4&transport=polling", nil)
	rec := httptest.NewRecorder()
	h.ServeHTTP(rec, req)
	openPkt, err := engineio.Decode(rec.Body.Bytes())
	if err != nil {
		t.Fatal(err)
	}
	hs, err := engineio.ParseHandshake(openPkt.Data)
	if err != nil {
		t.Fatal(err)
	}

	// POST message
	msgWire, _ := engineio.Encode(engineio.Packet{
		Type: engineio.Message,
		Data: []byte(`{"event":"ping","payload":{}}`),
	})
	postReq := httptest.NewRequest(http.MethodPost, "/?EIO=4&transport=polling&sid="+hs.SID, strings.NewReader(string(msgWire)))
	postRec := httptest.NewRecorder()
	h.ServeHTTP(postRec, postReq)
	if postRec.Code != http.StatusOK {
		t.Fatalf("post status = %d", postRec.Code)
	}

	pt, ok := h.manager.get(hs.SID)
	if !ok {
		t.Fatal("session missing")
	}
	_ = pt.SetReadDeadline(time.Now().Add(time.Second))
	payload, err := pt.Read()
	if err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(string(payload), "ping") {
		t.Fatalf("payload = %q", payload)
	}
}

func TestPollHandlerClosedServer(t *testing.T) {
	h := NewPollHandler(time.Second, time.Second, 1000, nil, func() bool { return true })
	req := httptest.NewRequest(http.MethodGet, "/?EIO=4&transport=polling", nil)
	rec := httptest.NewRecorder()
	h.ServeHTTP(rec, req)
	if rec.Code != http.StatusServiceUnavailable {
		t.Fatalf("status = %d", rec.Code)
	}
}

func TestPollHandlerPostTooLarge(t *testing.T) {
	h := NewPollHandler(time.Second, time.Second, 10, nil, nil)
	req := httptest.NewRequest(http.MethodGet, "/?EIO=4&transport=polling", nil)
	rec := httptest.NewRecorder()
	h.ServeHTTP(rec, req)
	openPkt, _ := engineio.Decode(rec.Body.Bytes())
	hs, _ := engineio.ParseHandshake(openPkt.Data)

	big := strings.Repeat("a", 20)
	postReq := httptest.NewRequest(http.MethodPost, "/?EIO=4&transport=polling&sid="+hs.SID, strings.NewReader(big))
	postRec := httptest.NewRecorder()
	h.ServeHTTP(postRec, postReq)
	if postRec.Code != http.StatusRequestEntityTooLarge {
		t.Fatalf("status = %d", postRec.Code)
	}
}

func TestPollHandlerMethodNotAllowed(t *testing.T) {
	h := NewPollHandler(time.Second, time.Second, 1000, nil, nil)
	req := httptest.NewRequest(http.MethodGet, "/?EIO=4&transport=polling", nil)
	rec := httptest.NewRecorder()
	h.ServeHTTP(rec, req)
	openPkt, _ := engineio.Decode(rec.Body.Bytes())
	hs, _ := engineio.ParseHandshake(openPkt.Data)

	putReq := httptest.NewRequest(http.MethodPut, "/?EIO=4&transport=polling&sid="+hs.SID, nil)
	putRec := httptest.NewRecorder()
	h.ServeHTTP(putRec, putReq)
	if putRec.Code != http.StatusMethodNotAllowed {
		t.Fatalf("status = %d", putRec.Code)
	}
}

func TestIsPollingRequest(t *testing.T) {
	req := httptest.NewRequest(http.MethodGet, "/?EIO=4&transport=polling", nil)
	if !IsPollingRequest(req) {
		t.Fatal("expected polling request")
	}
}

func TestPollHandlerPollGET(t *testing.T) {
	h := NewPollHandler(50*time.Millisecond, 100*time.Millisecond, 1000, nil, nil)
	req := httptest.NewRequest(http.MethodGet, "/?EIO=4&transport=polling", nil)
	rec := httptest.NewRecorder()
	h.ServeHTTP(rec, req)
	openPkt, _ := engineio.Decode(rec.Body.Bytes())
	hs, _ := engineio.ParseHandshake(openPkt.Data)

	pt, _ := h.manager.get(hs.SID)
	_ = pt.Write([]byte(`{"event":"x"}`))

	pollReq := httptest.NewRequest(http.MethodGet, "/?EIO=4&transport=polling&sid="+hs.SID, nil)
	pollRec := httptest.NewRecorder()
	h.ServeHTTP(pollRec, pollReq)
	if pollRec.Code != http.StatusOK {
		t.Fatalf("status = %d", pollRec.Code)
	}
	if len(pollRec.Body.Bytes()) == 0 {
		t.Fatal("expected poll body")
	}
}

func TestPollHandlerBadPostBatch(t *testing.T) {
	h := NewPollHandler(time.Second, time.Second, 1000, nil, nil)
	req := httptest.NewRequest(http.MethodGet, "/?EIO=4&transport=polling", nil)
	rec := httptest.NewRecorder()
	h.ServeHTTP(rec, req)
	openPkt, _ := engineio.Decode(rec.Body.Bytes())
	hs, _ := engineio.ParseHandshake(openPkt.Data)

	postReq := httptest.NewRequest(http.MethodPost, "/?EIO=4&transport=polling&sid="+hs.SID, strings.NewReader("not-a-packet"))
	postRec := httptest.NewRecorder()
	h.ServeHTTP(postRec, postReq)
	if postRec.Code != http.StatusBadRequest {
		t.Fatalf("status = %d", postRec.Code)
	}
}

func TestPollHandlerRejectsBadTransport(t *testing.T) {
	h := NewPollHandler(time.Second, time.Second, 1000, nil, nil)
	req := httptest.NewRequest(http.MethodGet, "/?EIO=4&transport=websocket", nil)
	rec := httptest.NewRecorder()
	h.ServeHTTP(rec, req)
	if rec.Code != http.StatusBadRequest {
		t.Fatalf("status = %d", rec.Code)
	}
}

func TestPollHandlerPollAfterClose(t *testing.T) {
	h := NewPollHandler(time.Second, time.Second, 1000, nil, nil)
	req := httptest.NewRequest(http.MethodGet, "/?EIO=4&transport=polling", nil)
	rec := httptest.NewRecorder()
	h.ServeHTTP(rec, req)
	openPkt, _ := engineio.Decode(rec.Body.Bytes())
	hs, _ := engineio.ParseHandshake(openPkt.Data)

	closeWire, _ := engineio.Encode(engineio.Packet{Type: engineio.Close})
	postReq := httptest.NewRequest(http.MethodPost, "/?EIO=4&transport=polling&sid="+hs.SID, strings.NewReader(string(closeWire)))
	postRec := httptest.NewRecorder()
	h.ServeHTTP(postRec, postReq)

	pollReq := httptest.NewRequest(http.MethodGet, "/?EIO=4&transport=polling&sid="+hs.SID, nil)
	pollRec := httptest.NewRecorder()
	h.ServeHTTP(pollRec, pollReq)
	if pollRec.Code != http.StatusBadRequest {
		t.Fatalf("status = %d body=%s", pollRec.Code, pollRec.Body.String())
	}
}

func TestPollHandlerPollEmptyWake(t *testing.T) {
	h := NewPollHandler(50*time.Millisecond, 100*time.Millisecond, 1000, nil, nil)
	req := httptest.NewRequest(http.MethodGet, "/?EIO=4&transport=polling", nil)
	rec := httptest.NewRecorder()
	h.ServeHTTP(rec, req)
	openPkt, _ := engineio.Decode(rec.Body.Bytes())
	hs, _ := engineio.ParseHandshake(openPkt.Data)

	pt, _ := h.manager.get(hs.SID)
	pt.signalPoll()

	pollReq := httptest.NewRequest(http.MethodGet, "/?EIO=4&transport=polling&sid="+hs.SID, nil)
	pollRec := httptest.NewRecorder()
	h.ServeHTTP(pollRec, pollReq)
	if pollRec.Code != http.StatusOK {
		t.Fatalf("status = %d", pollRec.Code)
	}
}

func TestPollHandlerCloseViaPost(t *testing.T) {
	h := NewPollHandler(time.Second, time.Second, 1000, nil, nil)
	req := httptest.NewRequest(http.MethodGet, "/?EIO=4&transport=polling", nil)
	rec := httptest.NewRecorder()
	h.ServeHTTP(rec, req)
	openPkt, _ := engineio.Decode(rec.Body.Bytes())
	hs, _ := engineio.ParseHandshake(openPkt.Data)

	closeWire, _ := engineio.Encode(engineio.Packet{Type: engineio.Close})
	postReq := httptest.NewRequest(http.MethodPost, "/?EIO=4&transport=polling&sid="+hs.SID, strings.NewReader(string(closeWire)))
	postRec := httptest.NewRecorder()
	h.ServeHTTP(postRec, postReq)
	if postRec.Code != http.StatusBadRequest {
		t.Fatalf("status = %d body=%s", postRec.Code, postRec.Body.String())
	}
	_ = io.EOF
}
