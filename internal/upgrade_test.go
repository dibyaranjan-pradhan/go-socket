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
	"github.com/gorilla/websocket"
)

func TestEngineIOUpgradeProbeFlow(t *testing.T) {
	h := NewEngineIOHandler(time.Second, 2*time.Second, 1<<20, NewWSUpgrader(4096, 4096, nil), nil, nil)

	// Handshake
	req := httptest.NewRequest(http.MethodGet, "/?EIO=4&transport=polling", nil)
	rec := httptest.NewRecorder()
	h.ServeHTTP(rec, req)
	openPkt, _ := engineio.Decode(rec.Body.Bytes())
	hs, _ := engineio.ParseHandshake(openPkt.Data)

	// Probe ping
	probeWire, _ := engineio.Encode(engineio.Packet{Type: engineio.Ping, Data: []byte(engineio.ProbePayload)})
	postReq := httptest.NewRequest(http.MethodPost, "/?EIO=4&transport=polling&sid="+hs.SID, strings.NewReader(string(probeWire)))
	postRec := httptest.NewRecorder()
	h.ServeHTTP(postRec, postReq)

	// Upgrade packet
	upWire, _ := engineio.Encode(engineio.Packet{Type: engineio.Upgrade})
	postReq2 := httptest.NewRequest(http.MethodPost, "/?EIO=4&transport=polling&sid="+hs.SID, strings.NewReader(string(upWire)))
	postRec2 := httptest.NewRecorder()
	h.ServeHTTP(postRec2, postReq2)

	if !h.manager.readyForUpgrade(hs.SID) {
		t.Fatal("session not ready for upgrade")
	}
}

func TestEngineIOUpgradeRejectsWithoutProbe(t *testing.T) {
	h := NewEngineIOHandler(time.Second, time.Second, 1000, NewWSUpgrader(0, 0, nil), nil, nil)
	req := httptest.NewRequest(http.MethodGet, "/?EIO=4&transport=polling", nil)
	rec := httptest.NewRecorder()
	h.ServeHTTP(rec, req)
	openPkt, _ := engineio.Decode(rec.Body.Bytes())
	hs, _ := engineio.ParseHandshake(openPkt.Data)

	wsReq := httptest.NewRequest(http.MethodGet, "/?EIO=4&transport=websocket&sid="+hs.SID, nil)
	wsReq.Header.Set("Connection", "Upgrade")
	wsReq.Header.Set("Upgrade", "websocket")
	wsReq.Header.Set("Sec-WebSocket-Version", "13")
	wsReq.Header.Set("Sec-WebSocket-Key", "dGhlIHNhbXBsZSBub25jZQ==")
	wsRec := httptest.NewRecorder()
	h.ServeHTTP(wsRec, wsReq)
	if wsRec.Code != http.StatusBadRequest {
		t.Fatalf("status = %d", wsRec.Code)
	}
}

func TestSessionTransportSwap(t *testing.T) {
	pt := newPollingTransport(testSession())
	st := newSessionTransport(pt)
	ft := newFakeTransport()
	st.Swap(ft)
	if st.Name() != "fake" {
		t.Fatalf("name = %q", st.Name())
	}
}

func TestEIOWebSocketTransportWriteRead(t *testing.T) {
	ft := newFakeTransport()
	eio := newEIOWebSocketTransport(ft)
	payload := []byte(`{"event":"x"}`)
	if err := eio.Write(payload); err != nil {
		t.Fatal(err)
	}
	select {
	case wire := <-ft.writes:
		pkt, err := engineio.Decode(wire)
		if err != nil || pkt.Type != engineio.Message {
			t.Fatalf("pkt=%+v err=%v", pkt, err)
		}
	case <-time.After(time.Second):
		t.Fatal("timeout")
	}

	msgWire, _ := engineio.Encode(engineio.Packet{Type: engineio.Message, Data: payload})
	ft.reads <- msgWire
	got, err := eio.Read()
	if err != nil {
		t.Fatal(err)
	}
	if string(got) != string(payload) {
		t.Fatalf("got %q", got)
	}
}

func TestNativeTransportImplementsTransport(t *testing.T) {
	var _ Transport = (*nativeTransport)(nil)
}

func TestApplyUpgradeDirect(t *testing.T) {
	m := newPollManager(time.Second, time.Second, 1000)
	sess, st := m.create()
	m.markProbed(sess.ID)
	m.markUpgrade(sess.ID)
	ft := newFakeTransport()
	got, ok := m.applyUpgrade(sess.ID, ft)
	if !ok || got != st {
		t.Fatalf("applyUpgrade ok=%v got=%p st=%p", ok, got, st)
	}
	if got.Name() != engineio.TransportWebSocket {
		t.Fatalf("name = %q", got.Name())
	}
}

func TestEIOWebSocketTransportPingClose(t *testing.T) {
	ft := newFakeTransport()
	eio := newEIOWebSocketTransport(ft)
	eio.SetReadLimit(100)
	_ = eio.SetReadDeadline(time.Now().Add(time.Second))
	_ = eio.SetWriteDeadline(time.Now().Add(time.Second))
	eio.OnPong(func() {})
	if err := eio.Ping(); err != nil {
		t.Fatal(err)
	}
	if err := eio.SendClose(); err != nil {
		t.Fatal(err)
	}
	_ = eio.Close()
}

func TestEIOWebSocketTransportPongAndClosePacket(t *testing.T) {
	ft := newFakeTransport()
	eio := newEIOWebSocketTransport(ft)
	ponged := false
	eio.OnPong(func() { ponged = true })
	pongWire, _ := engineio.Encode(engineio.Packet{Type: engineio.Pong})
	closeWire, _ := engineio.Encode(engineio.Packet{Type: engineio.Close})
	ft.reads <- pongWire
	ft.reads <- closeWire
	_, err := eio.Read()
	if err != io.EOF {
		t.Fatalf("err = %v", err)
	}
	if !ponged {
		t.Fatal("pong handler not called")
	}
}

func TestWriteEIOBatchHelper(t *testing.T) {
	body, err := writeEIOBatch([]engineio.Packet{{Type: engineio.Ping}, {Type: engineio.Message, Data: []byte("x")}})
	if err != nil || len(body) == 0 {
		t.Fatalf("body=%q err=%v", body, err)
	}
}

func TestEIOWebSocketTransportInboundPing(t *testing.T) {
	ft := newFakeTransport()
	eio := newEIOWebSocketTransport(ft)
	pingWire, _ := engineio.Encode(engineio.Packet{Type: engineio.Ping, Data: []byte("hb")})
	ft.reads <- pingWire
	msgWire, _ := engineio.Encode(engineio.Packet{Type: engineio.Message, Data: []byte("ok")})
	ft.reads <- msgWire
	got, err := eio.Read()
	if err != nil {
		t.Fatal(err)
	}
	if string(got) != "ok" {
		t.Fatalf("got %q", got)
	}
	select {
	case wire := <-ft.writes:
		pkt, _ := engineio.Decode(wire)
		if pkt.Type != engineio.Pong {
			t.Fatalf("expected pong, got %v", pkt.Type)
		}
	case <-time.After(time.Second):
		t.Fatal("timeout waiting for pong reply")
	}
}

func TestSessionTransportReadAfterUpgradeSwap(t *testing.T) {
	pt := newPollingTransport(testSession())
	st := newSessionTransport(pt)
	ft := newFakeTransport()
	ft.reads <- []byte("after-upgrade")
	go func() {
		time.Sleep(5 * time.Millisecond)
		st.Swap(ft)
		pt.signalUpgrade()
	}()
	got, err := st.Read()
	if err != nil {
		t.Fatal(err)
	}
	if string(got) != "after-upgrade" {
		t.Fatalf("got %q", got)
	}
}

func TestEngineIOWebSocketUpgradeHTTP(t *testing.T) {
	h := NewEngineIOHandler(time.Second, 2*time.Second, 1<<20, NewWSUpgrader(4096, 4096, nil), nil, nil)
	req := httptest.NewRequest(http.MethodGet, "/?EIO=4&transport=polling", nil)
	rec := httptest.NewRecorder()
	h.ServeHTTP(rec, req)
	openPkt, _ := engineio.Decode(rec.Body.Bytes())
	hs, _ := engineio.ParseHandshake(openPkt.Data)

	probeWire, _ := engineio.Encode(engineio.Packet{Type: engineio.Ping, Data: []byte(engineio.ProbePayload)})
	postRec := httptest.NewRecorder()
	h.ServeHTTP(postRec, httptest.NewRequest(http.MethodPost, "/?EIO=4&transport=polling&sid="+hs.SID, strings.NewReader(string(probeWire))))
	if postRec.Code != http.StatusOK {
		t.Fatalf("probe status = %d", postRec.Code)
	}
	upWire, _ := engineio.Encode(engineio.Packet{Type: engineio.Upgrade})
	upRec := httptest.NewRecorder()
	h.ServeHTTP(upRec, httptest.NewRequest(http.MethodPost, "/?EIO=4&transport=polling&sid="+hs.SID, strings.NewReader(string(upWire))))

	srv := httptest.NewServer(http.HandlerFunc(h.ServeHTTP))
	defer srv.Close()
	wsURL := "ws" + strings.TrimPrefix(srv.URL, "http") + "/?EIO=4&transport=websocket&sid=" + hs.SID
	conn, _, err := websocket.DefaultDialer.Dial(wsURL, nil)
	if err != nil {
		t.Fatal(err)
	}
	defer conn.Close()
	msgWire, _ := engineio.Encode(engineio.Packet{Type: engineio.Message, Data: []byte(`{"event":"x"}`)})
	if err := conn.WriteMessage(websocket.TextMessage, msgWire); err != nil {
		t.Fatal(err)
	}
}

func TestSessionTransportDelegates(t *testing.T) {
	ft := newFakeTransport()
	st := newSessionTransport(ft)
	st.SetReadLimit(10)
	_ = st.SetReadDeadline(time.Now().Add(time.Second))
	_ = st.SetWriteDeadline(time.Now().Add(time.Second))
	st.OnPong(func() {})
	_ = st.Ping()
	_ = st.SendClose()
}
