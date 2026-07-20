package main

import (
	"bytes"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/gorilla/websocket"
)

type sessionResponse struct {
	Code         string `json:"code"`
	BrowserToken string `json:"browserToken"`
}

func createTestSession(t *testing.T, url string) sessionResponse {
	t.Helper()
	response, err := http.Post(url+"/api/sessions", "application/json", nil)
	if err != nil {
		t.Fatal(err)
	}
	defer response.Body.Close()
	var result sessionResponse
	if err := json.NewDecoder(response.Body).Decode(&result); err != nil {
		t.Fatal(err)
	}
	return result
}

func pairTestRole(t *testing.T, url, code, role string) string {
	t.Helper()
	body, _ := json.Marshal(map[string]string{"code": code, "role": role})
	response, err := http.Post(url+"/api/sessions/pair", "application/json", bytes.NewReader(body))
	if err != nil {
		t.Fatal(err)
	}
	defer response.Body.Close()
	if response.StatusCode != http.StatusOK {
		t.Fatalf("pair %s: status %d", role, response.StatusCode)
	}
	var result struct {
		Token string `json:"token"`
	}
	_ = json.NewDecoder(response.Body).Decode(&result)
	return result.Token
}

func dialRole(t *testing.T, baseURL, role, token string) *websocket.Conn {
	t.Helper()
	wsURL := "ws" + strings.TrimPrefix(baseURL, "http") + "/ws/" + role + "?token=" + token
	conn, _, err := websocket.DefaultDialer.Dial(wsURL, nil)
	if err != nil {
		t.Fatalf("dial %s: %v", role, err)
	}
	return conn
}

func TestSessionRoutesJPEGAndAnglesWithinOneSession(t *testing.T) {
	server := httptest.NewServer(newServer().routes())
	defer server.Close()
	session := createTestSession(t, server.URL)
	pythonToken := pairTestRole(t, server.URL, session.Code, rolePython)
	unityToken := pairTestRole(t, server.URL, session.Code, roleUnity)
	browser := dialRole(t, server.URL, roleBrowser, session.BrowserToken)
	python := dialRole(t, server.URL, rolePython, pythonToken)
	unity := dialRole(t, server.URL, roleUnity, unityToken)
	defer browser.Close()
	defer python.Close()
	defer unity.Close()

	jpeg := []byte{0xff, 0xd8, 0x01, 0x02, 0xff, 0xd9}
	if err := browser.WriteMessage(websocket.BinaryMessage, jpeg); err != nil {
		t.Fatal(err)
	}
	_ = python.SetReadDeadline(time.Now().Add(2 * time.Second))
	messageType, received, err := python.ReadMessage()
	if err != nil || messageType != websocket.BinaryMessage || !bytes.Equal(received, jpeg) {
		t.Fatalf("python JPEG: type=%d payload=%v err=%v", messageType, received, err)
	}

	angles := GestureData{Type: "angles", Angles: []float64{10, 20, 30, 40, 50, 60}, Timestamp: 1.5, Sequence: 1}
	if err := python.WriteJSON(angles); err != nil {
		t.Fatal(err)
	}
	_ = unity.SetReadDeadline(time.Now().Add(2 * time.Second))
	_, payload, err := unity.ReadMessage()
	if err != nil {
		t.Fatal(err)
	}
	var receivedAngles GestureData
	_ = json.Unmarshal(payload, &receivedAngles)
	if receivedAngles.Sequence != 1 || receivedAngles.Angles[5] != 60 {
		t.Fatalf("unexpected angles: %+v", receivedAngles)
	}
}

func TestSessionsAreIsolated(t *testing.T) {
	server := httptest.NewServer(newServer().routes())
	defer server.Close()
	a := createTestSession(t, server.URL)
	b := createTestSession(t, server.URL)
	pythonA := dialRole(t, server.URL, rolePython, pairTestRole(t, server.URL, a.Code, rolePython))
	pythonB := dialRole(t, server.URL, rolePython, pairTestRole(t, server.URL, b.Code, rolePython))
	browserA := dialRole(t, server.URL, roleBrowser, a.BrowserToken)
	defer pythonA.Close()
	defer pythonB.Close()
	defer browserA.Close()

	jpeg := []byte{0xff, 0xd8, 0x42, 0xff, 0xd9}
	_ = browserA.WriteMessage(websocket.BinaryMessage, jpeg)
	_ = pythonA.SetReadDeadline(time.Now().Add(time.Second))
	if _, _, err := pythonA.ReadMessage(); err != nil {
		t.Fatalf("session A did not receive JPEG: %v", err)
	}
	_ = pythonB.SetReadDeadline(time.Now().Add(150 * time.Millisecond))
	if _, _, err := pythonB.ReadMessage(); err == nil {
		t.Fatal("session B received session A JPEG")
	}
}

func TestPairingRejectsDuplicateRoleAndInvalidCode(t *testing.T) {
	server := httptest.NewServer(newServer().routes())
	defer server.Close()
	session := createTestSession(t, server.URL)
	_ = pairTestRole(t, server.URL, session.Code, rolePython)
	body, _ := json.Marshal(map[string]string{"code": session.Code, "role": rolePython})
	response, _ := http.Post(server.URL+"/api/sessions/pair", "application/json", bytes.NewReader(body))
	if response.StatusCode != http.StatusConflict {
		t.Fatalf("duplicate role status: %d", response.StatusCode)
	}
	body, _ = json.Marshal(map[string]string{"code": "BAD-CODE", "role": roleUnity})
	response, _ = http.Post(server.URL+"/api/sessions/pair", "application/json", bytes.NewReader(body))
	if response.StatusCode != http.StatusBadRequest {
		t.Fatalf("invalid code status: %d", response.StatusCode)
	}
}

func TestCleanupRemovesExpiredSession(t *testing.T) {
	manager := newSessionManager()
	session, token, err := manager.create(time.Now().Add(-maxSessionAge - time.Minute))
	if err != nil {
		t.Fatal(err)
	}
	manager.cleanup(time.Now())
	if manager.sessionForToken(token, roleBrowser) != nil || manager.codes[session.Code] != nil {
		t.Fatal("expired session was not removed")
	}
}

func TestValidAngles(t *testing.T) {
	if !validAngles([]float64{0, 1, 2, 3, 4, 180}) {
		t.Fatal("valid angles rejected")
	}
	if validAngles([]float64{0, 1, 2}) || validAngles([]float64{0, 1, 2, 3, 4, 181}) {
		t.Fatal("invalid angles accepted")
	}
}
