package main

import (
	"bytes"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/gorilla/websocket"
)

type sessionResponse struct {
	Code         string `json:"code"`
	BrowserToken string `json:"browserToken"`
}

type activation struct{ code, token string }
type fakeAnalyzer struct {
	mu            sync.Mutex
	activations   []activation
	deactivations []string
	err           error
}

func (f *fakeAnalyzer) Ready() bool { return true }
func (f *fakeAnalyzer) Activate(code, token string, _ time.Time) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.activations = append(f.activations, activation{code, token})
	return f.err
}
func (f *fakeAnalyzer) Deactivate(code string) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.deactivations = append(f.deactivations, code)
	return nil
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

func pairUnity(t *testing.T, url, code string) string {
	t.Helper()
	body, _ := json.Marshal(map[string]string{"code": code, "role": roleUnity})
	response, err := http.Post(url+"/api/sessions/pair", "application/json", bytes.NewReader(body))
	if err != nil {
		t.Fatal(err)
	}
	defer response.Body.Close()
	if response.StatusCode != http.StatusOK {
		t.Fatalf("pair status: %d", response.StatusCode)
	}
	var result struct {
		Token string `json:"token"`
	}
	_ = json.NewDecoder(response.Body).Decode(&result)
	return result.Token
}

func dialRole(t *testing.T, baseURL, role, token string) *websocket.Conn {
	t.Helper()
	url := "ws" + strings.TrimPrefix(baseURL, "http") + "/ws/" + role + "?token=" + token
	conn, _, err := websocket.DefaultDialer.Dial(url, nil)
	if err != nil {
		t.Fatalf("dial %s: %v", role, err)
	}
	return conn
}

func readType(t *testing.T, conn *websocket.Conn, wanted string) []byte {
	t.Helper()
	_ = conn.SetReadDeadline(time.Now().Add(2 * time.Second))
	for {
		_, payload, err := conn.ReadMessage()
		if err != nil {
			t.Fatal(err)
		}
		var envelope struct {
			Type string `json:"type"`
		}
		_ = json.Unmarshal(payload, &envelope)
		if envelope.Type == wanted {
			return payload
		}
	}
}

func waitActivation(t *testing.T, analyzer *fakeAnalyzer, code string) activation {
	t.Helper()
	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		analyzer.mu.Lock()
		for _, item := range analyzer.activations {
			if item.code == code {
				analyzer.mu.Unlock()
				return item
			}
		}
		analyzer.mu.Unlock()
		time.Sleep(10 * time.Millisecond)
	}
	t.Fatal("analysis activation was not requested")
	return activation{}
}

func TestSessionAutomaticallyActivatesPythonAndRoutesData(t *testing.T) {
	analyzer := &fakeAnalyzer{}
	control := newServerWithAnalyzer(analyzer)
	server := httptest.NewServer(control.routes())
	defer server.Close()
	session := createTestSession(t, server.URL)
	activation := waitActivation(t, analyzer, session.Code)
	if activation.token == "" {
		t.Fatal("empty Python token")
	}

	browser := dialRole(t, server.URL, roleBrowser, session.BrowserToken)
	python := dialRole(t, server.URL, rolePython, activation.token)
	unity := dialRole(t, server.URL, roleUnity, pairUnity(t, server.URL, session.Code))
	defer browser.Close()
	defer python.Close()
	defer unity.Close()

	jpeg := []byte{0xff, 0xd8, 1, 2, 0xff, 0xd9}
	_ = browser.WriteMessage(websocket.BinaryMessage, jpeg)
	messageType, received, err := python.ReadMessage()
	if err != nil || messageType != websocket.BinaryMessage || !bytes.Equal(received, jpeg) {
		t.Fatalf("Python JPEG: %v %v", received, err)
	}
	_ = python.WriteJSON(AnalysisMessage{
		Type: "analysis", Detected: true, Angles: []float64{10, 20, 30, 40, 50, 60}, ProcessingMS: 8,
	})
	var webResult AnalysisMessage
	_ = json.Unmarshal(readType(t, browser, "analysis"), &webResult)
	var unityResult GestureData
	_ = json.Unmarshal(readType(t, unity, "angles"), &unityResult)
	if webResult.Angles[5] != 60 || unityResult.Angles[5] != 60 || webResult.Sequence != unityResult.Sequence {
		t.Fatalf("unexpected results: web=%+v unity=%+v", webResult, unityResult)
	}
}

func TestSessionsAreIsolated(t *testing.T) {
	analyzer := &fakeAnalyzer{}
	control := newServerWithAnalyzer(analyzer)
	server := httptest.NewServer(control.routes())
	defer server.Close()
	first := createTestSession(t, server.URL)
	second := createTestSession(t, server.URL)
	firstPython := dialRole(t, server.URL, rolePython, waitActivation(t, analyzer, first.Code).token)
	secondPython := dialRole(t, server.URL, rolePython, waitActivation(t, analyzer, second.Code).token)
	firstBrowser := dialRole(t, server.URL, roleBrowser, first.BrowserToken)
	secondBrowser := dialRole(t, server.URL, roleBrowser, second.BrowserToken)
	defer firstPython.Close()
	defer secondPython.Close()
	defer firstBrowser.Close()
	defer secondBrowser.Close()
	_ = firstBrowser.WriteMessage(websocket.BinaryMessage, []byte{0xff, 0xd8, 1, 0xff, 0xd9})
	_, frame, err := firstPython.ReadMessage()
	if err != nil || frame[2] != 1 {
		t.Fatal("first session did not receive its frame")
	}
	_ = secondPython.SetReadDeadline(time.Now().Add(150 * time.Millisecond))
	if _, _, err := secondPython.ReadMessage(); err == nil {
		t.Fatal("frame crossed sessions")
	}
}

func TestPythonTokenCannotBeUsedForAnotherRole(t *testing.T) {
	analyzer := &fakeAnalyzer{}
	control := newServerWithAnalyzer(analyzer)
	server := httptest.NewServer(control.routes())
	defer server.Close()
	session := createTestSession(t, server.URL)
	token := waitActivation(t, analyzer, session.Code).token
	url := "ws" + strings.TrimPrefix(server.URL, "http") + "/ws/unity?token=" + token
	_, response, err := websocket.DefaultDialer.Dial(url, nil)
	if err == nil || response.StatusCode != http.StatusUnauthorized {
		t.Fatal("Python token accepted for Unity")
	}
}

func TestHTTPActivationContract(t *testing.T) {
	var method, path, secret string
	var body map[string]string
	worker := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		method, path, secret = r.Method, r.URL.Path, r.Header.Get("X-Internal-Token")
		_ = json.NewDecoder(r.Body).Decode(&body)
		w.WriteHeader(http.StatusCreated)
	}))
	defer worker.Close()
	service := newHTTPAnalysisService(worker.URL, "secret")
	if err := service.Activate("ABCD-EFGH", "python-token", time.Now().Add(time.Hour)); err != nil {
		t.Fatal(err)
	}
	if method != http.MethodPost || path != "/internal/sessions" || secret != "secret" ||
		body["code"] != "ABCD-EFGH" || body["token"] != "python-token" {
		t.Fatalf("invalid activation: %v", body)
	}
}

func TestCleanupRemovesExpiredSession(t *testing.T) {
	manager := newSessionManager()
	session, token, err := manager.create(time.Now().Add(-maxSessionAge - time.Minute))
	if err != nil {
		t.Fatal(err)
	}
	removed := manager.cleanup(time.Now())
	if len(removed) != 1 || manager.sessionForToken(token, roleBrowser) != nil || manager.codes[session.Code] != nil {
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
