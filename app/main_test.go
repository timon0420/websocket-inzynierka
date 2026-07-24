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

type fakeAnalyzer struct {
	mu      sync.Mutex
	results map[string]AnalysisData
	frames  map[string][][]byte
}

func (f *fakeAnalyzer) Ready() bool { return true }

func (f *fakeAnalyzer) Analyze(sessionCode string, jpeg []byte) (AnalysisData, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.frames[sessionCode] = append(f.frames[sessionCode], append([]byte(nil), jpeg...))
	return f.results[sessionCode], nil
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

func TestJPEGIsAnalyzedAndAnglesReachBrowserAndUnity(t *testing.T) {
	analyzer := &fakeAnalyzer{
		results: make(map[string]AnalysisData), frames: make(map[string][][]byte),
	}
	control := newServerWithAnalyzer(analyzer)
	server := httptest.NewServer(control.routes())
	defer server.Close()
	session := createTestSession(t, server.URL)
	analyzer.results[session.Code] = AnalysisData{
		Detected: true, Angles: []float64{10, 20, 30, 40, 50, 60}, ProcessingMS: 12.5,
	}
	unityToken := pairTestRole(t, server.URL, session.Code, roleUnity)
	browser := dialRole(t, server.URL, roleBrowser, session.BrowserToken)
	unity := dialRole(t, server.URL, roleUnity, unityToken)
	defer browser.Close()
	defer unity.Close()

	jpeg := []byte{0xff, 0xd8, 0x01, 0x02, 0xff, 0xd9}
	if err := browser.WriteMessage(websocket.BinaryMessage, jpeg); err != nil {
		t.Fatal(err)
	}

	var analysis AnalysisMessage
	_ = json.Unmarshal(readType(t, browser, "analysis"), &analysis)
	if !analysis.Detected || analysis.Angles[5] != 60 || analysis.ProcessingMS != 12.5 {
		t.Fatalf("unexpected browser analysis: %+v", analysis)
	}
	var angles GestureData
	_ = json.Unmarshal(readType(t, unity, "angles"), &angles)
	if angles.Sequence != analysis.Sequence || angles.Angles[5] != 60 {
		t.Fatalf("unexpected Unity angles: %+v", angles)
	}
	analyzer.mu.Lock()
	defer analyzer.mu.Unlock()
	if len(analyzer.frames[session.Code]) != 1 || !bytes.Equal(analyzer.frames[session.Code][0], jpeg) {
		t.Fatal("analyzer did not receive the browser JPEG with the public session code")
	}
}

func TestNoDetectionDoesNotSendAnglesToUnity(t *testing.T) {
	analyzer := &fakeAnalyzer{results: make(map[string]AnalysisData), frames: make(map[string][][]byte)}
	control := newServerWithAnalyzer(analyzer)
	server := httptest.NewServer(control.routes())
	defer server.Close()
	session := createTestSession(t, server.URL)
	unity := dialRole(t, server.URL, roleUnity, pairTestRole(t, server.URL, session.Code, roleUnity))
	browser := dialRole(t, server.URL, roleBrowser, session.BrowserToken)
	defer unity.Close()
	defer browser.Close()
	_ = browser.WriteMessage(websocket.BinaryMessage, []byte{0xff, 0xd8, 0xff, 0xd9})
	var analysis AnalysisMessage
	_ = json.Unmarshal(readType(t, browser, "analysis"), &analysis)
	if analysis.Detected {
		t.Fatal("no detection was reported as detected")
	}
	_ = unity.SetReadDeadline(time.Now().Add(150 * time.Millisecond))
	if _, _, err := unity.ReadMessage(); err == nil {
		t.Fatal("Unity received angles without a detected hand")
	}
}

func TestSessionsUseIndependentAnalysisState(t *testing.T) {
	analyzer := &fakeAnalyzer{results: make(map[string]AnalysisData), frames: make(map[string][][]byte)}
	control := newServerWithAnalyzer(analyzer)
	server := httptest.NewServer(control.routes())
	defer server.Close()

	first := createTestSession(t, server.URL)
	second := createTestSession(t, server.URL)
	analyzer.results[first.Code] = AnalysisData{Detected: true, Angles: []float64{1, 2, 3, 4, 5, 6}}
	analyzer.results[second.Code] = AnalysisData{Detected: true, Angles: []float64{11, 12, 13, 14, 15, 16}}

	firstBrowser := dialRole(t, server.URL, roleBrowser, first.BrowserToken)
	secondBrowser := dialRole(t, server.URL, roleBrowser, second.BrowserToken)
	firstUnity := dialRole(t, server.URL, roleUnity, pairTestRole(t, server.URL, first.Code, roleUnity))
	secondUnity := dialRole(t, server.URL, roleUnity, pairTestRole(t, server.URL, second.Code, roleUnity))
	defer firstBrowser.Close()
	defer secondBrowser.Close()
	defer firstUnity.Close()
	defer secondUnity.Close()

	_ = firstBrowser.WriteMessage(websocket.BinaryMessage, []byte{0xff, 0xd8, 0x01, 0xff, 0xd9})
	_ = secondBrowser.WriteMessage(websocket.BinaryMessage, []byte{0xff, 0xd8, 0x02, 0xff, 0xd9})
	var firstAngles, secondAngles GestureData
	_ = json.Unmarshal(readType(t, firstUnity, "angles"), &firstAngles)
	_ = json.Unmarshal(readType(t, secondUnity, "angles"), &secondAngles)
	if firstAngles.Angles[5] != 6 || secondAngles.Angles[5] != 16 {
		t.Fatalf("sessions crossed: first=%v second=%v", firstAngles.Angles, secondAngles.Angles)
	}
}

func TestOnlyUnityCanBePaired(t *testing.T) {
	server := httptest.NewServer(newServerWithAnalyzer(&fakeAnalyzer{}).routes())
	defer server.Close()
	session := createTestSession(t, server.URL)
	body, _ := json.Marshal(map[string]string{"code": session.Code, "role": "python"})
	response, _ := http.Post(server.URL+"/api/sessions/pair", "application/json", bytes.NewReader(body))
	if response.StatusCode != http.StatusBadRequest {
		t.Fatalf("python role status: %d", response.StatusCode)
	}
	_ = pairTestRole(t, server.URL, session.Code, roleUnity)
	response, _ = http.Post(server.URL+"/api/sessions/pair", "application/json",
		bytes.NewReader([]byte(`{"code":"`+session.Code+`","role":"unity"}`)))
	if response.StatusCode != http.StatusConflict {
		t.Fatalf("duplicate Unity role status: %d", response.StatusCode)
	}
}

func TestHTTPAnalysisServiceSendsPublicSessionCode(t *testing.T) {
	var receivedCode, receivedToken string
	worker := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		receivedCode = r.Header.Get("X-Session-Code")
		receivedToken = r.Header.Get("X-Internal-Token")
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write([]byte(`{"detected":false,"processingMs":3.5}`))
	}))
	defer worker.Close()

	service := newHTTPAnalysisService(worker.URL, "internal-secret")
	result, err := service.Analyze("ABCD-EFGH", []byte{0xff, 0xd8, 0xff, 0xd9})
	if err != nil {
		t.Fatal(err)
	}
	if receivedCode != "ABCD-EFGH" || receivedToken != "internal-secret" {
		t.Fatalf("unexpected internal headers: code=%q token=%q", receivedCode, receivedToken)
	}
	if result.Detected || result.ProcessingMS != 3.5 {
		t.Fatalf("unexpected analysis result: %+v", result)
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
