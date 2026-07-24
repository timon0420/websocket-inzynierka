package main

import (
	"crypto/rand"
	"encoding/base64"
	"encoding/json"
	"errors"
	"fmt"
	"log"
	"net"
	"net/http"
	"os"
	"strings"
	"sync"
	"time"

	"github.com/gorilla/websocket"
)

const (
	roleBrowser         = "browser"
	roleUnity           = "unity"
	maxJPEGSize         = 200 * 1024
	pairingTTL          = 15 * time.Minute
	maxSessionAge       = 4 * time.Hour
	disconnectedTTL     = 5 * time.Minute
	writeWait           = 5 * time.Second
	pongWait            = 70 * time.Second
	pingPeriod          = 25 * time.Second
	minFrameInterval    = 100 * time.Millisecond
	maxPairingPerMinute = 10
)

type GestureData struct {
	Type      string    `json:"type"`
	Angles    []float64 `json:"angles"`
	Timestamp float64   `json:"timestamp"`
	Sequence  uint64    `json:"sequence"`
}

type AnalysisData struct {
	Detected     bool      `json:"detected"`
	Angles       []float64 `json:"angles,omitempty"`
	ProcessingMS float64   `json:"processingMs"`
}

type AnalysisMessage struct {
	Type         string    `json:"type"`
	Detected     bool      `json:"detected"`
	Angles       []float64 `json:"angles,omitempty"`
	ProcessingMS float64   `json:"processingMs"`
	Sequence     uint64    `json:"sequence"`
}

type outboundMessage struct {
	messageType int
	payload     []byte
}

type client struct {
	conn *websocket.Conn
	send chan outboundMessage
}

type Session struct {
	ID             string
	Code           string
	CreatedAt      time.Time
	PairingExpires time.Time
	LastActive     time.Time
	Tokens         map[string]string
	Paired         map[string]bool
	Clients        map[string]*client
	LastSequence   uint64
}

type SessionManager struct {
	mu       sync.RWMutex
	sessions map[string]*Session
	tokens   map[string]*Session
	codes    map[string]*Session
}

func newSessionManager() *SessionManager {
	return &SessionManager{
		sessions: make(map[string]*Session),
		tokens:   make(map[string]*Session),
		codes:    make(map[string]*Session),
	}
}

func (m *SessionManager) create(now time.Time) (*Session, string, error) {
	m.mu.Lock()
	defer m.mu.Unlock()

	if len(m.sessions) >= 20 {
		return nil, "", errors.New("session_limit_reached")
	}
	code, err := m.uniqueCodeLocked()
	if err != nil {
		return nil, "", err
	}
	id, err := randomToken(18)
	if err != nil {
		return nil, "", err
	}
	browserToken, err := randomToken(32)
	if err != nil {
		return nil, "", err
	}
	session := &Session{
		ID:             id,
		Code:           code,
		CreatedAt:      now,
		PairingExpires: now.Add(pairingTTL),
		LastActive:     now,
		Tokens:         map[string]string{roleBrowser: browserToken},
		Paired:         map[string]bool{roleBrowser: true},
		Clients:        make(map[string]*client),
	}
	m.sessions[id] = session
	m.codes[code] = session
	m.tokens[browserToken] = session
	return session, browserToken, nil
}

func (m *SessionManager) pair(code, role string, now time.Time) (*Session, string, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	if role != roleUnity {
		return nil, "", errors.New("invalid_role")
	}
	session := m.codes[normalizeCode(code)]
	if session == nil || now.After(session.PairingExpires) {
		return nil, "", errors.New("invalid_or_expired_code")
	}
	if session.Paired[role] {
		return nil, "", errors.New("role_already_paired")
	}
	token, err := randomToken(32)
	if err != nil {
		return nil, "", err
	}
	session.Paired[role] = true
	session.Tokens[role] = token
	session.LastActive = now
	m.tokens[token] = session
	return session, token, nil
}

func (m *SessionManager) sessionForToken(token, role string) *Session {
	m.mu.RLock()
	defer m.mu.RUnlock()
	session := m.tokens[token]
	if session == nil || session.Tokens[role] != token {
		return nil
	}
	return session
}

func (m *SessionManager) register(session *Session, role string, c *client) *client {
	m.mu.Lock()
	defer m.mu.Unlock()
	previous := session.Clients[role]
	session.Clients[role] = c
	session.LastActive = time.Now()
	return previous
}

func (m *SessionManager) unregister(session *Session, role string, c *client) {
	m.mu.Lock()
	defer m.mu.Unlock()
	if session.Clients[role] == c {
		delete(session.Clients, role)
		session.LastActive = time.Now()
	}
}

func (m *SessionManager) route(session *Session, role string, message outboundMessage, latest bool) bool {
	m.mu.RLock()
	target := session.Clients[role]
	m.mu.RUnlock()
	if target == nil {
		return false
	}
	if latest {
		select {
		case target.send <- message:
			return true
		default:
		}
		select {
		case <-target.send:
		default:
		}
	}
	select {
	case target.send <- message:
		return true
	default:
		return false
	}
}

func (m *SessionManager) nextSequence(session *Session) uint64 {
	m.mu.Lock()
	defer m.mu.Unlock()
	session.LastSequence++
	session.LastActive = time.Now()
	return session.LastSequence
}

func (m *SessionManager) cleanup(now time.Time) {
	m.mu.Lock()
	defer m.mu.Unlock()
	for id, session := range m.sessions {
		noClients := len(session.Clients) == 0
		if now.Sub(session.CreatedAt) < maxSessionAge && (!noClients || now.Sub(session.LastActive) < disconnectedTTL) {
			continue
		}
		for _, token := range session.Tokens {
			delete(m.tokens, token)
		}
		delete(m.codes, session.Code)
		delete(m.sessions, id)
	}
}

func (m *SessionManager) uniqueCodeLocked() (string, error) {
	const alphabet = "0123456789ABCDEFGHJKMNPQRSTVWXYZ"
	for attempt := 0; attempt < 10; attempt++ {
		bytes := make([]byte, 8)
		if _, err := rand.Read(bytes); err != nil {
			return "", err
		}
		for index := range bytes {
			bytes[index] = alphabet[int(bytes[index])%len(alphabet)]
		}
		code := string(bytes[:4]) + "-" + string(bytes[4:])
		if m.codes[code] == nil {
			return code, nil
		}
	}
	return "", errors.New("code_generation_failed")
}

func normalizeCode(code string) string {
	clean := strings.ToUpper(strings.ReplaceAll(strings.TrimSpace(code), "-", ""))
	if len(clean) == 8 {
		return clean[:4] + "-" + clean[4:]
	}
	return strings.ToUpper(strings.TrimSpace(code))
}

func randomToken(size int) (string, error) {
	buffer := make([]byte, size)
	if _, err := rand.Read(buffer); err != nil {
		return "", err
	}
	return base64.RawURLEncoding.EncodeToString(buffer), nil
}

type rateEntry struct {
	window time.Time
	count  int
}

type Server struct {
	manager       *SessionManager
	upgrader      websocket.Upgrader
	rateMu        sync.Mutex
	pairRate      map[string]rateEntry
	analyzer      analysisService
	analysisMu    sync.Mutex
	analysisBusy  map[string]bool
	pendingFrames map[string][]byte
}

func newServer() *Server {
	return newServerWithAnalyzer(newHTTPAnalysisService(
		os.Getenv("ANALYSIS_SERVICE_URL"),
		os.Getenv("INTERNAL_SERVICE_TOKEN"),
	))
}

func newServerWithAnalyzer(analyzer analysisService) *Server {
	allowedOrigin := os.Getenv("ALLOWED_ORIGIN")
	return &Server{
		manager: newSessionManager(),
		upgrader: websocket.Upgrader{CheckOrigin: func(r *http.Request) bool {
			origin := r.Header.Get("Origin")
			return allowedOrigin == "" || origin == "" || origin == allowedOrigin
		}},
		pairRate:      make(map[string]rateEntry),
		analyzer:      analyzer,
		analysisBusy:  make(map[string]bool),
		pendingFrames: make(map[string][]byte),
	}
}

func (s *Server) routes() http.Handler {
	mux := http.NewServeMux()
	mux.HandleFunc("/api/sessions", s.handleSessions)
	mux.HandleFunc("/api/sessions/pair", s.handlePair)
	mux.HandleFunc("/ws/browser", func(w http.ResponseWriter, r *http.Request) { s.handleWebSocket(roleBrowser, w, r) })
	mux.HandleFunc("/ws/unity", func(w http.ResponseWriter, r *http.Request) { s.handleWebSocket(roleUnity, w, r) })
	mux.HandleFunc("/healthz", func(w http.ResponseWriter, _ *http.Request) { w.WriteHeader(http.StatusNoContent) })
	return cors(mux)
}

func cors(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		allowedOrigin := os.Getenv("ALLOWED_ORIGIN")
		if allowedOrigin == "" {
			allowedOrigin = "*"
		}
		w.Header().Set("Access-Control-Allow-Origin", allowedOrigin)
		w.Header().Set("Access-Control-Allow-Headers", "Content-Type")
		w.Header().Set("Access-Control-Allow-Methods", "GET, POST, OPTIONS")
		if r.Method == http.MethodOptions {
			w.WriteHeader(http.StatusNoContent)
			return
		}
		next.ServeHTTP(w, r)
	})
}

func (s *Server) handleSessions(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		writeError(w, http.StatusMethodNotAllowed, "method_not_allowed")
		return
	}
	session, token, err := s.manager.create(time.Now())
	if err != nil {
		writeError(w, http.StatusServiceUnavailable, err.Error())
		return
	}
	writeJSON(w, http.StatusCreated, map[string]any{
		"code":         session.Code,
		"browserToken": token,
		"expiresAt":    session.PairingExpires.UTC().Format(time.RFC3339),
	})
}

func (s *Server) handlePair(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		writeError(w, http.StatusMethodNotAllowed, "method_not_allowed")
		return
	}
	if !s.allowPair(r) {
		writeError(w, http.StatusTooManyRequests, "rate_limit_exceeded")
		return
	}
	var request struct {
		Code string `json:"code"`
		Role string `json:"role"`
	}
	decoder := json.NewDecoder(http.MaxBytesReader(w, r.Body, 4096))
	if err := decoder.Decode(&request); err != nil {
		writeError(w, http.StatusBadRequest, "invalid_json")
		return
	}
	_, token, err := s.manager.pair(request.Code, request.Role, time.Now())
	if err != nil {
		status := http.StatusBadRequest
		if err.Error() == "role_already_paired" {
			status = http.StatusConflict
		}
		writeError(w, status, err.Error())
		return
	}
	writeJSON(w, http.StatusOK, map[string]string{
		"token":         token,
		"websocketPath": "/ws/" + request.Role,
	})
}

func (s *Server) allowPair(r *http.Request) bool {
	host, _, err := net.SplitHostPort(r.RemoteAddr)
	if err != nil {
		host = r.RemoteAddr
	}
	now := time.Now()
	s.rateMu.Lock()
	defer s.rateMu.Unlock()
	entry := s.pairRate[host]
	if now.Sub(entry.window) >= time.Minute {
		entry = rateEntry{window: now}
	}
	entry.count++
	s.pairRate[host] = entry
	return entry.count <= maxPairingPerMinute
}

func (s *Server) handleWebSocket(role string, w http.ResponseWriter, r *http.Request) {
	token := r.URL.Query().Get("token")
	session := s.manager.sessionForToken(token, role)
	if session == nil {
		writeError(w, http.StatusUnauthorized, "invalid_token")
		return
	}
	conn, err := s.upgrader.Upgrade(w, r, nil)
	if err != nil {
		return
	}
	queueSize := 8
	c := &client{conn: conn, send: make(chan outboundMessage, queueSize)}
	previous := s.manager.register(session, role, c)
	if previous != nil {
		_ = previous.conn.Close()
	}
	defer func() {
		s.manager.unregister(session, role, c)
		_ = conn.Close()
		s.sendStatus(session)
	}()

	conn.SetReadLimit(maxJPEGSize + 1024)
	_ = conn.SetReadDeadline(time.Now().Add(pongWait))
	conn.SetPongHandler(func(string) error {
		return conn.SetReadDeadline(time.Now().Add(pongWait))
	})
	go s.writePump(c)
	s.sendStatus(session)

	var lastFrame time.Time
	for {
		messageType, payload, err := conn.ReadMessage()
		if err != nil {
			return
		}
		switch role {
		case roleBrowser:
			if messageType != websocket.BinaryMessage || len(payload) > maxJPEGSize || !isJPEG(payload) {
				continue
			}
			now := time.Now()
			if now.Sub(lastFrame) < minFrameInterval {
				continue
			}
			lastFrame = now
			s.queueAnalysis(session, payload)
		}
	}
}

func (s *Server) queueAnalysis(session *Session, jpeg []byte) {
	frame := append([]byte(nil), jpeg...)
	s.analysisMu.Lock()
	if s.analysisBusy[session.ID] {
		s.pendingFrames[session.ID] = frame
		s.analysisMu.Unlock()
		return
	}
	s.analysisBusy[session.ID] = true
	s.analysisMu.Unlock()
	go s.analysisLoop(session, frame)
}

func (s *Server) analysisLoop(session *Session, frame []byte) {
	for {
		result, err := s.analyzer.Analyze(session.Code, frame)
		if err != nil {
			payload, _ := json.Marshal(map[string]any{
				"type": "analysis_error", "message": "Analiza obrazu jest chwilowo niedostępna.",
			})
			s.manager.route(session, roleBrowser, outboundMessage{websocket.TextMessage, payload}, true)
		} else {
			sequence := s.manager.nextSequence(session)
			browserMessage := AnalysisMessage{
				Type: "analysis", Detected: result.Detected, Angles: result.Angles,
				ProcessingMS: result.ProcessingMS, Sequence: sequence,
			}
			payload, _ := json.Marshal(browserMessage)
			s.manager.route(session, roleBrowser, outboundMessage{websocket.TextMessage, payload}, true)
			if result.Detected && validAngles(result.Angles) {
				unityMessage, _ := json.Marshal(GestureData{
					Type: "angles", Angles: result.Angles,
					Timestamp: float64(time.Now().UnixMilli()) / 1000, Sequence: sequence,
				})
				s.manager.route(session, roleUnity, outboundMessage{websocket.TextMessage, unityMessage}, true)
			}
		}

		s.analysisMu.Lock()
		next := s.pendingFrames[session.ID]
		delete(s.pendingFrames, session.ID)
		if next == nil {
			delete(s.analysisBusy, session.ID)
			s.analysisMu.Unlock()
			return
		}
		s.analysisMu.Unlock()
		frame = next
	}
}

func (s *Server) writePump(c *client) {
	ticker := time.NewTicker(pingPeriod)
	defer ticker.Stop()
	for {
		select {
		case message := <-c.send:
			_ = c.conn.SetWriteDeadline(time.Now().Add(writeWait))
			if err := c.conn.WriteMessage(message.messageType, message.payload); err != nil {
				return
			}
		case <-ticker.C:
			_ = c.conn.SetWriteDeadline(time.Now().Add(writeWait))
			if err := c.conn.WriteMessage(websocket.PingMessage, nil); err != nil {
				return
			}
		}
	}
}

func (s *Server) sendStatus(session *Session) {
	s.manager.mu.RLock()
	unityConnected := session.Clients[roleUnity] != nil
	s.manager.mu.RUnlock()
	analysisStatus := "unavailable"
	if s.analyzer.Ready() {
		analysisStatus = "ready"
	}
	payload, _ := json.Marshal(map[string]any{
		"type": "status", "analysis": analysisStatus, "unity": unityConnected,
	})
	s.manager.route(session, roleBrowser, outboundMessage{websocket.TextMessage, payload}, true)
}

func validAngles(angles []float64) bool {
	if len(angles) != 6 {
		return false
	}
	for _, angle := range angles {
		if angle < 0 || angle > 180 {
			return false
		}
	}
	return true
}

func isJPEG(payload []byte) bool {
	return len(payload) >= 4 && payload[0] == 0xff && payload[1] == 0xd8 && payload[len(payload)-2] == 0xff && payload[len(payload)-1] == 0xd9
}

func writeJSON(w http.ResponseWriter, status int, value any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(value)
}

func writeError(w http.ResponseWriter, status int, code string) {
	writeJSON(w, status, map[string]string{"error": code})
}

func main() {
	server := newServer()
	go func() {
		ticker := time.NewTicker(time.Minute)
		defer ticker.Stop()
		for now := range ticker.C {
			server.manager.cleanup(now)
		}
	}()
	port := os.Getenv("PORT")
	if port == "" {
		port = "8080"
	}
	address := ":" + port
	fmt.Printf("Session WebSocket server działa na %s\n", address)
	if err := http.ListenAndServe(address, server.routes()); err != nil {
		log.Fatal(err)
	}
}
