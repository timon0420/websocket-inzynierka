package main

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/gorilla/websocket"
)

func TestListenerReceivesAnglesPublishedOnWS(t *testing.T) {
	hub.Lock()
	hub.clients = make(map[*websocket.Conn]bool)
	hub.Unlock()

	mux := http.NewServeMux()
	mux.HandleFunc("/ws", wsHandler)
	mux.HandleFunc("/listen", listenHandler)
	server := httptest.NewServer(mux)
	defer server.Close()

	go handleMessages()
	baseURL := "ws" + strings.TrimPrefix(server.URL, "http")
	listener, _, err := websocket.DefaultDialer.Dial(baseURL+"/listen", nil)
	if err != nil {
		t.Fatalf("połączenie /listen: %v", err)
	}
	defer listener.Close()
	publisher, _, err := websocket.DefaultDialer.Dial(baseURL+"/ws", nil)
	if err != nil {
		t.Fatalf("połączenie /ws: %v", err)
	}
	defer publisher.Close()

	expected := GestureData{Angles: []float64{10, 20, 30, 40, 50, 60}, Timestamp: 123.5}
	if err := publisher.WriteJSON(expected); err != nil {
		t.Fatalf("publikacja: %v", err)
	}
	_ = listener.SetReadDeadline(time.Now().Add(2 * time.Second))
	_, message, err := listener.ReadMessage()
	if err != nil {
		t.Fatalf("odbiór /listen: %v", err)
	}
	var received GestureData
	if err := json.Unmarshal(message, &received); err != nil {
		t.Fatalf("dekodowanie: %v", err)
	}
	if len(received.Angles) != 6 || received.Angles[5] != 60 || received.Timestamp != 123.5 {
		t.Fatalf("nieoczekiwane dane: %+v", received)
	}
}

func TestValidAngles(t *testing.T) {
	if !validAngles([]float64{0, 1, 2, 3, 4, 180}) {
		t.Fatal("poprawne kąty zostały odrzucone")
	}
	if validAngles([]float64{0, 1, 2}) || validAngles([]float64{0, 1, 2, 3, 4, 181}) {
		t.Fatal("niepoprawne kąty zostały zaakceptowane")
	}
}
