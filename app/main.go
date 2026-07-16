package main

import (
	"encoding/json"
	"fmt"
	"log"
	"net/http"
	"os"
	"sync"
	"time"

	"github.com/gorilla/websocket"
)

type GestureData struct {
	Angles    []float64 `json:"angles"`
	Timestamp float64   `json:"timestamp"`
}

var upgrader = websocket.Upgrader{
	CheckOrigin: func(r *http.Request) bool { return true },
}

var hub = struct {
	sync.RWMutex
	clients map[*websocket.Conn]bool
}{clients: make(map[*websocket.Conn]bool)}

var broadcast = make(chan []byte, 64)

func wsHandler(w http.ResponseWriter, r *http.Request) {
	conn, err := upgradeAndRegister(w, r)
	if err != nil {
		log.Printf("Błąd połączenia /ws: %v", err)
		return
	}
	defer unregister(conn)

	for {
		_, message, err := conn.ReadMessage()
		if err != nil {
			if !websocket.IsCloseError(err, websocket.CloseNormalClosure, websocket.CloseGoingAway) {
				log.Printf("Rozłączono klienta /ws: %v", err)
			}
			return
		}

		var data GestureData
		if err := json.Unmarshal(message, &data); err != nil {
			log.Printf("Nieprawidłowy JSON: %v", err)
			continue
		}
		if !validAngles(data.Angles) {
			log.Printf("Nieprawidłowe kąty: wymagane 6 wartości z zakresu 0-180")
			continue
		}

		payload, err := json.Marshal(data)
		if err == nil {
			broadcast <- payload
		}
	}
}

func listenHandler(w http.ResponseWriter, r *http.Request) {
	conn, err := upgradeAndRegister(w, r)
	if err != nil {
		log.Printf("Błąd połączenia /listen: %v", err)
		return
	}
	defer unregister(conn)

	for {
		if _, _, err := conn.ReadMessage(); err != nil {
			if !websocket.IsCloseError(err, websocket.CloseNormalClosure, websocket.CloseGoingAway) {
				log.Printf("Rozłączono słuchacza /listen: %v", err)
			}
			return
		}
	}
}

func upgradeAndRegister(w http.ResponseWriter, r *http.Request) (*websocket.Conn, error) {
	conn, err := upgrader.Upgrade(w, r, nil)
	if err != nil {
		return nil, err
	}
	conn.SetReadLimit(4096)
	conn.SetPongHandler(func(string) error {
		return conn.SetReadDeadline(time.Now().Add(70 * time.Second))
	})
	_ = conn.SetReadDeadline(time.Now().Add(70 * time.Second))

	hub.Lock()
	hub.clients[conn] = true
	hub.Unlock()
	return conn, nil
}

func unregister(conn *websocket.Conn) {
	hub.Lock()
	delete(hub.clients, conn)
	hub.Unlock()
	_ = conn.WriteControl(
		websocket.CloseMessage,
		websocket.FormatCloseMessage(websocket.CloseNormalClosure, "closing"),
		time.Now().Add(time.Second),
	)
	_ = conn.Close()
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

func handleMessages() {
	pingTicker := time.NewTicker(25 * time.Second)
	defer pingTicker.Stop()

	for {
		select {
		case message := <-broadcast:
			writeToAll(websocket.TextMessage, message)
		case <-pingTicker.C:
			writeToAll(websocket.PingMessage, nil)
		}
	}
}

func writeToAll(messageType int, message []byte) {
	hub.Lock()
	defer hub.Unlock()
	for client := range hub.clients {
		_ = client.SetWriteDeadline(time.Now().Add(5 * time.Second))
		if err := client.WriteMessage(messageType, message); err != nil {
			log.Printf("Błąd wysyłania do klienta: %v", err)
			_ = client.Close()
			delete(hub.clients, client)
		}
	}
}

func main() {
	http.HandleFunc("/ws", wsHandler)
	http.HandleFunc("/listen", listenHandler)
	go handleMessages()

	port := os.Getenv("PORT")
	if port == "" {
		port = "8080"
	}
	address := ":" + port
	fmt.Printf("WebSocket server działa na %s (/ws, /listen)\n", address)
	if err := http.ListenAndServe(address, nil); err != nil {
		log.Fatal(err)
	}
}
