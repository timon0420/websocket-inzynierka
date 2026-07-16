package main

import (
	"fmt"
	"github.com/gorilla/websocket"
	"net/http"
	"log"
	"encoding/json"
)

type GestureData struct {
	Angles []float64 `json:"angles"`
	Timestamp float64 `json:"timestamp"`
}

var upgrader = websocket.Upgrader{
	CheckOrigin: func(r *http.Request) bool {
		return true
	},
}

var clients = make(map[*websocket.Conn]bool)
var broadcast = make(chan []byte)
func wsHandler(w http.ResponseWriter, r *http.Request) {
	
	conn, err := upgrader.Upgrade(w, r, nil)
	if err != nil {
		fmt.Println("Error upgrading connection: ", err)
		return
	}
	defer conn.Close()

	clients[conn] = true

	for {
		_, message, err := conn.ReadMessage()
		if err != nil {
			fmt.Println("Error reading message: ", err)
			delete(clients, conn)
			break
		}
		
		var data GestureData

		if err := json.Unmarshal(message, &data); err != nil {
			fmt.Println("Error unmarshaling message: ", err)
			continue
		}
		
		if len(data.Angles) != 6 {
			fmt.Println("Invalid number of angles: ", len(data.Angles))
			continue
		}

		valid := true

		for _, angle := range data.Angles {
			if angle < 0 || angle > 180 {
				valid = false
				break
			}
		}

		if valid {
			broadcast <- message
		}
	}
}

func handleMessage() {
	for {
		message := <-broadcast
		for client := range clients {
			err := client.WriteMessage(websocket.TextMessage, message)
			if err != nil {
				fmt.Println("Error writing message: ", err)
				client.Close()
				delete(clients, client)
			}
		}
	}
}

func main() {
	http.HandleFunc("/ws", wsHandler)
	go handleMessage()
	fmt.Println("WebSocket server started on :8080")
	err := http.ListenAndServe(":8080", nil)
	if err != nil {
		log.Fatal("Error starting server: ", nil)
	}
}