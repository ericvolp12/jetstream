package main

import (
	"fmt"
	"log"
	"time"

	"github.com/gorilla/websocket"
)

const (
	serverAddr = "ws://localhost:8080/subscribe"
)

func main() {
	// Connect to WebSocket server
	c, _, err := websocket.DefaultDialer.Dial(serverAddr, nil)
	if err != nil {
		log.Fatal("Failed to connect:", err)
	}
	defer c.Close()

	for {
		_, msg, err := c.ReadMessage()
		if err != nil {
			log.Fatal("Failed to read message:", err)
		}

		// Print the received message
		fmt.Printf("Received: %s\n", msg)

		// Wait 1 second
		time.Sleep(1 * time.Second)
	}
}
