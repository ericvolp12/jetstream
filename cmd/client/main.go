package main

import (
	"fmt"
	"log"

	"github.com/gorilla/websocket"
)

const (
	serverAddr = "ws://localhost:6008/subscribe"
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
	}
}
