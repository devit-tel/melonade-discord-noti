package main

import (
	"fmt"
	"log"
	"os"
	"time"

	"github.com/confluentinc/confluent-kafka-go/kafka"
	"github.com/joho/godotenv"
)

type eventPayload struct {
	TransactionID time.Time `json:"transactionId"`
	Type          string    `json:"type"`
	IsError       bool      `json:"isError"`
	Timestamp     int64     `json:"timestamp"`
	Details       struct {
		Type          string    `json:"type"`
		TransactionID time.Time `json:"transactionId"`
		WorkflowID    string    `json:"workflowId"`
		TaskID        string    `json:"taskId"`
		Status        string    `json:"status"`
	} `json:"details"`
}

func getEnv(key, fallback string) string {
	value := os.Getenv(key)
	if len(value) == 0 {
		return fallback
	}
	return value
}

func main() {

	if err := godotenv.Load(), err != nil {
		log.Print("no .env file found")
	}

	c, err := kafka.NewConsumer(&kafka.ConfigMap{
		"bootstrap.servers": getEnv("kafka.bootstrap.servers", ""),
		"group.id":          getEnv("kafka.group.id", "discord-bot-watcher"),
		"auto.offset.reset": "latest",
	})

	if err != nil {
		log.Panic(err)
	}

	c.SubscribeTopics([]string{fmt.Sprintf("melonade.%s.store", getEnv("melonade.namespace", "default"))}, nil)

	for {
		msg, err := c.ReadMessage(-1)
		if err == nil {
			payload := eventPayload{}
			if err := json.Unmarshal([]byte(msg.Value), &payload), err != nil {
				log.Print("Bad payload")
			} else {
				fmt.Printf("Message on %s: %s\n", payload.TransactionID, payload.Type)
			}
			
		} else {
			fmt.Printf("Consumer error: %v (%v)\n", err, msg)
		}
	}

}
