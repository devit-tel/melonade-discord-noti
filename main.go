package main

import (
	"encoding/json"
	"fmt"
	"log"
	"os"

	"github.com/bwmarrin/discordgo"
	"github.com/confluentinc/confluent-kafka-go/kafka"
	"github.com/joho/godotenv"
)

var interestedTaskStatus = []string{"FAILED", "ACK_TIMEOUT", "TIMEOUT"}
var interestedWorkflowStatus = []string{"FAILED", "TIMEOUT"}
var interestedTransactionStatus = []string{"FAILED", "CANCELLED", "COMPENSATED"}

type eventPayload struct {
	TransactionID string `json:"transactionId"`
	Type          string `json:"type"`
	IsError       bool   `json:"isError"`
	Timestamp     int64  `json:"timestamp"`
	Details       struct {
		Type               string      `json:"type"`
		TransactionID      string      `json:"transactionId"`
		WorkflowID         string      `json:"workflowId"`
		TaskID             string      `json:"taskId"`
		Status             string      `json:"status"`
		TaskName           string      `json:"taskName"`
		Output             interface{} `json:"output"`
		IsSystem           bool        `json:"isSystem"`
		WorkflowDefinition struct {
			Name string `json:"name"`
			Rev  string `json:"rev"`
		} `json:"workflowDefinition"`
	} `json:"details"`
}

func contains(s []string, e string) bool {
	for _, a := range s {
		if a == e {
			return true
		}
	}
	return false
}

func getEnv(key, fallback string) string {
	value := os.Getenv(key)
	if len(value) == 0 {
		return fallback
	}
	return value
}

func main() {
	if err := godotenv.Load(); err != nil {
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

	dg, err := discordgo.New("Bot " + getEnv("discord.token", ""))
	if err != nil {
		log.Fatal("error creating Discord session,", err)
	}

	if err := dg.Open(); err != nil {
		log.Fatal("error opening connection,", err)
	}

	notiChannel := getEnv("discord.channel", "")
	frontEndURL := getEnv("frontend.url", "")

	embed := &discordgo.MessageEmbed{
		Title: "I'm up boi!",
		Image: &discordgo.MessageEmbedImage{
			URL: "https://i.imgur.com/siGne5i.gif",
		},
		URL: frontEndURL,
	}

	_, err = dg.ChannelMessageSendEmbed(notiChannel, embed)
	if err != nil {
		log.Print("Error while sending message", err)
	}

	for {
		msg, err := c.ReadMessage(-1)
		if err != nil {
			fmt.Printf("Consumer error: %v (%v)\n", err, msg)
			continue
		}

		payload := eventPayload{}

		if err = json.Unmarshal([]byte(msg.Value), &payload); err != nil {
			log.Print("Bad payload", err)
			continue
		}

		if payload.IsError == true && payload.Details.IsSystem != true {
			embed := &discordgo.MessageEmbed{
				Title: "What the hell is happening",
				Image: &discordgo.MessageEmbedImage{
					URL: "https://i.kym-cdn.com/photos/images/facebook/000/918/810/a22.jpg",
				},
				URL:         fmt.Sprintf("%s/transaction/%s", frontEndURL, payload.TransactionID),
				Description: "Please have a look something not right in this transaction",
			}

			_, err = dg.ChannelMessageSendEmbed(notiChannel, embed)
			if err != nil {
				log.Print("Error while sending message", err)
			}
		} else if payload.IsError == true && payload.Details.IsSystem != false {
			// don't give a fuck

		} else if payload.Type == "TRANSACTION" && contains(interestedTransactionStatus, payload.Details.Status) {
			jsonByte, _ := json.MarshalIndent(payload.Details.Output, "", "    ")
			embed := &discordgo.MessageEmbed{
				Title:       fmt.Sprintf("Transaction: %s", payload.Details.Status),
				URL:         fmt.Sprintf("%s/transaction/%s", frontEndURL, payload.TransactionID),
				Description: fmt.Sprintf("Transaction: %s had failed with Output  ```js\n%s```", payload.TransactionID, string(jsonByte)),
			}

			_, err = dg.ChannelMessageSendEmbed(notiChannel, embed)
			if err != nil {
				log.Print("Error while sending message", err)
			}
		} else if payload.Type == "WORKFLOW" && contains(interestedWorkflowStatus, payload.Details.Status) {
			jsonByte, _ := json.MarshalIndent(payload.Details.Output, "", "    ")
			embed := &discordgo.MessageEmbed{
				Title:       fmt.Sprintf("Workflow: %s", payload.Details.Status),
				URL:         fmt.Sprintf("%s/transaction/%s", frontEndURL, payload.TransactionID),
				Description: fmt.Sprintf("Workflow: %s | %s had failed with Output  ```js\n%s```", payload.Details.WorkflowDefinition.Name, payload.Details.WorkflowDefinition.Rev, string(jsonByte)),
			}

			_, err = dg.ChannelMessageSendEmbed(notiChannel, embed)
			if err != nil {
				log.Print("Error while sending message", err)
			}
		} else if payload.Type == "TASK" && contains(interestedTaskStatus, payload.Details.Status) {
			jsonByte, _ := json.MarshalIndent(payload.Details.Output, "", "    ")
			embed := &discordgo.MessageEmbed{
				Title:       fmt.Sprintf("Task: %s", payload.Details.Status),
				URL:         fmt.Sprintf("%s/transaction/%s", frontEndURL, payload.TransactionID),
				Description: fmt.Sprintf("Task: %s had failed with Output  ```js\n%s```", payload.Details.TaskName, string(jsonByte)),
			}

			_, err = dg.ChannelMessageSendEmbed(notiChannel, embed)
			if err != nil {
				log.Print("Error while sending message", err)
			}
		}

	}

}
