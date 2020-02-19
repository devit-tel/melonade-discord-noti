package main

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"os"
	"os/signal"
	"strings"
	"sync"
	"syscall"

	"github.com/Shopify/sarama"
	"github.com/bwmarrin/discordgo"
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

var (
	notiChannel string = ""
	frontEndURL string = ""
	dg          *discordgo.Session
)

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

	notiChannel = getEnv("discord.channel", "")
	frontEndURL = getEnv("frontend.url", "")

	version, err := sarama.ParseKafkaVersion(getEnv("kafka.version", "2.1.1"))
	if err != nil {
		log.Panicf("Error parsing Kafka version: %v", err)
	}

	config := sarama.NewConfig()
	config.Version = version
	config.Consumer.Group.Rebalance.Strategy = sarama.BalanceStrategySticky
	config.Consumer.Offsets.Initial = sarama.OffsetNewest
	config.Consumer.Fetch.Max = 100

	consumer := Consumer{
		ready: make(chan bool),
	}

	ctx, cancel := context.WithCancel(context.Background())
	client, err := sarama.NewConsumerGroup(strings.Split(getEnv("kafka.bootstrap.servers", ""), ","), getEnv("kafka.group.id", "discord-bot-watcher"), config)
	if err != nil {
		log.Panicf("Error creating consumer group client: %v", err)
	}

	wg := &sync.WaitGroup{}
	wg.Add(1)
	go func() {
		defer wg.Done()
		for {
			if err := client.Consume(ctx, []string{fmt.Sprintf("melonade.%s.store", getEnv("melonade.namespace", "default"))}, &consumer); err != nil {
				log.Panicf("Error from consumer: %v", err)
			}
			if ctx.Err() != nil {
				return
			}
			consumer.ready = make(chan bool)
		}
	}()

	// init discord
	dg, err = discordgo.New("Bot " + getEnv("discord.token", ""))
	if err != nil {
		log.Fatal("error creating Discord session,", err)
	}

	if err := dg.Open(); err != nil {
		log.Fatal("error opening connection,", err)
	}

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

	// Await till the consumer has been set up
	<-consumer.ready
	log.Println("Sarama consumer up and running!...")

	sigterm := make(chan os.Signal, 1)
	signal.Notify(sigterm, syscall.SIGINT, syscall.SIGTERM)
	select {
	case <-ctx.Done():
		log.Println("terminating: context cancelled")
	case <-sigterm:
		log.Println("terminating: via signal")
	}
	cancel()
	wg.Wait()

	if err = client.Close(); err != nil {
		log.Panicf("Error closing client: %v", err)
	}

	// for {
	// 	msg, err := c.ReadMessage(-1)
	// 	if err != nil {
	// 		fmt.Printf("Consumer error: %v (%v)\n", err, msg)
	// 		continue
	// 	}

	// }
}

// Consumer represents a Sarama consumer group consumer
type Consumer struct {
	ready chan bool
}

// Setup is run at the beginning of a new session, before ConsumeClaim
func (consumer *Consumer) Setup(sarama.ConsumerGroupSession) error {
	// Mark the consumer as ready
	close(consumer.ready)
	return nil
}

// Cleanup is run at the end of a session, once all ConsumeClaim goroutines have exited
func (consumer *Consumer) Cleanup(sarama.ConsumerGroupSession) error {
	return nil
}

// ConsumeClaim must start a consumer loop of ConsumerGroupClaim's Messages().
func (consumer *Consumer) ConsumeClaim(session sarama.ConsumerGroupSession, claim sarama.ConsumerGroupClaim) error {

	// NOTE:
	// Do not move the code below to a goroutine.
	// The `ConsumeClaim` itself is called within a goroutine, see:
	// https://github.com/Shopify/sarama/blob/master/consumer_group.go#L27-L29
	for message := range claim.Messages() {
		payload := eventPayload{}

		if err := json.Unmarshal([]byte(message.Value), &payload); err != nil {
			log.Print("Bad payload", err)
			continue
		}

		if payload.IsError == true && payload.Details.IsSystem != true {
			jsonByte, _ := json.MarshalIndent(payload, "", "    ")

			embed := &discordgo.MessageEmbed{
				Title: "What the hell is happening",
				Image: &discordgo.MessageEmbedImage{
					URL: "https://i.kym-cdn.com/photos/images/facebook/000/918/810/a22.jpg",
				},
				URL:         fmt.Sprintf("%s/transaction/%s", frontEndURL, payload.TransactionID),
				Description: fmt.Sprintf("Please have a look something not right in this transaction ```js\n%s```", string(jsonByte)),
			}

			_, err := dg.ChannelMessageSendEmbed(notiChannel, embed)
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

			_, err := dg.ChannelMessageSendEmbed(notiChannel, embed)
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

			_, err := dg.ChannelMessageSendEmbed(notiChannel, embed)
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

			_, err := dg.ChannelMessageSendEmbed(notiChannel, embed)
			if err != nil {
				log.Print("Error while sending message", err)
			}
		}

		session.MarkMessage(message, "")
	}

	return nil
}
