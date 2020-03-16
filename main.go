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
}

type eventTransactionPayload struct {
	Details struct {
		TransactionID      string      `json:"transactionId"`
		Status             string      `json:"status"`
		Input              interface{} `json:"input"`
		Output             interface{} `json:"output"`
		CreateTime         int64       `json:"createTime"`
		EndTime            int64       `json:"endTime"`
		Tags               []string    `json:"tags"`
		WorkflowDefinition struct {
			Name string `json:"name"`
			Rev  string `json:"rev"`
		} `json:"workflowDefinition"`
	} `json:"details"`
}

type eventWorkflowPayload struct {
	Details struct {
		TransactionID      string      `json:"transactionId"`
		Type               string      `json:"type"`
		WorkflowID         string      `json:"workflowId"`
		Status             string      `json:"status"`
		Retries            int         `json:"retries"`
		Input              interface{} `json:"input"`
		Output             interface{} `json:"output"`
		CreateTime         int64       `json:"createTime"`
		StartTime          int64       `json:"startTime"`
		EndTime            int64       `json:"endTime"`
		WorkflowDefinition struct {
			Name string `json:"name"`
			Rev  string `json:"rev"`
		} `json:"workflowDefinition"`
	} `json:"details"`
}

type eventTaskPayload struct {
	Details struct {
		Logs              []string    `json:"logs"`
		TaskID            string      `json:"taskId"`
		TaskName          string      `json:"taskName"`
		TaskReferenceName string      `json:"taskReferenceName"`
		WorkflowID        string      `json:"workflowId"`
		TransactionID     string      `json:"transactionId"`
		Type              string      `json:"type"`
		Status            string      `json:"status"`
		IsRetried         bool        `json:"isRetried"`
		Input             interface{} `json:"input"`
		Output            interface{} `json:"output"`
		CreateTime        int64       `json:"createTime"`
		StartTime         int64       `json:"startTime"`
		EndTime           int64       `json:"endTime"`
		Retries           int         `json:"retries"`
		RetryDelay        int         `json:"retryDelay"`
		AckTimeout        int         `json:"ackTimeout"`
		Timeout           int         `json:"timeout"`
	} `json:"details"`
}

type eventFalseTransactionPayload struct {
	Error   string `json:"error"`
	Details struct {
		TransactionID string      `json:"transactionId"`
		Status        string      `json:"status"`
		Output        interface{} `json:"output"`
	} `json:"details"`
}

type eventFalseWorkflowPayload struct {
	Error   string `json:"error"`
	Details struct {
		TransactionID string      `json:"transactionId"`
		WorkflowID    string      `json:"workflowId"`
		Status        string      `json:"status"`
		Output        interface{} `json:"output"`
	} `json:"details"`
}

type eventFalseTaskPayload struct {
	Error   string `json:"error"`
	Details struct {
		TransactionID string      `json:"transactionId"`
		TaskID        string      `json:"taskId"`
		Status        string      `json:"status"`
		Output        interface{} `json:"output"`
		IsSystem      bool        `json:"isSystem"`
	} `json:"details"`
}

type eventFalseSystemPayload struct {
	Error   string      `json:"error"`
	Details interface{} `json:"details"`
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

	// Await till the consumer has been set up
	<-consumer.ready
	log.Println("Sarama consumer up and running!...")
	sendDiscordEmbed(embed)

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

	embed = &discordgo.MessageEmbed{
		Title: "Byeeee",
		Image: &discordgo.MessageEmbedImage{
			URL: "https://i.imgur.com/LzfTUGd.gif",
		},
		URL: frontEndURL,
	}
	sendDiscordEmbed(embed)

	if err = client.Close(); err != nil {
		log.Panicf("Error closing client: %v", err)
	}
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
		if err := json.Unmarshal(message.Value, &payload); err != nil {
			log.Print("Bad payload", err)
			continue
		}

		if payload.IsError == true {
			if payload.Type == "TASK" {
				falseTaskPayload := eventFalseTaskPayload{}
				if err := json.Unmarshal(message.Value, &falseTaskPayload); err != nil {
					log.Println("Bad falseTaskPayload", err)
					continue
				}

				if falseTaskPayload.Details.IsSystem != true {
					jsonByteDetails, _ := json.MarshalIndent(falseTaskPayload.Details, "", "    ")
					jsonByteError, _ := json.MarshalIndent(falseTaskPayload.Error, "", "    ")

					if len(jsonByteDetails) > 1000 {
						jsonByteDetails = append(jsonByteDetails[0:1000], '.', '.', '.')
					}

					embed := &discordgo.MessageEmbed{
						Title:       "False task event had reported",
						URL:         fmt.Sprintf("%s/transaction/%s", frontEndURL, payload.TransactionID),
						Description: "Click on the title to view the details",
						Fields: []*discordgo.MessageEmbedField{{
							Name:  "Transaction ID",
							Value: fmt.Sprintf("```%s```", payload.TransactionID),
						}, {
							Name:  "Details",
							Value: fmt.Sprintf("```json\n%s\n```", string(jsonByteDetails)),
						}, {
							Name:  "Error",
							Value: fmt.Sprintf("```json\n%s\n```", string(jsonByteError)),
						}},
					}
					sendDiscordEmbed(embed)
				}
			} else if payload.Type == "WORKFLOW" {
				falseWorkflowPayload := eventFalseWorkflowPayload{}
				if err := json.Unmarshal(message.Value, &falseWorkflowPayload); err != nil {
					log.Println("Bad falseTaskPayload", err)
					continue
				}
				jsonByteDetails, _ := json.MarshalIndent(falseWorkflowPayload.Details, "", "    ")
				jsonByteError, _ := json.MarshalIndent(falseWorkflowPayload.Error, "", "    ")

				if len(jsonByteDetails) > 1000 {
					jsonByteDetails = append(jsonByteDetails[0:1000], '.', '.', '.')
				}

				embed := &discordgo.MessageEmbed{
					Title:       "False workflow event had reported",
					URL:         fmt.Sprintf("%s/transaction/%s", frontEndURL, payload.TransactionID),
					Description: "Click on the title to view the details",
					Fields: []*discordgo.MessageEmbedField{{
						Name:  "Transaction ID",
						Value: fmt.Sprintf("```%s```", payload.TransactionID),
					}, {
						Name:  "Details",
						Value: fmt.Sprintf("```json\n%s\n```", string(jsonByteDetails)),
					}, {
						Name:  "Error",
						Value: fmt.Sprintf("```json\n%s\n```", string(jsonByteError)),
					}},
				}
				sendDiscordEmbed(embed)
			} else if payload.Type == "TRANSACTION" {
				falseTransactionPayload := eventFalseTransactionPayload{}
				if err := json.Unmarshal(message.Value, &falseTransactionPayload); err != nil {
					log.Println("Bad falseTaskPayload", err)
					continue
				}
				jsonByteDetails, _ := json.MarshalIndent(falseTransactionPayload.Details, "", "    ")
				jsonByteError, _ := json.MarshalIndent(falseTransactionPayload.Error, "", "    ")

				if len(jsonByteDetails) > 1000 {
					jsonByteDetails = append(jsonByteDetails[0:1000], '.', '.', '.')
				}

				embed := &discordgo.MessageEmbed{
					Title:       "False transaction event had reported",
					URL:         fmt.Sprintf("%s/transaction/%s", frontEndURL, payload.TransactionID),
					Description: "Click on the title to view the details",
					Fields: []*discordgo.MessageEmbedField{{
						Name:  "Transaction ID",
						Value: fmt.Sprintf("```%s```", payload.TransactionID),
					}, {
						Name:  "Details",
						Value: fmt.Sprintf("```json\n%s\n```", string(jsonByteDetails)),
					}, {
						Name:  "Error",
						Value: fmt.Sprintf("```json\n%s\n```", string(jsonByteError)),
					}},
				}
				sendDiscordEmbed(embed)
			} else if payload.Type == "SYSTEM" {
				embed := &discordgo.MessageEmbed{
					Title: "Something went wrong",
					Fields: []*discordgo.MessageEmbedField{{
						Name:  "Transaction ID",
						Value: fmt.Sprintf("```%s```", payload.TransactionID),
					}, {
						Name:  "Details",
						Value: fmt.Sprintf("```json\n%s\n```", string(message.Value)),
					}},
				}
				sendDiscordEmbed(embed)
			}
		} else {
			if payload.Type == "TASK" {
				taskEventPayload := eventTaskPayload{}
				if err := json.Unmarshal(message.Value, &taskEventPayload); err != nil {
					log.Println("Bad taskEventPayload", err)
					continue
				}

				if contains(interestedTaskStatus, taskEventPayload.Details.Status) {
					jsonByteOutput, _ := json.MarshalIndent(taskEventPayload.Details.Output, "", "    ")

					if len(jsonByteOutput) > 1000 {
						jsonByteOutput = append(jsonByteOutput[0:1000], '.', '.', '.')
					}

					embed := &discordgo.MessageEmbed{
						Title:       fmt.Sprintf("Task: %s", taskEventPayload.Details.Status),
						URL:         fmt.Sprintf("%s/transaction/%s", frontEndURL, payload.TransactionID),
						Description: "Click on the title to view the details",
						Fields: []*discordgo.MessageEmbedField{{
							Name:  "Transaction ID",
							Value: fmt.Sprintf("```%s```", payload.TransactionID),
						}, {
							Name:  "Task ID",
							Value: fmt.Sprintf("```%s```", taskEventPayload.Details.TaskID),
						}, {
							Name:  "Task Name",
							Value: fmt.Sprintf("```\n%s\n(%s)\n```", taskEventPayload.Details.TaskName, taskEventPayload.Details.TaskReferenceName),
						}, {
							Name:  "Output",
							Value: fmt.Sprintf("```json\n%s\n```", string(jsonByteOutput)),
						}},
					}

					sendDiscordEmbed(embed)
				}
			} else if payload.Type == "Workflow" {
				workflowPayload := eventWorkflowPayload{}
				if err := json.Unmarshal(message.Value, &workflowPayload); err != nil {
					log.Println("Bad workflowPayload", err)
					continue
				}

				if contains(interestedWorkflowStatus, workflowPayload.Details.Status) {
					jsonByteOutput, _ := json.MarshalIndent(workflowPayload.Details.Output, "", "    ")
					jsonByteWorkflow, _ := json.MarshalIndent(workflowPayload.Details.WorkflowDefinition, "", "    ")

					if len(jsonByteOutput) > 1000 {
						jsonByteOutput = append(jsonByteOutput[0:1000], '.', '.', '.')
					}

					embed := &discordgo.MessageEmbed{
						Title:       fmt.Sprintf("Workflow: %s|%s", workflowPayload.Details.WorkflowDefinition.Name, workflowPayload.Details.WorkflowDefinition.Rev),
						URL:         fmt.Sprintf("%s/transaction/%s", frontEndURL, payload.TransactionID),
						Description: "Click on the title to view the details",
						Fields: []*discordgo.MessageEmbedField{{
							Name:  "Transaction ID",
							Value: fmt.Sprintf("```%s```", payload.TransactionID),
						}, {
							Name:  "Output",
							Value: fmt.Sprintf("```json\n%s\n```", string(jsonByteOutput)),
						}, {
							Name:  "Workflow Definition",
							Value: fmt.Sprintf("```json\n%s\n```", string(jsonByteWorkflow)),
						}},
					}

					sendDiscordEmbed(embed)
				}
			} else if payload.Type == "TRANSACTION" {
				transactionPayload := eventTransactionPayload{}
				if err := json.Unmarshal(message.Value, &transactionPayload); err != nil {
					log.Println("Bad falseTaskPayload", err)
					continue
				}

				if contains(interestedTransactionStatus, transactionPayload.Details.Status) {
					jsonByteOutput, _ := json.MarshalIndent(transactionPayload.Details.Output, "", "    ")
					jsonByteWorkflow, _ := json.MarshalIndent(transactionPayload.Details.WorkflowDefinition, "", "    ")

					if len(jsonByteOutput) > 1000 {
						jsonByteOutput = append(jsonByteOutput[0:1000], '.', '.', '.')
					}

					embed := &discordgo.MessageEmbed{
						Title:       fmt.Sprintf("Transaction: %s", transactionPayload.Details.Status),
						URL:         fmt.Sprintf("%s/transaction/%s", frontEndURL, payload.TransactionID),
						Description: "Click on the title to view the details",
						Fields: []*discordgo.MessageEmbedField{{
							Name:  "Transaction ID",
							Value: fmt.Sprintf("```%s```", payload.TransactionID),
						}, {
							Name:  "Output",
							Value: fmt.Sprintf("```json\n%s\n```", string(jsonByteOutput)),
						}, {
							Name:  "Workflow Definition",
							Value: fmt.Sprintf("```json\n%s\n```", string(jsonByteWorkflow)),
						}},
					}
					sendDiscordEmbed(embed)
				}
			}
		}

		session.MarkMessage(message, "")
	}

	return nil
}

func sendDiscordEmbed(embed *discordgo.MessageEmbed) error {
	_, err := dg.ChannelMessageSendEmbed(notiChannel, embed)
	if err != nil {
		log.Print("Error while sending message", err)
	}
	return err
}
