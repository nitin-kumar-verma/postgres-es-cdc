package main

import (
	"encoding/json"
	"fmt"
	"log"
	"os"
	"strings"

	"github.com/IBM/sarama"
	"github.com/joho/godotenv"
)

type AppConfig struct {
	pg_conn_url string
	mode        string
	kafka_url   string
	es_url      string
}

type DatabaseEvent struct {
	OperationType string                 `json:"operationType"`
	TableName     string                 `json:"tableName"`
	Payload       map[string]interface{} `json:"payload"`
}

const (
	TOPIC         string = "CDC"
	PRODUCER_MODE string = "PRODUCER"
	CONSUMER_MODE string = "CONSUMER"
)

func GetConsumer() (sarama.PartitionConsumer, error) {
	config := sarama.NewConfig()
	consumer, err := sarama.NewConsumer(strings.Split(AppConfigs.kafka_url, ","), config)
	if err != nil {
		return nil, err
	}
	defer consumer.Close()

	partitionConsumer, err := consumer.ConsumePartition(TOPIC, 0, sarama.OffsetOldest)
	if err != nil {
		return nil, err
	}
	defer partitionConsumer.Close()
	return partitionConsumer, nil

}

func GetProducer() (sarama.SyncProducer, error) {
	config := sarama.NewConfig()
	config.Producer.Return.Successes = true
	producer, err := sarama.NewSyncProducer(strings.Split(AppConfigs.kafka_url, ","), config)
	if err != nil {
		return nil, err
	}

	return producer, nil
}

func ProduceMessage(opType, tableName string, values map[string]interface{}) {

	event := DatabaseEvent{
		OperationType: opType,
		TableName:     tableName,
		Payload:       values,
	}
	messageJSON, err := json.Marshal(event)
	if err != nil {
		log.Printf("Failed to marshal message to JSON: %v", err)
		return
	}
	message := &sarama.ProducerMessage{
		Topic: TOPIC,
		Value: sarama.StringEncoder(messageJSON),
	}

	_, _, err = producer.SendMessage(message)
	if err != nil {
		//This case needs to handled with utmost priority as this will lead to data corruption
		// fix the code and resync the data missed(update records manually)
		log.Printf("Failed to send message: %v", err)
	}
}

func ConsumeMessages() {

	for {
		select {
		case msg := <-consumer.Messages():
			fmt.Printf("Consumed message: %s\n", string(msg.Value))
		}
	}
}

func LoadConfigs() AppConfig {
	//Mode denotes consumer mode or producer mode
	err := godotenv.Load()
	if err != nil {
		panic("Error loading environment variables")
	}
	appConfig := AppConfig{}
	mode := os.Getenv("MODE")
	if mode == PRODUCER_MODE {

		pgURL := os.Getenv("PG_CONNECTION_URL")
		if pgURL == "" {
			panic("PG_CONNECTION_URL environment variable not set")
		}
		appConfig.pg_conn_url = pgURL

	} else if mode == CONSUMER_MODE {
		esURL := os.Getenv("ES_URL")
		if esURL == "" {
			panic("ES_URL environment variable not set")
		}
		appConfig.es_url = esURL
	} else {
		panic("INVALID MODE")
	}
	appConfig.mode = mode

	kafkaURL := os.Getenv("KAFKA_URL")
	if kafkaURL == "" {
		panic("KAFKA_URL environment variable not set")
	}
	appConfig.kafka_url = kafkaURL

	return appConfig
}
