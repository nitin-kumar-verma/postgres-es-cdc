package main

import (
	"encoding/json"
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
	es_username string
	es_password string
}

type DatabaseEvent struct {
	OperationType OperationType          `json:"operationType"`
	TableName     string                 `json:"tableName"`
	Payload       map[string]interface{} `json:"payload"`
}

type OperationType string

const (
	TOPIC         string        = "CDC"
	PRODUCER_MODE string        = "PRODUCER"
	CONSUMER_MODE string        = "CONSUMER"
	INSERT        OperationType = "INSERT"
	UPDATE        OperationType = "UPDATE"
	DELETE        OperationType = "DELETE"
)

func GetConsumer() (sarama.Consumer, error) {
	config := sarama.NewConfig()
	consumer, err := sarama.NewConsumer(strings.Split(AppConfigs.kafka_url, ","), config)
	if err != nil {
		return nil, err
	}
	return consumer, nil
}

func GetPartitionConsumer(consumer sarama.Consumer) (sarama.PartitionConsumer, error) {
	partitionConsumer, err := consumer.ConsumePartition(TOPIC, 0, sarama.OffsetOldest)
	if err != nil {
		return nil, err
	}
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

func ProduceMessage(opType OperationType, tableName string, values map[string]interface{}) {

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

	var dbEvent DatabaseEvent
	for {
		select {
		case msg := <-partitionConsumer.Messages():
			err := json.Unmarshal(msg.Value, &dbEvent)
			if err != nil {
				log.Printf("Error unmarshalling event")
				continue
			}
			switch dbEvent.OperationType {
			case INSERT:
				if !IndexExists(dbEvent.TableName) {
					CreateIndex(dbEvent.TableName)
				}
				InsertDocument(dbEvent.TableName, dbEvent.Payload)
			case UPDATE:
				UpdateDocument(dbEvent.TableName, dbEvent.Payload)
			case DELETE:
				DeleteDocument(dbEvent.TableName, dbEvent.Payload)
			}

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

		esUserName := os.Getenv("ES_USERNAME")
		if esUserName == "" {
			panic("ES_URL environment variable not set")
		}
		esPassword := os.Getenv("ES_PASSWORD")
		if esPassword == "" {
			panic("ES_URL environment variable not set")
		}
		appConfig.es_url = esURL
		appConfig.es_username = esUserName
		appConfig.es_password = esPassword

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
