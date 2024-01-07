package main

import (
	"crypto/tls"
	"log"
	"net/http"

	"github.com/IBM/sarama"
	"github.com/elastic/go-elasticsearch/v8"
)

var partitionConsumer sarama.PartitionConsumer
var esClient *elasticsearch.Client

func ConsumerWorkFlow() {

	consumer, err := GetConsumer()
	if err != nil {
		log.Fatalln("error creating consumer:", err)
		return
	}
	defer consumer.Close()
	partitionConsumer, err = GetPartitionConsumer(consumer)
	if err != nil {
		log.Fatalln("error creating partition consumer:", err)
		return
	}
	defer partitionConsumer.Close()

	cfg := elasticsearch.Config{

		Addresses: []string{AppConfigs.es_url},
		Username:  AppConfigs.es_username,
		Password:  AppConfigs.es_password,
		Transport: &http.Transport{
			TLSClientConfig: &tls.Config{InsecureSkipVerify: true},
		},
		//CertificateFingerprint: "181a26fecb8075d81d2f9789a93a395fb0431a8aa0abd2e454bc4b87c160f1ae",

	}

	// Create Elasticsearch client
	esClient, err = elasticsearch.NewClient(cfg)

	if err != nil {
		log.Fatalf("Error creating Elasticsearch client: %v", err)
	}

	// Check Elasticsearch server status
	_, err = esClient.Info()
	if err != nil {
		log.Fatalf("Error checking Elasticsearch server status: %v", err)
	}

	ConsumeMessages()
}
