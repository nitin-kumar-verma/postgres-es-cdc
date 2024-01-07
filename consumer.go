package main

import (
	"log"

	"github.com/IBM/sarama"
)

var consumer sarama.PartitionConsumer

func ConsumerWorkFlow() {
	var err error
	consumer, err = GetConsumer()
	if err != nil {
		log.Fatalln("error creating kafka producer:", err)
		return
	}
	defer consumer.Close()
	ConsumeMessages()

}
