package main

import (
	"fmt"
	"log"

	"github.com/confluentinc/confluent-kafka-go/kafka"
)

func main() {
	deliveryChan := make(chan kafka.Event)
	producer := NewKafkaProducer()
	Publish("Estorno realizado com sucesso!", "teste", producer, nil, deliveryChan)
	go DeliveryReport(deliveryChan)
	fmt.Println("Leo Silva")
	producer.Flush(1000)
}

func NewKafkaProducer() *kafka.Producer {
	configMap := &kafka.ConfigMap{
		"bootstrap.servers": "kafka:9092",
		"delivery.timeout.ms": "0",
		"acks": "all",
		"enable.idempotence": "true",
	}
	p, err := kafka.NewProducer(configMap)
	if err != nil {
    log.Println(err.Error())
  }
	return p
}

func Publish(msg string, topic string, producer *kafka.Producer, key []byte, deliveryChan chan kafka.Event) error {
	message := &kafka.Message{
		Value: []byte(msg),
		TopicPartition: kafka.TopicPartition{Topic: &topic, Partition: kafka.PartitionAny},
		Key: key,
	}
	err := producer.Produce(message, deliveryChan)
	if err!= nil {
    return err
  }
	return nil
}

func DeliveryReport(deliveryChan chan kafka.Event) {
	for e := range deliveryChan {
    switch ev := e.(type) {
    case *kafka.Message:
      fmt.Printf("Delivered message to %v: %v\n", ev.TopicPartition, string(ev.Value))
			// encerrar programa 
    case kafka.Error:
      fmt.Printf("Delivery failed: %v\n", ev)
    }
  }
}
