package main;

import (
	"github.com/confluentinc/confluent-kafka-go/kafka"
	"fmt"
	"flag"
	"os"
	"log"
)

func produce(broker string, topic string, value string, key string) {
	log.Println("delivery message")
	p, err := kafka.NewProducer(&kafka.ConfigMap{"bootstrap.servers": "localhost"})

	if nil != err {
		panic(err)
	}

	defer p.Close()

	go func() {
		for e := range p.Events() {
			switch ev := e.(type) {
			case *kafka.Message:
				if ev.TopicPartition.Error != nil {
					fmt.Printf("Delivery failed: %v\n", ev.TopicPartition)
				} else {
					//fmt.Printf("Delivered message to %v\n", ev.TopicPartition)
					log.Println("success")
				}
			}
		}
	}()


	p.Produce(&kafka.Message{
		TopicPartition: kafka.TopicPartition{Topic: &topic, Partition: kafka.PartitionAny},
		Value:          []byte(value),
		// Key: []byte(strconv.Itoa(j)),
	}, nil)

	p.Flush(15 * 1000)
	log.Println("delivered")
}

func consume(broker string, topic string, group string) {
	log.Println("consuming")
	c, err := kafka.NewConsumer(&kafka.ConfigMap{
		"bootstrap.servers": broker,
		"group.id":          group,
		"auto.offset.reset": "earliest",
	})

	if nil != err {
		panic(err)
	}

	c.SubscribeTopics([]string{topic}, nil)

	for {
		message, err := c.ReadMessage(-1)

		if err != nil {
			fmt.Printf("Consumer error: %v (%v)\n", err, message)
		} else {
			fmt.Println(message.Key)
			log.Printf("Message on %s: %s\n", message.TopicPartition, message.Value)
		}
	}

	c.Close()
}

func main() {
	switch os.Args[1] {
	case "produce":
		producerFlatSet := flag.NewFlagSet("producer", flag.ContinueOnError)
		broker := producerFlatSet.String("broker", "localhost:9092", "broker address")
		topic := producerFlatSet.String("topic", "test", "topic")
		value := producerFlatSet.String("value", "test", "message of producer")
		key := producerFlatSet.String("key", "", "key")

		producerFlatSet.Parse(os.Args[2:])
		produce(*broker, *topic, *value, *key)

	case "consume":
		consumerFlagSet := flag.NewFlagSet("consumer", flag.ExitOnError)
		broker := consumerFlagSet.String("broker", "localhost:9092", "broker address")
		topic := consumerFlagSet.String("topic", "test", "topic")
		group := consumerFlagSet.String("group", "1", "group")
		consumerFlagSet.Parse(os.Args[2:])
		consume(*broker, *topic, *group)
	}
}