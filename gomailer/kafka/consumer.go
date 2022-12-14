package kafka

import (
	"fmt"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/confluentinc/examples/clients/cloud/go/ccloud"
	// "go.mongodb.org/mongo-driver/bson"
	"gopkg.in/confluentinc/confluent-kafka-go.v1/kafka"
	"gopkg.in/mgo.v2/bson"
)

// RecordValue represents the struct of the value in a Kafka message
type RecordValue ccloud.RecordValue
type EmailType struct {
	recipent string
	body     string
	subject  string
}

func ReadEvent() {

	// Initialization
	// configFile, topic := ccloud.ParseArgs()
	// conf := ccloud.ReadCCloudConfig(*configFile)

	// Create Consumer instance
	topic := "mailer"
	c, err := kafka.NewConsumer(&kafka.ConfigMap{
		"bootstrap.servers": "127.0.0.1:29092", // conf["bootstrap.servers"],
		"sasl.mechanisms":   "PLAIN",           // conf["sasl.mechanisms"],
		"security.protocol": "PLAINTEXT",       //conf["security.protocol"],
		"sasl.username":     "",                //  conf["sasl.username"],
		"sasl.password":     "",                //  conf["sasl.password"],
		"group.id":          "go_example_group_1",
		"auto.offset.reset": "earliest",
	})
	if err != nil {
		fmt.Printf("Failed to create consumer: %s", err)
		os.Exit(1)
	}

	// Subscribe to topic
	err = c.SubscribeTopics([]string{topic}, nil)
	// Set up a channel for handling Ctrl-C, etc
	sigchan := make(chan os.Signal, 1)
	signal.Notify(sigchan, syscall.SIGINT, syscall.SIGTERM)

	// Process messages
	// totalCount := 0
	run := true
	for run == true {
		select {
		case sig := <-sigchan:
			fmt.Printf("Caught signal %v: terminating\n", sig)
			run = false
		default:
			msg, err := c.ReadMessage(100 * time.Millisecond)
			if err != nil {
				// Errors are informational and automatically handled by the consumer
				continue
			}
			// recordKey := string(msg.Key)
			recordValue := msg.Value
			data := EmailType{}
			err = bson.UnmarshalJSON(recordValue, &data)
			if err != nil {
				fmt.Printf("Failed to decode JSON at offset %d: %v", msg.TopicPartition.Offset, err)
				// fmt.Println(string(recordValue))
				continue
			}
			// count := data.Count
			// totalCount += count
			// fmt.Printf(data.body, data.recipent, data.subject)
			fmt.Printf("Checking body %s", data.body)
			// fmt.Printf(data.body)) //, data.recipent, data.subject)
			// fmt.Printf("Consumed record with key %s and value %s, and updated total count to %d\n", recordKey, recordValue, totalCount)
		}
	}

	fmt.Printf("Closing consumer\n")
	c.Close()

}
