package main

import (
	"encoding/json"
	"fmt"
	"log"
	"os"
	"time"

	amqp "github.com/rabbitmq/amqp091-go"
)

type IncidentEvent struct {
	TargetID  string    `json:"target_id"`
	URL       string    `json:"url"`
	Status    string    `json:"status"`
	Timestamp time.Time `json:"timestamp"`
}

func main() {
	rabbitURI := os.Getenv("RABBITMQ_URI")
	if rabbitURI == "" {
		rabbitURI = "amqp://guest:guest@localhost:5672/"
	}

	var conn *amqp.Connection
	var err error

	for range 15 {
		conn, err = amqp.Dial(rabbitURI)
		if err == nil {
			break
		}
		fmt.Println("Consumer waiting for RabbitMQ to be ready...")
		time.Sleep(2 * time.Second)
	}
	if err != nil {
		log.Fatalf("Consumer failed to connect to RabbitMQ: %v", err)
	}
	defer conn.Close()

	ch, err := conn.Channel()
	if err != nil {
		log.Fatalf("Failed to open channel: %v", err)
	}
	defer ch.Close()

	q, err := ch.QueueDeclare("incidents", true, false, false, false, nil)
	if err != nil {
		log.Fatalf("Failed to declare queue: %v", err)
	}

	msgs, err := ch.Consume(q.Name, "", false, false, false, false, nil)
	if err != nil {
		log.Fatalf("Failed to register consumer: %v", err)
	}

	fmt.Println("Alert Consumer connected and waiting for incident events on 'incidents' queue...")

	forever := make(chan bool)

	go func() {
		for d := range msgs {
			var event IncidentEvent
			err := json.Unmarshal(d.Body, &event)
			if err != nil {
				log.Fatalf("Failed to unmarshal event: %v", err)
				d.Nack(false, false)
			}
			processAlert(event)
			d.Ack(false)
		}
	}()

	<-forever
}

func processAlert(event IncidentEvent) {
	fmt.Printf("\n--->\n")
	fmt.Printf("ALERT TRIGGERED FOR TARGET:\n")
	fmt.Printf("ID:        %s\n", event.TargetID)
	fmt.Printf("URL:       %s\n", event.URL)
	fmt.Printf("Status:    %s\n", event.Status)
	fmt.Printf("Timestamp: %s\n", event.Timestamp.Format(time.RFC3339))
	fmt.Printf("Action:    Dispatched webhook / alert notification\n")
	fmt.Printf("<---\n\n")
}
