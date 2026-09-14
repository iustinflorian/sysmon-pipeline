package main

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"net/http"
	"os"
	"time"

	amqp "github.com/rabbitmq/amqp091-go"
	"go.mongodb.org/mongo-driver/v2/bson"
	"go.mongodb.org/mongo-driver/v2/mongo"
	"go.mongodb.org/mongo-driver/v2/mongo/options"
)

type Target struct {
	ID        bson.ObjectID `json:"id" bson:"_id,omitempty"`
	URL       string        `json:"url" bson:"url"`
	Interval  int           `json:"interval" bson:"interval"`
	Status    string        `json:"status" bson:"status"`
	CreatedAt time.Time     `json:"created_at" bson:"created_at"`
}

type IncidentEvent struct {
	TargetID  string    `json:"target_id"`
	URL       string    `json:"url"`
	Status    string    `json:"status"`
	Timestamp time.Time `json:"timestamp"`
}

func main() {
	mongoURI := os.Getenv("MONGO_URI")
	if mongoURI == "" {
		mongoURI = "mongodb://root:secretpassword@localhost:27017"
	}

	rabbitURI := os.Getenv("RABBITMQ_URI")
	if rabbitURI == "" {
		rabbitURI = "amqp://guest:guest@localhost:5672/"
	}

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	mongoClient, err := mongo.Connect(options.Client().ApplyURI(mongoURI))
	if err != nil {
		log.Fatalf("Worker failed connection to MongoDB: %v", err)
	}

	if err := mongoClient.Ping(ctx, nil); err != nil {
		log.Fatalf("Worker MongoDB ping failed: %v", err)
	}
	fmt.Println("Worker connected to MongoDB successfully!")

	var rabbitConn *amqp.Connection
	for range 5 {
		rabbitConn, err = amqp.Dial(rabbitURI)
		if err == nil {
			break
		}
		fmt.Println("Waiting for RabbitMQ to be ready...")
		time.Sleep(2 * time.Second)
	}
	if err != nil {
		log.Fatalf("Failed to connect to RabbitMQ: %v", err)
	}
	defer rabbitConn.Close()

	ch, err := rabbitConn.Channel()
	if err != nil {
		log.Fatalf("Failed to open RabbitMQ channel: %v", err)
	}
	defer ch.Close()

	q, err := ch.QueueDeclare("incidents", true, false, false, false, nil)
	if err != nil {
		log.Fatalf("Failed to declare RabbitMQ queue: %v", err)
	}
	fmt.Println("Worker connected to RabbitMQ and declared 'incidents' queue!")

	collection := mongoClient.Database("sysmon").Collection("targets")

	ticker := time.NewTicker(5 * time.Second)
	defer ticker.Stop()

	httpClient := &http.Client{
		Timeout: 5 * time.Second,
	}

	for range ticker.C {
		pollTargets(collection, httpClient, ch, q.Name)
	}
}

func pollTargets(collection *mongo.Collection, client *http.Client, ch *amqp.Channel, queueName string) {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	cursor, err := collection.Find(ctx, bson.M{})
	if err != nil {
		log.Printf("Worker failed to fetch targets: %v", err)
		return
	}
	defer cursor.Close(ctx)

	var targets []Target
	if err := cursor.All(ctx, &targets); err != nil {
		log.Printf("Worker failed to decode targets: %v", err)
		return
	}

	for _, target := range targets {
		go probeTarget(collection, client, target, ch, queueName)
	}
}

func probeTarget(collection *mongo.Collection, client *http.Client, target Target, ch *amqp.Channel, queueName string) {
	resp, err := client.Get(target.URL)
	newStatus := "DOWN"

	if err == nil {
		if resp.StatusCode >= 200 && resp.StatusCode < 300 {
			newStatus = "UP"
		}
		resp.Body.Close()
	}

	if target.Status == newStatus {
		return
	}

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	filter := bson.M{"_id": target.ID}
	update := bson.M{"$set": bson.M{"status": newStatus}}

	_, err = collection.UpdateOne(ctx, filter, update)
	if err != nil {
		log.Printf("Failed to update status for %s: %v", target.URL, err)
		return
	}

	fmt.Printf("Target %s status changed: %s -> %s\n", target.URL, target.Status, newStatus)

	if newStatus == "DOWN" {
		publishIncidentEvent(ch, queueName, target)
	}
}

func publishIncidentEvent(ch *amqp.Channel, queueName string, target Target) {
	event := IncidentEvent{
		TargetID:  target.ID.Hex(),
		URL:       target.URL,
		Status:    "DOWN",
		Timestamp: time.Now(),
	}

	body, err := json.Marshal(event)
	if err != nil {
		log.Printf("Failed to marshal incident event: %v", err)
		return
	}

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	err = ch.PublishWithContext(ctx, "", queueName, false, false,
		amqp.Publishing{
			ContentType: "application/json",
			Body:        body,
		},
	)

	if err != nil {
		log.Printf("Failed to publish incident event to RabbitMQ: %v", err)
		return
	}

	fmt.Printf("Published incident event to RabbitMQ for URL: %s\n", target.URL)
}
