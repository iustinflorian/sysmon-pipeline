package main

import (
	"context"
	"fmt"
	"log"
	"net/http"
	"os"
	"time"

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

func main() {
	uri := os.Getenv("MONGO_URI")
	if uri == "" {
		uri = "mongodb://root:secretpassword@localhost:27017"
	}

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	client, err := mongo.Connect(options.Client().ApplyURI(uri))
	if err != nil {
		log.Fatalf("Worker failed connection to MongoDB: %v", err)
	}

	if err := client.Ping(ctx, nil); err != nil {
		log.Fatalf("Worker MongoDB ping failed: %v", err)
	}
	fmt.Println("Worker connected to MongoDB successfully!")

	collection := client.Database("sysmon").Collection("targets")

	ticker := time.NewTicker(5 * time.Second)
	defer ticker.Stop()

	httpClient := &http.Client{
		Timeout: 5 * time.Second,
	}

	for range ticker.C {
		pollTargets(collection, httpClient)
	}
}

func pollTargets(collection *mongo.Collection, client *http.Client) {
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
		go probeTarget(collection, client, target)
	}
}

func probeTarget(collection *mongo.Collection, client *http.Client, target Target) {
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
}
