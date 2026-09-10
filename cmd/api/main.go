package main

import (
	"context"
	"encoding/json"
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

type Server struct {
	client     *mongo.Client
	collection *mongo.Collection
}

func main() {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	uri := os.Getenv("MONGO_URI")
	if uri == "" {
		uri = "mongodb://root:secretpassword@localhost:27017"
	}

	client, err := mongo.Connect(options.Client().ApplyURI(uri))
	if err != nil {
		log.Fatalf("Failed to create MongoDB client: %v", err)
	}

	if err := client.Ping(ctx, nil); err != nil {
		log.Fatalf("MongoDB ping failed: %v", err)
	}
	fmt.Println("Successfully connected to MongoDB!")

	srv := &Server{
		client:     client,
		collection: client.Database("sysmon").Collection("targets"),
	}

	http.HandleFunc("/targets", srv.handleTargets)

	fmt.Println("sysmon-pipeline API listening on :8080...")
	if err := http.ListenAndServe(":8080", nil); err != nil {
		log.Fatal(err)
	}
}

func (srv *Server) handleTargets(w http.ResponseWriter, r *http.Request) {
	switch r.Method {
	case http.MethodPost:
		var req struct {
			URL      string `json:"url"`
			Interval int    `json:"interval"`
		}

		if err := json.NewDecoder(r.Body).Decode(&req); err != nil || req.URL == "" {
			http.Error(w, "Invalid request body: 'url' is required", http.StatusBadRequest)
			return
		}

		if req.Interval <= 0 {
			req.Interval = 30
		}

		target := Target{
			ID:        bson.NewObjectID(),
			URL:       req.URL,
			Interval:  req.Interval,
			Status:    "PENDING",
			CreatedAt: time.Now(),
		}

		ctx, cancel := context.WithTimeout(r.Context(), 5*time.Second)
		defer cancel()

		_, err := srv.collection.InsertOne(ctx, target)
		if err != nil {
			http.Error(w, "Failed to insert target into MongoDB", http.StatusInternalServerError)
			return
		}

		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusCreated)
		json.NewEncoder(w).Encode(target)

	case http.MethodGet:
		ctx, cancel := context.WithTimeout(r.Context(), 5*time.Second)
		defer cancel()

		cursor, err := srv.collection.Find(ctx, bson.M{})
		if err != nil {
			http.Error(w, "Failed to query MongoDB", http.StatusInternalServerError)
			return
		}
		defer cursor.Close(ctx)

		targets := make([]Target, 0)
		if err := cursor.All(ctx, &targets); err != nil {
			http.Error(w, "Failed to decode targets from MongoDB", http.StatusInternalServerError)
			return
		}

		w.Header().Set("Content-Type", "application/json")
		json.NewEncoder(w).Encode(targets)

	default:
		http.Error(w, "Method not allowed", http.StatusMethodNotAllowed)
	}
}
