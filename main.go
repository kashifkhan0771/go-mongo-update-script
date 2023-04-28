package main

import (
	"context"
	"encoding/json"
	"fmt"
	"io/ioutil"
	"log"
	"time"

	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"
	"go.mongodb.org/mongo-driver/mongo/readpref"
)

const (
	// database details
	DatabaseURL    = ""
	DatabaseName   = ""
	CollectionName = ""

	// update column keys
	Description = "description"

	// MaxMatchLimit - is the max limit for matched document
	MaxMatchLimit = 1

	// Timeout - secure your database connection and will disconnect after timeout pass
	Timeout = 150
)

// Record - hold the JSON file schema
type Record struct {
	Name        string `json:"name"`
	Description string `json:"description"`
}

// readJSONFile returns records in JSON file.
func readJSONFile() ([]Record, error) {
	// Load the JSON file into memory
	data, err := ioutil.ReadFile("example.json")
	if err != nil {
		log.Fatalf("Failed to read file: %v", err)
	}

	// Parse the JSON data into a slice of Record structs
	var records []Record
	if err = json.Unmarshal(data, &records); err != nil {
		log.Fatalf("Failed to parse JSON: %v", err)
	}
	return records, err
}

// This is a user defined method to close resources.
// This method closes mongoDB connection and cancel context.
func close(client *mongo.Client, ctx context.Context,
	cancel context.CancelFunc) {

	// CancelFunc to cancel to context
	defer cancel()

	// client provides a method to close
	// a mongoDB connection.
	defer func() {

		// client.Disconnect method also has deadline.
		// returns error if any,
		if err := client.Disconnect(ctx); err != nil {
			panic(err)
		}
	}()
}

// This is a user defined method that returns mongo.Client,
// context.Context, context.CancelFunc and error.
// mongo.Client will be used for further database operation.
// context.Context will be used set deadlines for process.
// context.CancelFunc will be used to cancel context and
// resource associated with it.

func connect(uri string) (*mongo.Client, context.Context,
	context.CancelFunc, error) {

	// ctx will be used to set deadline for process, here
	// deadline will of 30 seconds.
	ctx, cancel := context.WithTimeout(context.Background(),
		150*time.Second)

	// mongo.Connect return mongo.Client method
	client, err := mongo.Connect(ctx, options.Client().ApplyURI(uri))
	return client, ctx, cancel, err
}

// This is a user defined method that accepts
// mongo.Client and context.Context
// This method used to ping the mongoDB, return error if any.
func ping(client *mongo.Client, ctx context.Context) error {

	// mongo.Client has Ping to ping mongoDB, deadline of
	// the Ping method will be determined by cxt
	// Ping method return error if any occurred, then
	// the error can be handled.
	if err := client.Ping(ctx, readpref.Primary()); err != nil {
		return err
	}

	fmt.Println("connected successfully with the database")

	return nil
}

// findAndValidate method find document based on query and validate if total documents filtered are greater than max limit.
func findAndValidate(client *mongo.Client, ctx context.Context,
	dataBase, col string, query, field interface{}) bool {

	// select database and collection.
	collection := client.Database(dataBase).Collection(col)

	// collection has an method Find,
	// that returns a mongo.cursor
	// based on query and field.
	result, err := collection.Find(ctx, query,
		options.Find().SetProjection(field))

	var results []bson.D

	// to get bson object  from cursor,
	// returns error if any.
	if err = result.All(ctx, &results); err != nil {
		// handle the error
		panic(err)
	}

	// if number of documents are more than limit return false
	if len(results) > MaxMatchLimit {
		return false
	}

	return true
}

func updateOne(client *mongo.Client, ctx context.Context, dataBase,
	col string, filter, update interface{}) (result *mongo.UpdateResult, err error) {

	// select the database and the collection
	collection := client.Database(dataBase).Collection(col)

	// A single document that match with the
	// filter will get updated.
	// update contains the filed which should get updated.
	result, err = collection.UpdateOne(ctx, filter, update)
	return
}

func main() {
	records, err := readJSONFile()
	if err != nil {
		panic(err)
	}

	// Get Client, Context, CancelFunc and
	// err from connect method.
	client, ctx, cancel, err := connect(DatabaseURL)
	if err != nil {
		panic(err)
	}

	// Release resource when the main
	// function is returned.
	defer close(client, ctx, cancel)

	// Ping mongoDB with ping method to check if database is available and connected
	if err = ping(client, ctx); err != nil {
		log.Fatalf("Failed to ping the database %v", DatabaseURL)
	}

	// total matched and modified count for the analysis at the end
	var totalMatched, totalModified int

	// Loop through each record and update the corresponding document in the collection
	for _, record := range records {
		// filter documents based on name
		filter := bson.M{"name": record.Name}

		if !findAndValidate(client, ctx, DatabaseName, CollectionName, filter, nil) {
			fmt.Printf("Skipping update for document with filter %s, because total number of matched documents "+
				"is greater then max limit of match\n", record.Name)
		}

		// update the required attributes
		update := bson.M{"$set": bson.M{Description: record.Description}}

		updated, updateErr := updateOne(client, ctx, DatabaseName, CollectionName, filter, update)
		if updateErr != nil {
			log.Fatalf("Failed to update document: %s", record.Name)
		}

		// update total matched and modified count
		totalMatched++
		if updated.ModifiedCount >= 1 {
			totalModified++
		}

		fmt.Printf("-> Matched %d and Updated %d document with filter %s\n",
			updated.MatchedCount, updated.ModifiedCount, record.Name)
	}

	fmt.Printf("Result: Successfully Matched %d and Updated %d documents", totalMatched, totalModified)
}
