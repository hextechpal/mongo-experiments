package main

import (
	"context"
	"flag"
	"fmt"
	"log"
	"time"

	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"
)

// Token represents the structure of the document to be inserted into MongoDB
type Token struct {
	ID        int       `bson:"_id, omitempty"`
	Name      string    `bson:"name"`
	CreatedAt time.Time `bson:"created_at"`
	ExpiresOn time.Time `bson:"expires_on"`
}

const (
	mongoURI       = "mongodb://localhost:27017" // Update with your MongoDB URI
	databaseName   = "mongo-experiments"
	collectionName = "tokens"
	batches        = 1
	totalDocuments = 100_000

	expireIndex = "expires_on"
)

func main() {
	var exp int
	flag.IntVar(&exp, "Exp", 2, "which experiment to run")
	flag.Parse()

	ctx := context.Background()
	client, err := mongo.Connect(ctx, options.Client().ApplyURI(mongoURI))
	if err != nil {
		log.Fatal(err)
	}
	defer func() {
		if err = client.Disconnect(ctx); err != nil {
			log.Fatal(err)
		}
	}()

	coll := client.Database(databaseName).Collection(collectionName)
	err = manageIndex(ctx, coll)

	if err != nil {
		log.Fatal(err)
	}

	if exp != 1 && exp != 2 {
		return
	}
	d := insertRecords(coll, ctx)
	fmt.Printf("inserted %d records in %v\n", totalDocuments, d.Seconds())

	start := time.Now()
	switch exp {
	case 1:
		_, err := coll.DeleteMany(ctx, bson.D{})
		if err != nil {
			log.Fatal(err)
		}
	case 2:
		_, err := coll.UpdateMany(ctx, bson.D{}, bson.D{{"$set", bson.D{{"expires_on", time.Now()}}}})
		if err != nil {
			log.Fatal(err)
		}
	}
	fmt.Printf("Deleted exp=%d, time taken %v\n", exp, time.Since(start).Seconds())
}

func insertRecords(coll *mongo.Collection, ctx context.Context) time.Duration {
	batchSize := totalDocuments / batches
	var d time.Duration
	for i := 0; i < totalDocuments; i += batchSize {
		batch := getBatch(i, batchSize)
		start := time.Now()
		_, err := coll.InsertMany(ctx, batch)
		if err != nil {
			log.Fatal(err)
		}
		since := time.Since(start)
		d += since
	}
	return d
}

func getBatch(i, batchSize int) []interface{} {
	batch := make([]interface{}, batchSize)
	for j := 0; j < batchSize; j++ {
		token := Token{
			ID:        i + j,
			Name:      fmt.Sprintf("token_%d", i+j),
			CreatedAt: time.Now(),
			ExpiresOn: time.Now().Add(2 * time.Hour),
		}
		batch[j] = token
	}
	return batch
}

func manageIndex(ctx context.Context, coll *mongo.Collection) error {
	indexView := coll.Indexes()
	cursor, err := indexView.List(ctx)
	if err != nil {
		log.Fatal(err)
	}

	defer func(cursor *mongo.Cursor, ctx context.Context) {
		_ = cursor.Close(ctx)
	}(cursor, ctx)

	indexExists := false
	for cursor.Next(ctx) {
		var result bson.M
		if err := cursor.Decode(&result); err != nil {
			return err
		}
		if name, ok := result["name"].(string); ok && name == expireIndex {
			indexExists = true
			break
		}
	}

	// If the TTL index doesn't exist, create it
	if !indexExists {
		// Create an index model for the TTL index
		indexModel := mongo.IndexModel{
			Keys: bson.M{
				"expires_on": 1,
			},
			Options: options.Index().SetName(expireIndex).SetExpireAfterSeconds(0),
		}

		_, err = indexView.CreateOne(ctx, indexModel)
		fmt.Println("TTL index created.")
		return err
	}

	fmt.Println("TTL index exists.")
	return nil
}
