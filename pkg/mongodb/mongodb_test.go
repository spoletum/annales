package mongodb_test

import (
	"context"
	"os"
	"testing"
	"time"

	"github.com/joho/godotenv"
	"github.com/rs/zerolog"
	"github.com/rs/zerolog/log"
	annales "github.com/spoletum/annales/gen"
	"github.com/spoletum/annales/pkg/errors"
	"github.com/spoletum/annales/pkg/mongodb"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"
)

func TestAppendEvent(t *testing.T) {
	const dbName = "test_db"
	// Set up the MongoDB client and database for testing
	client, _ := mongo.NewClient(options.Client().ApplyURI("mongodb://localhost:27017"))
	client.Connect(context.Background())
	defer client.Disconnect(context.Background())

	// Ensure the database is clean before running the tests
	client.Database(dbName).Drop(context.Background())

	mongoJournal, err := mongodb.NewMongoJournal(context.Background(), client, dbName, 100)
	require.NoError(t, err)

	t.Run("append new event to new stream", func(t *testing.T) {
		req := &annales.AppendEventRequest{
			StreamId:        "stream1",
			ExpectedVersion: 0,
			EventType:       "test_event",
			Source:          "test_source",
			Encoding:        "text",
			Data:            []byte("Hello, world!"),
		}

		resp, err := mongoJournal.AppendEvent(context.Background(), req)

		assert.NoError(t, err)
		assert.NotNil(t, resp)
	})

	t.Run("append new event to existing stream", func(t *testing.T) {

		req := &annales.AppendEventRequest{
			StreamId:        "stream2",
			ExpectedVersion: 0,
			EventType:       "test_event",
			Source:          "test_source",
			Encoding:        "text",
			Data:            []byte("Hello, world!"),
		}
		resp, err := mongoJournal.AppendEvent(context.Background(), req)
		assert.NoError(t, err)
		assert.NotNil(t, resp)

		req = &annales.AppendEventRequest{
			StreamId:        "stream2",
			ExpectedVersion: 1,
			EventType:       "test_event",
			Source:          "test_source",
			Encoding:        "text",
			Data:            []byte("Hello again, world!"),
		}

		resp, err = mongoJournal.AppendEvent(context.Background(), req)
		assert.NoError(t, err)
		assert.NotNil(t, resp)
	})

	t.Run("append event with incorrect expected version", func(t *testing.T) {
		req := &annales.AppendEventRequest{
			StreamId:        "stream3",
			ExpectedVersion: 0, // incorrect version
			EventType:       "test_event",
			Source:          "test_source",
			Encoding:        "text",
			Data:            []byte("This should fail"),
		}
		_, err := mongoJournal.AppendEvent(context.Background(), req)
		assert.NoError(t, err)

		req = &annales.AppendEventRequest{
			StreamId:        "stream3",
			ExpectedVersion: 0, // incorrect version
			EventType:       "test_event",
			Source:          "test_source",
			Encoding:        "text",
			Data:            []byte("This should fail"),
		}

		_, err = mongoJournal.AppendEvent(context.Background(), req)
		assert.Error(t, err)
		assert.Equal(t, errors.InvalidStreamVersionError(), err)
	})
}

func TestGetEventsForStream(t *testing.T) {
	require.NoError(t, godotenv.Load())
	ctx := context.Background()
	client, err := mongo.Connect(ctx, options.Client().ApplyURI(os.Getenv("MONGO_URL")))
	require.NoError(t, err)
	defer func() {
		require.NoError(t, client.Disconnect(ctx))
	}()

	dbName := "test_db"
	driver, err := mongodb.NewMongoJournal(context.Background(), client, dbName, 100)
	require.NoError(t, err)

	t.Run("ExistingStream", func(t *testing.T) {
		// Drop the collections to ensure a clean state
		eventsCollection := client.Database(dbName).Collection(mongodb.EventsCollectionName)
		_, err = eventsCollection.DeleteMany(ctx, bson.M{})
		require.NoError(t, err)

		// Insert some test events
		streamID := "test_stream"
		req1 := annales.AppendEventRequest{StreamId: streamID, ExpectedVersion: 0, EventType: "test_event_1", Source: "test_source", Encoding: "test_encoding", Data: []byte("test_data_1")}
		req2 := annales.AppendEventRequest{StreamId: streamID, ExpectedVersion: 1, EventType: "test_event_2", Source: "test_source", Encoding: "test_encoding", Data: []byte("test_data_2")}
		driver.AppendEvent(ctx, &req1)
		driver.AppendEvent(ctx, &req2)

		// Get the events for the stream
		res, err := driver.GetStreamEvents(ctx, &annales.GetStreamEventsRequest{StreamId: streamID})
		require.NoError(t, err)

		// Verify that the events have the expected values
		require.Equal(t, 2, len(res.Events))
		assert.Equal(t, streamID, res.Events[0].StreamId)
		assert.Equal(t, int64(1), res.Events[0].Version)
		assert.Equal(t, "test_event_1", res.Events[0].EventType)
		assert.Equal(t, "test_encoding", res.Events[0].Encoding)
		assert.Equal(t, "test_source", res.Events[0].Source)
		assert.Equal(t, []byte("test_data_1"), res.Events[0].Data)

		assert.Equal(t, streamID, res.Events[1].StreamId)
		assert.Equal(t, int64(2), res.Events[1].Version)
		assert.Equal(t, "test_event_2", res.Events[1].EventType)
		assert.Equal(t, "test_encoding", res.Events[1].Encoding)
		assert.Equal(t, "test_source", res.Events[1].Source)
		assert.Equal(t, []byte("test_data_2"), res.Events[1].Data)
	})
	t.Run("NonExistingStream", func(t *testing.T) {
		// Attempt to retrieve events for a non-existing stream
		events, err := driver.GetStreamEvents(ctx, &annales.GetStreamEventsRequest{StreamId: "non_existing_stream"})
		require.NoError(t, err)

		// Verify that an empty list of events is returned
		assert.Empty(t, events.Events)
	})

}

func TestGetStreamInfo(t *testing.T) {
	// Load environment variables
	err := godotenv.Load()
	require.NoError(t, err)

	mongoURL := os.Getenv("MONGO_URL")
	require.NotEmpty(t, mongoURL)

	// Connect to MongoDB
	client, err := mongo.Connect(context.Background(), options.Client().ApplyURI(mongoURL))
	require.NoError(t, err)

	// Drops the collection to start from squeaky clean
	db := client.Database("test")
	eventsCollection := db.Collection(mongodb.EventsCollectionName)
	err = eventsCollection.Drop(context.Background())
	require.NoError(t, err)

	// Initialize the MongoJournal instance
	mongoJournal, err := mongodb.NewMongoJournal(context.Background(), client, "test", 100)
	require.NoError(t, err)

	t.Run("empty collection", func(t *testing.T) {
		resp, err := mongoJournal.GetStreamInfo(context.Background(), &annales.GetStreamInfoRequest{StreamId: "stream0"})
		assert.NoError(t, err)
		assert.Equal(t, int64(0), resp.Version)
	})

	t.Run("stream with one event", func(t *testing.T) {
		_, err = eventsCollection.InsertOne(context.Background(), bson.M{
			"streamid":  "stream1",
			"version":   1,
			"eventtype": "test_event",
			"encoding":  "text",
			"source":    "test_source",
			"data":      []byte("Hello, world!"),
			"timestamp": time.Now().String(),
		})
		require.NoError(t, err)

		resp, err := mongoJournal.GetStreamInfo(context.Background(), &annales.GetStreamInfoRequest{StreamId: "stream1"})
		assert.NoError(t, err)
		assert.Equal(t, int64(1), resp.Version)
	})

	t.Run("stream with two events", func(t *testing.T) {
		_, err = eventsCollection.InsertMany(context.Background(), []any{
			bson.M{
				"streamid":  "stream2",
				"version":   1,
				"eventtype": "test_event",
				"encoding":  "text",
				"source":    "test_source",
				"data":      []byte("Hello, world!"),
				"timestamp": time.Now().String(),
			},
			bson.M{
				"streamid":  "stream2",
				"version":   2,
				"eventtype": "test_event_2",
				"encoding":  "text",
				"source":    "test_source",
				"data":      []byte("Hello again, world!"),
				"timestamp": time.Now().String(),
			},
		})
		require.NoError(t, err)

		resp, err := mongoJournal.GetStreamInfo(context.Background(), &annales.GetStreamInfoRequest{StreamId: "stream2"})
		assert.NoError(t, err)
		assert.Equal(t, int64(2), resp.Version)
	})
}

func TestNewMongoDriver(t *testing.T) {
	require.NoError(t, godotenv.Load())
	ctx := context.Background()
	client, err := mongo.Connect(ctx, options.Client().ApplyURI(os.Getenv("MONGO_URL")))
	require.NoError(t, err)
	defer func() {
		require.NoError(t, client.Disconnect(ctx))
	}()

	dbName := "test_db"

	// Drop the events collection to ensure a clean state
	eventsCollection := client.Database(dbName).Collection(mongodb.EventsCollectionName)
	require.NoError(t, eventsCollection.Drop(ctx))

	// Create a new MongoDriver instance
	driver, err := mongodb.NewMongoJournal(ctx, client, dbName, 100)
	require.NoError(t, err)
	assert.NotNil(t, driver)

	// Verify that the events collection has the expected indexes
	indexesCursor, err := eventsCollection.Indexes().List(ctx)
	require.NoError(t, err)
	var indexes []map[string]interface{}
	err = indexesCursor.All(ctx, &indexes)
	require.NoError(t, err)
	assert.Equal(t, 2, len(indexes))
}

func BenchmarkAppendEventSameStreamId(b *testing.B) {
	require.NoError(b, godotenv.Load())
	ctx := context.Background()
	client, err := mongo.Connect(ctx, options.Client().ApplyURI(os.Getenv("MONGO_URL")))
	require.NoError(b, err)
	defer func() {
		require.NoError(b, client.Disconnect(ctx))
	}()

	// Raise the logging bar to Error so that we do not burn unnecessary cycles
	log.Logger = log.With().Logger().Level(zerolog.ErrorLevel)

	dbName := "test_db"
	driver, err := mongodb.NewMongoJournal(context.Background(), client, dbName, 100)
	require.NoError(b, err)

	// Drop the collections to ensure a clean state
	eventsCollection := client.Database(dbName).Collection(mongodb.EventsCollectionName)
	require.NoError(b, eventsCollection.Drop(ctx))

	// Generate test data for the new event
	streamID := "test_stream"
	eventType := "test_event"
	eventSource := "test_source"
	eventEncoding := "test_encoding"
	eventData := []byte("test_data")

	b.StartTimer()

	// Execute the test multiple times
	for i := 0; i < b.N; i++ {
		ev := &annales.AppendEventRequest{StreamId: streamID, ExpectedVersion: int64(i), EventType: eventType, Source: eventSource, Encoding: eventEncoding, Data: eventData}
		res, err := driver.AppendEvent(ctx, ev)
		require.NoError(b, err)
		assert.NotNil(b, res)
	}

	b.StopTimer()

	// Check that the event was inserted into the events collection with the correct values
	eventsCursor, err := eventsCollection.Find(ctx, bson.M{"streamid": streamID})
	require.NoError(b, err)
	defer eventsCursor.Close(ctx)

	eventCount := 0
	for eventsCursor.Next(ctx) {
		eventCount++
		var event annales.Event
		err := eventsCursor.Decode(&event)
		require.NoError(b, err)
		assert.Equal(b, streamID, event.StreamId)
		assert.Equal(b, int64(eventCount), event.Version)
		assert.Equal(b, eventType, event.EventType)
		assert.Equal(b, eventEncoding, event.Encoding)
		assert.Equal(b, eventSource, event.Source)
		assert.Equal(b, eventData, event.Data)
		assert.NotNil(b, event.Timestamp)
	}

	assert.Equal(b, b.N, eventCount)
}
