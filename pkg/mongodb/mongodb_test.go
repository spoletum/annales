package mongodb_test

import (
	"context"
	"os"
	"testing"

	"github.com/joho/godotenv"
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
	require.NoError(t, godotenv.Load())
	ctx := context.Background()
	// Set up a new MongoDriver instance with a test database
	client, err := mongo.Connect(ctx, options.Client().ApplyURI(os.Getenv("MONGO_URL")))
	require.NoError(t, err)
	defer func() {
		require.NoError(t, client.Disconnect(ctx))
	}()
	dbName := "test_db"
	driver := &mongodb.MongoJournal{Client: client, DatabaseName: dbName}

	t.Run("EventStreamEmpty", func(t *testing.T) {
		// Drop the streams and events collections before the test
		streamsCollection := client.Database(dbName).Collection(mongodb.StreamsCollectionName)
		require.NoError(t, streamsCollection.Drop(ctx))
		eventsCollection := client.Database(dbName).Collection(mongodb.EventsCollectionName)
		require.NoError(t, eventsCollection.Drop(ctx))

		// Generate test data for the new event
		streamID := "test_stream"
		expectedVersion := int64(0)
		eventType := "test_event"
		eventSource := "test_source"
		eventEncoding := "test_encoding"
		eventData := []byte("test_data")

		req := &annales.AppendEventRequest{StreamId: streamID, ExpectedVersion: 0, EventType: eventType, Source: eventSource, Encoding: eventEncoding, Data: eventData}

		// Append the new event to the stream
		res, err := driver.AppendEvent(ctx, req)
		require.NoError(t, err)
		assert.NotNil(t, res)

		// Check that the event was inserted into the events collection with the correct values
		eventCursor, err := eventsCollection.Find(ctx, bson.M{"stream_id": streamID})
		require.NoError(t, err)
		defer eventCursor.Close(ctx)

		eventCount := 0
		for eventCursor.Next(ctx) {
			var event annales.Event
			err := eventCursor.Decode(&event)
			require.NoError(t, err)

			assert.Equal(t, streamID, event.StreamId)
			assert.Equal(t, expectedVersion+1, event.Version)
			assert.Equal(t, eventType, event.EventType)
			assert.Equal(t, eventEncoding, event.Encoding)
			assert.Equal(t, eventSource, event.Source)
			assert.Equal(t, eventData, event.Data)
			assert.NotNil(t, event.Timestamp)

			eventCount++
		}
		assert.Equal(t, 1, eventCount)
	})
	t.Run("EventStreamExists", func(t *testing.T) {
		// Drop the streams and events collections before testing
		streamsCollection := client.Database(dbName).Collection(mongodb.StreamsCollectionName)
		eventsCollection := client.Database(dbName).Collection(mongodb.EventsCollectionName)
		err = streamsCollection.Drop(ctx)
		require.NoError(t, err)
		err = eventsCollection.Drop(ctx)
		require.NoError(t, err)

		// Generate test data for the new events
		streamID := "test_stream"
		eventType := "test_event"
		eventSource := "test_source"
		eventEncoding := "test_encoding"
		eventData1 := []byte("test_data_1")
		eventData2 := []byte("test_data_2")

		req1 := &annales.AppendEventRequest{StreamId: streamID, ExpectedVersion: 0, EventType: eventType, Source: eventSource, Encoding: eventEncoding, Data: eventData1}
		req2 := &annales.AppendEventRequest{StreamId: streamID, ExpectedVersion: 1, EventType: eventType, Source: eventSource, Encoding: eventEncoding, Data: eventData2}

		// Append the first event to the stream
		res, err := driver.AppendEvent(ctx, req1)
		require.NoError(t, err)
		assert.NotNil(t, res)

		// Append the second event to the stream
		res, err = driver.AppendEvent(ctx, req2)
		require.NoError(t, err)
		assert.NotNil(t, res)

		// Check that the latest event was inserted into the events collection with the correct values
		eventCursor, err := eventsCollection.Find(ctx, bson.M{"stream_id": streamID})
		require.NoError(t, err)
		defer eventCursor.Close(ctx)

		eventCount := 0
		for eventCursor.Next(ctx) {
			var event annales.Event
			err := eventCursor.Decode(&event)
			require.NoError(t, err)

			assert.Equal(t, streamID, event.StreamId)
			assert.Equal(t, eventType, event.EventType)
			assert.Equal(t, eventEncoding, event.Encoding)
			assert.Equal(t, eventSource, event.Source)
			if eventCount == 0 {
				assert.Equal(t, eventData1, event.Data)
				assert.Equal(t, int64(1), event.Version)
			} else if eventCount == 1 {
				assert.Equal(t, eventData2, event.Data)
				assert.Equal(t, int64(2), event.Version)
			} else {
				t.Fatal("unexpected number of events")
			}
			assert.NotNil(t, event.Timestamp)

			eventCount++
		}

		assert.Equal(t, 2, eventCount)

		// Check that the stream version is correct
		stream := &mongodb.Stream{}
		err = streamsCollection.FindOne(ctx, bson.M{"_id": streamID}).Decode(stream)
		require.NoError(t, err)

		assert.Equal(t, int64(2), stream.StreamVersion)
	})
	t.Run("OptimisticConcurrencyViolation", func(t *testing.T) {
		// Drop the streams and events collections before testing
		streamsCollection := client.Database(dbName).Collection(mongodb.StreamsCollectionName)
		eventsCollection := client.Database(dbName).Collection(mongodb.EventsCollectionName)
		err = streamsCollection.Drop(ctx)
		require.NoError(t, err)
		err = eventsCollection.Drop(ctx)
		require.NoError(t, err)

		// Generate test data for the new events
		streamID := "test_stream"
		eventType := "test_event"
		eventSource := "test_source"
		eventEncoding := "test_encoding"
		eventData1 := []byte("test_data_1")
		eventData2 := []byte("test_data_2")

		req1 := &annales.AppendEventRequest{StreamId: streamID, ExpectedVersion: 0, EventType: eventType, Source: eventSource, Encoding: eventEncoding, Data: eventData1}
		req2 := &annales.AppendEventRequest{StreamId: streamID, ExpectedVersion: 0, EventType: eventType, Source: eventSource, Encoding: eventEncoding, Data: eventData2}

		// Append the first event to the stream
		res, err := driver.AppendEvent(ctx, req1)
		require.NoError(t, err)
		assert.NotNil(t, res)

		// Try to append the second event to the stream with an incorrect expected version
		res, err = driver.AppendEvent(ctx, req2)
		require.Error(t, err)
		assert.Nil(t, res)
		assert.Equal(t, err, errors.InvalidStreamVersionError())

		// Check that only one event was inserted into the events collection with the correct values
		eventCursor, err := eventsCollection.Find(ctx, bson.M{"stream_id": streamID})
		require.NoError(t, err)
		defer eventCursor.Close(ctx)

		eventCount := 0
		for eventCursor.Next(ctx) {
			var event annales.Event
			err := eventCursor.Decode(&event)
			require.NoError(t, err)

			assert.Equal(t, streamID, event.StreamId)
			assert.Equal(t, eventType, event.EventType)
			assert.Equal(t, eventEncoding, event.Encoding)
			assert.Equal(t, eventSource, event.Source)
			if eventCount == 0 {
				assert.Equal(t, eventData1, event.Data)
				assert.Equal(t, int64(1), event.Version)
			} else {
				t.Fatal("unexpected number of events")
			}
			assert.NotNil(t, event.Timestamp)

			eventCount++
		}

		assert.Equal(t, 1, eventCount)

		// Check that the stream version is correct
		stream := &mongodb.Stream{}
		err = streamsCollection.FindOne(ctx, bson.M{"_id": streamID}).Decode(stream)
		require.NoError(t, err)

		assert.Equal(t, int64(1), stream.StreamVersion)
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
	driver := &mongodb.MongoJournal{Client: client, DatabaseName: dbName}

	t.Run("ExistingStream", func(t *testing.T) {
		// Drop the collections to ensure a clean state
		streamsCollection := client.Database(dbName).Collection(mongodb.StreamsCollectionName)
		eventsCollection := client.Database(dbName).Collection(mongodb.EventsCollectionName)
		_, err = streamsCollection.DeleteMany(ctx, bson.M{})
		require.NoError(t, err)
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
		// Drop the events collection to ensure a clean state
		eventsCollection := client.Database(dbName).Collection(mongodb.EventsCollectionName)
		require.NoError(t, eventsCollection.Drop(ctx))

		// Attempt to retrieve events for a non-existing stream
		events, err := driver.GetStreamEvents(ctx, &annales.GetStreamEventsRequest{StreamId: "non_existing_stream"})
		require.NoError(t, err)

		// Verify that an empty list of events is returned
		assert.Empty(t, events)
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
	streamsCollection := client.Database(dbName).Collection(mongodb.StreamsCollectionName)
	require.NoError(t, eventsCollection.Drop(ctx))
	require.NoError(t, streamsCollection.Drop(ctx))

	// Create a new MongoDriver instance
	driver, err := mongodb.NewMongoJournal(ctx, client, dbName)
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

func BenchmarkAppendEvent(b *testing.B) {
	require.NoError(b, godotenv.Load())
	ctx := context.Background()
	client, err := mongo.Connect(ctx, options.Client().ApplyURI(os.Getenv("MONGO_URL")))
	require.NoError(b, err)
	defer func() {
		require.NoError(b, client.Disconnect(ctx))
	}()

	dbName := "test_db"
	driver := &mongodb.MongoJournal{Client: client, DatabaseName: dbName}

	// Drop the collections to ensure a clean state
	streamsCollection := client.Database(dbName).Collection(mongodb.StreamsCollectionName)
	eventsCollection := client.Database(dbName).Collection(mongodb.EventsCollectionName)
	require.NoError(b, streamsCollection.Drop(ctx))
	require.NoError(b, eventsCollection.Drop(ctx))

	// Generate test data for the new event
	streamID := "test_stream"
	eventType := "test_event"
	eventSource := "test_source"
	eventEncoding := "test_encoding"
	eventData := []byte("test_data")

	// Execute the test multiple times
	for i := 0; i < b.N; i++ {
		ev := &annales.AppendEventRequest{StreamId: streamID, ExpectedVersion: int64(i), EventType: eventType, Source: eventSource, Encoding: eventEncoding, Data: eventData}
		res, err := driver.AppendEvent(ctx, ev)
		require.NoError(b, err)
		assert.NotNil(b, res)
	}

	// Check that the event was inserted into the events collection with the correct values
	eventsCursor, err := eventsCollection.Find(ctx, bson.M{"stream_id": streamID})
	require.NoError(b, err)
	defer eventsCursor.Close(ctx)

	eventCount := 0
	for eventsCursor.Next(ctx) {
		eventCount++
		var event annales.Event
		err := eventsCursor.Decode(&event)
		require.NoError(b, err)
		assert.Equal(b, streamID, event.StreamId)
		assert.Equal(b, eventCount, event.Version)
		assert.Equal(b, eventType, event.EventType)
		assert.Equal(b, eventEncoding, event.Encoding)
		assert.Equal(b, eventSource, event.Source)
		assert.Equal(b, eventData, event.Data)
		assert.NotNil(b, event.Timestamp)
	}

	assert.Equal(b, b.N, eventCount)
}
