package repository_test

import (
	"context"
	"database/sql"
	"fmt"
	"os"
	"path/filepath"
	"testing"

	"github.com/google/uuid"
	"github.com/joho/godotenv"
	_ "github.com/lib/pq"
	"github.com/rs/zerolog/log"
	annales "github.com/spoletum/annales/gen"
	"github.com/spoletum/annales/pkg/errors"
	"github.com/spoletum/annales/pkg/repository"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestAppendEvent(t *testing.T) {
	err := godotenv.Load()
	if err != nil {
		require.True(t, os.IsNotExist(err))
	}
	// Open a connection to the database
	db, err := sql.Open("postgres", os.Getenv(("POSTGRES_URL")))
	log.Info().Str("url", os.Getenv("POSTGRES_URL")).Msg("starting TestAppendEvent")
	require.NoError(t, err)
	defer db.Close()

	// Clear the events and streams tables before starting the test
	_, err = db.Exec("DELETE FROM events")
	require.NoError(t, err)
	_, err = db.Exec("DELETE FROM streams")
	require.NoError(t, err)

	driver, err := repository.NewPostgresRepository(db)
	require.NoError(t, err)

	// Create a common stream UUID for all tests
	streamId := uuid.NewString()

	t.Run("FirstInsertReturns1", func(t *testing.T) {
		// Create a new stream_id and event_id
		request := &annales.AppendEventRequest{
			StreamId:        streamId,
			ExpectedVersion: 0,
			EventType:       "event_type_1",
			Encoding:        "event_encoding_1",
			Source:          "event_source_1",
			Data:            []byte(`{"key_1": "value_1"}`),
		}
		// Call the function with expected_version = 0 (should insert the first event)
		res, err := driver.AppendEvent(context.Background(), request)
		require.NoError(t, err)
		assert.NotNil(t, res)
	})

	t.Run("SecondInsertReturns2", func(t *testing.T) {
		// Create a new stream_id and event_id
		request := &annales.AppendEventRequest{
			StreamId:        streamId,
			ExpectedVersion: 1,
			EventType:       "event_type_2",
			Encoding:        "event_encoding_2",
			Source:          "event_source_2",
			Data:            []byte(`{"key_2": "value_2"}`),
		}
		// Call the function with expected_version = 1 (should insert the second event)
		res, err := driver.AppendEvent(context.Background(), request)
		require.NoError(t, err)
		assert.NotNil(t, res)
	})

	t.Run("StaleInsertReturnsError", func(t *testing.T) {
		// Create a new stream_id and event_id
		request := &annales.AppendEventRequest{
			StreamId:        streamId,
			ExpectedVersion: 1,
			EventType:       "event_type_3",
			Encoding:        "event_encoding_",
			Source:          "event_source_3",
			Data:            []byte(`{"key_3": "value_3"}`),
		}
		// Call the function with expected_version = 0 (should fail because stream version is 2)
		res, err := driver.AppendEvent(context.Background(), request)
		assert.Error(t, err)
		assert.Equal(t, errors.InvalidStreamVersionError, err)
		assert.Nil(t, res)
	})
}

func TestGetEventsByStream(t *testing.T) {
	// Set up the database connection
	db, err := sql.Open("postgres", os.Getenv(("POSTGRES_URL")))
	require.NoError(t, err)
	defer db.Close()

	driver, err := repository.NewPostgresRepository(db)
	require.NoError(t, err)

	// Clear the events and streams tables before starting the test
	_, err = db.Exec("DELETE FROM events")
	require.NoError(t, err)
	_, err = db.Exec("DELETE FROM streams")
	require.NoError(t, err)

	// Define some test data
	streamID := uuid.NewString()
	eventData1 := []byte(`{"name": "event1"}`)
	eventData2 := []byte(`{"name": "event2"}`)

	// Insert some test data into the database using AppendEvent
	_, err = driver.AppendEvent(context.Background(), &annales.AppendEventRequest{StreamId: streamID, ExpectedVersion: 0, EventType: "event1", Encoding: "encoding1", Source: "source1", Data: eventData1})
	require.NoError(t, err)
	_, err = driver.AppendEvent(context.Background(), &annales.AppendEventRequest{StreamId: streamID, ExpectedVersion: 1, EventType: "event2", Encoding: "encoding2", Source: "source2", Data: eventData2})
	require.NoError(t, err)

	// Call the function being tested
	res, err := driver.GetStreamEvents(context.Background(), &annales.GetStreamEventsRequest{StreamId: streamID})

	// Verify the results
	require.NoError(t, err)
	assert.Len(t, res.Events, 2)
	assert.Equal(t, eventData1, res.Events[0].Data)
	assert.Equal(t, eventData2, res.Events[1].Data)
	assert.Equal(t, streamID, res.Events[0].StreamId)
	assert.Equal(t, streamID, res.Events[1].StreamId)
	assert.Equal(t, int64(1), res.Events[0].Version)
	assert.Equal(t, int64(2), res.Events[1].Version)
	assert.Equal(t, "event1", res.Events[0].EventType)
	assert.Equal(t, "event2", res.Events[1].EventType)
	assert.Equal(t, "encoding1", res.Events[0].Encoding)
	assert.Equal(t, "encoding2", res.Events[1].Encoding)
	assert.Equal(t, "source1", res.Events[0].Source)
	assert.Equal(t, "source2", res.Events[1].Source)
}

func BenchmarkAppendEvent(b *testing.B) {
	// Load environment variables from .env file
	err := godotenv.Load(filepath.Join(".", ".env"))
	if err != nil {
		b.Fatal("Error loading .env file")
	}

	// Get the database URL from the environment
	dbURL := os.Getenv("POSTGRES_URL")

	// Use the driver name and data source name to create a new database connection
	driverName := "postgres"
	dataSourceName := dbURL

	// Create a new database connection
	db, err := sql.Open(driverName, dataSourceName)
	if err != nil {
		b.Fatal(err)
	}
	defer db.Close()

	// Create a new PostgresJournal using the database connection
	journal, err := repository.NewPostgresRepository(db)
	if err != nil {
		b.Fatal(err)
	}
	streamID := uuid.NewString()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		// Append a new event to the journal with a UUID stream ID to avoid collisions
		_, err = journal.AppendEvent(context.Background(), &annales.AppendEventRequest{
			StreamId:        streamID,
			ExpectedVersion: int64(i),
			EventType:       "test",
			Encoding:        "json",
			Source:          "test",
			Data:            []byte(fmt.Sprintf(`{"value": %d}`, i)),
		})
		if err != nil {
			b.Fatalf("Error at iteration %d: %v", i, err)
		}
	}
}
