package repository_test

import (
	"context"
	"database/sql"
	"os"
	"testing"

	"github.com/joho/godotenv"
	_ "github.com/lib/pq"
	annales "github.com/spoletum/annales/gen"
	"github.com/spoletum/annales/pkg/errors"
	"github.com/spoletum/annales/pkg/repository"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestAppendEvent(t *testing.T) {

	ctx := context.Background()

	err := godotenv.Load()
	if err != nil {
		require.True(t, os.IsNotExist(err))
	}

	// Open a connection to the database
	db, err := sql.Open("postgres", os.Getenv(("POSTGRESQL_URL")))
	require.NoError(t, err)
	defer db.Close()

	// Clear the events and streams tables before starting the test
	_, err = db.Exec("DELETE FROM events")
	require.NoError(t, err)

	repo, err := repository.NewPostgresRepository(db)
	require.NoError(t, err)

	t.Run("Multiple streams with different numbers of events", func(t *testing.T) {

		streams := map[string]int{
			"stream_a": 2,
			"stream_b": 3,
			"stream_c": 4,
		}

		// Append the events
		for streamID, numEvents := range streams {
			for i := 0; i < numEvents; i++ {
				req := &annales.AppendEventRequest{
					StreamId:        streamID,
					ExpectedVersion: int64(i),
					EventType:       "testEvent",
					Encoding:        "JSON",
					Source:          "testSource",
					Data:            []byte("testData"),
				}

				_, err := repo.AppendEvent(ctx, req)
				assert.NoError(t, err)
			}
		}

		// Verify GetStreamInfo and GetStreamEvents for each stream
		for streamID, numEvents := range streams {
			infoReq := &annales.GetStreamInfoRequest{
				StreamId: streamID,
			}

			infoResp, err := repo.GetStreamInfo(ctx, infoReq)
			assert.NoError(t, err)
			assert.Equal(t, int64(numEvents), infoResp.Version)

			eventsReq := &annales.GetStreamEventsRequest{
				StreamId: streamID,
			}

			eventsResp, err := repo.GetStreamEvents(ctx, eventsReq)
			assert.NoError(t, err)
			assert.Len(t, eventsResp.Events, numEvents)

			for _, event := range eventsResp.Events {
				assert.Equal(t, "testData", string(event.Data))
			}
		}
	})

	t.Run("Duplicate append results in InvalidStreamVersionError", func(t *testing.T) {
		req := &annales.AppendEventRequest{
			StreamId:        "duplicateStream",
			ExpectedVersion: 0,
			EventType:       "testEvent",
			Encoding:        "JSON",
			Source:          "testSource",
			Data:            []byte("testData"),
		}
		_, err := repo.AppendEvent(ctx, req)
		require.NoError(t, err)
		_, err = repo.AppendEvent(ctx, req)
		assert.ErrorIs(t, errors.InvalidStreamVersionError, err)
	})

	t.Run("Get info and events from empty repository", func(t *testing.T) {

		streamID := "testStream"

		// Verify GetStreamInfo on empty repository
		infoReq := &annales.GetStreamInfoRequest{
			StreamId: streamID,
		}

		infoResp, err := repo.GetStreamInfo(ctx, infoReq)
		assert.NoError(t, err)
		assert.Equal(t, int64(0), infoResp.Version)

		// Verify GetStreamEvents on empty repository
		eventsReq := &annales.GetStreamEventsRequest{
			StreamId: streamID,
		}

		eventsResp, err := repo.GetStreamEvents(ctx, eventsReq)
		assert.NoError(t, err)
		assert.Empty(t, eventsResp.Events)
	})

}
