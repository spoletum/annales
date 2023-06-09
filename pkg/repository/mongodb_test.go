package repository_test

import (
	"context"
	"os"
	"testing"

	"github.com/joho/godotenv"
	annales "github.com/spoletum/annales/gen"
	"github.com/spoletum/annales/pkg/repository"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"
)

func TestMongoRepository(t *testing.T) {

	ctx := context.Background()

	// Read the URL of the mongodb connection from dotenv
	err := godotenv.Load()
	if err != nil {
		require.True(t, os.IsNotExist(err))
	}

	url := os.Getenv("MONGODB_URL")

	// Establish a connection with MongoDB
	client, err := mongo.NewClient(options.Client().ApplyURI(url))
	require.NoError(t, err)
	require.NoError(t, client.Connect(ctx))
	defer client.Disconnect(ctx)

	// Delete any previous test data
	client.Database("eventstore").Collection("events").Drop(ctx)

	// Initialize the repository
	repo, err := repository.NewMongoRepository(ctx, client, "eventstore")
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

	t.Run("Get info and events from empty repository", func(t *testing.T) {

		repo := repository.NewInMemoryRepository()

		ctx := context.Background()
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
