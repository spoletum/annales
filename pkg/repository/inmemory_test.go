package repository_test

import (
	"context"
	"testing"

	annales "github.com/spoletum/annales/gen"
	"github.com/spoletum/annales/pkg/repository"
	"github.com/stretchr/testify/assert"
)

func TestInMemory(t *testing.T) {
	t.Run("Multiple streams with different numbers of events", func(t *testing.T) {
		repo := repository.NewInMemoryRepository()

		ctx := context.Background()
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
