package journal_test

import (
	"context"
	"testing"

	annales "github.com/spoletum/annales/gen"
	"github.com/spoletum/annales/pkg/errors"
	"github.com/spoletum/annales/pkg/journal"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// A mock repository to provide the responses we need in the test
type mockRepository struct {
}

func (mj *mockRepository) GetStreamInfo(ctx context.Context, in *annales.GetStreamInfoRequest) (*annales.GetStreamInfoResponse, error) {
	return &annales.GetStreamInfoResponse{Version: 1}, nil
}

func (mj *mockRepository) AppendEvent(ctx context.Context, in *annales.AppendEventRequest) (*annales.AppendEventResponse, error) {
	return &annales.AppendEventResponse{}, nil
}

func TestCachingJournal(t *testing.T) {

	j, err := journal.NewCachingJournal(&mockRepository{}, 100)
	require.NoError(t, err)

	ctx := context.Background()

	t.Run("AppendEvent", func(t *testing.T) {
		_, err := j.AppendEvent(ctx, &annales.AppendEventRequest{StreamId: "123", ExpectedVersion: 1})
		assert.NoError(t, err)
	})

	t.Run("AppendEvent with wrong version", func(t *testing.T) {
		_, err := j.AppendEvent(ctx, &annales.AppendEventRequest{StreamId: "123", ExpectedVersion: 2})
		assert.Error(t, err)
		assert.Equal(t, errors.InvalidStreamVersionError, err)
	})

	t.Run("GetStreamInfo", func(t *testing.T) {
		res, err := j.GetStreamInfo(ctx, &annales.GetStreamInfoRequest{StreamId: "123"})
		assert.NoError(t, err)
		assert.Equal(t, int64(1), res.Version)
	})

	t.Run("GetStreamInfo with cache", func(t *testing.T) {
		j.StreamVersions.Add("123", int64(2))
		res, err := j.GetStreamInfo(ctx, &annales.GetStreamInfoRequest{StreamId: "123"})
		assert.NoError(t, err)
		assert.Equal(t, int64(2), res.Version)
	})
}
