package journal

import (
	"context"

	lru "github.com/hashicorp/golang-lru/v2"
	"github.com/rs/zerolog/log"
	annales "github.com/spoletum/annales/gen"
	"github.com/spoletum/annales/pkg/errors"
)

type AbstractCachingJournal struct {
	annales.UnimplementedJournalServer                                   // required by gRPC implementation
	StreamVersions                     *lru.TwoQueueCache[string, int64] // a cache that keeps the latest version of the cache in memory
}

func (md *AbstractCachingJournal) AppendEvent(ctx context.Context, in *annales.AppendEventRequest) (*annales.AppendEventResponse, error) {
	// logger := log.With().Str("streamId", in.StreamId).Str("service", "AppendEvent").Logger()

	// Get the information about the stream
	info, err := md.GetStreamInfo(ctx, &annales.GetStreamInfoRequest{StreamId: in.StreamId})
	if err != nil {
		return nil, err
	}

	if info.Version != in.ExpectedVersion {
		return nil, errors.InvalidStreamVersionError()
	}

	// At this point, we have ensured that the stream version is correct, so we can insert the new event
	_, err = md.AppendEventImpl(ctx, in)
	if err != nil {
		return nil, err
	}

	md.StreamVersions.Add(in.StreamId, info.Version)

	return &annales.AppendEventResponse{}, nil
}

func (md *AbstractCachingJournal) GetStreamInfo(ctx context.Context, in *annales.GetStreamInfoRequest) (*annales.GetStreamInfoResponse, error) {

	logger := log.With().Str("streamId", in.StreamId).Str("service", "GetStreamInfo").Logger()

	// Try to extract the latest version of the stream from the cache
	version, found := md.StreamVersions.Get(in.StreamId)

	if !found {
		// We have a cache miss, try to load the stream version from the persistence
		logger.Debug().Msg("cache miss")
		res, err := md.GetStreamInfoImpl(ctx, in)
		if err != nil {
			return nil, err
		}
		// Update the cache
		md.StreamVersions.Add(in.StreamId, res.Version)
		logger.Debug().Int64("version", version).Msg("Added entry in cache")
	}
	return &annales.GetStreamInfoResponse{Version: version}, nil
}

func (md *AbstractCachingJournal) GetStreamInfoImpl(ctx context.Context, in *annales.GetStreamInfoRequest) (*annales.GetStreamInfoResponse, error) {
	panic("not implemented")
}

func (md *AbstractCachingJournal) AppendEventImpl(ctx context.Context, in *annales.AppendEventRequest) (*annales.AppendEventResponse, error) {
	panic("not implemented")
}
