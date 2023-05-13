package journal

import (
	"context"
	"fmt"
	"sync/atomic"

	lru "github.com/hashicorp/golang-lru/v2"
	"github.com/rs/zerolog/log"
	annales "github.com/spoletum/annales/gen"
	"github.com/spoletum/annales/pkg/errors"
	"github.com/spoletum/annales/pkg/repository"
)

var ErrInvalidRepository = fmt.Errorf("event appender cannot be null")

type CachingJournal struct {
	annales.UnimplementedJournalServer                                   // required by gRPC implementation
	StreamVersions                     *lru.TwoQueueCache[string, int64] // a cache that keeps the latest version of the cache in memory
	repository                         repository.Repository
	Metrics                            struct {
		WriteRequests        int64
		ReadRequests         int64
		InvalidWriteRequests int64
		FailedWriteRequests  int64
		FailedReadRequests   int64
		CacheHits            int64
		CacheMisses          int64
		AverageWriteLatency  float64
		AverageReadLatency   float64
	} // Datapoints that report the current stats for the event store
}

func (md *CachingJournal) AppendEvent(ctx context.Context, in *annales.AppendEventRequest) (*annales.AppendEventResponse, error) {
	// logger := log.With().Str("streamId", in.StreamId).Str("service", "AppendEvent").Logger()
	atomic.AddInt64(&md.Metrics.WriteRequests, 1)
	// Get the information about the stream
	info, err := md.GetStreamInfo(ctx, &annales.GetStreamInfoRequest{StreamId: in.StreamId})
	if err != nil {
		atomic.AddInt64(&md.Metrics.FailedWriteRequests, 1)
		return nil, err
	}

	// The expected version does not match the actual, the request is deemed invalid
	if info.Version != in.ExpectedVersion {
		atomic.AddInt64(&md.Metrics.InvalidWriteRequests, 1)
		return nil, errors.InvalidStreamVersionError
	}

	// At this point, we have ensured that the stream version is correct, so we can insert the new event
	_, err = md.repository.AppendEvent(ctx, in)
	if err != nil {
		atomic.AddInt64(&md.Metrics.FailedWriteRequests, 1)
		return nil, err
	}

	md.StreamVersions.Add(in.StreamId, info.Version)

	return &annales.AppendEventResponse{}, nil
}

func (j *CachingJournal) GetStreamInfo(ctx context.Context, in *annales.GetStreamInfoRequest) (*annales.GetStreamInfoResponse, error) {

	logger := log.With().Str("streamId", in.StreamId).Str("service", "GetStreamInfo").Logger()

	// Try to extract the latest version of the stream from the cache
	version, found := j.StreamVersions.Get(in.StreamId)

	if !found {

		// Update the missed cache metrics
		atomic.AddInt64(&j.Metrics.CacheMisses, 1)

		// Try to load the stream version from the persistence
		logger.Debug().Msg("cache miss")
		res, err := j.repository.GetStreamInfo(ctx, in)
		if err != nil {
			return nil, err
		}

		// Update the result
		version = res.Version

		// Update the cache
		j.StreamVersions.Add(in.StreamId, res.Version)
		logger.Debug().Int64("version", version).Msg("Added entry in cache")

	} else {
		// Increment cache hit counters
		atomic.AddInt64(&j.Metrics.CacheHits, 1)
	}
	// Return the result object
	return &annales.GetStreamInfoResponse{Version: version}, nil
}

// Creates new instances of a caching jornal
func NewCachingJournal(repository repository.Repository, cacheSize int) (*CachingJournal, error) {
	if repository == nil {
		return nil, ErrInvalidRepository
	}
	cache, err := lru.New2Q[string, int64](128)
	if err != nil {
		return nil, err
	}
	return &CachingJournal{
		StreamVersions: cache,
		repository:     repository,
	}, nil
}
