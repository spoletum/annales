package repository

import (
	"context"
	"sync"
	"time"

	annales "github.com/spoletum/annales/gen"
)

// An in-memory repository for test purposes only.
type InMemoryRepository struct {
	events     []*annales.Event // the events log
	index      map[string][]int // An index that keeps a list of which events belong to a given stream
	appendLock sync.Mutex       // A lock to ensure correct concurrency when appending events
}

func (r *InMemoryRepository) AppendEvent(ctx context.Context, req *annales.AppendEventRequest) (*annales.AppendEventResponse, error) {

	// Acquire a lock to avoid access contention
	r.appendLock.Lock()
	defer r.appendLock.Unlock()

	// Append the event
	event := &annales.Event{
		Data:      req.Data,
		Encoding:  req.Encoding,
		EventType: req.EventType,
		Source:    req.Source,
		StreamId:  req.StreamId,
		Timestamp: time.Now().String(),
		Version:   req.ExpectedVersion + 1,
	}
	r.events = append(r.events, event)

	// Update the index
	r.index[req.StreamId] = append(r.index[req.StreamId], len(r.events)-1)

	// Return the response
	return &annales.AppendEventResponse{}, nil
}

// FIXME events are not immutable as they would be from a database. I do not think it is necessary to fix for now.
func (r *InMemoryRepository) GetStreamEvents(ctx context.Context, req *annales.GetStreamEventsRequest) (*annales.GetStreamEventsResponse, error) {

	// Initialize the results array
	events := make([]*annales.Event, 0)

	// Extract the event indexes for the requested stream ID
	positions, found := r.index[req.StreamId]

	// If the stream exists, iterate the index and append the events to the response
	if found {
		for _, position := range positions {
			events = append(events, r.events[position])
		}
	}

	// Return the result
	return &annales.GetStreamEventsResponse{
		Events: events,
	}, nil

}

func (r *InMemoryRepository) GetStreamInfo(ctx context.Context, in *annales.GetStreamInfoRequest) (*annales.GetStreamInfoResponse, error) {

	// Result variable
	result := &annales.GetStreamInfoResponse{
		Version: int64(len(r.index[in.StreamId])),
	}

	// Return the response
	return result, nil
}

func NewInMemoryRepository() *InMemoryRepository {
	return &InMemoryRepository{
		events: make([]*annales.Event, 0),
		index:  make(map[string][]int),
	}
}
