package server

import (
	"context"
	"errors"

	annales "github.com/spoletum/annales/gen"
	"github.com/spoletum/annales/pkg/validation"
)

type (
	LockManager         func(ctx context.Context, streamId string, expectedVersion int64) error
	EventAppender       func(ctx context.Context, req *annales.AppendEventRequest) error
	LatestVersionLoader func(ctx context.Context, streamId string) (int64, error)
)

type JournalServer struct {
	annales.UnimplementedJournalServer
	acquireLock func(context.Context, *annales.AppendEventRequest) error
	appendEvent func(context.Context, *annales.AppendEventRequest) error
}

func (j *JournalServer) AppendEvent(ctx context.Context, req *annales.AppendEventRequest) (*annales.AppendEventResponse, error) {

	// Validate the request
	validator := validation.NewValidator()
	validator.Add("streamId", req.StreamId, validation.NotEmpty())
	validator.Add("expectedVersion", req.ExpectedVersion, validation.Int64GreaterThan(0))
	if err := validator.Validate().Error(); err != nil {
		return nil, err
	}

	// Try to acquire a lock on the stream. If the lock fails, we do not write the event and report an error
	if err := j.acquireLock(ctx, req); err != nil {
		return nil, err
	}

	// Lock is acquired, we can the event
	if err := j.appendEvent(ctx, req); err != nil {
		return nil, err
	}

	return &annales.AppendEventResponse{}, nil
}

func OptimisticLockingError() error {
	return errors.New("optimistic locking error")
}

func IsOptimistcLockingError(actual error) bool {
	return actual == OptimisticLockingError()
}
