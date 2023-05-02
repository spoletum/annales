package lock

import (
	"context"

	annales "github.com/spoletum/annales/gen"
)

// A function that
type LockStreamVersionFn func(ctx context.Context, streamId string, version int) error

type AppendEventFn func(ctx context.Context, request annales.AppendEventRequest) (*annales.AppendEventResponse, error)
