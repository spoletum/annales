package repository

import (
	"context"

	annales "github.com/spoletum/annales/gen"
)

// A Repository is a database-specific implementation of the appender and the peeker.
// It is not require to think about optimistic locking or caching, that is a responsibility of the journal.
// Yes, the interface definition is a bit opaque but the objective is to avoid unnecessary allocations
// and data mappings.
type Repository interface {
	AppendEvent(ctx context.Context, in *annales.AppendEventRequest) (*annales.AppendEventResponse, error)
	GetStreamInfo(ctx context.Context, in *annales.GetStreamInfoRequest) (*annales.GetStreamInfoResponse, error)
}
