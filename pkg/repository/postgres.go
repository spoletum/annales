package repository

import (
	"context"
	"database/sql"
	"fmt"

	"github.com/lib/pq"
	"github.com/rs/zerolog/log"
	annales "github.com/spoletum/annales/gen"
	"github.com/spoletum/annales/pkg/errors"
)

const (
	sqlAppendEvent       = "INSERT INTO events (stream_id, stream_version, event_type, event_encoding, event_source, event_data) VALUES ($1, $2, $3, $4, $5, $6)"
	sqlGetEventsByStream = "SELECT stream_id, stream_version, event_type, event_encoding, event_source, event_data, event_ts FROM events WHERE stream_id=$1 ORDER BY stream_version"
	sqlGetStreamVersion  = "SELECT COALESCE(MAX(stream_version), 0) FROM events WHERE stream_id = $1"
	errDuplicateKey      = "23505" // Can't believe the driver does not provide a constant
)

type PostgresRepository struct {
	annales.UnimplementedJournalServer
	db *sql.DB
}

func (pd *PostgresRepository) AppendEvent(ctx context.Context, req *annales.AppendEventRequest) (*annales.AppendEventResponse, error) {

	// Initialize a transaction so that the writes are atomic
	tx, err := pd.db.BeginTx(ctx, nil)
	if err != nil {
		return nil, err
	}

	// Executes the insert in the table
	_, err = tx.Exec(sqlAppendEvent, req.StreamId, req.ExpectedVersion+1, req.EventType, req.Encoding, req.Source, req.Data)
	if err != nil {
		// In case of error, we translate a known error and attempt a rollback
		pqerr := err.(*pq.Error)
		if pqerr.Code == errDuplicateKey {
			err = errors.InvalidStreamVersionError
		}
		_ = tx.Rollback()
		return nil, err
	}

	// Executes the commit and reports an error if it fails, or the response if it succeeds
	if err = tx.Commit(); err != nil {
		return nil, err
	}
	return &annales.AppendEventResponse{}, nil
}

// GetEventsByStream retrieves all events belonging to a specific stream_id
func (pd *PostgresRepository) GetStreamEvents(ctx context.Context, req *annales.GetStreamEventsRequest) (*annales.GetStreamEventsResponse, error) {
	logger := log.With().Str("function", "Get").Str("stream_id", req.StreamId).Logger() // Create a logger with the function name

	logger.Info().Msg("Retrieving events by stream_id") // Log an info message

	rows, err := pd.db.Query(sqlGetEventsByStream, req.StreamId)
	if err != nil {
		logger.Error().Err(err).Msg("Failed to get events by stream_id") // Log an error message
		return nil, fmt.Errorf("failed to get events by stream_id: %v", err)
	}
	defer rows.Close()

	events := make([]*annales.Event, 0)
	for rows.Next() {
		event := &annales.Event{}
		err := rows.Scan(&event.StreamId, &event.Version, &event.EventType, &event.Encoding, &event.Source, &event.Data, &event.Timestamp)
		if err != nil {
			logger.Error().Err(err).Msg("Failed to scan events") // Log an error message
			return nil, fmt.Errorf("failed to scan events: %v", err)
		}
		events = append(events, event)
	}
	if err := rows.Err(); err != nil {
		logger.Error().Err(err).Msg("Error iterating events") // Log an error message
		return nil, fmt.Errorf("error iterating events: %v", err)
	}

	logger.Info().Int("num_events", len(events)).Msg("Retrieved events by stream_id") // Log an info message

	return &annales.GetStreamEventsResponse{Events: events}, nil
}

func (pd *PostgresRepository) GetStreamInfo(ctx context.Context, in *annales.GetStreamInfoRequest) (*annales.GetStreamInfoResponse, error) {

	logger := log.With().Str("stream_id", in.StreamId).Logger() // Create a logger with the function name

	var version annales.GetStreamInfoResponse

	// Run the query
	rs, err := pd.db.Query(sqlGetStreamVersion, in.StreamId)
	if err != nil && err != sql.ErrNoRows {
		logger.Error().Err(err).Msg("Error executing query") // Log an error message
		return nil, err
	}
	defer rs.Close()

	// Map the result against the result object
	if rs.Next() {
		if err := rs.Scan(&version.Version); err != nil {
			return nil, err
		}
	}

	// Return the result
	return &version, nil
}

func NewPostgresRepository(db *sql.DB) (*PostgresRepository, error) {
	if db == nil {
		return nil, fmt.Errorf("database must not be nil")
	}
	return &PostgresRepository{db: db}, nil
}
