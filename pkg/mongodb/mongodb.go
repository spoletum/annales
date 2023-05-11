package mongodb

import (
	"context"
	"time"

	lru "github.com/hashicorp/golang-lru/v2"
	"github.com/rs/zerolog/log"
	annales "github.com/spoletum/annales/gen"
	"github.com/spoletum/annales/pkg/journal"
	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/bson/primitive"
	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"
)

const (
	EventsCollectionName = "events"
	fieldStreamId        = "streamid"
	fieldStreamVersion   = "version"
)

type MongoJournal struct {
	journal.AbstractCachingJournal
	client       *mongo.Client // the MongoDB driver
	databaseName string        // the name of the database to use
}

// AppendEventImpl appends a new event to the specified stream in MongoDB
// Assumes that all the optimistic locking has been performed by AbstractCachingJournal
func (md *MongoJournal) AppendEventImpl(ctx context.Context, in *annales.AppendEventRequest) (*annales.AppendEventResponse, error) {

	eventsCollection := md.client.Database(md.databaseName).Collection(EventsCollectionName)
	newEvent := annales.Event{
		StreamId:  in.GetStreamId(),
		Version:   in.ExpectedVersion + 1,
		EventType: in.GetEventType(),
		Source:    in.GetSource(),
		Encoding:  in.GetEncoding(),
		Data:      in.GetData(),
		Timestamp: time.Now().String(),
	}

	_, err := eventsCollection.InsertOne(ctx, &newEvent)
	if err != nil {
		return nil, err
	}

	return &annales.AppendEventResponse{}, nil
}

func (j *MongoJournal) GetStreamEvents(ctx context.Context, req *annales.GetStreamEventsRequest) (*annales.GetStreamEventsResponse, error) {

	// Run the query and exit if an error occurs
	filter := bson.M{fieldStreamId: req.StreamId}
	eventsCollection := j.client.Database(j.databaseName).Collection(EventsCollectionName)
	cur, err := eventsCollection.Find(ctx, filter, options.Find().SetSort(bson.D{{Key: fieldStreamVersion, Value: 1}}))
	if err != nil {
		return nil, err
	}

	events := make([]*annales.Event, 0)
	// Query executed successfully, build the result set
	defer cur.Close(ctx)

	// Check that no event is missing
	for cur.Next(ctx) {
		event := &annales.Event{}
		cur.Decode(event)
		events = append(events, event)
	}
	return &annales.GetStreamEventsResponse{Events: events}, nil
}

func (md *MongoJournal) GetStreamInfoImpl(ctx context.Context, in *annales.GetStreamInfoRequest) (*annales.GetStreamInfoResponse, error) {

	eventsCollection := md.client.Database(md.databaseName).Collection(EventsCollectionName)
	logger := log.With().Str("streamId", in.StreamId).Str("service", "GetStreamInfo").Logger()

	// Define a filter and update document for the upsert operation
	streamEvents := primitive.M{fieldStreamId: in.StreamId}
	fromLastEvent := options.FindOne().SetSort(bson.D{{Key: fieldStreamVersion, Value: -1}}).SetProjection(bson.M{fieldStreamVersion: 1})

	// We have a cache miss, try to load the stream version from the persistence
	res := eventsCollection.FindOne(ctx, streamEvents, fromLastEvent)

	//
	out := &annales.GetStreamInfoResponse{Version: 0}

	if res.Err() != nil {

		// We have an error we cannot manage, just return it to the caller
		if res.Err() != mongo.ErrNoDocuments {
			return nil, res.Err()
		}
		logger.Debug().Msg("stream does not exist in persistence")

	} else {

		// Decode the result set
		var rec primitive.M
		if err := res.Decode(&rec); err != nil {
			return nil, err
		}

		logger.Debug().Any("record", &rec).Msg("stream exists in persistence")
		version32 := rec[fieldStreamVersion].(int32)
		out.Version = int64(version32)
	}
	return out, nil
}

func NewMongoJournal(ctx context.Context, client *mongo.Client, dbName string, cacheSize int) (*MongoJournal, error) {

	// Configure logging
	logger := log.With().Str("service", "NewMongoJournal").Logger()

	// Ensures that the MongoDB collection hasa a unique index on streamId and streamVersion
	eventsCollection := client.Database(dbName).Collection(EventsCollectionName)
	streamIndex := mongo.IndexModel{
		Keys: bson.D{
			{Key: fieldStreamId, Value: 1},
			{Key: fieldStreamVersion, Value: 1},
		},
		Options: options.Index().SetUnique(true),
	}
	index, err := eventsCollection.Indexes().CreateOne(ctx, streamIndex)
	if err != nil {
		return nil, err
	}
	logger.Info().Str("index_name", index).Msg("Index created successfully")

	// Initialize the cache to store the stream versions
	// TODO cache size should be set using an external parameter
	cache, err := lru.New2Q[string, int64](cacheSize)
	if err != nil {
		return nil, err
	}

	logger.Debug().Int("cache_size", cacheSize).Msg("Cache initialized")

	logger.Info().Msg("mongo driver initialized")
	return &MongoJournal{
		AbstractCachingJournal: journal.AbstractCachingJournal{
			StreamVersions: cache,
		},
		client:       client,
		databaseName: dbName,
	}, nil
}
