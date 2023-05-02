package mongodb

import (
	"context"
	"time"

	"github.com/rs/zerolog/log"
	annales "github.com/spoletum/annales/gen"
	"github.com/spoletum/annales/pkg/errors"
	"github.com/spoletum/annales/pkg/lock"
	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"
)

const (
	StreamsCollectionName = "streams"
	EventsCollectionName  = "events"
)

type MongoJournal struct {
	annales.UnimplementedJournalServer               // required by gRPC implementation
	Client                             *mongo.Client // the MongoDB driver
	DatabaseName                       string        // the name of the database to use
	LockStreamVersion                  lock.LockStreamVersionFn
}

// Stream represents a stream document in the MongoDB streams collection.
type Stream struct {
	ID            string `bson:"_id"`
	StreamVersion int64  `bson:"stream_version"`
}

// Append appends a new event to the specified stream.
func (md *MongoJournal) AppendEvent(ctx context.Context, req *annales.AppendEventRequest) (*annales.AppendEventResponse, error) {
	log.Debug().Msg("Start Append")

	// Get the streams and events collections
	streamsCollection := md.Client.Database(md.DatabaseName).Collection(StreamsCollectionName)
	eventsCollection := md.Client.Database(md.DatabaseName).Collection(EventsCollectionName)

	// Define a filter and update document for the upsert operation
	filter := bson.M{"_id": req.StreamId, "stream_version": req.ExpectedVersion}
	update := bson.M{"$inc": bson.M{"stream_version": 1}}

	if req.ExpectedVersion == 0 {
		// Perform the upsert operation on the streams collection
		_, err := streamsCollection.InsertOne(ctx, &Stream{ID: req.StreamId, StreamVersion: 1})
		if err != nil {
			if mongo.IsDuplicateKeyError(err) {
				return nil, errors.InvalidStreamVersionError()
			}
			return nil, err
		}
	} else {
		// Perform the update operation on the streams collection
		updateResult, err := streamsCollection.UpdateOne(ctx, filter, update)
		if err != nil {
			return nil, err
		}
		if updateResult.ModifiedCount != 1 {
			return nil, errors.InvalidStreamVersionError()
		}
	}

	// At this point, we have ensured that the stream version is correct, so we can insert the new event
	newEvent := annales.Event{
		StreamId:  req.GetStreamId(),
		Version:   req.ExpectedVersion + 1,
		EventType: req.GetEventType(),
		Source:    req.GetSource(),
		Encoding:  req.GetEncoding(),
		Data:      req.GetData(),
		Timestamp: time.Now().String(),
	}

	_, err := eventsCollection.InsertOne(ctx, newEvent)
	if err != nil {
		return nil, err
	}

	log.Debug().Msg("End Append")
	return &annales.AppendEventResponse{}, nil
}

func (j *MongoJournal) GetStreamEvents(ctx context.Context, req *annales.GetStreamEventsRequest) (*annales.GetStreamEventsResponse, error) {

	// Run the query and exit if an error occurs
	filter := bson.M{"stream_id": req.StreamId}
	eventsCollection := j.Client.Database(j.DatabaseName).Collection(EventsCollectionName)
	cur, err := eventsCollection.Find(ctx, filter, options.Find().SetSort(bson.D{{Key: "stream_version", Value: 1}}))
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

func NewMongoJournal(ctx context.Context, client *mongo.Client, dbName string) (*MongoJournal, error) {
	eventsCollection := client.Database(dbName).Collection(EventsCollectionName)
	streamIndex := mongo.IndexModel{
		Keys: bson.D{
			{Key: "stream_id", Value: 1},
			{Key: "stream_version", Value: 1},
		},
		Options: options.Index().SetUnique(true),
	}
	_, err := eventsCollection.Indexes().CreateOne(ctx, streamIndex)
	if err != nil {
		return nil, err
	}
	log.Info().Msg("initialized mongo driver")
	return &MongoJournal{
		Client:       client,
		DatabaseName: dbName,
	}, nil
}
