package client

import (
	"context"
	"errors"
	"net"
	"os"
	"testing"
	"time"

	"github.com/joho/godotenv"
	"github.com/serialx/hashring"
	annales "github.com/spoletum/annales/gen"
	"github.com/spoletum/annales/pkg/mongodb"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"
	"google.golang.org/grpc"
)

func TestNewAnnalesClient(t *testing.T) {
	go func() {

		// Load the configuration parameters from .dotenv
		require.NoError(t, godotenv.Load())

		// Establish a connection to MongoDB
		client, err := mongo.NewClient(options.Client().ApplyURI(os.Getenv("MONGODB_URL")))
		require.NoError(t, err)

		// Create the journal server
		journal, err := mongodb.NewMongoJournal(context.Background(), client, "test", 100)
		require.NoError(t, err)

		// Add the code to start a new server
		listener, err := net.Listen("tcp", "localhost:8000")
		require.NoError(t, err)

		server := grpc.NewServer()
		annales.RegisterJournalServer(server, journal)
		require.NoError(t, server.Serve(listener))
	}()
	// Replace with your actual gRPC server addresses
	addresses := []string{"localhost:8000"}

	t.Run("success", func(t *testing.T) {
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()

		client, err := NewAnnalesClient(ctx, addresses, grpc.WithInsecure(), grpc.WithBlock())

		assert.NoError(t, err)
		assert.NotNil(t, client)
		assert.Len(t, client.clients, len(addresses))

		for _, addr := range addresses {
			assert.Contains(t, client.clients, addr)
			assert.NotNil(t, client.clients[addr])
		}
	})

	t.Run("dial_failure", func(t *testing.T) {
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()

		invalidAddresses := []string{"localhost:9999"}
		client, err := NewAnnalesClient(ctx, invalidAddresses, grpc.WithBlock())

		assert.Error(t, err)
		assert.Nil(t, client)
	})

	t.Run("empty_addresses", func(t *testing.T) {
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()

		client, err := NewAnnalesClient(ctx, []string{}, grpc.WithBlock())

		assert.Error(t, err)
		assert.True(t, errors.Is(err, ErrNoNodeForStreamId()))
		assert.Nil(t, client)
	})
}

func TestAppendEvent(t *testing.T) {
	// Replace with your actual gRPC server addresses
	addresses := []string{"localhost:8080", "localhost:8081", "localhost:8082"}

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	client, err := NewAnnalesClient(ctx, addresses, grpc.WithInsecure(), grpc.WithBlock())
	assert.NoError(t, err)
	assert.NotNil(t, client)

	t.Run("success", func(t *testing.T) {
		in := &annales.AppendEventRequest{StreamId: "stream1", Data: []byte("testdata")}
		response, err := client.AppendEvent(ctx, in)

		assert.NoError(t, err)
		assert.NotNil(t, response)
	})

	t.Run("no_server_for_stream_id", func(t *testing.T) {
		emptyClient := &AnnalesClient{
			ring:    hashring.New([]string{}),
			clients: make(map[string]annales.JournalClient),
		}

		in := &annales.AppendEventRequest{StreamId: "stream1", Data: []byte("testdata")}
		response, err := emptyClient.AppendEvent(ctx, in)

		assert.Error(t, err)
		assert.True(t, errors.Is(err, ErrNoServerForStreamId()))
		assert.Nil(t, response)
	})
}
