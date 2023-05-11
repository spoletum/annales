package client

import (
	"context"
	"errors"

	"github.com/rs/zerolog/log"
	"github.com/serialx/hashring"
	annales "github.com/spoletum/annales/gen"
	"github.com/spoletum/annales/pkg/validation"
	"google.golang.org/grpc"
)

type AnnalesClientOption interface {
	Apply(*AnnalesClient) error
}

type AnnalesClient struct {
	ring    *hashring.HashRing
	clients map[string]annales.JournalClient
}

func NewAnnalesClient(ctx context.Context, addresses []string, grpcOptions ...grpc.DialOption) (*AnnalesClient, error) {

	if len(addresses) == 0 {
		return nil, ErrNoNodeForStreamId()
	}

	result := &AnnalesClient{
		ring:    hashring.New(addresses),                // Build a hashring with the list of servers
		clients: make(map[string]annales.JournalClient), // Initialize streamID <-> server map
	}

	// Establish a client for each server in the cluster
	// TODO Code does not release connections in case of failure
	for _, address := range addresses {
		conn, err := grpc.DialContext(ctx, address, grpcOptions...)
		if err != nil {
			return nil, err
		}
		result.clients[address] = annales.NewJournalClient(conn)
	}
	return result, nil
}

// Selects the correct node for the clusters and forwards the request
// TODO Should we validate the request client-side to avoid flodding nodes with incorrect requests?
func (client *AnnalesClient) AppendEvent(ctx context.Context, in *annales.AppendEventRequest, opts ...grpc.CallOption) (*annales.AppendEventResponse, error) {

	// Validate the key fields of the command
	v := validation.NewValidator()
	v.Add("streamId", in.GetStreamId(), validation.NotEmpty())
	v.Add("expectedVersion", in.ExpectedVersion, validation.Int64GreaterThanOrEqual(0))
	v.Add("eventType", in.EventType, validation.NotEmpty())
	v.Add("data", in.Data, validation.NotEmptyArray())
	if err := v.Validate().Error(); err != nil {
		return nil, err
	}

	// Determine which node will receive the command
	journal, err := client.getShard(in.StreamId)
	if err != nil {
		return nil, err
	}

	// Execute the command
	return journal.AppendEvent(ctx, in, opts...)
}

// Returns all the events belonging to a given stream ID
func (client *AnnalesClient) GetStreamEvents(ctx context.Context, in *annales.GetStreamEventsRequest, opts ...grpc.CallOption) (*annales.GetStreamEventsResponse, error) {

	// Validates the command before sending it over the wire
	v := validation.NewValidator()
	v.Add("streamId", in.GetStreamId(), validation.NotEmpty())
	if err := v.Validate().Error(); err != nil {
		return nil, err
	}

	// Determine which node will receive the command
	journal, err := client.getShard(in.StreamId)
	if err != nil {
		return nil, err
	}

	// Execute the command
	return journal.GetStreamEvents(ctx, in, opts...)
}

// Returns the current version of the stream
func (client *AnnalesClient) GetStreamInfo(ctx context.Context, in *annales.GetStreamInfoRequest, opts ...grpc.CallOption) (*annales.GetStreamInfoResponse, error) {

	// Validates the command before sending it over the wire
	v := validation.NewValidator()
	v.Add("streamId", in.GetStreamId(), validation.NotEmpty())
	if err := v.Validate().Error(); err != nil {
		return nil, err
	}

	// Determine which node will receive the command
	journal, err := client.getShard(in.StreamId)
	if err != nil {
		return nil, err
	}

	// Execute the command
	return journal.GetStreamInfo(ctx, in, opts...)
}

func (client *AnnalesClient) GetEvent(ctx context.Context, in *annales.GetEventRequest, opts ...grpc.CallOption) (*annales.GetEventResponse, error) {

	// Validates the command before sending it over the wire
	v := validation.NewValidator()
	v.Add("streamId", in.GetStreamId(), validation.NotEmpty())
	v.Add("streamVersion", in.GetStreamId(), validation.NotEmpty())

	if err := v.Validate().Error(); err != nil {
		return nil, err
	}

	journal, err := client.getShard(in.StreamId)
	if err != nil {
		return nil, err
	}
	return journal.GetEvent(ctx, in, opts...)
}

// Returns the Journal service instance for the streamId provided, or an error if one could not be determined
// TODO should we simply panic if the configuration is such that no server can be addressed?
func (client *AnnalesClient) getShard(streamId string) (annales.JournalClient, error) {
	addr, ok := client.ring.GetNode(streamId)
	if !ok {
		err := ErrNoAddressForStreamId()
		log.Error().Err(err).Str("streamId", streamId).Msg("Could not find a node in the hashring")
		return nil, err
	}
	server, ok := client.clients[addr]
	if !ok {
		err := ErrNoServerForStreamId()
		log.Error().Err(err).Str("streamId", streamId).Msg("Could not find the server in the connection pool")
		return nil, err
	}
	return server, nil
}

func ErrNoServerForStreamId() error {
	return errors.New("there is no server for the given stream ID - most likely the server list is empty")
}

func ErrNoNodeForStreamId() error {
	return errors.New("could not find a node for the stream ID")
}

func ErrNoAddressForStreamId() error {
	return errors.New("could not find an address for the stream ID")
}
