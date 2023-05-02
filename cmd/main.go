package main

import (
	"context"
	"fmt"
	"net"
	"net/http"
	"os"
	"sync"
	"time"

	"github.com/rs/zerolog"
	"github.com/rs/zerolog/log"
	annales "github.com/spoletum/annales/gen"
	"github.com/spoletum/annales/pkg/mongodb"
	"github.com/urfave/cli/v2"
	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"
	"google.golang.org/grpc"
)

const (
	flagGrpcAddr  = "grpc-addr"
	flagHttpAddr  = "http-addr"
	flagLogFormat = "log-format"
	flagDbUrl     = "db-url"
)

func main() {
	app := &cli.App{
		Usage: "a simple event store that is friendly with Dev and Ops",
		Commands: []*cli.Command{
			{
				Name:  "server",
				Usage: "Starts or controls a new server",
				Subcommands: []*cli.Command{
					{
						Name:   "start",
						Usage:  "Starts a new Annales instance",
						Action: startServer,
						Flags: []cli.Flag{
							&cli.StringFlag{
								Name:  flagGrpcAddr,
								Usage: "Address to bind the gRPC endpoint",
								Value: "0.0.0.0:9001",
							},
							&cli.StringFlag{
								Name:  flagHttpAddr,
								Usage: "Address to bind the HTTP/2 endpoint",
								Value: "0.0.0.0:9000",
							},
							&cli.StringFlag{
								Name:  flagLogFormat,
								Usage: "Allows to choose between json and console logging",
								Value: "json",
							},
							&cli.StringFlag{
								Name:     flagDbUrl,
								Usage:    "Specifies the URL to connect to the database",
								Value:    "",
								Required: true,
							},
						},
					},
				},
			},
		},
		Copyright: "(C) 2023 Alessandro Santini, all rights reserved",
	}

	if err := app.Run(os.Args); err != nil {
		fmt.Println(err)
		os.Exit(1)
	}
}

func startServer(ctx *cli.Context) error {

	// This variable points to the shared Journal service, so that both endpoints can reference it
	var driver annales.JournalServer

	// Extract the flags
	logFormat := ctx.String(flagLogFormat)
	httpAddr := ctx.String(flagHttpAddr)
	grpcAddr := ctx.String(flagGrpcAddr)
	dbUrl := ctx.String(flagDbUrl)

	// Setup the logger
	switch logFormat {
	case "json":
		break
	case "console":
		log.Logger = zerolog.New(zerolog.ConsoleWriter{Out: os.Stderr, TimeFormat: time.RFC3339}).With().Timestamp().Logger()
	default:
		return fmt.Errorf("--%s: value '%s' is not permitted (json, console)", flagLogFormat, logFormat)
	}

	// Display a welcome string
	log.Info().Str("version", "0.0.1").Msg("Starting Annales - Copyright (C) 2023, Alessandro Santini, all rights reserved.")

	// Setup a WaitGroup that will be used to read connection on two different ports (GRPC and HTTP)
	wg := sync.WaitGroup{}
	wg.Add(1)

	// Establish a connection with the MongoDB database
	// TODO: we will have to make this an option
	log.Info().Str("db-url", dbUrl).Msg("Establishing a connection with MongoDB")
	client, err := mongo.Connect(context.Background(), options.Client().ApplyURI(dbUrl))
	if err != nil {
		log.Fatal().Str("db-url", dbUrl).Err(err).Msg("Error establishing a connection to mongodb")
	}
	defer func() {
		_ = client.Disconnect(context.Background())
	}()

	// Create the MongoDB Driver
	driver, err = mongodb.NewMongoJournal(context.Background(), client, "eventstore")
	if err != nil {
		log.Fatal().Err(err).Msg("Error starting the MongoDB driver")
	}

	// Start the HTTP server on its own goroutine
	go func() {
		defer wg.Done()
		log.Info().Str("http-addr", httpAddr).Msg("Starting HTTP endpoint")
		if err := http.ListenAndServe(httpAddr, nil); err != nil {
			log.Fatal().Err(err).Msg("Error starting the HTTP endpoint")
		}
	}()

	// Start the GRPC endpoint in its own goroutine
	go func() {
		defer wg.Done()

		// Open a GRPC TPC endpoint
		log.Info().Str("grpc-addr", grpcAddr).Msg("Starting GRPC endpoint")
		listener, err := net.Listen("tcp", grpcAddr)
		if err != nil {
			log.Fatal().Err(err).Msg("Error starting the GRPC endpoint")
		}

		// Configure and start the GRPC endpoint
		server := grpc.NewServer()
		annales.RegisterJournalServer(server, driver)
		if err := server.Serve(listener); err != nil {
			log.Fatal().Err(err).Msg("Error starting the GRPC endpoint")
		}
	}()

	// Waits for the two listeners to complete
	wg.Wait()

	return nil
}
