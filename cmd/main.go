package main

import (
	kwikmedicalDbClient "github.com/jamieyoung5/kwikmedical-db-lib/pkg/client"
	dbConfig "github.com/jamieyoung5/kwikmedical-db-lib/pkg/config"
	"github.com/jamieyoung5/kwikmedical-eventstream/pkg/eventutil"
	"github.com/jamieyoung5/kwikmedical-request-service/internal/handler"
	"go.uber.org/zap"
	"os"
)

func main() {
	logger, err := zap.NewDevelopment()
	if err != nil {
		logger.Error("Error initializing logger", zap.Error(err))
		os.Exit(1)
	}

	client, err := eventutil.CreateConnection("localhost:4343")
	if err != nil {
		logger.Error("Error initializing client", zap.Error(err))
		os.Exit(1)
	}

	dbClient := CreateDbClient(logger)

	h := handler.NewHandler(logger, client, dbClient)
	err = h.ProcessEvents()
	if err != nil {
		logger.Error("Error processing events", zap.Error(err))
	}
}

func CreateDbClient(logger *zap.Logger) *kwikmedicalDbClient.KwikMedicalDBClient {
	config := dbConfig.NewConfig()

	client, err := kwikmedicalDbClient.NewClient(logger, config)
	if err != nil {
		logger.Error("Error initializing client", zap.Error(err))
		os.Exit(1)
	}

	return client
}
