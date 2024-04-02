package main

import (
	_ "github.com/whosonfirst/go-whosonfirst-spatial-sqlite"
)

import (
	"context"
	"log"

	"github.com/whosonfirst/go-whosonfirst-spatial/app/pip"	
)

func main() {

	ctx := context.Background()

	logger := log.Default()

	err := pip.Run(ctx, logger)

	if err != nil {
		logger.Fatalf("Failed to run PIP application, %v", err)
	}

}
