package main

import (
	_ "github.com/whosonfirst/go-whosonfirst-spatial-sqlite"
)

import (
	"context"
	"log"

	"github.com/whosonfirst/go-whosonfirst-spatial-pip/app/query"	
)

func main() {

	ctx := context.Background()

	logger := log.Default()

	err := query.Run(ctx, logger)

	if err != nil {
		logger.Fatalf("Failed to run PIP application, %v", err)
	}

}
