package main

import (
	"context"
	"encoding/json"
	"flag"
	"fmt"
	"github.com/whosonfirst/go-whosonfirst-spatial-database-sqlite"
	"github.com/whosonfirst/go-whosonfirst-spatial/filter"
	"github.com/whosonfirst/go-whosonfirst-spatial/geo"
	"log"
)

func main() {

	database_uri := flag.String("uri", "", "...")
	latitude := flag.Float64("latitude", 0.0, "...")
	longitude := flag.Float64("longitude", 0.0, "...")

	flag.Parse()

	ctx := context.Background()
	db, err := sqlite.NewSQLiteSpatialDatabase(ctx, *database_uri)

	if err != nil {
		log.Fatalf("Failed to create database for '%s', %v", *database_uri, err)
	}

	c, err := geo.NewCoordinate(*longitude, *latitude)

	if err != nil {
		log.Fatalf("Failed to create new coordinate, %v", err)
	}

	f, err := filter.NewSPRFilter()

	if err != nil {
		log.Fatalf("Failed to create SPR filter, %v", err)
	}

	r, err := db.PointInPolygon(ctx, c, f)

	if err != nil {
		log.Fatalf("Failed to query database with coord %v, %v", c, err)
	}

	enc, err := json.Marshal(r)

	if err != nil {
		log.Fatalf("Failed to marshal results, %v", err)
	}

	fmt.Println(string(enc))
}
