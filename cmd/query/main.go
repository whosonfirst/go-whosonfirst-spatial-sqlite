package main

import (
	"context"
	"encoding/json"
	"flag"
	"fmt"
	_ "github.com/whosonfirst/go-whosonfirst-spatial-sqlite"
	"github.com/whosonfirst/go-whosonfirst-spatial/database"
	"github.com/whosonfirst/go-whosonfirst-spatial/filter"
	"github.com/whosonfirst/go-whosonfirst-spatial/geo"
	"github.com/whosonfirst/go-whosonfirst-spatial/properties"
	"github.com/whosonfirst/go-whosonfirst-spr"
	// "github.com/sfomuseum/go-flags/multi"
	"log"
)

func main() {

	database_uri := flag.String("database-uri", "", "...")
	properties_uri := flag.String("properties-uri", "", "...")
	latitude := flag.Float64("latitude", 0.0, "...")
	longitude := flag.Float64("longitude", 0.0, "...")

	// var props multi.MultString
	//flag.Var(&props, "properties", "...")

	flag.Parse()

	props := []string{
		"wof:concordances",
		"wof:hierarchy",
		"sfomuseum:*",
	}

	ctx := context.Background()
	db, err := database.NewSpatialDatabase(ctx, *database_uri)

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

	var rsp interface{}

	r, err := db.PointInPolygon(ctx, c, f)

	if err != nil {
		log.Fatalf("Failed to query database with coord %v, %v", c, err)
	}

	rsp = r

	if len(props) > 0 {

		pr, err := properties.NewPropertiesReader(ctx, *properties_uri)

		if err != nil {
			log.Fatalf("Failed to create properties reader, %v", err)
		}

		r, err := pr.PropertiesResponseResultsWithStandardPlacesResults(ctx, rsp.(spr.StandardPlacesResults), props)

		if err != nil {
			log.Fatalf("Failed to generate properties response, %v", err)
		}

		rsp = r
	}

	enc, err := json.Marshal(rsp)

	if err != nil {
		log.Fatalf("Failed to marshal results, %v", err)
	}

	fmt.Println(string(enc))
}
