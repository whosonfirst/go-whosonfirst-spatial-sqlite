package main

import (
	"context"
	"encoding/json"
	"fmt"
	_ "github.com/whosonfirst/go-whosonfirst-spatial-sqlite"
	"github.com/whosonfirst/go-whosonfirst-spatial/database"
	"github.com/whosonfirst/go-whosonfirst-spatial/flags"
	"github.com/whosonfirst/go-whosonfirst-spatial/geo"
	"github.com/whosonfirst/go-whosonfirst-spatial/properties"
	"github.com/whosonfirst/go-whosonfirst-spr"
	"github.com/sfomuseum/go-flags/flagset"	
	"log"
)

func main() {

	fs, err := flags.CommonFlags()

	if err != nil {
		log.Fatal(err)
	}

	err = flags.AppendQueryFlags(fs)

	if err != nil {
		log.Fatal(err)
	}

	flagset.Parse(fs)

	err = flags.ValidateCommonFlags(fs)

	if err != nil {
		log.Fatal(err)
	}

	err = flags.ValidateQueryFlags(fs)

	if err != nil {
		log.Fatal(err)
	}

	database_uri, _ := flags.StringVar(fs, "spatial-database-uri")
	properties_uri, _ := flags.StringVar(fs, "properties-reader-uri")

	props, _ := flags.MultiStringVar(fs, "properties")

	latitude, _ := flags.Float64Var(fs, "latitude")
	longitude, _ := flags.Float64Var(fs, "longitude")

	ctx := context.Background()
	db, err := database.NewSpatialDatabase(ctx, database_uri)

	if err != nil {
		log.Fatalf("Failed to create database for '%s', %v", database_uri, err)
	}

	c, err := geo.NewCoordinate(longitude, latitude)

	if err != nil {
		log.Fatalf("Failed to create new coordinate, %v", err)
	}

	f, err := flags.NewSPRFilterFromFlagSet(fs)

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

		pr, err := properties.NewPropertiesReader(ctx, properties_uri)

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
