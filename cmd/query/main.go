package main

import (
	"context"
	"encoding/json"
	"flag"
	"fmt"
	"github.com/sfomuseum/go-flags/multi"
	_ "github.com/whosonfirst/go-whosonfirst-spatial-sqlite"
	"github.com/whosonfirst/go-whosonfirst-spatial/database"
	"github.com/whosonfirst/go-whosonfirst-spatial/filter"
	"github.com/whosonfirst/go-whosonfirst-spatial/geo"
	"github.com/whosonfirst/go-whosonfirst-spatial/properties"
	"github.com/whosonfirst/go-whosonfirst-spr"
	"github.com/whosonfirst/go-whosonfirst-flags"		
	"github.com/whosonfirst/go-whosonfirst-flags/geometry"	
	"log"
)

func main() {

	database_uri := flag.String("database-uri", "", "...")
	properties_uri := flag.String("properties-uri", "", "...")
	latitude := flag.Float64("latitude", 0.0, "...")
	longitude := flag.Float64("longitude", 0.0, "...")

	var props multi.MultiString
	flag.Var(&props, "properties", "...")

	var alt_geoms multi.MultiString
	flag.Var(&alt_geoms, "alternate-geometry", "...")
	
	flag.Parse()

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

	/*
	af, err := geometry.NewIsAlternateGeometryFlag(true)

	if err != nil {
		log.Fatalf("Failed to create alternate geometry flag, %v", err)
	}
	*/
	
	if len(alt_geoms) > 0 {

		alt_flags := make([]flags.AlternateGeometryFlag, 0)

		for _, label := range alt_geoms {
			
			fl, err := geometry.NewAlternateGeometryFlagWithLabel(label)

			if err != nil {
				log.Fatalf("Failed to ... for '%s', %v", label, err)
			}

			alt_flags = append(alt_flags, fl)
		}
		
		f.AlternateGeometries = alt_flags
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
