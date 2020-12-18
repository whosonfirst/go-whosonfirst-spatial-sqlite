package main

import (
	"context"
	"encoding/json"
	"fmt"
	"github.com/sfomuseum/go-flags/multi"
	_ "github.com/whosonfirst/go-whosonfirst-spatial-sqlite"
	"github.com/whosonfirst/go-whosonfirst-spatial/database"
	"github.com/whosonfirst/go-whosonfirst-spatial/filter"
	"github.com/whosonfirst/go-whosonfirst-spatial/flags"	
	"github.com/whosonfirst/go-whosonfirst-spatial/geo"
	"github.com/whosonfirst/go-whosonfirst-spatial/properties"
	"github.com/whosonfirst/go-whosonfirst-spr"
	"log"
	"net/url"
)

func main() {

	fs, err := flags.CommonFlags()

	if err != nil {
		log.Fatal(err)
	}
	
	// flags.AppendQueryFlags(fs)
	
	latitude := fs.Float64("latitude", 0.0, "A valid latitude.")
	longitude := fs.Float64("longitude", 0.0, "A valid longitude.")

	geometries := fs.String("geometries", "all", "Valid options are: all, alt, default.")
	
	var props multi.MultiString
	fs.Var(&props, "properties", "One or more Who's On First properties to append to each result.")

	var pts multi.MultiString
	fs.Var(&pts, "placetype", "One or more place types to filter results by.")
	
	var alt_geoms multi.MultiString
	fs.Var(&alt_geoms, "alternate-geometry", "One or more alternate geometry labels (wof:alt_label) values to filter results by.")

	var is_current multi.MultiString
	fs.Var(&is_current, "is-current", "One or more existential flags (-1, 0, 1) to filter results by.")

	var is_ceased multi.MultiString
	fs.Var(&is_ceased, "is-ceased", "One or more existential flags (-1, 0, 1) to filter results by.")
	
	var is_deprecated multi.MultiString
	fs.Var(&is_deprecated, "is-deprecated", "One or more existential flags (-1, 0, 1) to filter results by.")
	
	var is_superseded multi.MultiString
	fs.Var(&is_superseded, "is-superseded", "One or more existential flags (-1, 0, 1) to filter results by.")
	
	var is_superseding multi.MultiString
	fs.Var(&is_superseding, "is-superseding", "One or more existential flags (-1, 0, 1) to filter results by.")	
	
	flags.Parse(fs)

	err = flags.ValidateCommonFlags(fs)

	if err != nil {
		log.Fatal(err)
	}

	// flags.ValidateQueryFlags(fs)
	
	database_uri, _ := flags.StringVar(fs, "spatial-database-uri")
	properties_uri, _ := flags.StringVar(fs, "properties-reader-uri")

	ctx := context.Background()
	db, err := database.NewSpatialDatabase(ctx, database_uri)

	if err != nil {
		log.Fatalf("Failed to create database for '%s', %v", database_uri, err)
	}

	c, err := geo.NewCoordinate(*longitude, *latitude)

	if err != nil {
		log.Fatalf("Failed to create new coordinate, %v", err)
	}

	// START OF put me in a WithFlagSet(fs) function

	q := url.Values{}
	q.Set("geometries", *geometries)

	for _, v := range alt_geoms {
		q.Add("alternate_geometry", v)
	}
	
	for _, v := range pts {
		q.Add("placetype", v)
	}

	for _, v := range is_ceased {
		q.Add("is_ceased", v)
	}

	for _, v := range is_deprecated {
		q.Add("is_deprecated", v)
	}

	for _, v := range is_superseded {
		q.Add("is_superseded", v)
	}

	for _, v := range is_superseding {
		q.Add("is_superseding", v)
	}

	f, err := filter.NewSPRFilterFromQuery(q)

	if err != nil {
		log.Fatalf("Failed to create SPR filter, %v", err)
	}

	// END OF put me in a WithFlagSet(fs) function
	
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
