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
	"github.com/whosonfirst/go-whosonfirst-flags/placetypes"			
	"github.com/whosonfirst/go-whosonfirst-flags/existential"		
	"github.com/whosonfirst/go-whosonfirst-flags/geometry"	
	"log"
)

func main() {

	// some or all of these should be put in go-whosonfirst-spatials/flags/query.go
	// or equivalent (20201217/thisisaaronland)
	
	database_uri := flag.String("database-uri", "", "A valid whosonfirst/go-whosonfirst-spatial database URI.")
	properties_uri := flag.String("properties-uri", "", "A valid whosonfirst/go-whosonfirst-spatial properties reader URI.")
	latitude := flag.Float64("latitude", 0.0, "A valid latitude.")
	longitude := flag.Float64("longitude", 0.0, "A valid longitude.")

	geometries := flag.String("geometries", "all", "Valid options are: all, alt, default.")
	
	var props multi.MultiString
	flag.Var(&props, "properties", "One or more Who's On First properties to append to each result.")

	var pts multi.MultiString
	flag.Var(&pts, "placetype", "One or more place types to filter results by.")
	
	var alt_geoms multi.MultiString
	flag.Var(&alt_geoms, "alternate-geometry", "One or more alternate geometry labels (wof:alt_label) values to filter results by.")

	var is_current multi.MultiInt64
	flag.Var(&is_current, "is-current", "One or more existential flags (-1, 0, 1) to filter results by.")

	var is_ceased multi.MultiInt64
	flag.Var(&is_ceased, "is-ceased", "One or more existential flags (-1, 0, 1) to filter results by.")
	
	var is_deprecated multi.MultiInt64
	flag.Var(&is_deprecated, "is-deprecated", "One or more existential flags (-1, 0, 1) to filter results by.")
	
	var is_superseded multi.MultiInt64
	flag.Var(&is_superseded, "is-superseded", "One or more existential flags (-1, 0, 1) to filter results by.")
	
	var is_superseding multi.MultiInt64
	flag.Var(&is_superseding, "is-superseding", "One or more existential flags (-1, 0, 1) to filter results by.")	
	
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

	// START OF put me in a WithFlagSet(fs) function
	
	f, err := filter.NewSPRFilter()

	if err != nil {
		log.Fatalf("Failed to create SPR filter, %v", err)
	}

	switch *geometries {
	case "all":
		// pass
	case "alt", "alternate":

		af, err := geometry.NewIsAlternateGeometryFlag(true)

		if err != nil {
			log.Fatalf("Failed to create alternate geometry flag, %v", err)
		}

		f.AlternateGeometry = af
		
	case "default":

		af, err := geometry.NewIsAlternateGeometryFlag(false)

		if err != nil {
			log.Fatalf("Failed to create alternate geometry flag, %v", err)
		}

		f.AlternateGeometry = af
		
	default:
		log.Fatalf("Invalid -geometries flag")
	}
			
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

	if len(pts) > 0 {

		pt_flags := make([]flags.PlacetypeFlag, 0)

		for _, pt := range pts {
			
			fl, err := placetypes.NewPlacetypeFlag(pt)

			if err != nil {
				log.Fatalf("Failed to ... for '%s', %v", pt, err)
			}

			pt_flags = append(pt_flags, fl)
		}
		
		f.Placetypes = pt_flags
	}
	
	if len(is_current) > 0 {

		existential_flags := make([]flags.ExistentialFlag, 0)

		for _, v := range is_current {

			fl, err := existential.NewKnownUnknownFlag(v)
			
			if err != nil {
				log.Fatalf("Failed to ... for '%s', %v", v, err)
			}
			
			existential_flags = append(existential_flags, fl)
		}

		f.Current = existential_flags
	}

	if len(is_ceased) > 0 {

		existential_flags := make([]flags.ExistentialFlag, 0)

		for _, v := range is_ceased {

			fl, err := existential.NewKnownUnknownFlag(v)
			
			if err != nil {
				log.Fatalf("Failed to ... for '%s', %v", v, err)
			}
			
			existential_flags = append(existential_flags, fl)
		}

		f.Ceased = existential_flags
	}

	if len(is_deprecated) > 0 {

		existential_flags := make([]flags.ExistentialFlag, 0)

		for _, v := range is_deprecated {

			fl, err := existential.NewKnownUnknownFlag(v)
			
			if err != nil {
				log.Fatalf("Failed to ... for '%s', %v", v, err)
			}
			
			existential_flags = append(existential_flags, fl)
		}

		f.Deprecated = existential_flags
	}

	if len(is_superseded) > 0 {

		existential_flags := make([]flags.ExistentialFlag, 0)

		for _, v := range is_superseded {

			fl, err := existential.NewKnownUnknownFlag(v)
			
			if err != nil {
				log.Fatalf("Failed to ... for '%s', %v", v, err)
			}
			
			existential_flags = append(existential_flags, fl)
		}

		f.Superseded = existential_flags
	}
	
	if len(is_superseding) > 0 {

		existential_flags := make([]flags.ExistentialFlag, 0)

		for _, v := range is_superseding {

			fl, err := existential.NewKnownUnknownFlag(v)
			
			if err != nil {
				log.Fatalf("Failed to ... for '%s', %v", v, err)
			}
			
			existential_flags = append(existential_flags, fl)
		}

		f.Superseding = existential_flags
	}

	// END OF put me in a WithFlagSet(fs) function
	
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
