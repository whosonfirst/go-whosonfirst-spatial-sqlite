package main

import (
	"context"
	"encoding/json"
	"fmt"
	"github.com/aws/aws-lambda-go/lambda"
	"github.com/sfomuseum/go-flags/flagset"
	_ "github.com/whosonfirst/go-whosonfirst-spatial-sqlite"
	"github.com/whosonfirst/go-whosonfirst-spatial/api"
	"github.com/whosonfirst/go-whosonfirst-spatial/database"
	"github.com/whosonfirst/go-whosonfirst-spatial/filter"
	"github.com/whosonfirst/go-whosonfirst-spatial/flags"
	"github.com/whosonfirst/go-whosonfirst-spatial/geo"
	"github.com/whosonfirst/go-whosonfirst-spatial/properties"
	"github.com/whosonfirst/go-whosonfirst-spr"
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

	mode := fs.String("mode", "cli", "...")

	flags.Parse(fs)

	err = flagset.SetFlagsFromEnvVars(fs, "PIP")

	if err != nil {
		log.Fatalf("Failed to assign flags from environment variables, %v", err)
	}

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

	// props, _ := flags.MultiStringVar(fs, "properties")

	ctx := context.Background()
	db, err := database.NewSpatialDatabase(ctx, database_uri)

	if err != nil {
		log.Fatalf("Failed to create database for '%s', %v", database_uri, err)
	}

	// This is the meat of it which we're putting in its own function that
	// can be invoked in both a CLI and a Lambda context

	// TBD - update this to expect a *api.PointInPolygonRequest ?

	query := func(ctx context.Context, latitude float64, longitude float64, f filter.Filter, props ...string) (interface{}, error) {

		c, err := geo.NewCoordinate(longitude, latitude)

		if err != nil {
			return nil, fmt.Errorf("Failed to create new coordinate, %v", err)
		}

		var rsp interface{}

		r, err := db.PointInPolygon(ctx, c, f)

		if err != nil {
			return nil, fmt.Errorf("Failed to query database with coord %v, %v", c, err)
		}

		rsp = r

		if len(props) > 0 {

			pr, err := properties.NewPropertiesReader(ctx, properties_uri)

			if err != nil {
				return nil, fmt.Errorf("Failed to create properties reader, %v", err)
			}

			r, err := pr.PropertiesResponseResultsWithStandardPlacesResults(ctx, rsp.(spr.StandardPlacesResults), props)

			if err != nil {
				return nil, fmt.Errorf("Failed to generate properties response, %v", err)
			}

			rsp = r
		}

		return r, nil
	}

	switch *mode {

	case "cli":

		latitude, _ := flags.Float64Var(fs, "latitude")
		longitude, _ := flags.Float64Var(fs, "longitude")

		props, _ := flags.MultiStringVar(fs, "properties")

		f, err := flags.NewSPRFilterFromFlagSet(fs)

		if err != nil {
			log.Fatalf("Failed to create SPR filter, %v", err)
		}

		rsp, err := query(ctx, latitude, longitude, f, props...)

		if err != nil {
			log.Fatalf("Failed to query, %v", err)
		}

		enc, err := json.Marshal(rsp)

		if err != nil {
			log.Fatalf("Failed to marshal results, %v", err)
		}

		fmt.Println(string(enc))

	case "lambda":

		handler := func(ctx context.Context, req *api.PointInPolygonRequest) (interface{}, error) {

			f, err := api.NewSPRFilterFromPointInPolygonRequest(req)

			if err != nil {
				return nil, err
			}

			return query(ctx, req.Latitude, req.Longitude, f, req.Properties...)
		}

		lambda.Start(handler)

	default:
		log.Fatalf("Invalid or unsupported mode '%s'", *mode)
	}
}
