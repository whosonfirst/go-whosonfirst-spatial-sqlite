package main

import (
	"context"
	"encoding/json"
	"fmt"
	"github.com/aws/aws-lambda-go/lambda"
	"github.com/sfomuseum/go-flags/flagset"
	_ "github.com/whosonfirst/go-whosonfirst-spatial-sqlite"
	"github.com/whosonfirst/go-whosonfirst-spatial/database"
	"github.com/whosonfirst/go-whosonfirst-spatial/filter"
	"github.com/whosonfirst/go-whosonfirst-spatial/flags"
	"github.com/whosonfirst/go-whosonfirst-spatial/geo"
	"github.com/whosonfirst/go-whosonfirst-spatial/properties"
	"github.com/whosonfirst/go-whosonfirst-spr"
	"log"
	"net/url"
	"strconv"
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

		// TODO: move this in to go-whosonfirst-spatial
		// maybe an /aws/events or aws/lambda directory
		// TBD: consider whether this is just aws specific
		// or the ground work for a general purpose API
		// (20210305/thisisaaronland)

		type PointInPolygonEvent struct {
			Latitude            float64  `json:"latitude"`
			Longitude           float64  `json:"longitude"`
			Properties          []string `json:"properties"`
			Placetypes          []string `json:"placetypes,omitempty"`
			Geometries          string   `json:"geometries,omitempty"`
			AlternateGeometries []string `json:"alternate_geometries,omitempty"`
			IsCurrent           []int    `json:"is_current,omitempty"`
			IsCeased            []int    `json:"is_ceased,omitempty"`
			IsDeprecated        []int    `json:"is_deprecated,omitempty"`
			IsSuperseded        []int    `json:"is_superseded,omitempty"`
			IsSuperseding       []int    `json:"is_superseding,omitempty"`
		}

		handler := func(ctx context.Context, ev PointInPolygonEvent) (interface{}, error) {

			// TODO: move this in to go-whosonfirst-spatial
			// as in a filter.NewSPRFilterFromPointInPolygonEvent
			// method (20210305/thisisaaronland)

			q := url.Values{}
			q.Set("geometries", ev.Geometries)

			for _, v := range ev.AlternateGeometries {
				q.Add("alternate_geometry", v)
			}

			for _, v := range ev.Placetypes {
				q.Add("placetype", v)
			}

			for _, v := range ev.IsCurrent {
				q.Add("is_current", strconv.Itoa(v))
			}

			for _, v := range ev.IsCeased {
				q.Add("is_ceased", strconv.Itoa(v))
			}

			for _, v := range ev.IsDeprecated {
				q.Add("is_deprecated", strconv.Itoa(v))
			}

			for _, v := range ev.IsSuperseded {
				q.Add("is_superseded", strconv.Itoa(v))
			}

			for _, v := range ev.IsSuperseding {
				q.Add("is_superseding", strconv.Itoa(v))
			}

			f, err := filter.NewSPRFilterFromQuery(q)

			if err != nil {
				return nil, err
			}

			return query(ctx, ev.Latitude, ev.Longitude, f, ev.Properties...)
		}

		lambda.Start(handler)

	default:
		log.Fatalf("Invalid or unsupported mode '%s'", *mode)
	}
}
