package sqlite

import (
	"context"
	"fmt"
	"io"
	"os"
	"strconv"
	"testing"

	_ "github.com/mattn/go-sqlite3"

	"github.com/paulmach/orb/geojson"
	"github.com/whosonfirst/go-whosonfirst-spatial/database"
	"github.com/whosonfirst/go-whosonfirst-spatial/filter"
	"github.com/whosonfirst/go-whosonfirst-spatial/geo"
)

// 1360521545

func TestIntersectsQuery(t *testing.T) {

	ctx := context.Background()

	database_uri := "sqlite://sqlite3?dsn=fixtures/sfomuseum-architecture.db"

	db, err := database.NewSpatialDatabase(ctx, database_uri)

	if err != nil {
		t.Fatalf("Failed to create new spatial database, %v", err)
	}

	defer db.Close(ctx)

	t2 := int64(1360521545)

	r, err := db.Read(ctx, strconv.FormatInt(t2, 10))

	if err != nil {
		t.Fatalf("Failed to read data for %d, %v", t2, err)
	}

	defer r.Close()

	body, err := io.ReadAll(r)

	if err != nil {
		t.Fatalf("Failed to read record for %d, %v", t2, err)
	}

	f, err := geojson.UnmarshalFeature(body)

	if err != nil {
		t.Fatalf("Failed to unmarshal feature for %d, %v", t2, 344)
	}

	geom := f.Geometry

	rsp, err := db.Intersects(ctx, geom)

	if err != nil {
		t.Fatalf("Failed to perform intersects query, %v", err)
	}

	results := rsp.Results()
	count := len(results)

	expected := 24

	if count != expected {
		t.Fatalf("Invalid count for intersects (%d), expected %d", count, expected)
	}

	/*
		slog.Info("Results", "count", count)

		for _, r := range results {
			slog.Info("R", "id", r.Id())
		}
	*/
}

func TestPointInPolygonQuery(t *testing.T) {

	ctx := context.Background()

	database_uri := "sqlite://sqlite3?dsn=fixtures/sfomuseum-architecture.db"

	expected := int64(1947304591) // This test may fail if sfomuseum-data/sfomuseum-data-architecture is updated and there is a "newer" T2

	lat := 37.616951
	lon := -122.383747

	db, err := database.NewSpatialDatabase(ctx, database_uri)

	if err != nil {
		t.Fatalf("Failed to create new spatial database, %v", err)
	}

	defer db.Close(ctx)

	c, err := geo.NewCoordinate(lon, lat)

	if err != nil {
		t.Fatalf("Failed to create new coordinate, %v", err)
	}

	i, err := filter.NewSPRInputs()

	if err != nil {
		t.Fatalf("Failed to create SPR inputs, %v", err)
	}

	i.IsCurrent = []int64{1}
	i.Placetypes = []string{"wing"}

	f, err := filter.NewSPRFilterFromInputs(i)

	if err != nil {
		t.Fatalf("Failed to create SPR filter from inputs, %v", err)
	}

	for i := 0; i < 50; i++ {
		spr, err := db.PointInPolygon(ctx, c, f)

		if err != nil {
			t.Fatalf("Failed to perform point in polygon query, %v", err)
		}

		results := spr.Results()
		count := len(results)

		if count != 1 {
			t.Fatalf("Expected 1 result but got %d", count)
		}

		first := results[0]

		if first.Id() != strconv.FormatInt(expected, 10) {

			t.Fatalf("Expected %d but got %s", expected, first.Id())
		}

	}
}

func TestRemoveFeature(t *testing.T) {

	ctx := context.Background()

	database_uri := "sqlite://sqlite3?dsn=:memory:"

	db, err := database.NewSpatialDatabase(ctx, database_uri)

	if err != nil {
		t.Fatalf("Failed to create new spatial database, %v", err)
	}

	defer db.Close(ctx)

	id := 101737491
	lat := 46.852675
	lon := -71.330873

	test_data := fmt.Sprintf("fixtures/%d.geojson", id)

	fh, err := os.Open(test_data)

	if err != nil {
		t.Fatalf("Failed to open %s, %v", test_data, err)
	}

	defer fh.Close()

	body, err := io.ReadAll(fh)

	if err != nil {
		t.Fatalf("Failed to read %s, %v", test_data, err)
	}

	err = db.IndexFeature(ctx, body)

	if err != nil {
		t.Fatalf("Failed to index %s, %v", test_data, err)
	}

	c, err := geo.NewCoordinate(lon, lat)

	if err != nil {
		t.Fatalf("Failed to create new coordinate, %v", err)
	}

	spr, err := db.PointInPolygon(ctx, c)

	if err != nil {
		t.Fatalf("Failed to perform point in polygon query, %v", err)
	}

	results := spr.Results()
	count := len(results)

	if count != 1 {
		t.Fatalf("Expected 1 result but got %d", count)
	}

	err = db.RemoveFeature(ctx, "101737491")

	if err != nil {
		t.Fatalf("Failed to remove %s, %v", test_data, err)
	}

	spr, err = db.PointInPolygon(ctx, c)

	if err != nil {
		t.Fatalf("Failed to perform point in polygon query, %v", err)
	}

	results = spr.Results()
	count = len(results)

	if count != 0 {
		t.Fatalf("Expected 0 results but got %d", count)
	}
}
