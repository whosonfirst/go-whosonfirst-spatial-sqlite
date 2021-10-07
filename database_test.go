package sqlite

import (
	"context"
	_ "fmt"
	"github.com/whosonfirst/go-whosonfirst-spatial/database"
	"github.com/whosonfirst/go-whosonfirst-spatial/filter"
	"github.com/whosonfirst/go-whosonfirst-spatial/geo"
	"strconv"
	"testing"
)

func TestSpatialDatabase(t *testing.T) {

	ctx := context.Background()

	database_uri := "sqlite://?dsn=fixtures/sfomuseum-architecture.db"

	expected := int64(1745882085) // This test may fail if sfomuseum-data/sfomuseum-data-architecture is updated and there is a "newer" T2

	lat := 37.616951
	lon := -122.383747

	db, err := database.NewSpatialDatabase(ctx, database_uri)

	if err != nil {
		t.Fatalf("Failed to create new spatial database, %v", err)
	}

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
