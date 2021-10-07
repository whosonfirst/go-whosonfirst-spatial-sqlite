package sqlite

import (
	"context"
	"testing"
	"github.com/whosonfirst/go-whosonfirst-spatial/database"
	"github.com/whosonfirst/go-whosonfirst-spatial/geo"
	"fmt"
)

func TestSpatialDatabase(t *testing.T) {

	ctx := context.Background()
	
	database_uri := "sqlite://?dsn=fixtures/sfomuseum-architecture.db"

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
	
	r, err := db.PointInPolygon(ctx, c)
	
	if err != nil {
		t.Fatalf("Failed to perform point in polygon query, %v", err)
	}

	fmt.Println(r)
}
