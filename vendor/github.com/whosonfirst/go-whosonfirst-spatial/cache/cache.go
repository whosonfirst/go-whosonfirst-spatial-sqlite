package cache

import (
	wof_geojson "github.com/whosonfirst/go-whosonfirst-geojson-v2"
	"github.com/whosonfirst/go-whosonfirst-spatial/geojson"
	"github.com/whosonfirst/go-whosonfirst-spr"
)

type CacheItem interface {
	SPR() spr.StandardPlacesResult
	Geometry() geojson.GeoJSONGeometry
}
