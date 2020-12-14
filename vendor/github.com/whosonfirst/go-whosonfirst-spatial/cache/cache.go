package cache

import (
	"github.com/whosonfirst/go-whosonfirst-spatial/geojson"
	"github.com/whosonfirst/go-whosonfirst-spr"
)

type CacheItem interface {
	SPR() spr.StandardPlacesResult
	Geometry() geojson.GeoJSONGeometry
}
