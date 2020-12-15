package sqlite

import (
	"github.com/paulmach/go.geojson"
	"github.com/whosonfirst/go-whosonfirst-spr"
	"github.com/whosonfirst/go-whosonfirst-spatial/cache"	
)

type SQLiteCacheItem struct {
	cache.CacheItem `json:",omitempty"`
	spr spr.StandardPlacesResult
	geometry *geojson.Geometry
}

func (c *SQLiteCacheItem) SPR() (spr.StandardPlacesResult, error) {
	return c.spr, nil
}

func (c *SQLiteCacheItem) Geometry() (*geojson.Geometry, error) {
	return c.geometry, nil
}
