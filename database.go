package sqlite

// https://www.sqlite.org/rtree.html

// There is a bunch of code in here that could/should
// be reconciled with go-whosonfirst-spatial/database/rtree

import (
	"context"
	"errors"
	"fmt"
	gocache "github.com/patrickmn/go-cache"
	"github.com/skelterjohn/geom"
	wof_geojson "github.com/whosonfirst/go-whosonfirst-geojson-v2"
	wof_feature "github.com/whosonfirst/go-whosonfirst-geojson-v2/feature"
	"github.com/whosonfirst/go-whosonfirst-geojson-v2/geometry"
	"github.com/whosonfirst/go-whosonfirst-log"
	"github.com/whosonfirst/go-whosonfirst-spatial/cache"
	"github.com/whosonfirst/go-whosonfirst-spatial/database"
	"github.com/whosonfirst/go-whosonfirst-spatial/filter"
	"github.com/whosonfirst/go-whosonfirst-spatial/geojson"
	"github.com/whosonfirst/go-whosonfirst-spr"
	"github.com/whosonfirst/go-whosonfirst-sqlite"
	"github.com/whosonfirst/go-whosonfirst-sqlite-features/tables"
	sqlite_database "github.com/whosonfirst/go-whosonfirst-sqlite/database"
	"github.com/whosonfirst/go-whosonfirst-uri"
	// golog "log"
	"net/url"
	"strings"
	"sync"
	"time"
)

func init() {
	ctx := context.Background()
	database.RegisterSpatialDatabase(ctx, "sqlite", NewSQLiteSpatialDatabase)
}

type SQLiteSpatialDatabase struct {
	database.SpatialDatabase
	Logger        *log.WOFLogger
	mu            *sync.RWMutex
	db            *sqlite_database.SQLiteDatabase
	rtree_table   sqlite.Table
	geojson_table sqlite.Table
	gocache       *gocache.Cache
	dsn           string
	strict        bool
}

type RTreeSpatialIndex struct {
	bounds   geom.Rect
	Id       string
	IsAlt    bool
	AltLabel string
}

func (sp RTreeSpatialIndex) Bounds() geom.Rect {
	return sp.bounds
}

func (sp RTreeSpatialIndex) Path() string {

	if sp.IsAlt {
		return fmt.Sprintf("%s-alt-%s", sp.Id, sp.AltLabel)
	}

	return sp.Id
}

type SQLiteResults struct {
	spr.StandardPlacesResults `json:",omitempty"`
	Places                    []spr.StandardPlacesResult `json:"places"`
}

func (r *SQLiteResults) Results() []spr.StandardPlacesResult {
	return r.Places
}

func NewSQLiteSpatialDatabase(ctx context.Context, uri string) (database.SpatialDatabase, error) {

	u, err := url.Parse(uri)

	if err != nil {
		return nil, err
	}

	q := u.Query()

	dsn := q.Get("dsn")

	if dsn == "" {
		return nil, errors.New("Missing 'dsn' parameter")
	}

	sqlite_db, err := sqlite_database.NewDB(dsn)

	if err != nil {
		return nil, err
	}

	geojson_table, err := tables.NewGeoJSONTableWithDatabase(sqlite_db)

	if err != nil {
		return nil, err
	}

	rtree_table, err := tables.NewRTreeTableWithDatabase(sqlite_db)

	if err != nil {
		return nil, err
	}

	strict := true

	if q.Get("strict") == "false" {
		strict = false
	}

	logger := log.SimpleWOFLogger("index")

	expires := 5 * time.Minute
	cleanup := 30 * time.Minute

	gc := gocache.New(expires, cleanup)

	mu := new(sync.RWMutex)

	spatial_db := &SQLiteSpatialDatabase{
		Logger:        logger,
		db:            sqlite_db,
		rtree_table:   rtree_table,
		geojson_table: geojson_table,
		gocache:       gc,
		dsn:           dsn,
		strict:        strict,
		mu:            mu,
	}

	return spatial_db, nil
}

func (r *SQLiteSpatialDatabase) Close(ctx context.Context) error {
	return r.db.Close()
}

func (r *SQLiteSpatialDatabase) IndexFeature(ctx context.Context, f wof_geojson.Feature) error {

	err := r.setSPRCacheItem(ctx, f)

	if err != nil {
		return err
	}

	return r.rtree_table.IndexRecord(r.db, f)
}

func (r *SQLiteSpatialDatabase) PointInPolygon(ctx context.Context, coord *geom.Coord, filters ...filter.Filter) (spr.StandardPlacesResults, error) {

	ctx, cancel := context.WithCancel(ctx)
	defer cancel()

	rsp_ch := make(chan spr.StandardPlacesResult)
	err_ch := make(chan error)
	done_ch := make(chan bool)

	results := make([]spr.StandardPlacesResult, 0)
	working := true

	go r.PointInPolygonWithChannels(ctx, rsp_ch, err_ch, done_ch, coord, filters...)

	for {
		select {
		case <-ctx.Done():
			return nil, nil
		case <-done_ch:
			working = false
		case rsp := <-rsp_ch:
			results = append(results, rsp)
		case err := <-err_ch:
			return nil, err
		default:
			// pass
		}

		if !working {
			break
		}
	}

	spr_results := &SQLiteResults{
		Places: results,
	}

	return spr_results, nil
}

func (r *SQLiteSpatialDatabase) PointInPolygonWithChannels(ctx context.Context, rsp_ch chan spr.StandardPlacesResult, err_ch chan error, done_ch chan bool, coord *geom.Coord, filters ...filter.Filter) {

	defer func() {
		done_ch <- true
	}()

	rows, err := r.getIntersectsByCoord(ctx, coord)

	if err != nil {
		err_ch <- err
		return
	}

	r.inflateResultsWithChannels(ctx, rsp_ch, err_ch, rows, coord, filters...)
	return
}

func (r *SQLiteSpatialDatabase) PointInPolygonCandidates(ctx context.Context, coord *geom.Coord) (*geojson.GeoJSONFeatureCollection, error) {

	ctx, cancel := context.WithCancel(ctx)
	defer cancel()

	rsp_ch := make(chan geojson.GeoJSONFeature)
	err_ch := make(chan error)
	done_ch := make(chan bool)

	features := make([]geojson.GeoJSONFeature, 0)
	working := true

	go r.PointInPolygonCandidatesWithChannels(ctx, coord, rsp_ch, err_ch, done_ch)

	for {
		select {
		case <-ctx.Done():
			return nil, nil
		case <-done_ch:
			working = false
		case rsp := <-rsp_ch:
			features = append(features, rsp)
		case err := <-err_ch:
			return nil, err
		default:
			// pass
		}

		if !working {
			break
		}
	}

	fc := &geojson.GeoJSONFeatureCollection{
		Type:     "FeatureCollection",
		Features: features,
	}

	return fc, nil
}

func (r *SQLiteSpatialDatabase) PointInPolygonCandidatesWithChannels(ctx context.Context, coord *geom.Coord, rsp_ch chan geojson.GeoJSONFeature, err_ch chan error, done_ch chan bool) {

	defer func() {
		done_ch <- true
	}()

	intersects, err := r.getIntersectsByCoord(ctx, coord)

	if err != nil {
		err_ch <- err
		return
	}

	for _, sp := range intersects {

		str_id := sp.Id

		props := map[string]interface{}{
			"id": str_id,
		}

		b := sp.Bounds()

		swlon := b.Min.X
		swlat := b.Min.Y

		nelon := b.Max.X
		nelat := b.Max.Y

		sw := geojson.GeoJSONPoint{swlon, swlat}
		nw := geojson.GeoJSONPoint{swlon, nelat}
		ne := geojson.GeoJSONPoint{nelon, nelat}
		se := geojson.GeoJSONPoint{nelon, swlat}

		ring := geojson.GeoJSONRing{sw, nw, ne, se, sw}
		poly := geojson.GeoJSONPolygon{ring}
		multi := geojson.GeoJSONMultiPolygon{poly}

		geom := geojson.GeoJSONGeometry{
			Type:        "MultiPolygon",
			Coordinates: multi,
		}

		feature := geojson.GeoJSONFeature{
			Type:       "Feature",
			Properties: props,
			Geometry:   geom,
		}

		rsp_ch <- feature
	}

	return
}

func (r *SQLiteSpatialDatabase) getIntersectsByCoord(ctx context.Context, coord *geom.Coord) ([]*RTreeSpatialIndex, error) {

	// how small can this be?

	offset := geom.Coord{
		X: 0.00001,
		Y: 0.00001,
	}

	min := coord.Minus(offset)
	max := coord.Plus(offset)

	rect := &geom.Rect{
		Min: min,
		Max: max,
	}

	return r.getIntersectsByRect(ctx, rect)
}

func (r *SQLiteSpatialDatabase) getIntersectsByRect(ctx context.Context, rect *geom.Rect) ([]*RTreeSpatialIndex, error) {

	conn, err := r.db.Conn()

	if err != nil {
		return nil, err
	}

	q := fmt.Sprintf("SELECT id, wof_id, is_alt, alt_label, min_x, min_y, max_x, max_y FROM %s  WHERE min_x <= ? AND max_x >= ?  AND min_y <= ? AND max_y >= ?", r.rtree_table.Name())

	// golog.Println(q, rect.Min.X, rect.Max.X, rect.Min.Y, rect.Max.Y)

	rows, err := conn.QueryContext(ctx, q, rect.Min.X, rect.Max.X, rect.Min.Y, rect.Max.Y)

	if err != nil {
		return nil, err
	}

	defer rows.Close()

	intersects := make([]*RTreeSpatialIndex, 0)

	for rows.Next() {

		var id string
		var wof_id string
		var is_alt int32
		var alt_label string
		var minx float64
		var miny float64
		var maxx float64
		var maxy float64

		err := rows.Scan(&id, &wof_id, &is_alt, &alt_label, &minx, &miny, &maxx, &maxy)

		if err != nil {
			return nil, err
		}

		min := geom.Coord{
			X: minx,
			Y: miny,
		}

		max := geom.Coord{
			X: maxx,
			Y: maxy,
		}

		rect := geom.Rect{
			Min: min,
			Max: max,
		}

		i := &RTreeSpatialIndex{
			Id:     wof_id,
			bounds: rect,
		}

		if is_alt == 1 {
			i.IsAlt = true
			i.AltLabel = alt_label
		}

		intersects = append(intersects, i)
	}

	return intersects, nil
}

func (r *SQLiteSpatialDatabase) inflateResultsWithChannels(ctx context.Context, rsp_ch chan spr.StandardPlacesResult, err_ch chan error, possible []*RTreeSpatialIndex, c *geom.Coord, filters ...filter.Filter) {

	seen := make(map[string]bool)

	mu := new(sync.RWMutex)
	wg := new(sync.WaitGroup)

	for _, sp := range possible {

		wg.Add(1)

		go func(sp *RTreeSpatialIndex) {

			defer wg.Done()

			select {
			case <-ctx.Done():
				return
			default:
				// pass
			}

			str_id := fmt.Sprintf("%s:%s", sp.Id, sp.AltLabel)

			mu.RLock()
			_, ok := seen[str_id]
			mu.RUnlock()

			if ok {
				return
			}

			mu.Lock()
			seen[str_id] = true
			mu.Unlock()

			fc, err := r.retrieveSPRCacheItem(ctx, sp.Path())

			if err != nil {
				r.Logger.Error("Failed to retrieve feature cache for %s, %v", str_id, err)
				return
			}

			s := fc.SPR()

			for _, f := range filters {

				err = filter.FilterSPR(f, s)

				if err != nil {
					r.Logger.Debug("SKIP %s because filter error %s", str_id, err)
					return
				}
			}

			p := fc.Polygons()

			contains, err := geometry.PolygonsContainsCoord(p, *c)

			if err != nil {
				r.Logger.Error("failed to calculate intersection for %s, because %s", str_id, err)
				return
			}

			if !contains {
				r.Logger.Debug("SKIP %s because does not contain coord (%v)", str_id, c)
				return
			}

			rsp_ch <- s
		}(sp)
	}

	wg.Wait()
}

func (db *SQLiteSpatialDatabase) StandardPlacesResultsToFeatureCollection(ctx context.Context, results spr.StandardPlacesResults) (*geojson.GeoJSONFeatureCollection, error) {

	features := make([]geojson.GeoJSONFeature, 0)

	for _, r := range results.Results() {

		select {
		case <-ctx.Done():
			return nil, nil
		default:
			// pass
		}

		fc, err := db.retrieveSPRCacheItem(ctx, r.Path())

		if err != nil {
			return nil, err
		}

		f := geojson.GeoJSONFeature{
			Type:       "Feature",
			Properties: fc.SPR(),
			Geometry:   fc.Geometry(),
		}

		features = append(features, f)
	}

	pg := geojson.Pagination{
		TotalCount: len(features),
		Page:       1,
		PerPage:    len(features),
		PageCount:  1,
	}

	collection := geojson.GeoJSONFeatureCollection{
		Type:       "FeatureCollection",
		Features:   features,
		Pagination: pg,
	}

	return &collection, nil
}

func (r *SQLiteSpatialDatabase) setSPRCacheItem(ctx context.Context, f wof_geojson.Feature) error {

	return r.geojson_table.IndexRecord(r.db, f)
}

func (r *SQLiteSpatialDatabase) retrieveSPRCacheItem(ctx context.Context, uri_str string) (*cache.SPRCacheItem, error) {

	c, ok := r.gocache.Get(uri_str)

	if ok {
		return c.(*cache.SPRCacheItem), nil
	}

	id, uri_args, err := uri.ParseURI(uri_str)

	if err != nil {
		return nil, err
	}

	conn, err := r.db.Conn()

	if err != nil {
		return nil, err
	}

	args := []interface{}{
		id,
	}

	q := fmt.Sprintf("SELECT body FROM %s WHERE id = ?", r.geojson_table.Name())

	if uri_args.IsAlternate {

		source, err := uri_args.AltGeom.String()

		if err != nil {
			return nil, err
		}

		q = fmt.Sprintf("%s AND is_alt=1 AND source = ?", q)
		args = append(args, source)
	}

	row := conn.QueryRowContext(ctx, q, args...)

	var body string

	err = row.Scan(&body)

	if err != nil {
		return nil, err
	}

	feature_r := strings.NewReader(body)

	f, err := wof_feature.LoadFeatureFromReader(feature_r)

	if err != nil {
		return nil, err
	}

	cache_item, err := cache.NewSPRCacheItem(f)

	if err != nil {
		return nil, err
	}

	r.gocache.Set(uri_str, cache_item, -1)

	return cache_item.(*cache.SPRCacheItem), nil
}
