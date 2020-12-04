package sqlite

// https://www.sqlite.org/rtree.html

import (
	"context"
	"database/sql"
	"errors"
	_ "github.com/mattn/go-sqlite3"
	gocache "github.com/patrickmn/go-cache"
	"github.com/skelterjohn/geom"
	wof_geojson "github.com/whosonfirst/go-whosonfirst-geojson-v2"
	_ "github.com/whosonfirst/go-whosonfirst-geojson-v2/geometry"
	"github.com/whosonfirst/go-whosonfirst-log"
	"github.com/whosonfirst/go-whosonfirst-spatial/cache"
	"github.com/whosonfirst/go-whosonfirst-spatial/database"
	"github.com/whosonfirst/go-whosonfirst-spatial/filter"
	"github.com/whosonfirst/go-whosonfirst-spatial/geojson"
	"github.com/whosonfirst/go-whosonfirst-spr"
	golog "log"
	"net/url"
	"strconv"
	"sync"
	"time"
)

func init() {
	ctx := context.Background()
	database.RegisterSpatialDatabase(ctx, "rtree", NewSQLiteSpatialDatabase)
}

// PLEASE DISCUSS WHY patrickm/go-cache AND NOT whosonfirst/go-cache HERE

type SQLiteSpatialDatabase struct {
	database.SpatialDatabase
	Logger  *log.WOFLogger
	gocache *gocache.Cache
	mu      *sync.RWMutex
	conn    *sql.DB
	dsn     string
	strict  bool
}

type RTreeSpatialIndex struct {
	bounds geom.Rect
	Id     string
}

func (sp RTreeSpatialIndex) Bounds() geom.Rect {
	return sp.bounds
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

	conn, err := sql.Open("sqlite3", dsn)

	if err != nil {
		return nil, err
	}

	strict := true

	if q.Get("strict") == "false" {
		strict = false
	}

	expires := 0 * time.Second
	cleanup := 0 * time.Second

	str_exp := q.Get("default_expiration")
	str_cleanup := q.Get("cleanup_interval")

	if str_exp != "" {

		int_expires, err := strconv.Atoi(str_exp)

		if err != nil {
			return nil, err
		}

		expires = time.Duration(int_expires) * time.Second
	}

	if str_cleanup != "" {

		int_cleanup, err := strconv.Atoi(str_cleanup)

		if err != nil {
			return nil, err
		}

		cleanup = time.Duration(int_cleanup) * time.Second
	}

	gc := gocache.New(expires, cleanup)

	logger := log.SimpleWOFLogger("index")

	mu := new(sync.RWMutex)

	db := &SQLiteSpatialDatabase{
		Logger:  logger,
		conn:    conn,
		dsn:     dsn,
		gocache: gc,
		strict:  strict,
		mu:      mu,
	}

	return db, nil
}

func (r *SQLiteSpatialDatabase) Close(ctx context.Context) error {

	return nil
}

func (r *SQLiteSpatialDatabase) IndexFeature(ctx context.Context, f wof_geojson.Feature) error {

	// str_id := f.Id()

	bboxes, err := f.BoundingBoxes()

	if err != nil {
		return err
	}

	err = r.setSPRCacheItem(ctx, f)

	if err != nil {
		return err
	}

	for _, bbox := range bboxes.Bounds() {

		sw := bbox.Min
		ne := bbox.Max

		llat := ne.Y - sw.Y
		llon := ne.X - sw.X

		/*

INSERT INTO demo_index VALUES(
    2,                   -- NC 12th Congressional District in 2010
    -81.0, -79.6,
    35.0, 36.2
);

		*/
		
		golog.Println("INDEX", llat, llon)
	}

	return nil
}

func (r *SQLiteSpatialDatabase) PointInPolygon(ctx context.Context, coord *geom.Coord, filters filter.Filter) (spr.StandardPlacesResults, error) {

	ctx, cancel := context.WithCancel(ctx)
	defer cancel()

	rsp_ch := make(chan spr.StandardPlacesResult)
	err_ch := make(chan error)
	done_ch := make(chan bool)

	results := make([]spr.StandardPlacesResult, 0)
	working := true

	go r.PointInPolygonWithChannels(ctx, coord, filters, rsp_ch, err_ch, done_ch)

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

func (r *SQLiteSpatialDatabase) PointInPolygonWithChannels(ctx context.Context, coord *geom.Coord, filters filter.Filter, rsp_ch chan spr.StandardPlacesResult, err_ch chan error, done_ch chan bool) {

	defer func() {
		done_ch <- true
	}()

	rows, err := r.getIntersectsByCoord(coord)

	if err != nil {
		err_ch <- err
		return
	}

	r.inflateResultsWithChannels(ctx, coord, filters, rows, rsp_ch, err_ch)
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

	intersects, err := r.getIntersectsByCoord(coord)

	if err != nil {
		err_ch <- err
		return
	}

	for _, raw := range intersects {

		sp := raw.(*RTreeSpatialIndex)
		str_id := sp.Id

		props := map[string]interface{}{
			"id": str_id,
		}

		b := sp.Bounds()

		swlon := b.PointCoord(0)
		swlat := b.PointCoord(1)

		nelon := swlon + b.LengthsCoord(0)
		nelat := swlat + b.LengthsCoord(1)

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

func (r *SQLiteSpatialDatabase) getIntersectsByCoord(coord *geom.Coord) ([]*RTreeSpatialIndex, error) {

	return nil, errors.New("Not implemented")

	/*
	lat := coord.Y
	lon := coord.X

	pt := rtreego.Point{lon, lat}

	rect, err := rtreego.NewRect(pt, []float64{0.0001, 0.0001}) // how small can I make this?

	if err != nil {
		return nil, err
	}

	return r.getIntersectsByRect(rect)
	*/
}

func (r *SQLiteSpatialDatabase) getIntersectsByRect(rect geom.Rect) ([]*RTreeSpatialIndex, error) {

	
	/*

	SELECT id FROM demo_index
 WHERE maxX>=-81.08 AND minX<=-80.58
   AND maxY>=35.00  AND minY<=35.44;

	*/

	return nil, errors.New("Not implemented")
}

func (r *SQLiteSpatialDatabase) inflateResultsWithChannels(ctx context.Context, c *geom.Coord, f filter.Filter, possible []*RTreeSpatialIndex, rsp_ch chan spr.StandardPlacesResult, err_ch chan error) {

	seen := make(map[string]bool)

	mu := new(sync.RWMutex)
	wg := new(sync.WaitGroup)

	for _, row := range possible {

		sp := row.(*RTreeSpatialIndex)
		wg.Add(1)

		go func(sp *RTreeSpatialIndex) {

			defer wg.Done()

			select {
			case <-ctx.Done():
				return
			default:
				// pass
			}

			str_id := sp.Id

			mu.RLock()
			_, ok := seen[str_id]
			mu.RUnlock()

			if ok {
				return
			}

			mu.Lock()
			seen[str_id] = true
			mu.Unlock()

			fc, err := r.retrieveSPRCacheItem(ctx, str_id)

			if err != nil {
				r.Logger.Error("Failed to retrieve feature cache for %s, %v", str_id, err)
				return
			}

			s := fc.SPR()

			err = filter.FilterSPR(f, s)

			if err != nil {
				r.Logger.Debug("SKIP %s because filter error %s", str_id, err)
				return
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

		fc, err := db.retrieveSPRCacheItem(ctx, r.Id())

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

	fc, err := cache.NewSPRCacheItem(f)

	if err != nil {
		return err
	}

	r.gocache.Set(f.Id(), fc, -1)
	return nil
}

func (r *SQLiteSpatialDatabase) retrieveSPRCacheItem(ctx context.Context, str_id string) (*cache.SPRCacheItem, error) {

	fc, ok := r.gocache.Get(str_id)

	if !ok {
		return nil, errors.New("Invalid cache ID")
	}

	return fc.(*cache.SPRCacheItem), nil
}
