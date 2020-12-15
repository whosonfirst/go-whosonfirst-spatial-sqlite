package sqlite

// https://www.sqlite.org/rtree.html

// There is a bunch of code in here that could/should
// be reconciled with go-whosonfirst-spatial/database/rtree

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	gocache "github.com/patrickmn/go-cache"
	"github.com/paulmach/go.geojson"
	"github.com/skelterjohn/geom"
	wof_geojson "github.com/whosonfirst/go-whosonfirst-geojson-v2"
	// wof_feature "github.com/whosonfirst/go-whosonfirst-geojson-v2/feature"
	"github.com/whosonfirst/go-whosonfirst-log"
	// "github.com/whosonfirst/go-whosonfirst-spatial/cache"
	"github.com/whosonfirst/go-whosonfirst-spatial/database"
	"github.com/whosonfirst/go-whosonfirst-spatial/filter"
	"github.com/whosonfirst/go-whosonfirst-spatial/geo"
	"github.com/whosonfirst/go-whosonfirst-spr"
	"github.com/whosonfirst/go-whosonfirst-sqlite"
	"github.com/whosonfirst/go-whosonfirst-sqlite-features/tables"
	sqlite_database "github.com/whosonfirst/go-whosonfirst-sqlite/database"
	"github.com/whosonfirst/go-whosonfirst-uri"
	golog "log"
	"net/url"
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
	spr_table     sqlite.Table
	gocache       *gocache.Cache
	dsn           string
	strict        bool
}

type RTreeSpatialIndex struct {
	geometry string
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

	spr_table, err := tables.NewSPRTableWithDatabase(sqlite_db)

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
		spr_table:     spr_table,
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

	t1 := time.Now()

	defer func() {
		golog.Printf("Time to point in polygon, %v\n", time.Since(t1))
	}()

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

func (r *SQLiteSpatialDatabase) PointInPolygonCandidates(ctx context.Context, coord *geom.Coord) (*geojson.FeatureCollection, error) {

	ctx, cancel := context.WithCancel(ctx)
	defer cancel()

	rsp_ch := make(chan *geojson.Feature)
	err_ch := make(chan error)
	done_ch := make(chan bool)

	features := make([]*geojson.Feature, 0)
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

	fc := &geojson.FeatureCollection{
		Type:     "FeatureCollection",
		Features: features,
	}

	return fc, nil
}

func (r *SQLiteSpatialDatabase) PointInPolygonCandidatesWithChannels(ctx context.Context, coord *geom.Coord, rsp_ch chan *geojson.Feature, err_ch chan error, done_ch chan bool) {

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

		sw := []float64{swlon, swlat}
		nw := []float64{swlon, nelat}
		ne := []float64{nelon, nelat}
		se := []float64{nelon, swlat}

		ring := [][]float64{
			sw, nw, ne, se, sw,
		}

		poly := [][][]float64{
			ring,
		}

		geom := geojson.NewPolygonGeometry(poly)

		feature := &geojson.Feature{
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

	/*
		t1 := time.Now()

		defer func() {
			golog.Printf("Time to get intersects by rect, %v\n", time.Since(t1))
		}()
	*/

	conn, err := r.db.Conn()

	if err != nil {
		return nil, err
	}

	q := fmt.Sprintf("SELECT id, wof_id, is_alt, alt_label, geometry, min_x, min_y, max_x, max_y FROM %s  WHERE min_x <= ? AND max_x >= ?  AND min_y <= ? AND max_y >= ?", r.rtree_table.Name())

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
		var geometry string
		var minx float64
		var miny float64
		var maxx float64
		var maxy float64

		err := rows.Scan(&id, &wof_id, &is_alt, &alt_label, &geometry, &minx, &miny, &maxx, &maxy)

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
			Id:       wof_id,
			bounds:   rect,
			geometry: geometry,
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

	/*
		t1 := time.Now()

		defer func() {
			golog.Printf("Time to inflate results, %v\n", time.Since(t1))
		}()
	*/

	seen := make(map[string]bool)
	mu := new(sync.RWMutex)

	wg := new(sync.WaitGroup)

	for _, sp := range possible {

		wg.Add(1)

		go func(sp *RTreeSpatialIndex) {
			defer wg.Done()
			r.inflateSpatialIndexWithChannels(ctx, rsp_ch, err_ch, seen, mu, sp, c, filters...)
		}(sp)
	}

	wg.Wait()
}

func (r *SQLiteSpatialDatabase) inflateSpatialIndexWithChannels(ctx context.Context, rsp_ch chan spr.StandardPlacesResult, err_ch chan error, seen map[string]bool, mu *sync.RWMutex, sp *RTreeSpatialIndex, c *geom.Coord, filters ...filter.Filter) {

	select {
	case <-ctx.Done():
		return
	default:
		// pass
	}

	str_id := fmt.Sprintf("%s:%s", sp.Id, sp.AltLabel)

	// have we already looked up the filters for this ID?
	// see notes below

	mu.RLock()
	_, ok := seen[str_id]
	mu.RUnlock()

	if ok {
		return
	}

	var coords [][][]float64

	err := json.Unmarshal([]byte(sp.geometry), &coords)

	if err != nil {
		err_ch <- err
		return
	}

	if len(coords) == 0 {
		err_ch <- errors.New("Missing coordinates for polygon")
		return
	}

	if !geo.GeoJSONPolygonContainsCoord(coords, c) {
		return
	}

	// there is at least one ring that contains the coord
	// now we check the filters - whether or not they pass
	// we can skip every subsequent polygon with the same
	// ID

	mu.Lock()
	seen[str_id] = true
	mu.Unlock()

	fc, err := r.retrieveSPRCacheItem(ctx, sp.Path())

	if err != nil {
		r.Logger.Error("Failed to retrieve feature cache for %s, %v", str_id, err)
		return
	}

	s, err := fc.SPR()

	if err != nil {
		r.Logger.Error("Failed to retrieve feature cache for %s, %v", str_id, err)
		return
	}

	for _, f := range filters {

		err = filter.FilterSPR(f, s)

		if err != nil {
			r.Logger.Debug("SKIP %s because filter error %s", str_id, err)
			return
		}
	}

	rsp_ch <- s
}

func (db *SQLiteSpatialDatabase) StandardPlacesResultsToFeatureCollection(ctx context.Context, results spr.StandardPlacesResults) (*geojson.FeatureCollection, error) {

	features := make([]*geojson.Feature, 0)

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

		spr, err := fc.SPR()

		if err != nil {
			return nil, err
		}

		spr_enc, err := json.Marshal(spr)

		if err != nil {
			return nil, err
		}

		var spr_map map[string]interface{}

		err = json.Unmarshal(spr_enc, &spr_map)

		if err != nil {
			return nil, err
		}

		geom, err := fc.Geometry()

		if err != nil {
			return nil, err
		}

		f := &geojson.Feature{
			Type:       "Feature",
			Properties: spr_map,
			Geometry:   geom,
		}

		features = append(features, f)
	}

	/*
		pg := geojson.Pagination{
			TotalCount: len(features),
			Page:       1,
			PerPage:    len(features),
			PageCount:  1,
		}
	*/

	collection := geojson.FeatureCollection{
		Type:     "FeatureCollection",
		Features: features,
		// Pagination: pg,
	}

	return &collection, nil
}

func (r *SQLiteSpatialDatabase) setSPRCacheItem(ctx context.Context, f wof_geojson.Feature) error {
	return r.geojson_table.IndexRecord(r.db, f)
}

// TBD - retrieve from "spr" table?

func (r *SQLiteSpatialDatabase) retrieveSPRCacheItem(ctx context.Context, uri_str string) (*SQLiteCacheItem, error) {

	c, ok := r.gocache.Get(uri_str)

	if ok {
		return c.(*SQLiteCacheItem), nil
	}

	id, uri_args, err := uri.ParseURI(uri_str)

	if err != nil {
		return nil, err
	}

	conn, err := r.db.Conn()

	if err != nil {
		return nil, err
	}

	alt_label := ""

	if uri_args.IsAlternate {

		source, err := uri_args.AltGeom.String()

		if err != nil {
			return nil, err
		}

		alt_label = source
	}

	args := []interface{}{
		id,
		alt_label,
	}

	spr_q := fmt.Sprintf(`SELECT 
		id, parent_id, name, placetype,
		country, repo,
		latitude, longitude,
		min_latitude, min_longitude,
		max_latitude, max_longitude,
		is_current, is_deprecated, is_ceased,
		is_superseded, is_superseding,
		superseded_by, supersedes,
		lastmodified
	FROM %s WHERE id = ? AND alt_label = ?`, r.spr_table.Name())

	// t1 := time.Now()

	row := conn.QueryRowContext(ctx, spr_q, args...)

	var spr_id string
	var parent_id string
	var name string
	var placetype string
	var country string
	var repo string

	var latitude float64
	var longitude float64
	var min_latitude float64
	var max_latitude float64
	var min_longitude float64
	var max_longitude float64

	var is_current int64
	var is_deprecated int64
	var is_ceased int64
	var is_superseded int64
	var is_superseding int64

	var superseded_by string
	var supersedes string

	var lastmodified int64

	err = row.Scan(
		&spr_id, &parent_id, &name, &placetype, &country, &repo,
		&latitude, &longitude, &min_latitude, &max_latitude, &min_longitude, &max_longitude,
		&is_current, &is_deprecated, &is_ceased, &is_superseded, &is_superseding,
		&superseded_by, &supersedes,
		&lastmodified,
	)

	if err != nil {
		golog.Println("SAD SPR QUERY", args)
		return nil, err
	}

	golog.Println("DEBUG", superseded_by, supersedes)

	path := "fixme"

	s := &SQLiteStandardPlacesResult{
		WOFId:          spr_id,
		WOFParentId:    parent_id,
		WOFName:        name,
		WOFCountry:     country,
		WOFPlacetype:   placetype,
		MZLatitude:     latitude,
		MZLongitude:    longitude,
		MZMinLatitude:  min_latitude,
		MZMaxLatitude:  max_latitude,
		MZMinLongitude: min_longitude,
		MZMaxLongitude: max_longitude,
		MZIsCurrent:    is_current,
		MZIsDeprecated: is_deprecated,
		MZIsCeased:     is_ceased,
		// is_superseded: is_superseded,
		// is_superseding: is_superseding,
		WOFPath:         path,
		WOFRepo:         repo,
		WOFLastModified: lastmodified,
	}

	// this takes too long because you can't put indexes on virtual tables

	geom_q := fmt.Sprintf("SELECT geometry FROM %s WHERE wof_id = ? AND alt_label = ?", r.rtree_table.Name())

	geom_rows, err := conn.QueryContext(ctx, geom_q, args...)

	if err != nil {
		return nil, err
	}

	geom_coords := make([][][][]float64, 0)

	for geom_rows.Next() {

		var str_geom string
		err := geom_rows.Scan(&str_geom)

		if err != nil {
			return nil, err
		}

		var coords [][][]float64

		err = json.Unmarshal([]byte(str_geom), &coords)

		if err != nil {
			return nil, err
		}

		geom_coords = append(geom_coords, coords)
	}

	err = geom_rows.Close()

	if err != nil {
		return nil, err
	}

	err = geom_rows.Err()

	if err != nil {
		return nil, err
	}

	if len(geom_coords) == 0 {
		return nil, errors.New("No coordinates")
	}

	var g *geojson.Geometry

	if len(geom_coords) == 1 {
		g = geojson.NewPolygonGeometry(geom_coords[0])
	} else {
		g = geojson.NewMultiPolygonGeometry(geom_coords...)
	}

	cache_item, err := NewSQLiteCacheItem(s, g)

	if err != nil {
		return nil, err
	}

	r.gocache.Set(uri_str, cache_item, -1)

	return cache_item.(*SQLiteCacheItem), nil
}
