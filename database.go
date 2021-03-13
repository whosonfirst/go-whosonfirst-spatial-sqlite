package sqlite

// https://www.sqlite.org/rtree.html

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	gocache "github.com/patrickmn/go-cache"
	"github.com/skelterjohn/geom"
	"github.com/whosonfirst/go-ioutil"
	wof_geojson "github.com/whosonfirst/go-whosonfirst-geojson-v2"
	"github.com/whosonfirst/go-whosonfirst-log"
	"github.com/whosonfirst/go-whosonfirst-spatial"
	"github.com/whosonfirst/go-whosonfirst-spatial/database"
	"github.com/whosonfirst/go-whosonfirst-spatial/filter"
	"github.com/whosonfirst/go-whosonfirst-spatial/geo"
	"github.com/whosonfirst/go-whosonfirst-spatial/timer"
	"github.com/whosonfirst/go-whosonfirst-spr/v2"
	"github.com/whosonfirst/go-whosonfirst-sqlite"
	"github.com/whosonfirst/go-whosonfirst-sqlite-features/tables"
	sqlite_spr "github.com/whosonfirst/go-whosonfirst-sqlite-spr"
	sqlite_database "github.com/whosonfirst/go-whosonfirst-sqlite/database"
	"github.com/whosonfirst/go-whosonfirst-uri"
	"io"
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
	Timer         *timer.Timer
	mu            *sync.RWMutex
	db            *sqlite_database.SQLiteDatabase
	rtree_table   sqlite.Table
	spr_table     sqlite.Table
	geojson_table sqlite.Table
	gocache       *gocache.Cache
	dsn           string
}

type RTreeSpatialIndex struct {
	geometry  string
	bounds    geom.Rect
	Id        string
	FeatureId string
	IsAlt     bool
	AltLabel  string
}

func (sp RTreeSpatialIndex) Bounds() geom.Rect {
	return sp.bounds
}

func (sp RTreeSpatialIndex) Path() string {

	if sp.IsAlt {
		return fmt.Sprintf("%s-alt-%s", sp.FeatureId, sp.AltLabel)
	}

	return sp.FeatureId
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

	return NewSQLiteSpatialDatabaseWithDatabase(ctx, uri, sqlite_db)
}

func NewSQLiteSpatialDatabaseWithDatabase(ctx context.Context, uri string, sqlite_db *sqlite_database.SQLiteDatabase) (database.SpatialDatabase, error) {

	u, err := url.Parse(uri)

	if err != nil {
		return nil, err
	}

	q := u.Query()

	dsn := q.Get("dsn")

	rtree_table, err := tables.NewRTreeTableWithDatabase(sqlite_db)

	if err != nil {
		return nil, err
	}

	spr_table, err := tables.NewSPRTableWithDatabase(sqlite_db)

	if err != nil {
		return nil, err
	}

	// This is so we can satisfy the reader.Reader requirement
	// in the spatial.SpatialDatabase interface

	geojson_table, err := tables.NewGeoJSONTableWithDatabase(sqlite_db)

	if err != nil {
		return nil, err
	}

	logger := log.SimpleWOFLogger("index")

	expires := 5 * time.Minute
	cleanup := 30 * time.Minute

	gc := gocache.New(expires, cleanup)

	mu := new(sync.RWMutex)

	t := timer.NewTimer()

	spatial_db := &SQLiteSpatialDatabase{
		Logger:        logger,
		Timer:         t,
		db:            sqlite_db,
		rtree_table:   rtree_table,
		spr_table:     spr_table,
		geojson_table: geojson_table,
		gocache:       gc,
		dsn:           dsn,
		mu:            mu,
	}

	return spatial_db, nil
}

func (r *SQLiteSpatialDatabase) Disconnect(ctx context.Context) error {
	return r.db.Close()
}

func (r *SQLiteSpatialDatabase) IndexFeature(ctx context.Context, f wof_geojson.Feature) error {

	r.mu.Lock()
	defer r.mu.Unlock()

	err := r.rtree_table.IndexRecord(r.db, f)

	if err != nil {
		return err
	}

	err = r.spr_table.IndexRecord(r.db, f)

	if err != nil {
		return err
	}

	if r.geojson_table != nil {

		err = r.geojson_table.IndexRecord(r.db, f)

		if err != nil {
			return err
		}
	}

	return nil
}

func (r *SQLiteSpatialDatabase) PointInPolygon(ctx context.Context, coord *geom.Coord, filters ...spatial.Filter) (spr.StandardPlacesResults, error) {

	ctx, cancel := context.WithCancel(ctx)
	defer cancel()

	/*
		t1 := time.Now()

		defer func() {
			golog.Printf("Time to point in polygon, %v\n", time.Since(t1))
		}()

	*/

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

	/*
		for label, timings := range r.Timer.Timings {

			for _, tm := range timings {
				golog.Printf("[%s] %s\n", label, tm)
			}
		}
	*/

	spr_results := &SQLiteResults{
		Places: results,
	}

	return spr_results, nil
}

func (r *SQLiteSpatialDatabase) PointInPolygonWithChannels(ctx context.Context, rsp_ch chan spr.StandardPlacesResult, err_ch chan error, done_ch chan bool, coord *geom.Coord, filters ...spatial.Filter) {

	defer func() {
		done_ch <- true
	}()

	rows, err := r.getIntersectsByCoord(ctx, coord, filters...)

	if err != nil {
		err_ch <- err
		return
	}

	r.inflateResultsWithChannels(ctx, rsp_ch, err_ch, rows, coord, filters...)
	return
}

func (r *SQLiteSpatialDatabase) PointInPolygonCandidates(ctx context.Context, coord *geom.Coord, filters ...spatial.Filter) ([]*spatial.PointInPolygonCandidate, error) {

	ctx, cancel := context.WithCancel(ctx)
	defer cancel()

	rsp_ch := make(chan *spatial.PointInPolygonCandidate)
	err_ch := make(chan error)
	done_ch := make(chan bool)

	candidates := make([]*spatial.PointInPolygonCandidate, 0)
	working := true

	go r.PointInPolygonCandidatesWithChannels(ctx, rsp_ch, err_ch, done_ch, coord, filters...)

	for {
		select {
		case <-ctx.Done():
			return nil, nil
		case <-done_ch:
			working = false
		case rsp := <-rsp_ch:
			candidates = append(candidates, rsp)
		case err := <-err_ch:
			return nil, err
		default:
			// pass
		}

		if !working {
			break
		}
	}

	return candidates, nil
}

func (r *SQLiteSpatialDatabase) PointInPolygonCandidatesWithChannels(ctx context.Context, rsp_ch chan *spatial.PointInPolygonCandidate, err_ch chan error, done_ch chan bool, coord *geom.Coord, filters ...spatial.Filter) {

	defer func() {
		done_ch <- true
	}()

	intersects, err := r.getIntersectsByCoord(ctx, coord, filters...)

	if err != nil {
		err_ch <- err
		return
	}

	for _, sp := range intersects {

		bounds := sp.Bounds()

		c := &spatial.PointInPolygonCandidate{
			Id:        sp.Id,
			FeatureId: sp.FeatureId,
			AltLabel:  sp.AltLabel,
			Bounds:    &bounds,
		}

		rsp_ch <- c
	}

	return
}

func (r *SQLiteSpatialDatabase) getIntersectsByCoord(ctx context.Context, coord *geom.Coord, filters ...spatial.Filter) ([]*RTreeSpatialIndex, error) {

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

	return r.getIntersectsByRect(ctx, rect, filters...)
}

func (r *SQLiteSpatialDatabase) getIntersectsByRect(ctx context.Context, rect *geom.Rect, filters ...spatial.Filter) ([]*RTreeSpatialIndex, error) {

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
		var feature_id string
		var is_alt int32
		var alt_label string
		var geometry string
		var minx float64
		var miny float64
		var maxx float64
		var maxy float64

		err := rows.Scan(&id, &feature_id, &is_alt, &alt_label, &geometry, &minx, &miny, &maxx, &maxy)

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
			Id:        fmt.Sprintf("%s#%s", feature_id, id),
			FeatureId: feature_id,
			bounds:    rect,
			geometry:  geometry,
		}

		if is_alt == 1 {
			i.IsAlt = true
			i.AltLabel = alt_label
		}

		intersects = append(intersects, i)
	}

	return intersects, nil
}

func (r *SQLiteSpatialDatabase) inflateResultsWithChannels(ctx context.Context, rsp_ch chan spr.StandardPlacesResult, err_ch chan error, possible []*RTreeSpatialIndex, c *geom.Coord, filters ...spatial.Filter) {

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

func (r *SQLiteSpatialDatabase) inflateSpatialIndexWithChannels(ctx context.Context, rsp_ch chan spr.StandardPlacesResult, err_ch chan error, seen map[string]bool, mu *sync.RWMutex, sp *RTreeSpatialIndex, c *geom.Coord, filters ...spatial.Filter) {

	select {
	case <-ctx.Done():
		return
	default:
		// pass
	}

	sp_id := fmt.Sprintf("%s:%s", sp.Id, sp.AltLabel)
	feature_id := fmt.Sprintf("%s:%s", sp.FeatureId, sp.AltLabel)

	t1 := time.Now()

	defer func() {
		r.Timer.Add(ctx, sp_id, "time to inflate", time.Since(t1))
	}()

	// have we already looked up the filters for this ID?
	// see notes below

	mu.RLock()
	_, ok := seen[feature_id]
	mu.RUnlock()

	if ok {
		return
	}

	t2 := time.Now()

	// this needs to be sped up (20201216/thisisaaronland)

	var coords [][][]float64

	err := json.Unmarshal([]byte(sp.geometry), &coords)

	r.Timer.Add(ctx, sp_id, "time to unmarshal geometry", time.Since(t2))

	if err != nil {
		err_ch <- err
		return
	}

	if len(coords) == 0 {
		err_ch <- errors.New("Missing coordinates for polygon")
		return
	}

	t3 := time.Now()

	if !geo.PolygonContainsCoord(coords, c) {
		return
	}

	r.Timer.Add(ctx, sp_id, "time to perform contains test", time.Since(t3))

	// there is at least one ring that contains the coord
	// now we check the filters - whether or not they pass
	// we can skip every subsequent polygon with the same
	// ID

	mu.Lock()
	seen[feature_id] = true
	mu.Unlock()

	t4 := time.Now()

	s, err := r.retrieveSPR(ctx, sp.Path())

	if err != nil {
		r.Logger.Error("Failed to retrieve feature cache for %s, %v", sp_id, err)
		return
	}

	r.Timer.Add(ctx, sp_id, "time to retrieve SPR", time.Since(t4))

	if err != nil {
		r.Logger.Error("Failed to retrieve feature cache for %s, %v", sp_id, err)
		return
	}

	t5 := time.Now()

	for _, f := range filters {

		err = filter.FilterSPR(f, s)

		if err != nil {
			r.Logger.Debug("SKIP %s because filter error %s", sp_id, err)
			return
		}
	}

	r.Timer.Add(ctx, sp_id, "time to filter SPR", time.Since(t5))

	rsp_ch <- s
}

func (r *SQLiteSpatialDatabase) retrieveSPR(ctx context.Context, uri_str string) (spr.StandardPlacesResult, error) {

	c, ok := r.gocache.Get(uri_str)

	if ok {
		return c.(*sqlite_spr.SQLiteStandardPlacesResult), nil
	}

	id, uri_args, err := uri.ParseURI(uri_str)

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

	s, err := sqlite_spr.RetrieveSPR(ctx, r.db, r.spr_table, id, alt_label)

	if err != nil {
		return nil, err
	}

	r.gocache.Set(uri_str, s, -1)
	return s, nil
}

// whosonfirst/go-reader interface

func (r *SQLiteSpatialDatabase) Read(ctx context.Context, str_uri string) (io.ReadSeekCloser, error) {

	id, _, err := uri.ParseURI(str_uri)

	if err != nil {
		return nil, err
	}

	conn, err := r.db.Conn()

	if err != nil {
		return nil, err
	}

	// TO DO : ALT STUFF HERE

	q := fmt.Sprintf("SELECT body FROM %s WHERE id = ?", r.geojson_table.Name())

	row := conn.QueryRowContext(ctx, q, id)

	var body string

	err = row.Scan(&body)

	if err != nil {
		return nil, err
	}

	sr := strings.NewReader(body)
	fh, err := ioutil.NewReadSeekCloser(sr)

	if err != nil {
		return nil, err
	}

	return fh, nil
}

func (r *SQLiteSpatialDatabase) ReaderURI(ctx context.Context, str_uri string) string {
	return str_uri
}

// whosonfirst/go-writer interface

func (r *SQLiteSpatialDatabase) Write(ctx context.Context, key string, fh io.ReadSeeker) (int64, error) {
	return 0, fmt.Errorf("Not implemented")
}

func (r *SQLiteSpatialDatabase) WriterURI(ctx context.Context, str_uri string) string {
	return str_uri
}

func (r *SQLiteSpatialDatabase) Close(ctx context.Context) error {
	return nil
}
