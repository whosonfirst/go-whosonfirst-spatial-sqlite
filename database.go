package sqlite

// https://www.sqlite.org/rtree.html

import (
	"context"
	"encoding/json"
	"fmt"
	"github.com/aaronland/go-sqlite"
	sqlite_database "github.com/aaronland/go-sqlite/database"
	gocache "github.com/patrickmn/go-cache"
	"github.com/paulmach/orb"
	"github.com/paulmach/orb/planar"
	"github.com/whosonfirst/go-ioutil"
	"github.com/whosonfirst/go-whosonfirst-geojson-v2/feature"
	"github.com/whosonfirst/go-whosonfirst-spatial"
	"github.com/whosonfirst/go-whosonfirst-spatial/database"
	"github.com/whosonfirst/go-whosonfirst-spatial/filter"
	"github.com/whosonfirst/go-whosonfirst-spatial/timer"
	"github.com/whosonfirst/go-whosonfirst-spr/v2"
	"github.com/whosonfirst/go-whosonfirst-sqlite-features/tables"
	sqlite_spr "github.com/whosonfirst/go-whosonfirst-sqlite-spr"
	"github.com/whosonfirst/go-whosonfirst-uri"
	"io"
	"log"
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
	Logger        *log.Logger
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
	bounds    orb.Bound
	Id        string
	FeatureId string
	IsAlt     bool
	AltLabel  string
}

func (sp RTreeSpatialIndex) Bounds() orb.Bound {
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
		return nil, fmt.Errorf("Failed to parse URI, %w", err)
	}

	q := u.Query()

	dsn := q.Get("dsn")

	if dsn == "" {
		return nil, fmt.Errorf("Missing 'dsn' parameter")
	}

	sqlite_db, err := sqlite_database.NewDB(ctx, dsn)

	if err != nil {
		return nil, fmt.Errorf("Failed to create new SQLite database, %w", err)
	}

	return NewSQLiteSpatialDatabaseWithDatabase(ctx, uri, sqlite_db)
}

func NewSQLiteSpatialDatabaseWithDatabase(ctx context.Context, uri string, sqlite_db *sqlite_database.SQLiteDatabase) (database.SpatialDatabase, error) {

	u, err := url.Parse(uri)

	if err != nil {
		return nil, fmt.Errorf("Failed to parse URI, %w", err)
	}

	q := u.Query()

	dsn := q.Get("dsn")

	rtree_table, err := tables.NewRTreeTableWithDatabase(ctx, sqlite_db)

	if err != nil {
		return nil, fmt.Errorf("Failed to create rtree table, %w", err)
	}

	spr_table, err := tables.NewSPRTableWithDatabase(ctx, sqlite_db)

	if err != nil {
		return nil, fmt.Errorf("Failed to create spr table, %w", err)
	}

	// This is so we can satisfy the reader.Reader requirement
	// in the spatial.SpatialDatabase interface

	geojson_table, err := tables.NewGeoJSONTableWithDatabase(ctx, sqlite_db)

	if err != nil {
		return nil, fmt.Errorf("Failed to create geojson table, %w", err)
	}

	logger := log.Default()

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

func (r *SQLiteSpatialDatabase) IndexFeature(ctx context.Context, body []byte) error {

	f, err := feature.LoadFeature(body)

	if err != nil {
		return fmt.Errorf("Failed to load feature, %w", err)
	}

	r.mu.Lock()
	defer r.mu.Unlock()

	err = r.rtree_table.IndexRecord(ctx, r.db, f)

	if err != nil {
		return fmt.Errorf("Failed to index record in rtree table, %w", err)
	}

	err = r.spr_table.IndexRecord(ctx, r.db, f)

	if err != nil {
		return fmt.Errorf("Failed to index record in spr table, %w", err)
	}

	if r.geojson_table != nil {

		err = r.geojson_table.IndexRecord(ctx, r.db, f)

		if err != nil {
			return fmt.Errorf("Failed to index record in geojson table, %w", err)
		}
	}

	return nil
}

func (r *SQLiteSpatialDatabase) RemoveFeature(ctx context.Context, id string) error {

	conn, err := r.db.Conn()

	if err != nil {
		return fmt.Errorf("Failed to establish database connection, %w", err)
	}

	tx, err := conn.Begin()

	if err != nil {
		return fmt.Errorf("Failed to create transaction, %w", err)
	}

	defer tx.Rollback()

	tables := []sqlite.Table{
		r.rtree_table,
		r.spr_table,
	}

	if r.geojson_table != nil {
		tables = append(tables, r.geojson_table)
	}

	for _, t := range tables {

		var q string

		switch t.Name() {
		case "rtree":
			q = fmt.Sprintf("DELETE FROM %s WHERE wof_id = ?", t.Name())
		default:
			q = fmt.Sprintf("DELETE FROM %s WHERE id = ?", t.Name())
		}

		stmt, err := tx.Prepare(q)

		if err != nil {
			return fmt.Errorf("Failed to create query statement for %s, %w", t.Name(), err)
		}

		_, err = stmt.ExecContext(ctx, id)

		if err != nil {
			return fmt.Errorf("Failed execute query statement for %s, %w", t.Name(), err)
		}
	}

	err = tx.Commit()

	if err != nil {
		return fmt.Errorf("Failed to commit transaction, %w", err)
	}

	return nil
}

func (r *SQLiteSpatialDatabase) PointInPolygon(ctx context.Context, coord *orb.Point, filters ...spatial.Filter) (spr.StandardPlacesResults, error) {

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
			return nil, fmt.Errorf("Point in polygon request failed, %w", err)
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

func (r *SQLiteSpatialDatabase) PointInPolygonWithChannels(ctx context.Context, rsp_ch chan spr.StandardPlacesResult, err_ch chan error, done_ch chan bool, coord *orb.Point, filters ...spatial.Filter) {

	defer func() {
		done_ch <- true
	}()

	rows, err := r.getIntersectsByCoord(ctx, coord, filters...)

	if err != nil {
		err_ch <- fmt.Errorf("Get intersects failed, %w", err)
		return
	}

	r.inflateResultsWithChannels(ctx, rsp_ch, err_ch, rows, coord, filters...)
	return
}

func (r *SQLiteSpatialDatabase) PointInPolygonCandidates(ctx context.Context, coord *orb.Point, filters ...spatial.Filter) ([]*spatial.PointInPolygonCandidate, error) {

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
			return nil, fmt.Errorf("Point in polygon (candidates) query failed, %w", err)
		default:
			// pass
		}

		if !working {
			break
		}
	}

	return candidates, nil
}

func (r *SQLiteSpatialDatabase) PointInPolygonCandidatesWithChannels(ctx context.Context, rsp_ch chan *spatial.PointInPolygonCandidate, err_ch chan error, done_ch chan bool, coord *orb.Point, filters ...spatial.Filter) {

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
			Bounds:    bounds,
		}

		rsp_ch <- c
	}

	return
}

func (r *SQLiteSpatialDatabase) getIntersectsByCoord(ctx context.Context, coord *orb.Point, filters ...spatial.Filter) ([]*RTreeSpatialIndex, error) {

	// how small can this be?

	padding := 0.00001

	b := coord.Bound()
	rect := b.Pad(padding)

	return r.getIntersectsByRect(ctx, &rect, filters...)
}

func (r *SQLiteSpatialDatabase) getIntersectsByRect(ctx context.Context, rect *orb.Bound, filters ...spatial.Filter) ([]*RTreeSpatialIndex, error) {

	conn, err := r.db.Conn()

	if err != nil {
		return nil, fmt.Errorf("Failed to establish database connection, %w", err)
	}

	q := fmt.Sprintf("SELECT id, wof_id, is_alt, alt_label, geometry, min_x, min_y, max_x, max_y FROM %s  WHERE min_x <= ? AND max_x >= ?  AND min_y <= ? AND max_y >= ?", r.rtree_table.Name())

	// Left returns the left of the bound.
	// Right returns the right of the bound.

	minx := rect.Left()
	miny := rect.Bottom()
	maxx := rect.Right()
	maxy := rect.Top()

	rows, err := conn.QueryContext(ctx, q, minx, maxx, miny, maxy)

	if err != nil {
		return nil, fmt.Errorf("SQL query failed, %w", err)
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
			return nil, fmt.Errorf("Result row scan failed, %w", err)
		}

		min := orb.Point{minx, miny}
		max := orb.Point{maxx, maxy}

		rect := orb.Bound{
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

func (r *SQLiteSpatialDatabase) inflateResultsWithChannels(ctx context.Context, rsp_ch chan spr.StandardPlacesResult, err_ch chan error, possible []*RTreeSpatialIndex, c *orb.Point, filters ...spatial.Filter) {

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

func (r *SQLiteSpatialDatabase) inflateSpatialIndexWithChannels(ctx context.Context, rsp_ch chan spr.StandardPlacesResult, err_ch chan error, seen map[string]bool, mu *sync.RWMutex, sp *RTreeSpatialIndex, c *orb.Point, filters ...spatial.Filter) {

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

	var poly orb.Polygon // [][][]float64

	err := json.Unmarshal([]byte(sp.geometry), &poly)

	r.Timer.Add(ctx, sp_id, "time to unmarshal geometry", time.Since(t2))

	if err != nil {
		err_ch <- fmt.Errorf("Failed to unmarshal geometry, %w", err)
		return
	}

	/*
		if len(coords) == 0 {
			err_ch <- errors.New("Missing coordinates for polygon")
			return
		}
	*/

	t3 := time.Now()

	if !planar.PolygonContains(poly, *c) {
		return
	}

	r.Timer.Add(ctx, sp_id, "time to perform contains test", time.Since(t3))

	// there is at least one ring that contains the coord
	// now we check the filters - whether or not they pass
	// we can skip every subsequent polygon with the same
	// ID

	mu.Lock()
	defer mu.Unlock()

	// Check to see whether seen[feature_id] has been assigned by another process
	// while waiting for mu to become available

	if seen[feature_id] {
		return
	}

	seen[feature_id] = true

	t4 := time.Now()

	s, err := r.retrieveSPR(ctx, sp.Path())

	if err != nil {
		r.Logger.Printf("Failed to retrieve feature cache for %s, %v", sp_id, err)
		return
	}

	r.Timer.Add(ctx, sp_id, "time to retrieve SPR", time.Since(t4))

	if err != nil {
		r.Logger.Printf("Failed to retrieve feature cache for %s, %v", sp_id, err)
		return
	}

	t5 := time.Now()

	for _, f := range filters {

		err = filter.FilterSPR(f, s)

		if err != nil {
			// r.Logger.Printf("SKIP %s because filter error %s", sp_id, err)
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

	body, err := io.ReadAll(fh)

	if err != nil {
		return 0, err
	}

	err = r.IndexFeature(ctx, body)

	if err != nil {
		return 0, err
	}

	return int64(len(body)), nil
}

func (r *SQLiteSpatialDatabase) WriterURI(ctx context.Context, str_uri string) string {
	return str_uri
}

func (r *SQLiteSpatialDatabase) Close(ctx context.Context) error {
	return nil
}
