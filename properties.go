package sqlite

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"github.com/tidwall/gjson"
	wof_geojson "github.com/whosonfirst/go-whosonfirst-geojson-v2"
	"github.com/whosonfirst/go-whosonfirst-spatial/geojson"
	spatial_properties "github.com/whosonfirst/go-whosonfirst-spatial/properties"
	"github.com/whosonfirst/go-whosonfirst-spr"
	"github.com/whosonfirst/go-whosonfirst-sqlite"
	"github.com/whosonfirst/go-whosonfirst-sqlite-features/tables"
	sqlite_database "github.com/whosonfirst/go-whosonfirst-sqlite/database"
	"net/url"
)

type SQLitePropertiesReader struct {
	spatial_properties.PropertiesReader
	db            *sqlite_database.SQLiteDatabase
	geojson_table sqlite.Table
	dsn           string
}

func init() {
	ctx := context.Background()
	spatial_properties.RegisterPropertiesReader(ctx, "sqlite", NewSQLitePropertiesReader)
}

func NewSQLitePropertiesReader(ctx context.Context, uri string) (spatial_properties.PropertiesReader, error) {

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

	pr := &SQLitePropertiesReader{
		dsn:           dsn,
		geojson_table: geojson_table,
		db:            sqlite_db,
	}

	return pr, nil
}

func (pr *SQLitePropertiesReader) IndexFeature(ctx context.Context, f wof_geojson.Feature) error {

	return pr.geojson_table.IndexRecord(pr.db, f)
}

func (pr *SQLitePropertiesReader) PropertiesResponseResultsWithStandardPlacesResults(ctx context.Context, results spr.StandardPlacesResults, properties []string) (*spatial_properties.PropertiesResponseResults, error) {

	conn, err := pr.db.Conn()

	if err != nil {
		return nil, err
	}

	ctx, cancel := context.WithCancel(ctx)
	defer cancel()

	previous_results := results.Results()
	new_results := make([]*spatial_properties.PropertiesResponse, len(previous_results))

	for idx, r := range previous_results {

		target, err := json.Marshal(r)

		if err != nil {
			return nil, err
		}

		str_id := r.Id()

		q := fmt.Sprintf("SELECT body FROM %s WHERE id = ?", pr.geojson_table.Name())

		row := conn.QueryRowContext(ctx, q, str_id)

		var body string

		err = row.Scan(&body)

		if err != nil {
			return nil, err
		}

		source := []byte(body)

		target, err = spatial_properties.AppendPropertiesWithJSON(ctx, source, target, properties, "")

		if err != nil {
			return nil, err
		}

		var props *spatial_properties.PropertiesResponse
		err = json.Unmarshal(target, &props)

		if err != nil {
			return nil, err
		}

		new_results[idx] = props
	}

	props_rsp := &spatial_properties.PropertiesResponseResults{
		Properties: new_results,
	}

	return props_rsp, nil
}

func (pr *SQLitePropertiesReader) AppendPropertiesWithFeatureCollection(ctx context.Context, fc *geojson.GeoJSONFeatureCollection, properties []string) error {

	ctx, cancel := context.WithCancel(ctx)
	defer cancel()

	rsp_ch := make(chan spatial_properties.ChannelResponse)
	err_ch := make(chan error)
	done_ch := make(chan bool)

	remaining := len(fc.Features)

	for idx, f := range fc.Features {
		go pr.appendPropertiesWithChannels(ctx, idx, f, properties, rsp_ch, err_ch, done_ch)
	}

	for remaining > 0 {
		select {
		case <-ctx.Done():
			return nil
		case <-done_ch:
			remaining -= 1
		case rsp := <-rsp_ch:
			fc.Features[rsp.Index] = rsp.Feature
		case err := <-err_ch:
			return err
		default:
			// pass
		}
	}

	return nil
}

func (pr *SQLitePropertiesReader) Close(ctx context.Context) error {
	return pr.db.Close()
}

func (pr *SQLitePropertiesReader) appendPropertiesWithChannels(ctx context.Context, idx int, f geojson.GeoJSONFeature, properties []string, rsp_ch chan spatial_properties.ChannelResponse, err_ch chan error, done_ch chan bool) {

	defer func() {
		done_ch <- true
	}()

	select {
	case <-ctx.Done():
		return
	default:
		// pass
	}

	conn, err := pr.db.Conn()

	if err != nil {
		err_ch <- err
		return
	}

	target, err := json.Marshal(f)

	if err != nil {
		err_ch <- err
		return
	}

	id_rsp := gjson.GetBytes(target, "properties.wof:id")

	if !id_rsp.Exists() {
		err_ch <- errors.New("Missing wof:id")
		return
	}

	str_id := id_rsp.String()

	q := fmt.Sprintf("SELECT body FROM %s WHERE id = ?", pr.geojson_table.Name())

	row := conn.QueryRowContext(ctx, q, str_id)

	var body string

	err = row.Scan(&body)

	if err != nil {
		err_ch <- err
		return
	}

	source := []byte(body)

	target, err = spatial_properties.AppendPropertiesWithJSON(ctx, source, target, properties, "properties")

	if err != nil {
		err_ch <- err
		return
	}

	var new_f geojson.GeoJSONFeature
	err = json.Unmarshal(target, &new_f)

	if err != nil {
		err_ch <- err
		return
	}

	rsp := spatial_properties.ChannelResponse{
		Index:   idx,
		Feature: new_f,
	}

	rsp_ch <- rsp
	return
}
