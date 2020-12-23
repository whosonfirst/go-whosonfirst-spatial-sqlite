package sqlite

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	wof_geojson "github.com/whosonfirst/go-whosonfirst-geojson-v2"
	"github.com/whosonfirst/go-whosonfirst-spatial"
	spatial_properties "github.com/whosonfirst/go-whosonfirst-spatial/properties"
	"github.com/whosonfirst/go-whosonfirst-spr"
	"github.com/whosonfirst/go-whosonfirst-sqlite"
	"github.com/whosonfirst/go-whosonfirst-sqlite-features/tables"
	sqlite_database "github.com/whosonfirst/go-whosonfirst-sqlite/database"
	"net/url"
	"sync"
)

type SQLitePropertiesReader struct {
	spatial_properties.PropertiesReader
	db               *sqlite_database.SQLiteDatabase
	properties_table sqlite.Table
	dsn              string
	mu               *sync.RWMutex
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

	properties_table, err := tables.NewPropertiesTableWithDatabase(sqlite_db)

	if err != nil {
		return nil, err
	}

	mu := new(sync.RWMutex)

	pr := &SQLitePropertiesReader{
		dsn:              dsn,
		properties_table: properties_table,
		db:               sqlite_db,
		mu:               mu,
	}

	return pr, nil
}

func (pr *SQLitePropertiesReader) IndexFeature(ctx context.Context, f wof_geojson.Feature) error {

	pr.mu.Lock()
	defer pr.mu.Unlock()

	return pr.properties_table.IndexRecord(pr.db, f)
}

func (pr *SQLitePropertiesReader) PropertiesResponseResultsWithStandardPlacesResults(ctx context.Context, results spr.StandardPlacesResults, properties []string) (*spatial.PropertiesResponseResults, error) {

	conn, err := pr.db.Conn()

	if err != nil {
		return nil, err
	}

	ctx, cancel := context.WithCancel(ctx)
	defer cancel()

	previous_results := results.Results()
	new_results := make([]*spatial.PropertiesResponse, len(previous_results))

	for idx, r := range previous_results {

		target, err := json.Marshal(r)

		if err != nil {
			return nil, err
		}

		str_id := r.Id()

		q := fmt.Sprintf("SELECT body FROM %s WHERE id = ?", pr.properties_table.Name())

		row := conn.QueryRowContext(ctx, q, str_id)

		var body string

		err = row.Scan(&body)

		if err != nil {
			return nil, err
		}

		source := []byte(body)

		append_opts := &spatial_properties.AppendPropertiesOptions{
			Keys:         properties,
			SourcePrefix: "",
			TargetPrefix: "",
		}

		target, err = spatial_properties.AppendPropertiesWithJSON(ctx, append_opts, source, target)

		if err != nil {
			return nil, err
		}

		var props *spatial.PropertiesResponse
		err = json.Unmarshal(target, &props)

		if err != nil {
			return nil, err
		}

		new_results[idx] = props
	}

	props_rsp := &spatial.PropertiesResponseResults{
		Properties: new_results,
	}

	return props_rsp, nil
}

func (pr *SQLitePropertiesReader) Close(ctx context.Context) error {
	return pr.db.Close()
}
