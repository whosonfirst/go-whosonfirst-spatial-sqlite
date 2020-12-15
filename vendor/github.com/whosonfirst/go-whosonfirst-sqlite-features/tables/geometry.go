package tables

import (
	"fmt"
	"github.com/tidwall/gjson"
	"github.com/whosonfirst/go-whosonfirst-geojson-v2"
	"github.com/whosonfirst/go-whosonfirst-geojson-v2/properties/whosonfirst"
	"github.com/whosonfirst/go-whosonfirst-sqlite"
	"github.com/whosonfirst/go-whosonfirst-sqlite-features"
	"github.com/whosonfirst/go-whosonfirst-sqlite/utils"
)

type GeometryTableOptions struct {
	IndexAltFiles bool
}

func DefaultGeometryTableOptions() (*GeometryTableOptions, error) {

	opts := GeometryTableOptions{
		IndexAltFiles: false,
	}

	return &opts, nil
}

type GeometryTable struct {
	features.FeatureTable
	name    string
	options *GeometryTableOptions
}

type GeometryRow struct {
	Id           int64
	Body         string
	LastModified int64
}

func NewGeometryTableWithDatabase(db sqlite.Database) (sqlite.Table, error) {

	opts, err := DefaultGeometryTableOptions()

	if err != nil {
		return nil, err
	}

	return NewGeometryTableWithDatabaseAndOptions(db, opts)
}

func NewGeometryTableWithDatabaseAndOptions(db sqlite.Database, opts *GeometryTableOptions) (sqlite.Table, error) {

	t, err := NewGeometryTableWithOptions(opts)

	if err != nil {
		return nil, err
	}

	err = t.InitializeTable(db)

	if err != nil {
		return nil, err
	}

	return t, nil
}

func NewGeometryTable() (sqlite.Table, error) {

	opts, err := DefaultGeometryTableOptions()

	if err != nil {
		return nil, err
	}

	return NewGeometryTableWithOptions(opts)
}

func NewGeometryTableWithOptions(opts *GeometryTableOptions) (sqlite.Table, error) {

	t := GeometryTable{
		name:    "geometry",
		options: opts,
	}

	return &t, nil
}

func (t *GeometryTable) Name() string {
	return t.name
}

func (t *GeometryTable) Schema() string {

	sql := `CREATE TABLE %s (
		id INTEGER NOT NULL,
		body TEXT,
		is_alt BOOLEAN,
		alt_label TEXT,
		lastmodified INTEGER
	);

	CREATE UNIQUE INDEX geometry_by_id ON %s (id, alt_label);
	CREATE INDEX geometry_by_alt ON %s (id, is_alt, alt_label);
	CREATE INDEX geometry_by_lastmod ON %s (lastmodified);
	`

	return fmt.Sprintf(sql, t.Name(), t.Name(), t.Name(), t.Name())
}

func (t *GeometryTable) InitializeTable(db sqlite.Database) error {

	return utils.CreateTableIfNecessary(db, t)
}

func (t *GeometryTable) IndexRecord(db sqlite.Database, i interface{}) error {
	return t.IndexFeature(db, i.(geojson.Feature))
}

func (t *GeometryTable) IndexFeature(db sqlite.Database, f geojson.Feature) error {

	conn, err := db.Conn()

	if err != nil {
		return err
	}

	str_id := f.Id()

	is_alt := whosonfirst.IsAlt(f)
	alt_label := whosonfirst.AltLabel(f)

	if is_alt && !t.options.IndexAltFiles {
		return nil
	}

	lastmod := whosonfirst.LastModified(f)

	tx, err := conn.Begin()

	if err != nil {
		return err
	}

	sql := fmt.Sprintf(`INSERT OR REPLACE INTO %s (
		id, body, is_alt, alt_label, lastmodified
	) VALUES (
		?, ?, ?, ?, ?
	)`, t.Name())

	stmt, err := tx.Prepare(sql)

	if err != nil {
		return err
	}

	defer stmt.Close()

	rsp_geom := gjson.GetBytes(f.Bytes(), "geometry")
	str_geom := rsp_geom.String()

	_, err = stmt.Exec(str_id, str_geom, is_alt, alt_label, lastmod)

	if err != nil {
		return err
	}

	return tx.Commit()
}
