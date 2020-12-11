package tables

// https://www.sqlite.org/rtree.html

import (
	"errors"
	"fmt"
	"github.com/whosonfirst/go-whosonfirst-geojson-v2"
	"github.com/whosonfirst/go-whosonfirst-geojson-v2/properties/geometry"
	"github.com/whosonfirst/go-whosonfirst-geojson-v2/properties/whosonfirst"
	"github.com/whosonfirst/go-whosonfirst-sqlite"
	"github.com/whosonfirst/go-whosonfirst-sqlite-features"
	"github.com/whosonfirst/go-whosonfirst-sqlite/utils"
	_ "log"
)

type RTreeTableOptions struct {
	IndexAltFiles bool
}

func DefaultRTreeTableOptions() (*RTreeTableOptions, error) {

	opts := RTreeTableOptions{
		IndexAltFiles: false,
	}

	return &opts, nil
}

type RTreeTable struct {
	features.FeatureTable
	name    string
	options *RTreeTableOptions
}

func NewRTreeTable() (sqlite.Table, error) {

	opts, err := DefaultRTreeTableOptions()

	if err != nil {
		return nil, err
	}

	return NewRTreeTableWithOptions(opts)
}

func NewRTreeTableWithOptions(opts *RTreeTableOptions) (sqlite.Table, error) {

	t := RTreeTable{
		name:    "rtree",
		options: opts,
	}

	return &t, nil
}

func NewRTreeTableWithDatabase(db sqlite.Database) (sqlite.Table, error) {

	opts, err := DefaultRTreeTableOptions()

	if err != nil {
		return nil, err
	}

	return NewRTreeTableWithDatabaseAndOptions(db, opts)
}

func NewRTreeTableWithDatabaseAndOptions(db sqlite.Database, opts *RTreeTableOptions) (sqlite.Table, error) {

	t, err := NewRTreeTableWithOptions(opts)

	if err != nil {
		return nil, err
	}

	err = t.InitializeTable(db)

	if err != nil {
		return nil, err
	}

	return t, nil
}

func (t *RTreeTable) Name() string {
	return t.name
}

func (t *RTreeTable) Schema() string {

	/*

		3.1.1. Column naming details

		In the argments to "rtree" in the CREATE VIRTUAL TABLE statement, the names of the columns are taken from the first token of each argument. All subsequent tokens within each argument are silently ignored. This means, for example, that if you try to give a column a type affinity or add a constraint such as UNIQUE or NOT NULL or DEFAULT to a column, those extra tokens are accepted as valid, but they do not change the behavior of the rtree. In an RTREE virtual table, the first column always has a type affinity of INTEGER and all other data columns have a type affinity of NUMERIC.

		Recommended practice is to omit any extra tokens in the rtree specification. Let each argument to "rtree" be a single ordinary label that is the name of the corresponding column, and omit all other tokens from the argument list.

		4.1. Auxiliary Columns

		Beginning with SQLite version 3.24.0 (2018-06-04), r-tree tables can have auxiliary columns that store arbitrary data. Auxiliary columns can be used in place of secondary tables such as "demo_data".

		Auxiliary columns are marked with a "+" symbol before the column name. Auxiliary columns must come after all of the coordinate boundary columns. There is a limit of no more than 100 auxiliary columns. The following example shows an r-tree table with auxiliary columns that is equivalent to the two tables "demo_index" and "demo_data" above:

		Note: Auxiliary columns must come at the end of a table definition
	*/

	sql := `CREATE VIRTUAL TABLE %s USING rtree (
		id,
		min_x,
		max_x,
		min_y,
		max_y,
		+wof_id INTEGER,
		+is_alt TINYINT,
		+alt_label TEXT,
		+lastmodified INTEGER
	);`

	return fmt.Sprintf(sql, t.Name())
}

func (t *RTreeTable) InitializeTable(db sqlite.Database) error {

	return utils.CreateTableIfNecessary(db, t)
}

func (t *RTreeTable) IndexRecord(db sqlite.Database, i interface{}) error {
	return t.IndexFeature(db, i.(geojson.Feature))
}

func (t *RTreeTable) IndexFeature(db sqlite.Database, f geojson.Feature) error {

	switch geometry.Type(f) {
	case "Polygon", "MultiPolygon":
		// pass
	default:
		return nil
	}

	conn, err := db.Conn()

	if err != nil {
		return err
	}

	wof_id := f.Id()
	is_alt := whosonfirst.IsAlt(f) // this returns a boolean which is interpreted as a float by SQLite

	if is_alt && !t.options.IndexAltFiles {
		return nil
	}

	alt_label := ""

	if is_alt {

		alt_label = whosonfirst.AltLabel(f)

		if alt_label == "" {
			return errors.New("Missing src:alt_label property")
		}
	}

	lastmod := whosonfirst.LastModified(f)

	// TBD: Store polygon alongside bounding box in rtree table
	// https://github.com/whosonfirst/go-whosonfirst-sqlite-features/issues/11
	
	bboxes, err := f.BoundingBoxes()

	if err != nil {
		return err
	}

	tx, err := conn.Begin()

	if err != nil {
		return err
	}

	sql := fmt.Sprintf(`INSERT OR REPLACE INTO %s (
		id, min_x, max_x, min_y, max_y, wof_id, is_alt, alt_label, lastmodified
	) VALUES (
		NULL, ?, ?, ?, ?, ?, ?, ?, ?
	)`, t.Name())

	stmt, err := tx.Prepare(sql)

	if err != nil {
		return err
	}

	defer stmt.Close()

	for _, bbox := range bboxes.Bounds() {

		sw := bbox.Min
		ne := bbox.Max

		_, err = stmt.Exec(sw.X, ne.X, sw.Y, ne.Y, wof_id, is_alt, alt_label, lastmod)

		if err != nil {
			return err
		}
	}

	return tx.Commit()
}
