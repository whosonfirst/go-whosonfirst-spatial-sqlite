package tables

import (
	"github.com/whosonfirst/go-whosonfirst-sqlite"
)

type TableOptions struct {
	IndexAltFiles bool
}

type CommonTablesOptions struct {
	GeoJSON       *GeoJSONTableOptions // DEPRECATED
	IndexAltFiles bool
}

func CommonTablesWithDatabase(db sqlite.Database) ([]sqlite.Table, error) {

	geojson_opts, err := DefaultGeoJSONTableOptions()

	if err != nil {
		return nil, err
	}

	table_opts := &CommonTablesOptions{
		GeoJSON:       geojson_opts,
		IndexAltFiles: false,
	}

	return CommonTablesWithDatabaseAndOptions(db, table_opts)
}

func CommonTablesWithDatabaseAndOptions(db sqlite.Database, table_opts *CommonTablesOptions) ([]sqlite.Table, error) {

	to_index := make([]sqlite.Table, 0)

	var geojson_opts *GeoJSONTableOptions

	// table_opts.GeoJSON is deprecated but maintained for backwards compatbility
	// (20201224/thisisaaronland)

	if table_opts.GeoJSON != nil {
		geojson_opts = table_opts.GeoJSON
	} else {

		opts, err := DefaultGeoJSONTableOptions()

		if err != nil {
			return nil, err
		}

		opts.IndexAltFiles = table_opts.IndexAltFiles
		geojson_opts = opts
	}

	gt, err := NewGeoJSONTableWithDatabaseAndOptions(db, geojson_opts)

	if err != nil {
		return nil, err
	}

	to_index = append(to_index, gt)

	st_opts, err := DefaultSPRTableOptions()

	if err != nil {
		return nil, err
	}

	st_opts.IndexAltFiles = table_opts.IndexAltFiles

	st, err := NewSPRTableWithDatabaseAndOptions(db, st_opts)

	if err != nil {
		return nil, err
	}

	to_index = append(to_index, st)

	nm, err := NewNamesTableWithDatabase(db)

	if err != nil {
		return nil, err
	}

	to_index = append(to_index, nm)

	an, err := NewAncestorsTableWithDatabase(db)

	if err != nil {
		return nil, err
	}

	to_index = append(to_index, an)

	cn, err := NewConcordancesTableWithDatabase(db)

	if err != nil {
		return nil, err
	}

	to_index = append(to_index, cn)

	return to_index, nil
}

func SpatialTablesWithDatabase(db sqlite.Database) ([]sqlite.Table, error) {

	to_index := make([]sqlite.Table, 0)

	st, err := NewGeometriesTableWithDatabase(db)

	if err != nil {
		return nil, err
	}

	to_index = append(to_index, st)
	return to_index, nil
}

func PointInPolygonTablesWithDatabase(db sqlite.Database) ([]sqlite.Table, error) {

	to_index, err := SpatialTablesWithDatabase(db)

	if err != nil {
		return nil, err
	}

	gt, err := NewGeoJSONTableWithDatabase(db)

	if err != nil {
		return nil, err
	}

	to_index = append(to_index, gt)

	return to_index, nil
}

func SearchTablesWithDatabase(db sqlite.Database) ([]sqlite.Table, error) {

	opts := &TableOptions{
		IndexAltFiles: false,
	}

	return SearchTablesWithDatabaseAndOptions(db, opts)
}

func SearchTablesWithDatabaseAndOptions(db sqlite.Database, opts *TableOptions) ([]sqlite.Table, error) {

	to_index := make([]sqlite.Table, 0)

	st, err := NewSearchTableWithDatabase(db)

	if err != nil {
		return nil, err
	}

	to_index = append(to_index, st)
	return to_index, nil
}

func RTreeTablesWithDatabase(db sqlite.Database) ([]sqlite.Table, error) {

	opts := &TableOptions{
		IndexAltFiles: false,
	}

	return RTreeTablesWithDatabaseAndOptions(db, opts)
}

func RTreeTablesWithDatabaseAndOptions(db sqlite.Database, opts *TableOptions) ([]sqlite.Table, error) {

	// https://github.com/whosonfirst/go-whosonfirst-spatial-sqlite#databases

	to_index := make([]sqlite.Table, 0)

	rtree_opts, err := DefaultRTreeTableOptions()

	if err != nil {
		return nil, err
	}

	rtree_opts.IndexAltFiles = opts.IndexAltFiles

	rt, err := NewRTreeTableWithDatabaseAndOptions(db, rtree_opts)

	if err != nil {
		return nil, err
	}

	to_index = append(to_index, rt)

	sprt_opts, err := DefaultSPRTableOptions()

	if err != nil {
		return nil, err
	}

	sprt_opts.IndexAltFiles = opts.IndexAltFiles

	sprt, err := NewSPRTableWithDatabaseAndOptions(db, sprt_opts)

	if err != nil {
		return nil, err
	}

	to_index = append(to_index, sprt)

	props_opts, err := DefaultPropertiesTableOptions()

	if err != nil {
		return nil, err
	}

	props_opts.IndexAltFiles = opts.IndexAltFiles

	props, err := NewPropertiesTableWithDatabaseAndOptions(db, props_opts)

	if err != nil {
		return nil, err
	}

	to_index = append(to_index, props)

	geom_opts, err := DefaultGeometryTableOptions()

	if err != nil {
		return nil, err
	}

	geom_opts.IndexAltFiles = opts.IndexAltFiles

	geom, err := NewGeometryTableWithDatabaseAndOptions(db, geom_opts)

	if err != nil {
		return nil, err
	}

	to_index = append(to_index, geom)

	return to_index, nil
}
