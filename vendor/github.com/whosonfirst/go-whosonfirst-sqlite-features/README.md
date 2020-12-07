# go-whosonfirst-sqlite-features

Go package for working with Who's On First features and SQLite databases.

## Install

You will need to have both `Go` (specifically version [Go 1.12](https://golang.org/dl/) or higher) and the `make` programs installed on your computer. Assuming you do just type:

```
make tools
```

All of this package's dependencies are bundled with the code in the `vendor` directory.

## Tables

### ancestors

```
CREATE TABLE ancestors (
	id INTEGER NOT NULL,
	ancestor_id INTEGER NOT NULL,
	ancestor_placetype TEXT,
	lastmodified INTEGER
);

CREATE INDEX ancestors_by_id ON ancestors (id,ancestor_placetype,lastmodified);
CREATE INDEX ancestors_by_ancestor ON ancestors (ancestor_id,ancestor_placetype,lastmodified);
CREATE INDEX ancestors_by_lastmod ON ancestors (lastmodified);
```

### concordances

```
CREATE TABLE concordances (
	id INTEGER NOT NULL,
	concordance_id INTEGER NOT NULL,
	concordance_souce TEXT,
	lastmodified INTEGER
);

CREATE INDEX concordances_by_id ON concordances (id,lastmodified);
CREATE INDEX concordances_by_other ON concordances (other_source,other_id);	
CREATE INDEX concordances_by_other_lastmod ON concordances (other_source,other_id,lastmodified);
CREATE INDEX ancestors_by_lastmod ON concordances (lastmodified);`
```

### geojson

```
CREATE TABLE geojson (
	id INTEGER NOT NULL PRIMARY KEY,
	body TEXT,
	lastmodified INTEGER
);

CREATE INDEX geojson_by_lastmod ON geojson (lastmodified);
```

### geometries

```
CREATE TABLE geometries (
	id INTEGER NOT NULL PRIMARY KEY,
	is_alt TINYINT,
	type TEXT,
	lastmodified INTEGER
);

SELECT InitSpatialMetaData();
SELECT AddGeometryColumn('geometries', 'geom', 4326, 'GEOMETRY', 'XY');
SELECT CreateSpatialIndex('geometries', 'geom');

CREATE INDEX geometries_by_lastmod ON geometries (lastmodified);`
```

#### Notes

* In order to index the `geometries` table you will need to have the [Spatialite extension](https://www.gaia-gis.it/fossil/libspatialite/index) installed.
* As of Decemeber 2020, I am no longer able to make this (indexing the `geometries` table) work under OS X. I am not sure if this is a `spatialite` thing or a `go-sqlite3` thing or something else. Any help resolving this issue would be welcome.

### names

```
CREATE TABLE names (
       id INTEGER NOT NULL,
       placetype TEXT,
       country TEXT,
       language TEXT,
       extlang TEXT,
       script TEXT,
       region TEXT,
       variant TEXT,
       extension TEXT,
       privateuse TEXT,
       name TEXT,
       lastmodified INTEGER
);

CREATE INDEX names_by_lastmod ON names (lastmodified);
CREATE INDEX names_by_country ON names (country,privateuse,placetype);
CREATE INDEX names_by_language ON names (language,privateuse,placetype);
CREATE INDEX names_by_placetype ON names (placetype,country,privateuse);
CREATE INDEX names_by_name ON names (name, placetype, country);
CREATE INDEX names_by_name_private ON names (name, privateuse, placetype, country);
CREATE INDEX names_by_wofid ON names (id);
```

### rtree

```
CREATE VIRTUAL TABLE %s USING rtree (
		id,
		min_x,
		min_y,
		max_x,
		max_y,
		is_alt,
		lastmodified
	);
```

#### Notes

Section `3.1.1` of the [SQLite RTree documentation](https://www.sqlite.org/rtree.html) states:

> In the argments to "rtree" in the CREATE VIRTUAL TABLE statement, the names of the columns are taken from the first token of each argument. All subsequent tokens within each argument are silently ignored. This means, for example, that if you try to give a column a type affinity or add a constraint such as UNIQUE or NOT NULL or DEFAULT to a column, those extra tokens are accepted as valid, but they do not change the behavior of the rtree. In an RTREE virtual table, the first column always has a type affinity of INTEGER and all other data columns have a type affinity of NUMERIC. Recommended practice is to omit any extra tokens in the rtree specification. Let each argument to "rtree" be a single ordinary label that is the name of the corresponding column, and omit all other tokens from the argument list.

For example, a given row in the `rtree` table looks like this:

```
1477856011|-122.387908935547|37.6149787902832|-122.384384155273|37.6177368164062|0.0|1568838528.0
```

As of this writing you _should not try to index alternate geometries_ in the `rtree` table. Pending a decision about how and where to store (and query) alternate geometry labels, and how to reconcile them with the unique ID constraint, there is no mechanism to prevent the last feature in a set of primary and alternate geometries from being indexed.

### search

```
CREATE VIRTUAL TABLE search USING fts4(
	id, placetype,
	name, names_all, names_preferred, names_variant, names_colloquial,		
	is_current, is_ceased, is_deprecated, is_superseded
);
```

### spr

```
CREATE TABLE spr (
	id INTEGER NOT NULL PRIMARY KEY,
	parent_id INTEGER,
	name TEXT,
	placetype TEXT,
	country TEXT,
	repo TEXT,
	latitude REAL,
	longitude REAL,
	min_latitude REAL,
	min_longitude REAL,
	max_latitude REAL,
	max_longitude REAL,
	is_current INTEGER,
	is_deprecated INTEGER,
	is_ceased INTEGER,
	is_superseded INTEGER,
	is_superseding INTEGER,
	superseded_by TEXT,
	supersedes TEXT,
	lastmodified INTEGER
);

CREATE INDEX spr_by_lastmod ON spr (lastmodified);
CREATE INDEX spr_by_parent ON spr (parent_id, is_current, lastmodified);
CREATE INDEX spr_by_placetype ON spr (placetype, is_current, lastmodified);
CREATE INDEX spr_by_country ON spr (country, placetype, is_current, lastmodified);
CREATE INDEX spr_by_name ON spr (name, placetype, is_current, lastmodified);
CREATE INDEX spr_by_centroid ON spr (latitude, longitude, is_current, lastmodified);
CREATE INDEX spr_by_bbox ON spr (min_latitude, min_longitude, max_latitude, max_longitude, placetype, is_current, lastmodified);
CREATE INDEX spr_by_repo ON spr (repo, lastmodified);
CREATE INDEX spr_by_current ON spr (is_current, lastmodified);
CREATE INDEX spr_by_deprecated ON spr (is_deprecated, lastmodified);
CREATE INDEX spr_by_ceased ON spr (is_ceased, lastmodified);
CREATE INDEX spr_by_superseded ON spr (is_superseded, lastmodified);
CREATE INDEX spr_by_superseding ON spr (is_superseding, lastmodified);
CREATE INDEX spr_obsolete ON spr (is_deprecated, is_superseded);
```

## Custom tables

Sure. You just need to write a per-table package that implements the `Table` interface as described in [go-whosonfirst-sqlite](https://github.com/whosonfirst/go-whosonfirst-sqlite#custom-tables).

## Dependencies and relationships

These are documented in the [Dependencies and relationships section](https://github.com/whosonfirst/go-whosonfirst-sqlite#dependencies-and-relationships) of the `go-whosonfirst-sqlite` package.

## See also

* https://sqlite.org/
* https://www.gaia-gis.it/fossil/libspatialite/index
* https://github.com/whosonfirst/go-whosonfirst-sqlite
* https://github.com/whosonfirst/go-whosonfirst-sqlite-feature-index