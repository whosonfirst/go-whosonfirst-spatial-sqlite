# pip

```
$> ./bin/pip -h
Perform an point-in-polygon operation for an input latitude, longitude coordinate and on a set of Who's on First records stored in a spatial database.
Usage:
	 ./bin/pip [options]
Valid options are:

  -alternate-geometry value
    	One or more alternate geometry labels (wof:alt_label) values to filter results by.
  -cessation string
    	A valid EDTF date string.
  -custom-placetypes string
    	A JSON-encoded string containing custom placetypes defined using the syntax described in the whosonfirst/go-whosonfirst-placetypes repository.
  -enable-custom-placetypes
    	Enable wof:placetype values that are not explicitly defined in the whosonfirst/go-whosonfirst-placetypes repository.
  -geometries string
    	Valid options are: all, alt, default. (default "all")
  -inception string
    	A valid EDTF date string.
  -is-ceased value
    	One or more existential flags (-1, 0, 1) to filter results by.
  -is-current value
    	One or more existential flags (-1, 0, 1) to filter results by.
  -is-deprecated value
    	One or more existential flags (-1, 0, 1) to filter results by.
  -is-superseded value
    	One or more existential flags (-1, 0, 1) to filter results by.
  -is-superseding value
    	One or more existential flags (-1, 0, 1) to filter results by.
  -iterator-uri value
    	Zero or more URIs denoting data sources to use for indexing the spatial database at startup. URIs take the form of {ITERATOR_URI} + "#" + {PIPE-SEPARATED LIST OF ITERATOR SOURCES}. Where {ITERATOR_URI} is expected to be a registered whosonfirst/go-whosonfirst-iterate/v2 iterator (emitter) URI and {ITERATOR SOURCES} are valid input paths for that iterator. Supported whosonfirst/go-whosonfirst-iterate/v2 iterator schemes are: cwd://, directory://, featurecollection://, file://, filelist://, geojsonl://, null://, repo://.
  -latitude float
    	A valid latitude.
  -longitude float
    	A valid longitude.
  -mode string
    	Valid options are: cli, lambda. (default "cli")
  -placetype value
    	One or more place types to filter results by.
  -properties-reader-uri string
    	A valid whosonfirst/go-reader.Reader URI. Available options are: [fs:// null:// repo:// sqlite:// stdin://]. If the value is {spatial-database-uri} then the value of the '-spatial-database-uri' implements the reader.Reader interface and will be used.
  -property value
    	One or more Who's On First properties to append to each result.
  -sort-uri value
    	Zero or more whosonfirst/go-whosonfirst-spr/sort URIs.
  -spatial-database-uri string
    	A valid whosonfirst/go-whosonfirst-spatial/data.SpatialDatabase URI. options are: [rtree:// sqlite://] (default "rtree://")
  -verbose
    	Enable verbose (debug) logging.
```

## Example

```
$> ./bin/pip \
	-spatial-database-uri 'sqlite://sqlite3?dsn=/usr/local/data/sfomuseum-data-architecture.db' \
	-latitude 37.616951 \
	-longitude -122.383747 \
	-properties 'wof:hierarchy' \
	-properties 'sfomuseum:*' \
| jq

{
  "properties": [
    {
      "mz:is_ceased": 1,
      "mz:is_current": 0,
      "mz:is_deprecated": 0,
      "mz:is_superseded": 1,
      "mz:is_superseding": 1,
      "mz:latitude": 37.617475,
      "mz:longitude": -122.383371,
      "mz:max_latitude": 37.61950174060331,
      "mz:max_longitude": -122.38139655218178,
      "mz:min_latitude": 37.61615511156664,
      "mz:min_longitude": -122.3853565208227,
      "mz:uri": "https://data.whosonfirst.org/115/939/616/5/1159396165.geojson",
      "sfomuseum:is_sfo": 1,
      "sfomuseum:placetype": "terminal",
      "sfomuseum:terminal_id": "CENTRAL",
      "wof:country": "US",
      "wof:hierarchy": [
        {
          "building_id": 1159396339,
          "campus_id": 102527513,
          "continent_id": 102191575,
          "country_id": 85633793,
          "county_id": 102087579,
          "locality_id": 85922583,
          "neighbourhood_id": -1,
          "region_id": 85688637,
          "wing_id": 1159396165
        }
      ],
      "wof:id": 1159396165,
      "wof:lastmodified": 1547232162,
      "wof:name": "Central Terminal",
      "wof:parent_id": 1159396339,
      "wof:path": "115/939/616/5/1159396165.geojson",
      "wof:placetype": "wing",
      "wof:repo": "sfomuseum-data-architecture",
      "wof:superseded_by": [
        1159396149
      ],
      "wof:supersedes": [
        1159396171
      ]
    },

    ... and so on
   }
]   
```

## Filters

### Existential flags

It is possible to filter results by one or more existential flags (`-is-current`, `-is-ceased`, `-is-deprecated`, `-is-superseded`, `-is-superseding`). For example, this query for a point at SFO airport returns 24 possible candidates:

```
$> ./bin/pip \
	-spatial-database-uri 'sqlite://sqlite3?dsn=/usr/local/data/sfom-arch.db' \
	-latitude 37.616951 \
	-longitude -122.383747

| jq | grep wof:id | wc -l

2020/12/17 17:01:16 Time to point in polygon, 38.131108ms
      24
```

But when filtered using the `-is-current 1` flag there is only a single result:

```
$> ./bin/pip \
	-spatial-database-uri 'sqlite://sqlite3?dsn=/usr/local/data/sfom-arch.db' \
	-latitude 37.616951 \
	-longitude -122.383747 \
	-is-current 1

| jq

2020/12/17 17:00:11 Time to point in polygon, 46.401411ms
{
  "places": [
    {
      "wof:id": "1477855655",
      "wof:parent_id": "1477855607",
      "wof:name": "Terminal 2 Main Hall",
      "wof:country": "US",
      "wof:placetype": "concourse",
      "mz:latitude": 37.617044,
      "mz:longitude": -122.383533,
      "mz:min_latitude": 37.61569458544746,
      "mz:min_longitude": 37.617044,
      "mz:max_latitude": -122.3849257355292,
      "mz:max_longitude": -122.38294919235318,
      "mz:is_current": 1,
      "mz:is_deprecated": 0,
      "mz:is_ceased": 1,
      "mz:is_superseded": 0,
      "mz:is_superseding": 1,
      "wof:path": "147/785/565/5/1477855655.geojson",
      "wof:repo": "sfomuseum-data-architecture",
      "wof:lastmodified": 1569430965
    }
  ]
}
```

### Alternate geometries

You can also filter results to one or more specific alternate geometry labels. For example here are the `quattroshapes` and `whosonfirst-reversegeo` geometries for a point in the city of Montreal, using a SQLite database created from the `whosonfirst-data-admin-ca` database:

```
$> ./bin/pip \
	-spatial-database-uri 'sqlite://sqlite3?dsn=/usr/local/data/ca-alt.db' \
	-latitude 45.572744 \
	-longitude -73.586295 \
	-alternate-geometry quattroshapes \
	-alternate-geometry whosonfirst-reversegeo

| jq | grep wof:name

2020/12/17 16:52:08 Time to point in polygon, 419.727612ms
      "wof:name": "136251273 alt geometry (quattroshapes)",
      "wof:name": "85633041 alt geometry (whosonfirst-reversegeo)",
      "wof:name": "85874359 alt geometry (quattroshapes)",
```

Note: These examples assumes a database that was previously indexed using the [whosonfirst/go-whosonfirst-database-sqlite](https://github.com/whosonfirst/go-whosonfirst-database-sqlite) `wof-sqlite-index` tool. For example:

```
$> ./bin/wof-sqlite-index \
	-rtree \
	-spr \
	-properties \
	-dsn /tmp/test.db
	-mode repo:// \
	/usr/local/data/sfomuseum-data-architecture/
```

The exclude alternate geometries from query results pass the `-geometries default` flag:

```
$> ./bin/pip \
	-spatial-database-uri 'sqlite://sqlite3?dsn=/usr/local/data/ca-alt.db' \
	-latitude 45.572744 \
	-longitude -73.586295 \
	-geometries default

| jq | grep wof:name

2020/12/17 17:07:31 Time to point in polygon, 405.430776ms
      "wof:name": "Canada",
      "wof:name": "Saint-Leonard",
      "wof:name": "Quartier Port-Maurice",
      "wof:name": "Montreal",
      "wof:name": "Quebec",
```

To limit query results to _only_ alternate geometries pass the `-geometries alternate` flag:

```
$> ./bin/pip \
	-spatial-database-uri 'sqlite://sqlite3?dsn=/usr/local/data/ca-alt.db' \
	-latitude 45.572744 \
	-longitude -73.586295 \
	-geometries alternate

2020/12/17 17:07:39 Time to point in polygon, 366.347365ms
      "wof:name": "85874359 alt geometry (quattroshapes)",
      "wof:name": "85633041 alt geometry (naturalearth)",
      "wof:name": "85633041 alt geometry (naturalearth-display-terrestrial-zoom6)",
      "wof:name": "136251273 alt geometry (whosonfirst)",
      "wof:name": "136251273 alt geometry (quattroshapes)",
      "wof:name": "85633041 alt geometry (whosonfirst-reversegeo)",
```

## Remote databases

Support for remotely-hosted SQLite databases is available. For example:

```
$> go run -mod vendor cmd/pip/main.go \
	-spatial-database-uri 'sqlite://sqlite3?dsn=http://localhost:8080/sfomuseum-architecture.db' \
	-latitude 37.616951 \
	-longitude -122.383747 \
	-is-current 1 \

| json_pp | grep "wof:name"

         "wof:name" : "Terminal Two Arrivals",
         "wof:name" : "Terminal 2",
         "wof:name" : "SFO Terminal Complex",
         "wof:name" : "Terminal 2 Main Hall",
         "wof:name" : "SFO Terminal Complex",
```

_Big thanks to @psanford 's [sqlitevfshttp](https://github.com/psanford/sqlite3vfshttp) package for making this possible._
