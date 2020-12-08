# go-whosonfirst-spatial-database-sqlite

## Important

This is work in progress. It may change, probably has bugs and isn't properly documented yet.

The goal is to have a package that conforms to the [database.SpatialDatabase](https://github.com/whosonfirst/go-whosonfirst-spatial#spatialdatabase) interface using [mattn/go-sqlite3](https://github.com/mattn/go-sqlite3) and SQLite's [RTree](https://www.sqlite.org/rtree.html) extension.

## Tools

### query

```
$> ./bin/query \
	-database-uri 'sqlite://?dsn=/usr/local/data/sfomuseum-data-architecture.db' \
	-properties-uri 'sqlite://?dsn=/usr/local/data/sfomuseum-data-architecture.db' \
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

Note: This assumes a database that was previously indexed using the [whosonfirst/go-whosonfirst-sqlite-features](https://github.com/whosonfirst/go-whosonfirst-sqlite-features) `wof-sqlite-index-features` tool. For example:

```
$> ./bin/wof-sqlite-index-features -rtree -geojson -dsn /tmp/test.db -mode repo:// /usr/local/data/sfomuseum-data-architecture/
```

## See also

* https://www.sqlite.org/rtree.html
* https://github.com/whosonfirst/go-whosonfirst-spatial
* https://github.com/whosonfirst/go-whosonfirst-sqlite
* https://github.com/whosonfirst/go-whosonfirst-sqlite-features