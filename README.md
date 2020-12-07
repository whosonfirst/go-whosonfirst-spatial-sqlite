# go-whosonfirst-spatial-database-sqlite

## Important

This is work in progress. It may change, probably has bugs and isn't properly documented yet.

The goal is to have a package that conforms to the [database.SpatialDatabase](https://github.com/whosonfirst/go-whosonfirst-spatial#spatialdatabase) interface using [mattn/go-sqlite3](https://github.com/mattn/go-sqlite3) and SQLite's [RTree](https://www.sqlite.org/rtree.html) extension.

## Tools

### query

```
$> ./bin/query \
	-uri 'sqlite3://?dsn=/tmp/test.db' \
	-latitude 37.616951 \
	-longitude -122.383747 \
| jq \
| grep wof:path

      "wof:path": "115/939/625/7/1159396257.geojson",
      "wof:path": "115/939/628/3/1159396283.geojson",
      "wof:path": "147/785/565/5/1477855655.geojson",
      "wof:path": "115/939/616/5/1159396165.geojson",
      "wof:path": "115/939/613/3/1159396133.geojson",
      "wof:path": "115/939/613/1/1159396131.geojson",
      "wof:path": "115/915/732/7/1159157327.geojson",
      "wof:path": "115/939/612/1/1159396121.geojson",
      "wof:path": "115/939/617/1/1159396171.geojson",
      "wof:path": "136/052/154/3/1360521543.geojson",
      "wof:path": "115/939/614/9/1159396149.geojson",
      "wof:path": "136/052/154/5/1360521545.geojson",
      "wof:path": "115/939/632/9/1159396329.geojson",
      "wof:path": "115/955/482/7/1159554827.geojson",
      "wof:path": "115/939/610/9/1159396109.geojson",
      "wof:path": "115/955/482/9/1159554829.geojson",
      "wof:path": "147/785/560/7/1477855607.geojson",
      "wof:path": "115/955/480/3/1159554803.geojson",
      "wof:path": "115/915/732/5/1159157325.geojson",
      "wof:path": "115/939/632/1/1159396321.geojson",
      "wof:path": "147/785/560/5/1477855605.geojson",
      "wof:path": "115/939/633/3/1159396333.geojson",
      "wof:path": "115/939/631/9/1159396319.geojson",
      "wof:path": "115/939/633/7/1159396337.geojson",
```

## See also

* https://www.sqlite.org/rtree.html
* https://github.com/whosonfirst/go-whosonfirst-spatial
* github.com/whosonfirst/go-whosonfirst-sqlite
* github.com/whosonfirst/go-whosonfirst-sqlite-features