package geojson

import (
	"github.com/skelterjohn/geom"
)

// would that we could have a single all-purpose geojson interface
// but since we don't necessarily know what *kind* of geometry we're
// dealing with (I mean here we do but that's not the point) it's
// not clear what we say gets returned by a Coordinates() method in
// a Geometry interface - is it really even that important? I suppose
// it would be nice to be able to have GetCandidatesByCoord return
// an interface-thingy but for now this will do (20170822/thisisaaronland)

// to whit: what is the relationship between this and all the GeoJSON
// structs in cache/utils.go... I am not sure yet (20170921/thisisaaronland)

type GeoJSONPoint []float64

type GeoJSONRing []GeoJSONPoint

type GeoJSONPolygon []GeoJSONRing

type GeoJSONMultiPolygon []GeoJSONPolygon

type GeoJSONGeometry struct {
	Type        string              `json:"type"`
	Coordinates GeoJSONMultiPolygon `json:"coordinates"`
}

type GeoJSONProperties interface{}

type GeoJSONFeature struct {
	Type       string            `json:"type"`
	Geometry   GeoJSONGeometry   `json:"geometry"`
	Properties GeoJSONProperties `json:"properties"`
}

type GeoJSONFeatureCollection struct {
	Type       string           `json:"type"`
	Features   []GeoJSONFeature `json:"features"`
	Pagination Pagination       `json:"pagination,omitempty"`
}

type GeoJSONFeatureCollectionSet struct {
	Type        string                      `json:"type"`
	Collections []*GeoJSONFeatureCollection `json:"features"`
	Pagination  Pagination                  `json:"pagination,omitempty"`
}

type Pagination struct {
	TotalCount int `json:"total_count"`
	Page       int `json:"page"`
	PerPage    int `json:"per_page"`
	PageCount  int `json:"page_count"`
}

func (g GeoJSONGeometry) ContainsCoordinate(c geom.Coord) bool {

	switch g.Type {
	case "Polygon", "MultiPolygon":
		return g.Coordinates.ContainsCoordinate(c)
	default:
		return false
	}
}

func (mp GeoJSONMultiPolygon) ContainsCoordinate(c geom.Coord) bool {
	
	for _, poly := range mp {

		if poly.ContainsCoordinate(c) {
			return true
		}
	}

	return false
}

func (p GeoJSONPolygon) ContainsCoordinate(c geom.Coord) bool {

	if !p[0].ContainsCoordinate(c){
		return false
	}

	if len(p) == 1 {
		return false
	}

	for _, r := range p[1:] {

		if r.ContainsCoordinate(c){
			return false
		}
	}

	return true
}

func (r GeoJSONRing) ContainsCoordinate(c geom.Coord) bool {

	path := geom.Polygon{}

	for _, pt := range r {
		path.AddVertex(geom.Coord{X: pt[0], Y: pt[1]})
	}

	return path.ContainsCoord(c)
}
