package spatial

import (
	"github.com/skelterjohn/geom"
)

type PointInPolygonCandidate struct {
	Id string
	WOFId int64
	AltLabel string
	Bounds *geom.Rect
}

type PropertiesResponse map[string]interface{}

type PropertiesResponseResults struct {
	Properties []*PropertiesResponse `json:"properties"`
}

