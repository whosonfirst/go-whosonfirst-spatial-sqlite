package sqlite

import (
	"github.com/whosonfirst/go-whosonfirst-spr"
	"github.com/whosonfirst/go-whosonfirst-flags"
	"github.com/whosonfirst/go-whosonfirst-flags/existential"	
)

type SQLiteStandardPlacesResult struct {
	spr.StandardPlacesResult
	id                    string  `json:"wof:id"`	
	parent_id                    string  `json:"wof:parent_id"`
	name                  string  `json:"wof:name"`
	placetype             string  `json:"wof:placetype"`
	latitude               float64 `json:"mz:latitude"`
	longitude              float64 `json:"mz:longitude"`
	min_latitude            float64 `json:"spr:min_latitude"`
	min_longitude           float64 `json:"mz:min_longitude"`
	max_latitude            float64 `json:"mz:max_latitude"`
	max_longitude           float64 `json:"mz:max_longitude"`
	is_current int64 `json:"mz:is_current"`
	is_deprecated int64 `json:"mz:is_deprecated"`
	is_ceased int64 `json:"mz:is_ceased"`
	is_superseded int64 `json:"mz:is_superseded"`
	is_superseding int64 `json:"mz:is_superseding"`	
	path                  string  `json:"wof:path"`
	repo                  string  `json:"wof:repo"`
	lastmodified int64 `json:wof:lastmodified"`
}

func (spr *SQLiteStandardPlacesResult) Id() string {
	return spr.id
}

func (spr *SQLiteStandardPlacesResult) ParentId() string {
	return spr.parent_id
}

func (spr *SQLiteStandardPlacesResult) Name() string {
	return spr.name
}

func (spr *SQLiteStandardPlacesResult) Placetype() string {
	return spr.placetype
}

func (spr *SQLiteStandardPlacesResult) Country() string {
	return "XX"
}

func (spr *SQLiteStandardPlacesResult) Repo() string {
	return spr.repo
}

func (spr *SQLiteStandardPlacesResult) Path() string {
	return spr.path
}

func (spr *SQLiteStandardPlacesResult) URI() string {
	return ""
}

func (spr *SQLiteStandardPlacesResult) Latitude() float64 {
	return spr.latitude
}

func (spr *SQLiteStandardPlacesResult) Longitude() float64 {
	return spr.longitude
}

func (spr *SQLiteStandardPlacesResult) MinLatitude() float64 {
	return spr.min_latitude
}

func (spr *SQLiteStandardPlacesResult) MinLongitude() float64 {
	return spr.min_longitude
}

func (spr *SQLiteStandardPlacesResult) MaxLatitude() float64 {
	return spr.max_latitude
}

func (spr *SQLiteStandardPlacesResult) MaxLongitude() float64 {
	return spr.max_longitude
}

func (spr *SQLiteStandardPlacesResult) IsCurrent() flags.ExistentialFlag {
	return existentialFlag(spr.is_current)
}

func (spr *SQLiteStandardPlacesResult) IsCeased() flags.ExistentialFlag {
	return existentialFlag(spr.is_ceased)
}

func (spr *SQLiteStandardPlacesResult) IsDeprecated() flags.ExistentialFlag {
	return existentialFlag(spr.is_deprecated)
}

func (spr *SQLiteStandardPlacesResult) IsSuperseded() flags.ExistentialFlag {
	return existentialFlag(spr.is_superseded)
}

func (spr *SQLiteStandardPlacesResult) IsSuperseding() flags.ExistentialFlag {
	return existentialFlag(spr.is_superseding)
}

func (spr *SQLiteStandardPlacesResult) SupersededBy() []int64 {
	return []int64{}
}

func (spr *SQLiteStandardPlacesResult) Supersedes() []int64 {
	return []int64{}
}

func (spr *SQLiteStandardPlacesResult) LastModified() int64 {
	return spr.lastmodified
}

func existentialFlag(i int64) flags.ExistentialFlag {
	fl, _ := existential.NewKnownUnknownFlag(i)
	return fl
}
