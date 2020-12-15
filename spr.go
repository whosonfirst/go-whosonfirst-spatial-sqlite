package sqlite

import (
	"github.com/whosonfirst/go-whosonfirst-flags"
	"github.com/whosonfirst/go-whosonfirst-flags/existential"
	"github.com/whosonfirst/go-whosonfirst-spr"
)

type SQLiteStandardPlacesResult struct {
	spr.StandardPlacesResult `json:",omitempty"`
	WOFId                    string  `json:"wof:id"`
	WOFParentId              string  `json:"wof:parent_id"`
	WOFName                  string  `json:"wof:name"`
	WOFCountry               string  `json:"wof:country"`
	WOFPlacetype             string  `json:"wof:placetype"`
	MZLatitude               float64 `json:"mz:latitude"`
	MZLongitude              float64 `json:"mz:longitude"`
	MZMinLatitude            float64 `json:"mz:min_latitude"`
	MZMinLongitude           float64 `json:"mz:min_longitude"`
	MZMaxLatitude            float64 `json:"mz:max_latitude"`
	MZMaxLongitude           float64 `json:"mz:max_longitude"`
	MZIsCurrent              int64   `json:"mz:is_current"`
	MZIsDeprecated           int64   `json:"mz:is_deprecated"`
	MZIsCeased               int64   `json:"mz:is_ceased"`
	MZIsSuperseded           int64   `json:"mz:is_superseded"`
	MZIsSuperseding          int64   `json:"mz:is_superseding"`

	// supersedes and superseding need to be added here pending
	// https://github.com/whosonfirst/go-whosonfirst-sqlite-features/issues/14

	WOFPath         string `json:"wof:path"`
	WOFRepo         string `json:"wof:repo"`
	WOFLastModified int64  `json:"wof:lastmodified"`
}

func (spr *SQLiteStandardPlacesResult) Id() string {
	return spr.WOFId
}

func (spr *SQLiteStandardPlacesResult) ParentId() string {
	return spr.WOFParentId
}

func (spr *SQLiteStandardPlacesResult) Name() string {
	return spr.WOFName
}

func (spr *SQLiteStandardPlacesResult) Placetype() string {
	return spr.WOFPlacetype
}

func (spr *SQLiteStandardPlacesResult) Country() string {
	return spr.WOFCountry
}

func (spr *SQLiteStandardPlacesResult) Repo() string {
	return spr.WOFRepo
}

func (spr *SQLiteStandardPlacesResult) Path() string {
	return spr.WOFPath
}

func (spr *SQLiteStandardPlacesResult) URI() string {
	return ""
}

func (spr *SQLiteStandardPlacesResult) Latitude() float64 {
	return spr.MZLatitude
}

func (spr *SQLiteStandardPlacesResult) Longitude() float64 {
	return spr.MZLongitude
}

func (spr *SQLiteStandardPlacesResult) MinLatitude() float64 {
	return spr.MZMinLatitude
}

func (spr *SQLiteStandardPlacesResult) MinLongitude() float64 {
	return spr.MZMinLongitude
}

func (spr *SQLiteStandardPlacesResult) MaxLatitude() float64 {
	return spr.MZMaxLatitude
}

func (spr *SQLiteStandardPlacesResult) MaxLongitude() float64 {
	return spr.MZMaxLongitude
}

func (spr *SQLiteStandardPlacesResult) IsCurrent() flags.ExistentialFlag {
	return existentialFlag(spr.MZIsCurrent)
}

func (spr *SQLiteStandardPlacesResult) IsCeased() flags.ExistentialFlag {
	return existentialFlag(spr.MZIsCeased)
}

func (spr *SQLiteStandardPlacesResult) IsDeprecated() flags.ExistentialFlag {
	return existentialFlag(spr.MZIsDeprecated)
}

func (spr *SQLiteStandardPlacesResult) IsSuperseded() flags.ExistentialFlag {
	return existentialFlag(spr.MZIsSuperseded)
}

func (spr *SQLiteStandardPlacesResult) IsSuperseding() flags.ExistentialFlag {
	return existentialFlag(spr.MZIsSuperseding)
}

// https://github.com/whosonfirst/go-whosonfirst-sqlite-features/issues/14

func (spr *SQLiteStandardPlacesResult) SupersededBy() []int64 {
	return []int64{}
}

// https://github.com/whosonfirst/go-whosonfirst-sqlite-features/issues/14

func (spr *SQLiteStandardPlacesResult) Supersedes() []int64 {
	return []int64{}
}

func (spr *SQLiteStandardPlacesResult) LastModified() int64 {
	return spr.WOFLastModified
}

func existentialFlag(i int64) flags.ExistentialFlag {
	fl, _ := existential.NewKnownUnknownFlag(i)
	return fl
}
