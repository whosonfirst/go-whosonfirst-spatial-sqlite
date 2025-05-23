package server

import (
	"context"
	"fmt"

	"github.com/whosonfirst/go-whosonfirst-flags"
	"github.com/whosonfirst/go-whosonfirst-spatial-grpc/request"
	"github.com/whosonfirst/go-whosonfirst-spatial-grpc/spatial"
	app "github.com/whosonfirst/go-whosonfirst-spatial/application"
	"github.com/whosonfirst/go-whosonfirst-spatial/query"
	"github.com/whosonfirst/go-whosonfirst-spr/v2"
)

type SpatialServer struct {
	spatial.UnimplementedSpatialServer // Go is weird...
	app                                *app.SpatialApplication
}

func NewSpatialServer(app *app.SpatialApplication) (*SpatialServer, error) {

	s := &SpatialServer{
		app: app,
	}

	return s, nil
}

func (s *SpatialServer) PointInPolygon(ctx context.Context, req *spatial.PointInPolygonRequest) (*spatial.StandardPlacesResults, error) {

	if s.app.IsIndexing() {
		return nil, fmt.Errorf("Indexing")
	}

	pip_q := request.PIPRequestFromSpatialRequest(req)

	pip_fn, err := query.NewSpatialFunction(ctx, "pip://")

	if err != nil {
		return nil, fmt.Errorf("Failed to create pip:// spatial function, %w", err)
	}

	pip_rsp, err := query.ExecuteQuery(ctx, s.app.SpatialDatabase, pip_fn, pip_q)

	if err != nil {
		return nil, fmt.Errorf("Failed to perform point in polygon operation, %w", err)
	}

	results := pip_rsp.Results()
	count := len(results)

	grpc_results := make([]*spatial.StandardPlaceResponse, count)

	for idx, spr_rsp := range results {
		grpc_rsp := sprResponseToGRPCResponse(spr_rsp)
		grpc_results[idx] = grpc_rsp
	}

	grpc_rsp := &spatial.StandardPlacesResults{
		Places: grpc_results,
	}

	return grpc_rsp, nil
}

func (s *SpatialServer) PointInPolygonStream(req *spatial.PointInPolygonRequest, stream spatial.Spatial_PointInPolygonStreamServer) error {

	coord, err := request.CoordsFromPointInPolygonRequest(req)

	if err != nil {
		return fmt.Errorf("Failed to derive coordinate from request, %w", err)
	}

	f, err := request.SPRFilterFromPointInPolygonRequest(req)

	if err != nil {
		return fmt.Errorf("Failed to derive filter from request, %w", err)
	}

	ctx := context.Background()

	spatial_db := s.app.SpatialDatabase

	for spr_rsp, err := range spatial_db.PointInPolygonWithIterator(ctx, &coord, f) {

		if err != nil {
			return err
		}

		grpc_rsp := sprResponseToGRPCResponse(spr_rsp)
		err := stream.SendMsg(grpc_rsp)

		if err != nil {
			return err
		}
	}

	return nil
}

func sprResponseToGRPCResponse(spr_result spr.StandardPlacesResult) *spatial.StandardPlaceResponse {

	is_current := existentialFlagToProtobufExistentialFlag(spr_result.IsCurrent())
	is_ceased := existentialFlagToProtobufExistentialFlag(spr_result.IsCeased())
	is_deprecated := existentialFlagToProtobufExistentialFlag(spr_result.IsDeprecated())
	is_superseding := existentialFlagToProtobufExistentialFlag(spr_result.IsSuperseding())
	is_superseded := existentialFlagToProtobufExistentialFlag(spr_result.IsSuperseded())

	lat32 := float32(spr_result.Latitude())
	lon32 := float32(spr_result.Longitude())

	var inception string
	var cessation string

	if spr_result.Inception() != nil {
		inception = spr_result.Inception().String()
	}

	if spr_result.Cessation() != nil {
		cessation = spr_result.Cessation().String()
	}

	grpc_rsp := &spatial.StandardPlaceResponse{
		Id:            spr_result.Id(),
		ParentId:      spr_result.ParentId(),
		Placetype:     spr_result.Placetype(),
		Country:       spr_result.Country(),
		Repo:          spr_result.Repo(),
		Path:          spr_result.Path(),
		Uri:           spr_result.URI(),
		Latitude:      lat32,
		Longitude:     lon32,
		IsCurrent:     is_current,
		IsCeased:      is_ceased,
		IsDeprecated:  is_deprecated,
		IsSuperseding: is_superseding,
		IsSuperseded:  is_superseded,
		Supersedes:    spr_result.Supersedes(),
		SupersededBy:  spr_result.SupersededBy(),
		BelongsTo:     spr_result.BelongsTo(),
		LastModified:  spr_result.LastModified(),
		Name:          spr_result.Name(),
		InceptionDate: inception,
		CessationDate: cessation,
	}

	return grpc_rsp
}

func existentialFlagToProtobufExistentialFlag(fl flags.ExistentialFlag) spatial.ExistentialFlag {

	if !fl.IsKnown() {
		return spatial.ExistentialFlag_UNKNOWN
	}

	if !fl.IsTrue() {
		return spatial.ExistentialFlag_FALSE
	}

	return spatial.ExistentialFlag_TRUE
}
