package handler

import (
	"context"
	"errors"
	"net/http"

	"connectrpc.com/connect"
	candledbv1 "github.com/0xc0d3d00d/candledb/apis/gen/go/okane/candledb/v1"
	"github.com/0xc0d3d00d/candledb/apis/gen/go/okane/candledb/v1/candledbv1connect"
	"github.com/0xc0d3d00d/candledb/internal/domain"
)

var ErrNotImplemented = errors.New("method not implemented")

type handler struct {
	candledb candledbClient
}

func NewHandler(candledb candledbClient) *handler {
	return &handler{
		candledb: candledb,
	}
}

func (h *handler) HTTPHandler(opts ...connect.HandlerOption) (string, http.Handler) {
	return candledbv1connect.NewCandleDBServiceHandler(h, opts...)
}

// Get a symbol information
func (h *handler) SaveCandles(ctx context.Context, req *connect.Request[candledbv1.SaveCandlesRequest]) (*connect.Response[candledbv1.SaveCandlesResponse], error) {
	err := h.candledb.SaveCandles(ctx, toDomainCandles(req.Msg.Candles))
	if err != nil {
		// FIXME: return correct error code
		return nil, errorToProto(err)
	}
	return connect.NewResponse(&candledbv1.SaveCandlesResponse{}), nil
}

func (h *handler) GetCandles(ctx context.Context, req *connect.Request[candledbv1.GetCandlesRequest]) (*connect.Response[candledbv1.GetCandlesResponse], error) {
	candles, err := h.candledb.GetCandles(
		ctx,
		req.Msg.TickerId,
		domain.Resolution(req.Msg.Resolution.AsDuration()),
		req.Msg.StartTime.AsTime(),
		req.Msg.EndTime.AsTime(),
	)

	if err != nil {
		// FIXME: return correct error code
		return nil, errorToProto(err)
	}

	return connect.NewResponse(&candledbv1.GetCandlesResponse{Candles: toProtoCandles(candles)}), nil
}
