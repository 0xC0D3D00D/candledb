package handler

import (
	"context"
	"time"

	"github.com/0xc0d3d00d/candledb/internal/domain"
)

// Interface requirements for the candle storage
type candledbClient interface {
	SaveCandles(ctx context.Context, candles []*domain.Candle) error
	GetCandles(ctx context.Context, tickerId string, resolution domain.Resolution, start_time time.Time, end_time time.Time) ([]*domain.Candle, error)
}
