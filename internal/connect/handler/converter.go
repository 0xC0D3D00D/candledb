package handler

import (
	"time"

	candledbv1 "github.com/0xc0d3d00d/candledb/apis/gen/go/okane/candledb/v1"
	"github.com/0xc0d3d00d/candledb/internal/domain"
	"google.golang.org/protobuf/types/known/durationpb"
	"google.golang.org/protobuf/types/known/timestamppb"
)

func toDomainCandles(cc []*candledbv1.Candle) []*domain.Candle {
	if cc == nil {
		return nil
	}

	candles := make([]*domain.Candle, 0, len(cc))
	for _, candle := range cc {
		candles = append(candles, toDomainCandle(candle))
	}

	return candles
}

func toDomainCandle(c *candledbv1.Candle) *domain.Candle {
	if c == nil {
		return nil
	}

	return &domain.Candle{
		Timestamp:  c.Timestamp.AsTime(),
		TickerId:   c.TickerId,
		Resolution: domain.Resolution(c.Resolution.AsDuration()),
		Open:       c.Open,
		High:       c.High,
		Low:        c.Low,
		Close:      c.Close,
		Volume:     c.Volume,
	}
}

func toProtoCandles(cc []*domain.Candle) []*candledbv1.Candle {
	if cc == nil {
		return nil
	}

	candles := make([]*candledbv1.Candle, 0, len(cc))
	for _, candle := range cc {
		candles = append(candles, toProtoCandle(candle))
	}

	return candles
}

func toProtoCandle(c *domain.Candle) *candledbv1.Candle {
	if c == nil {
		return nil
	}

	return &candledbv1.Candle{
		Timestamp:  timestamppb.New(c.Timestamp),
		TickerId:   c.TickerId,
		Resolution: durationpb.New(time.Duration(c.Resolution)),
		Open:       c.Open,
		High:       c.High,
		Low:        c.Low,
		Close:      c.Close,
		Volume:     c.Volume,
	}
}
