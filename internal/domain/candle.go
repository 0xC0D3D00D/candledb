package domain

import "time"

type Candle struct {
	Timestamp  time.Time
	TickerId   string
	Resolution Resolution
	Open       float64
	High       float64
	Low        float64
	Close      float64
	Volume     int64
}
