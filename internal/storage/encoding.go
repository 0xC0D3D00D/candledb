package storage

import (
	"encoding/binary"
	"errors"
	"math"
	"time"

	"github.com/0xc0d3d00d/candledb/internal/domain"
)

const candleByteSize = 49

var ErrCandleNotWritten = errors.New("candle not written")

func encodeCandle(candle *domain.Candle) []byte {
	buf := make([]byte, candleByteSize)

	timestamp := candle.Timestamp.UnixNano()

	binary.LittleEndian.PutUint64(buf, uint64(timestamp))
	binary.LittleEndian.PutUint64(buf[8:], math.Float64bits(candle.Open))
	binary.LittleEndian.PutUint64(buf[16:], math.Float64bits(candle.High))
	binary.LittleEndian.PutUint64(buf[24:], math.Float64bits(candle.Low))
	binary.LittleEndian.PutUint64(buf[32:], math.Float64bits(candle.Close))
	binary.LittleEndian.PutUint64(buf[40:], uint64(candle.Volume))
	// indicates the candle is written
	buf[48] = 1

	return buf
}

func decodeCandle(buf []byte, candle *domain.Candle) error {
	if len(buf) != candleByteSize {
		return errors.New("invalid buffer size")
	}
	if buf[48] == 0 {
		return ErrCandleNotWritten
	}

	timestamp := binary.LittleEndian.Uint64(buf[:8])
	candle.Timestamp = time.Unix(0, int64(timestamp)).UTC()
	candle.Open = math.Float64frombits(binary.LittleEndian.Uint64(buf[8:16]))
	candle.High = math.Float64frombits(binary.LittleEndian.Uint64(buf[16:24]))
	candle.Low = math.Float64frombits(binary.LittleEndian.Uint64(buf[24:32]))
	candle.Close = math.Float64frombits(binary.LittleEndian.Uint64(buf[32:40]))
	candle.Volume = int64(binary.LittleEndian.Uint64(buf[40:]))

	return nil
}
