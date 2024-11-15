package storage

import (
	"testing"
	"time"

	"github.com/0xc0d3d00d/candledb/internal/domain"
	"github.com/stretchr/testify/assert"
)

func TestEncodeCandle(t *testing.T) {
	candle := domain.Candle{
		Timestamp: time.Date(2024, 1, 1, 0, 0, 0, 0, time.UTC),
		Open:      1.0,
		High:      2.0,
		Low:       0.5,
		Close:     1.5,
		Volume:    100,
	}
	expected := []byte{
		0x0, 0x0, 0x65, 0x1, 0x17, 0x10, 0xa6, 0x17, // timestamp
		0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0xf0, 0x3f, // open
		0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x40, // high
		0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0xe0, 0x3f, // low
		0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0xf8, 0x3f, // close
		0x64, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, // volume
		0x1, // write flag
	}
	buf := encodeCandle(&candle)

	assert.Equal(t, expected, buf)
}

func TestDecodeCandle(t *testing.T) {
	buf := []byte{
		0x0, 0x0, 0x65, 0x1, 0x17, 0x10, 0xa6, 0x17, // timestamp
		0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0xf0, 0x3f, // open
		0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x40, // high
		0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0xe0, 0x3f, // low
		0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0xf8, 0x3f, // close
		0x64, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, // volume
		0x1, // write flag
	}

	expectedCandle := domain.Candle{
		Timestamp: time.Date(2024, 1, 1, 0, 0, 0, 0, time.UTC),
		Open:      1.0,
		High:      2.0,
		Low:       0.5,
		Close:     1.5,
		Volume:    100,
	}

	var candle domain.Candle
	err := decodeCandle(buf, &candle)
	assert.NoError(t, err)
	assert.Equal(t, expectedCandle, candle)
}
