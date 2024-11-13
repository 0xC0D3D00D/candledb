package storage

import (
	"encoding/binary"
	"fmt"
	"io"
	"time"

	"github.com/0xc0d3d00d/candledb/internal/domain"
	"github.com/spf13/afero"
)

type upsertCandleLog struct {
	timestamp time.Time
	fileKey   candleFileKey
	candle    *domain.Candle
	offset    int64
	written   bool
}

func encodeUpsertCandleLog(log upsertCandleLog) ([]byte, error) {
	tickerLen := len(log.fileKey.seriesKey.tickerId)
	logSize := 8 + // timestamp
		tickerLen +
		8 + // resolution
		8 + // timeRange.from
		8 + // timeRange.to
		candleByteSize +
		8 + // offset
		1 // written
	buf := make([]byte, logSize)

	timestamp := log.timestamp.UnixNano()

	binary.LittleEndian.PutUint64(buf, uint64(timestamp))

	n := copy(buf[8:], []byte(log.fileKey.seriesKey.tickerId))
	if n != tickerLen {
		return nil, io.ErrShortWrite
	}

	binary.LittleEndian.PutUint64(buf[8+tickerLen:], uint64(log.fileKey.seriesKey.resolution))
	binary.LittleEndian.PutUint64(buf[16+tickerLen:], uint64(log.fileKey.timeRange.from.UnixNano()))
	binary.LittleEndian.PutUint64(buf[24+tickerLen:], uint64(log.fileKey.timeRange.to.UnixNano()))

	n = copy(buf[32+tickerLen:], encodeCandle(log.candle))
	if n != candleByteSize {
		return nil, fmt.Errorf("failed to encode candle into wal log: %w", io.ErrShortWrite)
	}

	binary.LittleEndian.PutUint64(buf[32+tickerLen+candleByteSize:], uint64(log.offset))

	written := byte(0)
	if log.written {
		written = 1
	}

	buf[40+tickerLen+candleByteSize] = byte(written)

	return buf, nil
}

func (s *storage) writeCandleUpsert(fileKey candleFileKey, candle *domain.Candle, offset int64, written bool) error {
	if s.disableWal {
		return nil
	}

	log := upsertCandleLog{
		timestamp: time.Now(),
		fileKey:   fileKey,
		candle:    candle,
		offset:    offset,
		written:   written,
	}
	encodedLog, err := encodeUpsertCandleLog(log)
	if err != nil {
		return err
	}
	logLength := len(encodedLog)

	// FIXME: current wal
	var currentWal afero.File
	for _, wal := range s.walFiles {
		currentWal = wal
		break
	}

	_, err = currentWal.Seek(0, io.SeekEnd)
	if err != nil {
		return err
	}

	err = binary.Write(currentWal, binary.LittleEndian, uint64(logLength))
	if err != nil {
		return err
	}

	n, err := currentWal.Write(encodedLog)
	if n != logLength && err == nil {
		return io.ErrShortWrite
	}
	if err != nil {
		return err
	}

	return nil
}
