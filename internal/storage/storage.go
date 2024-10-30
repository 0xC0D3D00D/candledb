package storage

import (
	"context"
	"fmt"
	"io"
	"path"
	"sort"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/0xc0d3d00d/candledb/internal/domain"
	"github.com/spf13/afero"
)

type set[M comparable] map[M]struct{}

func (s set[M]) add(v M) {
	s[v] = struct{}{}
}

const oneDay = time.Hour * 24

type timeRange struct {
	from time.Time
	to   time.Time
}

func (tr timeRange) contains(t time.Time) bool {
	return tr.from.Before(t) && tr.to.After(t)
}

func (t1 timeRange) containsRange(t2 timeRange) bool {
	return (t1.from.Equal(t2.from) || t1.from.Before(t2.from)) && t1.to.After(t2.to)
}

type seriesKey struct {
	tickerId   string
	resolution domain.Resolution
}

type candleFileKey struct {
	seriesKey seriesKey
	timeRange timeRange
}

type storage struct {
	fs               afero.Fs
	dataDir          string
	walDir           string
	snapshotDir      string
	walFiles         map[string]afero.File
	candleFiles      map[candleFileKey]afero.File
	candleFilesMutex *sync.RWMutex
	// TODO: build binary search tree for time ranges
	timeRanges       []timeRange
	chunkCandleCount int
}

// wals
// - 0000000001.wal
// data
// - candles
//   - tickerId_resolution
//     - timestamp1_timestamp2.bin
// - snapshot
//   - snapshot-1730155456000.tar.gz

func NewStorage(rootDir string, chunkCandleCount int) (*storage, error) {
	fs := afero.NewOsFs()

	rootExists, err := afero.DirExists(fs, rootDir)
	if err != nil {
		return nil, err
	}

	walDir := path.Join(rootDir, "wal")
	dataDir := path.Join(rootDir, "data")
	snapshotDir := path.Join(rootDir, "snapshot")

	if !rootExists {
		err = fs.MkdirAll(walDir, 0755)
		if err != nil {
			return nil, fmt.Errorf("failed to create wal directory: %w", err)
		}

		err = fs.MkdirAll(dataDir, 0755)
		if err != nil {
			return nil, fmt.Errorf("failed to create data directory: %w", err)
		}

		err = fs.MkdirAll(snapshotDir, 0755)
		if err != nil {
			return nil, fmt.Errorf("failed to create snapshot directory: %w", err)
		}
	}

	wals, err := afero.ReadDir(fs, walDir)
	if err != nil {
		return nil, fmt.Errorf("failed to read wal directory: %w", err)
	}

	walFiles := make(map[string]afero.File, len(wals)+1)
	for _, wal := range wals {
		walFile, err := fs.Open(path.Join(walDir, wal.Name()))
		if err != nil {
			return nil, fmt.Errorf("failed to open wal `%s` file: %w", wal.Name(), err)
		}

		walFiles[wal.Name()] = walFile
	}

	if len(wals) == 0 {
		filename := fmt.Sprintf("%010d.wal", 1)
		walFile, err := fs.Create(path.Join(walDir, filename))
		if err != nil {
			return nil, fmt.Errorf("failed to create wal file: %w", err)
		}

		walFiles[filename] = walFile
	}

	tickerIdResolutions, err := afero.ReadDir(fs, dataDir)

	candleFiles := make(map[candleFileKey]afero.File)
	for _, tickerIdResolution := range tickerIdResolutions {
		tickerIdResolutionDir := path.Join(dataDir, tickerIdResolution.Name())
		candles, err := afero.ReadDir(fs, tickerIdResolutionDir)
		if err != nil {
			return nil, fmt.Errorf("failed to read candles directory: %w", err)
		}

		keyParts := strings.Split(tickerIdResolution.Name(), "_")

		// FIXME: server should not stop starting up for this
		if len(keyParts) != 2 {
			return nil, fmt.Errorf("invalid key parts: %s", tickerIdResolution.Name())
		}

		tickerId := keyParts[0]
		resolution, err := domain.ParseResolution(keyParts[1])

		if err != nil {
			return nil, fmt.Errorf("failed to parse resolution: %w", err)
		}

		for _, candle := range candles {
			candleFilename := strings.TrimSuffix(candle.Name(), path.Ext(candle.Name()))
			rangeParts := strings.Split(candleFilename, "_")

			// FIXME: server should not stop starting up for this
			if len(rangeParts) != 2 {
				return nil, fmt.Errorf("invalid candle file name: %s", candle.Name())
			}

			from, err := strconv.ParseInt(rangeParts[0], 10, 64)
			if err != nil {
				return nil, fmt.Errorf("failed to parse from timestamp: %w", err)
			}

			to, err := strconv.ParseInt(rangeParts[1], 10, 64)
			if err != nil {
				return nil, fmt.Errorf("failed to parse to timestamp: %w", err)
			}

			candleFile, err := fs.Open(path.Join(tickerIdResolutionDir, candle.Name()))
			if err != nil {
				return nil, fmt.Errorf("failed to open candle file: %w", err)
			}

			key := candleFileKey{
				seriesKey: seriesKey{
					tickerId:   tickerId,
					resolution: resolution,
				},
				timeRange: timeRange{
					from: time.UnixMicro(from),
					to:   time.UnixMicro(to),
				},
			}

			candleFiles[key] = candleFile
		}
	}

	timeRanges := make([]timeRange, 0, len(candleFiles))
	for candleKey := range candleFiles {
		timeRanges = append(timeRanges, candleKey.timeRange)
	}

	sort.Slice(timeRanges, func(i, j int) bool {
		return timeRanges[i].from.Before(timeRanges[j].from)
	})

	return &storage{
		fs:          fs,
		dataDir:     dataDir,
		walDir:      walDir,
		snapshotDir: snapshotDir,
		walFiles:    walFiles,
		candleFiles: candleFiles,

		candleFilesMutex: &sync.RWMutex{},

		timeRanges:       timeRanges,
		chunkCandleCount: chunkCandleCount,
	}, nil
}

func (s *storage) GetCandles(ctx context.Context, tickerId string, resolution domain.Resolution, from time.Time, to time.Time) ([]*domain.Candle, error) {
	panic("not implemented")
}

func (s *storage) SaveCandles(ctx context.Context, candles []*domain.Candle) error {
	candleBySeries := make(map[seriesKey][]*domain.Candle)
	for _, candle := range candles {
		key := seriesKey{
			tickerId:   candle.TickerId,
			resolution: candle.Resolution,
		}
		if _, ok := candleBySeries[key]; !ok {
			candleBySeries[key] = []*domain.Candle{}
		}
		candleBySeries[key] = append(candleBySeries[key], candle)
	}

	err := s.allocateCandleFiles(ctx, candleBySeries)
	if err != nil {
		return err
	}

	return nil
}

func (s *storage) allocateCandleFiles(ctx context.Context, candleBySeries map[seriesKey][]*domain.Candle) error {
	seriesKeys := s.buildSeriesPlan(ctx, candleBySeries)

	for _, key := range seriesKeys {
		err := s.allocateCandleFile(ctx, key)
		if err != nil {
			return err
		}
	}
	return nil
}

func (s *storage) allocateCandleFile(ctx context.Context, key candleFileKey) error {
	filename := path.Join(
		s.dataDir,
		fmt.Sprintf("%s_%s", key.seriesKey.tickerId, key.seriesKey.resolution),
		fmt.Sprintf("%d_%d.bin", key.timeRange.from.UnixMicro(), key.timeRange.to.UnixMicro()),
	)

	candleFile, err := s.fs.Create(filename)
	if err != nil {
		return fmt.Errorf("failed to create candle file: %w", err)
	}

	zeros := make([]byte, candleByteSize)
	for i := 0; i < s.chunkCandleCount; i++ {
		n, err := candleFile.Write(zeros)
		if n != candleByteSize && err == nil {
			err = io.ErrShortWrite
		}
		if err != nil {
			return fmt.Errorf("failed to write zeros to the candle file: %w", err)
		}
	}

	s.candleFilesMutex.Lock()
	s.candleFiles[key] = candleFile
	s.candleFilesMutex.Unlock()

	return nil
}

func (s *storage) buildSeriesPlan(ctx context.Context, candleBySeries map[seriesKey][]*domain.Candle) []candleFileKey {
	candleFileKeys := []candleFileKey{}
	for series, candles := range candleBySeries {
		ranges := s.buildMissingRanges(ctx, series, candles)

		for _, r := range ranges {
			candleFileKeys = append(candleFileKeys, candleFileKey{
				seriesKey: series,
				timeRange: r,
			})
		}
	}

	return candleFileKeys
}

func (s *storage) buildMissingRanges(ctx context.Context, series seriesKey, candles []*domain.Candle) []timeRange {
	uncoveredTimestamps := []time.Time{}

	for _, candle := range candles {
		contained := false
		for _, timeRange := range s.timeRanges {
			if timeRange.from.After(candle.Timestamp) {
				break
			}
			if timeRange.contains(candle.Timestamp) {
				contained = true
				break
			}
		}

		if !contained {
			uncoveredTimestamps = append(uncoveredTimestamps, candle.Timestamp)
		}
	}

	sort.Slice(uncoveredTimestamps, func(i, j int) bool {
		return uncoveredTimestamps[i].Before(uncoveredTimestamps[j])
	})

	chunkSize := s.chunkSize(series.resolution)

	// For large time ranges, we store them by year
	if chunkSize > oneDay {
		return buildYearlyRanges(uncoveredTimestamps)
	}

	timeRangeSet := map[timeRange]struct{}{}
	for _, t := range uncoveredTimestamps {
		timeRangeSet[timeRange{
			from: t.Truncate(chunkSize),
			to:   t.Truncate(chunkSize).Add(chunkSize),
		}] = struct{}{}
	}

	timeRanges := make([]timeRange, 0, len(timeRangeSet))
	for tr := range timeRangeSet {
		timeRanges = append(timeRanges, tr)
	}

	return timeRanges
}

func buildYearlyRanges(timestamps []time.Time) []timeRange {
	years := make(set[int])

	for _, t := range timestamps {
		years.add(t.Year())
	}

	timeRanges := []timeRange{}
	for year := range years {
		timeRanges = append(timeRanges, timeRange{
			from: time.Date(year, 1, 1, 0, 0, 0, 0, time.UTC),
			to:   time.Date(year+1, 1, 1, 0, 0, 0, 0, time.UTC),
		})
	}

	return timeRanges
}

func (s *storage) chunkSize(resolution domain.Resolution) time.Duration {
	return time.Duration(resolution) * time.Duration(s.chunkCandleCount)
}

func (s *storage) doesRangeExists(timestamp time.Time) bool {
	for _, tr := range s.timeRanges {
		if timestamp.After(tr.to) {
			break
		}
		if timestamp.After(tr.from) && timestamp.Before(tr.to) {
			return true
		}
	}

	return false
}
