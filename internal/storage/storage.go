package storage

import (
	"context"
	"fmt"
	"io"
	"log/slog"
	"path"
	"sort"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/0xc0d3d00d/candledb/internal/domain"
	"github.com/spf13/afero"
)

var ErrNoCandlesFound = fmt.Errorf("%w: no candles found", domain.ErrNotFound)

type set[M comparable] map[M]struct{}

func (s set[M]) add(v M) {
	s[v] = struct{}{}
}

const oneWeek = time.Hour * 24 * 7

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

func (t1 timeRange) intersects(t2 timeRange) bool {
	// t2.from < t1.from < t2.to
	// t2.from < t1.to   < t2.to
	// OR
	// t1.from < t2.from < t1.to
	// t1.from < t2.to   < t1.to

	return (t1.from.After(t2.from) && t1.from.Before(t2.to)) ||
		(t1.to.After(t2.from) && t1.from.Before(t2.to)) ||
		(t2.from.After(t1.from) && t2.from.Before(t1.to)) ||
		(t2.to.After(t1.from) && t2.from.Before(t1.to))

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
	seriesTimeRanges map[seriesKey][]timeRange
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
		slog.Debug("tickerIdResolution", "name", tickerIdResolution.Name())
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
					from: time.UnixMicro(from).UTC(),
					to:   time.UnixMicro(to).UTC(),
				},
			}
			slog.Debug("candleFileKey", "key", key)

			candleFiles[key] = candleFile
		}
	}

	timeRanges := make(map[seriesKey][]timeRange)
	for candleKey := range candleFiles {
		if _, ok := timeRanges[candleKey.seriesKey]; !ok {
			timeRanges[candleKey.seriesKey] = []timeRange{}
		}
		timeRanges[candleKey.seriesKey] = append(timeRanges[candleKey.seriesKey], candleKey.timeRange)
	}

	for _, ranges := range timeRanges {
		sort.Slice(ranges, func(i, j int) bool {
			return ranges[i].from.Before(ranges[j].from)
		})
	}

	return &storage{
		fs:          fs,
		dataDir:     dataDir,
		walDir:      walDir,
		snapshotDir: snapshotDir,
		walFiles:    walFiles,
		candleFiles: candleFiles,

		candleFilesMutex: &sync.RWMutex{},

		seriesTimeRanges: timeRanges,
		chunkCandleCount: chunkCandleCount,
	}, nil
}

func (s *storage) GetCandles(ctx context.Context, tickerId string, resolution domain.Resolution, from time.Time, to time.Time) ([]*domain.Candle, error) {
	slog.DebugContext(ctx, "get candles", "ticker_id", tickerId, "resolution", resolution, "from", from, "to", to)
	seriesKey := seriesKey{
		tickerId:   tickerId,
		resolution: resolution,
	}

	selectRange := timeRange{from: from, to: to}
	timeRanges := make([]timeRange, 0, len(s.seriesTimeRanges[seriesKey]))
	for _, tr := range s.seriesTimeRanges[seriesKey] {
		slog.DebugContext(ctx, "time range", "from", tr.from, "to", tr.to)
		if tr.intersects(selectRange) {
			timeRanges = append(timeRanges, tr)
		}
	}

	slog.DebugContext(ctx, "get candles", "time_ranges", timeRanges)

	if len(timeRanges) == 0 {
		return nil, ErrNoCandlesFound
	}

	candles := make([]*domain.Candle, 0, s.chunkCandleCount*len(timeRanges))
	for _, timeRange := range timeRanges {
		key := candleFileKey{
			seriesKey: seriesKey,
			timeRange: timeRange,
		}
		rangeCandles, err := s.getCandlesByKey(ctx, key, from, to)
		if err != nil {
			return nil, err
		}
		candles = append(candles, rangeCandles...)
	}

	slog.DebugContext(ctx, "get candles", "candle_count", len(candles))
	return candles, nil
}

func (s *storage) getCandlesByKey(ctx context.Context, key candleFileKey, from time.Time, to time.Time) ([]*domain.Candle, error) {
	_, err := s.candleFiles[key].Seek(0, io.SeekStart)
	if err != nil {
		return nil, fmt.Errorf("failed to seek to the start of the candle file: %w", err)
	}

	// FIXME: Read only the necessary data
	candlesBinary := make([]byte, s.chunkCandleCount*candleByteSize)
	_, err = io.ReadFull(s.candleFiles[key], candlesBinary)
	if err != nil {
		return nil, fmt.Errorf("failed to read candle file: %w", err)
	}

	slog.DebugContext(ctx, "getting candles for range", "range.from", key.timeRange.from, "range.to", key.timeRange.to, "request.from", from, "request.to", to)
	candles := make([]*domain.Candle, 0, s.chunkCandleCount)
	for i := 0; i < s.chunkCandleCount; i++ {
		var candle domain.Candle
		err := decodeCandle(candlesBinary[i*candleByteSize:(i+1)*candleByteSize], &candle)
		if err == ErrCandleNotWritten {
			continue
		}
		if err != nil {
			return nil, err
		}

		// FIXME: Read only the necessary data
		if candle.Timestamp.Before(from) || candle.Timestamp.After(to) {
			continue
		}
		slog.Debug("decodeCandle", "candle", candle, "err", err, "idx", i)

		candle.TickerId = key.seriesKey.tickerId
		candle.Resolution = key.seriesKey.resolution
		candles = append(candles, &candle)
	}

	return candles, nil

}

func (s *storage) SaveCandles(ctx context.Context, candles []*domain.Candle) error {
	slog.DebugContext(ctx, "save candles", "count", len(candles))
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

	candleByFile := make(map[candleFileKey][]*domain.Candle)
	for _, candle := range candles {
		key := candleFileKey{
			seriesKey: seriesKey{
				tickerId:   candle.TickerId,
				resolution: candle.Resolution,
			},
			timeRange: s.calcTimeRangeForTimestamp(candle.Resolution, candle.Timestamp),
		}

		if _, ok := candleByFile[key]; !ok {
			candleByFile[key] = []*domain.Candle{}
		}

		candleByFile[key] = append(candleByFile[key], candle)
	}

	for key, candles := range candleByFile {
		err = s.saveCandlesByFile(key, candles)
		if err != nil {
			return err
		}
	}

	return nil
}

func (s *storage) allocateCandleFiles(ctx context.Context, candleBySeries map[seriesKey][]*domain.Candle) error {
	slog.DebugContext(ctx, "allocateCandleFiles", "series_count", len(candleBySeries))
	seriesKeys := s.buildSeriesPlan(ctx, candleBySeries)

	for _, key := range seriesKeys {
		err := s.allocateCandleFile(ctx, key)
		if err != nil {
			return err
		}
	}
	return nil
}

func (s *storage) saveCandlesByFile(fileKey candleFileKey, candles []*domain.Candle) error {
	slog.Debug("saveCandlesByFile", "file_key", fileKey, "candle_count", len(candles))
	// FIXME: lock should be more fine-grained
	s.candleFilesMutex.Lock()
	defer s.candleFilesMutex.Unlock()

	sort.Slice(candles, func(i, j int) bool {
		return candles[i].Timestamp.Before(candles[j].Timestamp)
	})

	candleFile := s.candleFiles[fileKey]
	if candleFile == nil {
		panic("candle file not found")
	}

	for _, candle := range candles {
		err := s.saveCandleByFile(fileKey, candleFile, candle)
		if err != nil {
			return err
		}
	}

	return nil
}

func (s *storage) saveCandleByFile(fileKey candleFileKey, candleFile afero.File, candle *domain.Candle) error {
	slog.Debug("saveCandleByFile", "file_key", fileKey, "candle", candle)
	candle.Timestamp = candle.Timestamp.Truncate(time.Duration(candle.Resolution))
	encoded := encodeCandle(candle)
	offset := int64(candle.Timestamp.Sub(fileKey.timeRange.from) / time.Duration(fileKey.seriesKey.resolution))
	slog.Debug("saveCandleByFile", "from", fileKey.timeRange.from, "cdl_ts", candle.Timestamp, "offset", offset)

	err := s.writeCandleUpsert(fileKey, candle, offset, false)
	if err != nil {
		return err
	}

	n, err := candleFile.Seek(offset*candleByteSize, io.SeekStart)
	if n != offset*candleByteSize && err == nil {
		return fmt.Errorf("failed to seek to the offset: %w", io.ErrShortWrite)
	}

	written, err := candleFile.Write(encoded)
	if written != candleByteSize && err == nil {
		err = io.ErrShortWrite
	}
	if err != nil {
		return err
	}

	return s.writeCandleUpsert(fileKey, candle, offset, true)
}

func (s *storage) allocateCandleFile(ctx context.Context, key candleFileKey) error {
	slog.DebugContext(ctx, "allocateCandleFile", "key", key)
	seriesDir := path.Join(s.dataDir, fmt.Sprintf("%s_%s", key.seriesKey.tickerId, key.seriesKey.resolution))
	filename := path.Join(
		seriesDir,
		fmt.Sprintf("%d_%d.bin", key.timeRange.from.UnixMicro(), key.timeRange.to.UnixMicro()),
	)

	exists, err := afero.Exists(s.fs, seriesDir)
	if err != nil {
		return fmt.Errorf("failed to check if series directory exists: %w", err)
	}
	if !exists {
		err = s.fs.MkdirAll(seriesDir, 0755)
		if err != nil {
			return fmt.Errorf("failed to create series directory: %w", err)
		}
	}

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
	s.seriesTimeRanges[key.seriesKey] = append(s.seriesTimeRanges[key.seriesKey], key.timeRange)
	s.candleFilesMutex.Unlock()

	return nil
}

func (s *storage) buildSeriesPlan(ctx context.Context, candleBySeries map[seriesKey][]*domain.Candle) []candleFileKey {
	slog.DebugContext(ctx, "buildSeriesPlan", "series_count", len(candleBySeries))
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
	slog.DebugContext(ctx, "buildMissingRanges", "series", series, "candle_count", len(candles))
	uncoveredTimestamps := []time.Time{}

	for _, candle := range candles {
		contained := false
		for _, timeRange := range s.seriesTimeRanges[series] {
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

	return s.calcTimeRangesForTimestamps(series.resolution, uncoveredTimestamps)
}

func (s *storage) calcTimeRangesForTimestamps(resolution domain.Resolution, timestamps []time.Time) []timeRange {
	slog.Debug("calcTimeRangesForTimestamps", "resolution", resolution, "timestamp_count", len(timestamps))
	timeRangeSet := set[timeRange]{}
	for _, t := range timestamps {
		timeRangeSet.add(s.calcTimeRangeForTimestamp(resolution, t))
	}

	timeRanges := make([]timeRange, 0, len(timeRangeSet))
	for tr := range timeRangeSet {
		timeRanges = append(timeRanges, tr)
	}

	return timeRanges
}

func (s *storage) calcTimeRangeForTimestamp(resolution domain.Resolution, timestamp time.Time) timeRange {
	slog.Debug("calcTimeRangeForTimestamp", "resolution", resolution, "timestamp", timestamp)
	chunkSize := s.chunkSize(resolution)

	// For large time ranges, we store them by year
	if chunkSize > oneWeek {
		return buildYearlyRange(timestamp.Year())
	}

	return timeRange{
		from: timestamp.Truncate(chunkSize),
		to:   timestamp.Truncate(chunkSize).Add(chunkSize),
	}
}

func buildYearlyRange(year int) timeRange {
	return timeRange{
		from: time.Date(year, 1, 1, 0, 0, 0, 0, time.UTC),
		to:   time.Date(year+1, 1, 1, 0, 0, 0, 0, time.UTC),
	}
}

func (s *storage) chunkSize(resolution domain.Resolution) time.Duration {
	return time.Duration(resolution) * time.Duration(s.chunkCandleCount)
}
