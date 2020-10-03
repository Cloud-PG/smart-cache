package files

import (
	"math"
	"time"

	"simulator/v2/cache/functions"

	"go.uber.org/zap"
)

const (
	numHoursInADay    = 24.
	estimatedNumFiles = 1 << 19
)

// Manager collector of statistics for weight function cache
type Manager struct {
	Data                      map[int64]*Stats
	deletedFileMiss           map[int64]int
	deletedFilesMissLastCount int
	weightSum                 float64
	calcWeight                bool
	firstUpdateTime           time.Time
	lastUpdateTime            time.Time
	maxNumDayDiff             float64 // MaxNumDayDiff limit to stay in the stats
	deltaDaysStep             float64 // DeltaDaysStep limit the clean action of the stats
	logger                    *zap.Logger
}

// Init initialize Stats
func (s *Manager) Init(maxNumDayDiff float64, deltaDaysStep float64, calcWeight bool) {
	s.Data = make(map[int64]*Stats, estimatedNumFiles)
	s.deletedFileMiss = make(map[int64]int, estimatedNumFiles)
	s.calcWeight = calcWeight
	s.maxNumDayDiff = maxNumDayDiff
	s.deltaDaysStep = deltaDaysStep
	s.logger = zap.L()
}

func (s *Manager) AddDeletedFileMiss(filename int64) {
	s.deletedFileMiss[filename] = 0
}

func (s *Manager) IncDeletedFileMiss(filename int64) {
	_, inMap := s.deletedFileMiss[filename]
	if inMap {
		s.deletedFileMiss[filename]++
	}
}

func (s *Manager) ClearDeletedFileMiss() {
	for _, value := range s.deletedFileMiss {
		s.deletedFilesMissLastCount += value
	}
	s.deletedFileMiss = make(map[int64]int)
}

func (s *Manager) GetTotDeletedFileMiss() int {
	sum := s.deletedFilesMissLastCount

	for key, value := range s.deletedFileMiss {
		sum += value
		s.deletedFileMiss[key] = 0
	}

	s.deletedFilesMissLastCount = 0

	return sum
}

// Clear Stats after load
func (s *Manager) Clear() {
	for _, fileStats := range s.Data {
		fileStats.Recency = 0
	}
	s.weightSum = 0.0
}

// Dirty indicates if the stats needs a purge
func (s Manager) Dirty() bool {
	numDays := s.lastUpdateTime.Sub(s.firstUpdateTime).Hours() / numHoursInADay

	if numDays >= s.deltaDaysStep {
		s.logger.Debug("Dirty Stats", zap.Float64("numDays", numDays),
			zap.String("lastTime", s.lastUpdateTime.Format(time.UnixDate)),
			zap.String("firstTime", s.firstUpdateTime.Format(time.UnixDate)),
		)

		return true
	}

	return false
}

// Purge remove older stats
func (s *Manager) Purge(has func(int64) bool) {
	numDeletedFiles := 0

	for filename, stats := range s.Data {
		curFilename := filename

		if inCache := has(curFilename); !inCache && stats.DiffLastUpdate(s.lastUpdateTime) >= s.maxNumDayDiff {
			s.logger.Debug("Purge", zap.Bool("in cache", inCache))

			if s.calcWeight {
				s.weightSum -= stats.Weight
			}

			delete(s.Data, filename)
			numDeletedFiles++
		}
	}

	s.logger.Debug("Stats purged", zap.Int("NumDeletedFiles", numDeletedFiles))
	s.firstUpdateTime = s.lastUpdateTime
}

// Get returns the stats without update them
func (s Manager) Get(filename int64) *Stats {
	curStats, inStats := s.Data[filename]
	if !inStats {
		s.logger.Error("Get: no file found", zap.Int64("filename", filename))
	}

	return curStats
}

// GetOrCreate add the file into stats and returns (stats, is new file)
func (s *Manager) GetOrCreate(filename int64, size float64, reqTime time.Time, curTick int64) (stats *Stats, newFile bool) {
	// Stats age update
	if s.firstUpdateTime.IsZero() {
		s.logger.Info("Updated first time")
		s.firstUpdateTime = reqTime
	}

	s.lastUpdateTime = reqTime

	// Update file stats
	curStats, inStats := s.Data[filename]

	if !inStats {
		curStats = &Stats{
			Filename:         filename,
			Size:             size,
			StatInsertTime:   reqTime,
			DeltaLastRequest: math.MaxInt64,
			Recency:          curTick,
		}
		s.Data[filename] = curStats
	} else {
		curStats.Size = size
		curStats.DeltaLastRequest = curTick - curStats.Recency
		curStats.Recency = curTick
	}

	// fmt.Println(curTick, curStats.Recency, curStats.DeltaLastRequest)

	return curStats, !inStats
}

// UpdateWeight update the weight of a file and also the sum of all weights
func (s *Manager) UpdateWeight(stats *Stats, newFile bool, functionType functions.FunctionType, alpha float64, beta float64, gamma float64) {
	if newFile {
		s.weightSum += stats.updateWeight(functionType, alpha, beta, gamma)
	} else {
		s.weightSum -= stats.Weight
		s.weightSum += stats.updateWeight(functionType, alpha, beta, gamma)
	}
}

// GetWeightMedian returns the mean of the weight of all files
func (s *Manager) GetWeightMedian() float64 {
	return s.weightSum / float64(len(s.Data))
}

const (
	// NumRequestedTimes represents the  number of slots
	NumRequestedTimes int = 32
	// NumDaysStatsDecay is the number of days that stats are maintained
	NumDaysStatsDecay = 7.0
	// NumDaysPointsDecay is the number of days that points are maintained
	NumDaysPointsDecay = 3.0
)

// Stats contains file statistics collected by weight function caches
type Stats struct {
	Filename           int64       `json:"filename"`
	FileType           int64       `json:"fileType"`
	DataType           int64       `json:"dataType"`
	Weight             float64     `json:"weight"`
	Size               float64     `json:"size"`
	Frequency          int64       `json:"frequency"`
	FrequencyInCache   int64       `json:"frequencyInCache"`
	Recency            int64       `json:"recency"`
	NHits              int64       `json:"nHits"`
	NMiss              int64       `json:"nMiss"`
	StatInsertTime     time.Time   `json:"statInsertTime"`
	StatLastUpdateTime time.Time   `json:"statLastUpdateTime"`
	RequestedTimesMean float64     `json:"requestedTimesMean"`
	RequestedTimes     []time.Time `json:"requestedTimes"`
	DeltaLastRequest   int64       `json:"deltaLastRequest"`
	idxLastRequest     int
}

// DiffLastUpdate returns the number of days from the last update stats
func (stats *Stats) DiffLastUpdate(curTime time.Time) float64 {
	return curTime.Sub(stats.StatLastUpdateTime).Hours() / numHoursInADay
}

func (stats *Stats) UpdateStats(hit bool, size float64, userID int64, siteName int64, curTime time.Time) {
	stats.Size = size

	stats.Frequency++

	if hit {
		stats.NHits++
	} else {
		stats.NMiss++
	}

	stats.StatLastUpdateTime = curTime
	if len(stats.RequestedTimes) < NumRequestedTimes {
		stats.RequestedTimes = append(stats.RequestedTimes, time.Time{})
	}
	stats.RequestedTimes[stats.idxLastRequest] = curTime
	stats.idxLastRequest = (stats.idxLastRequest + 1) % NumRequestedTimes
}

func (stats *Stats) updateWeight(functionType functions.FunctionType, alpha float64, beta float64, gamma float64) float64 {
	stats.RequestedTimesMean = stats.getMeanReqTimes()

	switch functionType {
	case functions.FuncAdditive:
		stats.Weight = functions.FileAdditiveWeight(
			stats.Frequency,
			stats.Size,
			stats.RequestedTimesMean,
			alpha,
			beta,
			gamma,
		)
	case functions.FuncAdditiveExp:
		stats.Weight = functions.FileAdditiveExpWeight(
			stats.Frequency,
			stats.Size,
			stats.RequestedTimesMean,
			alpha,
			beta,
			gamma,
		)
	case functions.FuncMultiplicative:
		stats.Weight = functions.FileMultiplicativeWeight(
			stats.Frequency,
			stats.Size,
			stats.RequestedTimesMean,
			alpha,
			beta,
			gamma,
		)
	case functions.FuncWeightedRequests:
		stats.Weight = functions.FileWeightedRequest(
			stats.Frequency,
			stats.Size,
			stats.RequestedTimesMean,
		)
	}

	return stats.Weight
}

func (stats *Stats) getMeanReqTimes() float64 {
	var timeDiffSum time.Duration

	for idx := 0; idx < len(stats.RequestedTimes); idx++ {
		if !stats.RequestedTimes[idx].IsZero() {
			timeDiffSum += stats.StatLastUpdateTime.Sub(stats.RequestedTimes[idx])
		}
	}

	if timeDiffSum != 0. {
		return timeDiffSum.Minutes() / float64(NumRequestedTimes)
	}

	return 0.
}

type SortStatsByRecency []*Stats

func (s SortStatsByRecency) Len() int {
	return len(s)
}

func (s SortStatsByRecency) Less(i, j int) bool {
	return s[i].Recency < s[j].Recency
}

func (s SortStatsByRecency) Swap(i, j int) {
	s[i], s[j] = s[j], s[i]
}
