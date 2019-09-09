package service

import (
	"context"
	"math"
	"sort"
	"sync"
	"time"

	pb "./pluginProto"
	"github.com/golang/protobuf/ptypes/empty"
	empty "github.com/golang/protobuf/ptypes/empty"
)

const (
	// StatsMemorySize indicates the size of fileStats memory
	StatsMemorySize uint64 = 8
)

type cacheEmptyMsg struct{}

type weightedFileStats struct {
	filename          string
	weight            float32
	size              float32
	totRequests       uint32
	nHits             uint32
	nMiss             uint32
	lastTimeRequested time.Time
	requestTicks      [StatsMemorySize]time.Time
	requestLastIdx    int
}

func (stats *weightedFileStats) updateRequests(hit bool, curTime time.Time) {
	stats.totRequests++

	if hit {
		stats.nHits++
	} else {
		stats.nMiss++
	}

	stats.lastTimeRequested = curTime

	stats.requestTicks[stats.requestLastIdx] = curTime
	stats.requestLastIdx = (stats.requestLastIdx + 1) % int(StatsMemorySize)
}

func (stats weightedFileStats) getMeanReqTimes(curtime time.Time) float32 {
	var timeDiffSum time.Duration
	for idx := 0; idx < int(StatsMemorySize); idx++ {
		if !stats.requestTicks[idx].IsZero() {
			timeDiffSum += curtime.Sub(stats.requestTicks[idx])
		}
	}
	return float32(timeDiffSum.Seconds()) / float32(StatsMemorySize)
}

// FunctionType is used to select the weight function
type FunctionType int

const (
	// FuncFileWeight indicates the simple function for weighted cache
	FuncFileWeight FunctionType = iota
	// FuncFileWeightAndTime indicates the function that uses time
	FuncFileWeightAndTime
	// FuncFileWeightOnlyTime indicates the function that uses time
	FuncFileWeightOnlyTime
	// FuncWeightedRequests has a small memory for request time
	FuncWeightedRequests
)

func fileWeight(size float32, totRequests uint32, exp float32) float32 {
	return float32(math.Pow(float64(size)/float64(totRequests), float64(exp)))
}

func fileWeightAndTime(size float32, totRequests uint32, exp float32, lastTimeRequested time.Time) float32 {
	deltaLastTimeRequested := float64(time.Now().Sub(lastTimeRequested) / time.Second)
	return (size / float32(math.Pow(float64(totRequests), float64(exp)))) + float32(math.Pow(deltaLastTimeRequested, float64(exp)))
}

func fileWeightOnlyTime(totRequests uint32, exp float32, lastTimeRequested time.Time) float32 {
	deltaLastTimeRequested := float64(time.Now().Sub(lastTimeRequested) / time.Second)
	return (1. / float32(math.Pow(float64(totRequests), float64(exp)))) + float32(math.Pow(deltaLastTimeRequested, float64(exp)))
}

func fileWeightedRequest(size float32, totRequests uint32, meanTicks float32, exp float32) float32 {
	return meanTicks + (size / float32(math.Pow(
		float64(totRequests),
		float64(exp))))
}

// PluginServiceServer is the server API for PluginService service.
type PluginServiceServer struct {
	stats          []*weightedFileStats
	statsFilenames map[string]int
	statsWaitGroup sync.WaitGroup
	functionType   FunctionType
	exp            float32
}

// Init the PluginServiceServer struct
func (service *PluginServiceServer) Init(vars ...interface{}) {
	if len(vars) < 2 {
		panic("ERROR: you need to specify the weighted function to use and the exponent...")
	}
	service.stats = make([]*weightedFileStats, 0)
	service.statsFilenames = make(map[string]int)
	service.statsWaitGroup = sync.WaitGroup{}
	service.functionType = vars[0].(FunctionType)
	service.exp = vars[1].(float32)
}

// Clear the PluginServiceServer struct
func (service *PluginServiceServer) Clear() {
	service.stats = make([]*weightedFileStats, 0)
	service.statsFilenames = make(map[string]int)
	service.statsWaitGroup = sync.WaitGroup{}
}

func (service *PluginServiceServer) getThreshold() float32 {
	if len(service.stats) == 0 {
		return 0.0
	}

	for _, stats := range service.stats {
		service.statsWaitGroup.Add(1)

		go func(curStats *weightedFileStats, wg *sync.WaitGroup) {
			var weight float32

			switch service.functionType {
			case FuncFileWeight:
				weight = fileWeight(
					curStats.size,
					curStats.totRequests,
					service.exp,
				)
			case FuncFileWeightAndTime:
				weight = fileWeightAndTime(
					curStats.size,
					curStats.totRequests,
					service.exp,
					curStats.lastTimeRequested,
				)
			case FuncFileWeightOnlyTime:
				weight = fileWeightOnlyTime(
					curStats.totRequests,
					service.exp,
					curStats.lastTimeRequested,
				)
			case FuncWeightedRequests:
				weight = fileWeightedRequest(
					curStats.size,
					curStats.totRequests,
					curStats.getMeanReqTimes(time.Now()),
					service.exp,
				)
			}
			curStats.weight = weight
			wg.Done()
		}(stats, &service.statsWaitGroup)
	}

	service.statsWaitGroup.Wait()

	// Order from the highest weight to the smallest
	sort.Slice(
		service.stats,
		func(i, j int) bool {
			return service.stats[i].weight > service.stats[j].weight
		},
	)
	Q2 := service.stats[int(math.Floor(float64(0.5*float32(len(service.stats)))))].weight
	Q1Idx := int(math.Floor(float64(0.25 * float32(len(service.stats)))))
	Q1 := service.stats[Q1Idx].weight
	if Q1 > 2*Q2 {
		for idx := 0; idx < Q1Idx; idx++ {
			delete(service.statsFilenames, service.stats[idx].filename)
		}
		copy(service.stats, service.stats[Q1Idx:])
		service.stats = service.stats[:len(service.stats)-1]
	}
	return Q2
}

func (service *PluginServiceServer) getOrInsertStats(filename string, size float32) *weightedFileStats {
	var result *weightedFileStats
	if _, inStats := service.statsFilenames[filename]; !inStats {
		service.stats = append(service.stats, &weightedFileStats{
			filename,
			-1,
			size,
			0.,
			0,
			0,
			time.Now(),
			[StatsMemorySize]time.Time{},
			0,
		})
		service.statsFilenames[filename] = len(service.stats) - 1
		result = service.stats[len(service.stats)-1]
	} else {
		if service.stats[service.statsFilenames[filename]].filename != filename {
			for idx := 0; idx < len(service.stats); idx++ {
				if service.stats[idx].filename == filename {
					service.statsFilenames[filename] = idx
					break
				}
			}
		}
		result = service.stats[service.statsFilenames[filename]]
	}
	return result
}

// GetHint function for plugin service
func (service *PluginServiceServer) GetHint(ctx context.Context, curFile *pb.FileRequest) (*pb.FileHint, error) {
	curStats := service.getOrInsertStats(curFile.Filename, curFile.Size)
	Q2 := service.getThreshold()
	store := false
	if curStats.weight < Q2 {
		store = true
	}
	return &pb.FileHint{
		Filename: curFile.Filename,
		Store:    store,
	}, nil
}

// UpdateStats function for plugin service
func (service *PluginServiceServer) UpdateStats(ctx context.Context, curFile *pb.FileRequest) (*empty.Empty, error) {
	currentTime := time.Now()
	curStats := service.getOrInsertStats(curFile.Filename, curFile.Size)
	curStats.updateRequests(curFile.Hit, currentTime)
	return &empty.Empty{}, nil
}

// ResetHistory function for plugin service
func (service *PluginServiceServer) ResetHistory(ctx context.Context, _ *empty.Empty) (*empty.Empty, error) {
	service.Clear()
	return &empty.Empty{}, nil
}
