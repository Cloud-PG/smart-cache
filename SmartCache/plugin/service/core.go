package service

import (
	"context"
	"math"
	"sort"
	"time"

	pb "./pluginProto"
	empty "github.com/golang/protobuf/ptypes/empty"
)

// XCachePlugin is the base interface for a complete plugin
type XCachePlugin interface {
	Init(...interface{})
	GetHint(context.Context, *pb.FileHint) (*pb.FileHint, error)
	UpdateStats(context.Context, *pb.FileRequest) (*empty.Empty, error)
	ResetHistory(context.Context, *empty.Empty) (*empty.Empty, error)
}

// PluginServiceServer is the server API for PluginService curService.
type PluginServiceServer struct {
	stats           []*weightedFileStats
	statsFilenames  map[string]int
	SelFunctionType FunctionType
	Exp             float32
}

// Init the PluginServiceServer struct
func (curService PluginServiceServer) Init(vars ...interface{}) {
	curService.stats = make([]*weightedFileStats, 0)
	curService.statsFilenames = make(map[string]int)
}

// Clear the PluginServiceServer struct
func (curService *PluginServiceServer) Clear() {
	curService.stats = make([]*weightedFileStats, 0)
	curService.statsFilenames = make(map[string]int)
}

// GetHint function for plugin service
func (curService PluginServiceServer) GetHint(ctx context.Context, curFile *pb.FileHint) (*pb.FileHint, error) {
	curStats := curService.getOrInsertStats(curFile.Filename)
	Q2 := curService.getThreshold()
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
func (curService PluginServiceServer) UpdateStats(ctx context.Context, curFile *pb.FileRequest) (*empty.Empty, error) {
	currentTime := time.Now()
	curStats := curService.getOrInsertStats(curFile.Filename)
	curStats.updateStats(
		curFile.Hit, curFile.NAccess, curFile.Downloaded, currentTime, curFile.MeanTime,
	)
	curStats.updateWeight(curService.SelFunctionType, curService.Exp)
	return &empty.Empty{}, nil
}

// ResetHistory function for plugin service
func (curService PluginServiceServer) ResetHistory(ctx context.Context, _ *empty.Empty) (*empty.Empty, error) {
	curService.Clear()
	return &empty.Empty{}, nil
}

func (curService *PluginServiceServer) getThreshold() float32 {
	if len(curService.stats) == 0 {
		return 0.0
	}
	// Order from the highest weight to the smallest
	sort.Sort(ByWeight(curService.stats))

	Q2 := curService.stats[int(math.Floor(float64(0.5*float32(len(curService.stats)))))].weight
	Q1Idx := int(math.Floor(float64(0.25 * float32(len(curService.stats)))))
	Q1 := curService.stats[Q1Idx].weight
	if Q1 > 2*Q2 {
		for idx := 0; idx < Q1Idx; idx++ {
			delete(curService.statsFilenames, curService.stats[idx].filename)
		}
		copy(curService.stats, curService.stats[Q1Idx:])
		curService.stats = curService.stats[:len(curService.stats)-1]
	}
	return Q2
}

func (curService *PluginServiceServer) getOrInsertStats(filename string) *weightedFileStats {
	var result *weightedFileStats
	if _, inStats := curService.statsFilenames[filename]; !inStats {
		curService.stats = append(curService.stats, &weightedFileStats{
			filename,
			-1,
			0.,
			0.,
			0,
			0,
			time.Now(),
			[StatsMemorySize]time.Time{},
			0,
			0.,
		})
		curService.statsFilenames[filename] = len(curService.stats) - 1
		result = curService.stats[len(curService.stats)-1]
	} else {
		if curService.stats[curService.statsFilenames[filename]].filename != filename {
			for idx := 0; idx < len(curService.stats); idx++ {
				if curService.stats[idx].filename == filename {
					curService.statsFilenames[filename] = idx
					break
				}
			}
		}
		result = curService.stats[curService.statsFilenames[filename]]
	}
	return result
}
