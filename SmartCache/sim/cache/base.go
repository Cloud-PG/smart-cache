package cache

import (
	"context"

	pb "simulator/v2/cache/simService"

	empty "github.com/golang/protobuf/ptypes/empty"
)

// DumpRecord represents a record in the dump file
type DumpRecord struct {
	Info string `json:"info"`
	Data string `json:"data"`
}

// DumpInfo collects cache marshall info
type DumpInfo struct {
	Type string `json:"type"`
}

// FileDump represents the record of a dumped cache file
type FileDump struct {
	Filename string  `json:"filename"`
	Size     float32 `json:"size"`
}

// Cache is the base interface for the cache object
type Cache interface {
	Init(...interface{}) interface{}

	Dumps() *[][]byte
	Dump(filename string)
	Loads(*[][]byte)
	Load(filename string)

	Clear()
	ClearFiles()
	ClearHitMissStats()

	ExtraStats() string
	Report() []string

	HitRate() float32
	HitOverMiss() float32
	WeightedHitRate() float32
	Size() float32
	Capacity() float32
	DataWritten() float32
	DataRead() float32
	DataReadOnHit() float32
	DataReadOnMiss() float32
	DataDeleted() float32
	CPUEff() float32
	CPUHitEff() float32
	CPUMissEff() float32

	Check(string) bool
	CheckWatermark() bool
	BeforeRequest(hit bool, filename string, size float32, day int64, siteName string, userID int) *FileStats
	UpdatePolicy(fileStats *FileStats, hit bool, vars ...interface{}) bool
	AfterRequest(hit bool, added bool, size float32, wTime float32, cpuTime float32)

	SimGet(context.Context, *pb.SimCommonFile) (*pb.ActionResult, error)
	SimClear(context.Context, *empty.Empty) (*pb.SimCacheStatus, error)
	SimClearFiles(context.Context, *empty.Empty) (*pb.SimCacheStatus, error)
	SimClearHitMissStats(context.Context, *empty.Empty) (*pb.SimCacheStatus, error)
	SimGetInfoCacheStatus(context.Context, *empty.Empty) (*pb.SimCacheStatus, error)
	SimDumps(*empty.Empty, pb.SimService_SimDumpsServer) error
	SimLoads(pb.SimService_SimLoadsServer) error
}

// GetSimCacheStatus create a cache status message
func GetSimCacheStatus(cache Cache) *pb.SimCacheStatus {
	return &pb.SimCacheStatus{
		HitRate:         cache.HitRate(),
		WeightedHitRate: cache.WeightedHitRate(),
		HitOverMiss:     cache.HitOverMiss(),
		Size:            cache.Size(),
		Capacity:        cache.Capacity(),
		DataWritten:     cache.DataWritten(),
		DataRead:        cache.DataRead(),
		DataReadOnHit:   cache.DataReadOnHit(),
		DataReadOnMiss:  cache.DataReadOnMiss(),
		DataDeleted:     cache.DataDeleted(),
	}
}

// GetFile requests a file to the cache
func GetFile(cache Cache, vars ...interface{}) bool {
	/* vars:
	[0] -> filename string
	[1] -> size     float32
	[2] -> wTime    float32
	[3] -> cpuTime  float32
	[4] -> day      int64
	[5] -> siteName string
	[6] -> userID   int
	*/

	var (
		filename string
		size     float32
		wTime    float32
		cpuTime  float32
		day      int64
		siteName string
		userID   int
	)

	filename = vars[0].(string)

	switch {
	case len(vars) > 6:
		userID = vars[6].(int)
		fallthrough
	case len(vars) > 5:
		siteName = vars[5].(string)
		fallthrough
	case len(vars) > 4:
		day = vars[4].(int64)
		fallthrough
	case len(vars) > 3:
		cpuTime = vars[3].(float32)
		fallthrough
	case len(vars) > 2:
		wTime = vars[2].(float32)
		fallthrough
	case len(vars) > 1:
		size = vars[1].(float32)
	}

	hit := cache.Check(filename)
	fileStats := cache.BeforeRequest(hit, filename, size, day, siteName, userID)
	added := cache.UpdatePolicy(fileStats, hit, day)
	cache.AfterRequest(hit, added, size, wTime, cpuTime)
	cache.CheckWatermark()
	return added
}
