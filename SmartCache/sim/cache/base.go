package cache

import (
	"context"

	pb "./simService"
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
	check(string) bool
	updatePolicy(filename string, size float32, hit bool) bool

	Init(...interface{})

	Dumps() *[][]byte
	Dump(filename string)
	Loads(*[][]byte)
	Load(filename string)

	Clear()
	ClearFiles()
	ClearHitMissStats()

	HitRate() float32
	HitOverMiss() float32
	WeightedHitRate() float32
	Size() float32
	Capacity() float32
	DataWritten() float32
	DataRead() float32
	DataReadOnHit() float32

	Get(filename string, size float32, vars ...interface{}) bool
	GetReport() (*DatasetInput, error)

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
	}
}

// DatasetInput contains file statistics collected by weighted caches
type DatasetInput struct {
	CacheSize          float32
	CacheMaxSize       float32
	CacheLastFileHit   bool
	CacheLastFileAdded bool
	FileSize           float32
	FileTotRequests    uint32
	FileNHits          uint32
	FileNMiss          uint32
	FileMeanTimeReq    float32
	LastFileHitted     bool
	LastFileAdded      bool
}
