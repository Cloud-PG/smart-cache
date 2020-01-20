package cache

import (
	"context"
	"time"

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

// Request represent an ingestable request for the cache
type Request struct {
	Filename string
	Size     float32
	WTime    float32
	CPUTime  float32
	Day      int64
	DayTime  time.Time
	SiteName string
	UserID   int
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
	Free(amount float32, percentage bool) float32

	ExtraStats() string

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
	BeforeRequest(request *Request, hit bool) *FileStats
	UpdatePolicy(request *Request, fileStats *FileStats, hit bool) bool
	AfterRequest(request *Request, hit bool, added bool)

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

	cacheRequest := Request{
		Filename: vars[0].(string),
	}

	switch {
	case len(vars) > 6:
		cacheRequest.UserID = vars[6].(int)
		fallthrough
	case len(vars) > 5:
		cacheRequest.SiteName = vars[5].(string)
		fallthrough
	case len(vars) > 4:
		cacheRequest.Day = vars[4].(int64)
		cacheRequest.DayTime = time.Unix(cacheRequest.Day, 0)
		fallthrough
	case len(vars) > 3:
		cacheRequest.CPUTime = vars[3].(float32)
		fallthrough
	case len(vars) > 2:
		cacheRequest.WTime = vars[2].(float32)
		fallthrough
	case len(vars) > 1:
		cacheRequest.Size = vars[1].(float32)
	}

	hit := cache.Check(cacheRequest.Filename)
	fileStats := cache.BeforeRequest(&cacheRequest, hit)
	added := cache.UpdatePolicy(&cacheRequest, fileStats, hit)
	cache.AfterRequest(&cacheRequest, hit, added)
	cache.CheckWatermark()
	return added
}
