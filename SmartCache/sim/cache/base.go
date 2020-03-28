package cache

import (
	"time"

	pb "simulator/v2/cache/simService"
)

// DumpRecord represents a record in the dump file
type DumpRecord struct {
	Info     string `json:"info"`
	Data     string `json:"data"`
	Filename int64  `json:"filename"`
}

// DumpInfo collects cache marshall info
type DumpInfo struct {
	Type string `json:"type"`
}

// FileDump represents the record of a dumped cache file
type FileDump struct {
	Filename int64   `json:"filename"`
	Size     float64 `json:"size"`
}

// Request represent an ingestable request for the cache
type Request struct {
	Filename int64
	Size     float64
	CPUEff   float64
	Day      int64
	DayTime  time.Time
	SiteName int64
	UserID   int64
	DataType int64
	FileType int64
	Protocol int64
}

// Cache is the base interface for the cache object
type Cache interface {
	Init(...interface{}) interface{}
	SetRegion(string)
	SetBandwidth(float64)

	Dumps(fileAndStats bool) [][]byte
	Dump(filename string, fileAndStats bool)
	Loads([][]byte, ...interface{})
	Load(filename string) [][]byte

	Clear()
	ClearFiles()
	ClearHitMissStats()
	Free(amount float64, percentage bool) float64

	ExtraStats() string
	ExtraOutput(string) string

	HitRate() float64
	HitOverMiss() float64
	WeightedHitRate() float64
	Size() float64
	Capacity() float64
	DataWritten() float64
	DataRead() float64
	DataReadOnHit() float64
	DataReadOnMiss() float64
	DataDeleted() float64
	CPUEff() float64
	CPUHitEff() float64
	CPUMissEff() float64
	CPUEffUpperBound() float64
	CPUEffLowerBound() float64
	MeanSize() float64
	MeanFrequency() float64
	MeanRecency() float64
	BandwidthUsage() float64

	Check(int64) bool
	CheckWatermark() bool
	BeforeRequest(request *Request, hit bool) *FileStats
	UpdatePolicy(request *Request, fileStats *FileStats, hit bool) bool
	AfterRequest(request *Request, hit bool, added bool)
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
func GetFile(bandwidthManager bool, cache Cache, vars ...interface{}) (bool, bool) {
	/* vars:
	[0] -> filename int64
	[1] -> size     float64
	[2] -> wTime    float64
	[3] -> cpuTime  float64
	[4] -> day      int64
	[5] -> siteName int64
	[6] -> userID   int64
	[7] -> fileType   int64
	*/

	cacheRequest := Request{
		Filename: vars[0].(int64),
	}

	switch {
	case len(vars) > 7:
		cacheRequest.FileType = vars[7].(int64)
		fallthrough
	case len(vars) > 6:
		cacheRequest.UserID = vars[6].(int64)
		fallthrough
	case len(vars) > 5:
		cacheRequest.SiteName = vars[5].(int64)
		fallthrough
	case len(vars) > 4:
		cacheRequest.Day = vars[4].(int64)
		cacheRequest.DayTime = time.Unix(cacheRequest.Day, 0)
		fallthrough
	case len(vars) > 3:
		cacheRequest.CPUEff = vars[3].(float64)
		fallthrough
	case len(vars) > 2:
		cacheRequest.Protocol = vars[2].(int64)
		fallthrough
	case len(vars) > 1:
		cacheRequest.Size = vars[1].(float64)
	}

	hit := cache.Check(cacheRequest.Filename)
	if bandwidthManager && !hit && cache.BandwidthUsage() >= 95.0 {
		return false, true
	}
	fileStats := cache.BeforeRequest(&cacheRequest, hit)
	added := cache.UpdatePolicy(&cacheRequest, fileStats, hit)
	cache.AfterRequest(&cacheRequest, hit, added)
	cache.CheckWatermark()
	return added, false
}
