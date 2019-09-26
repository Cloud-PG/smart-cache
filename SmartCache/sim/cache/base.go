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
	WrittenData() float32
	ReadOnHit() float32

	Get(filename string, size float32) bool
	GetLatestDecision() (bool, bool)
	GetFileStats(string) (*DatasetInput, error)

	SimGet(context.Context, *pb.SimCommonFile) (*pb.ActionResult, error)
	SimClear(context.Context, *empty.Empty) (*pb.SimCacheStatus, error)
	SimClearFiles(context.Context, *empty.Empty) (*pb.SimCacheStatus, error)
	SimClearHitMissStats(context.Context, *empty.Empty) (*pb.SimCacheStatus, error)
	SimGetInfoCacheStatus(context.Context, *empty.Empty) (*pb.SimCacheStatus, error)
	SimDumps(*empty.Empty, pb.SimService_SimDumpsServer) error
	SimLoads(pb.SimService_SimLoadsServer) error
}

// DatasetInput contains file statistics collected by weighted caches
type DatasetInput struct {
	Size        float32
	TotRequests uint32
	NHits       uint32
	NMiss       uint32
	MeanTime    float32
}
