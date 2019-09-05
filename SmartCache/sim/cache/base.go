package cache

import (
	"context"

	pb "./simService"
	empty "github.com/golang/protobuf/ptypes/empty"
)

// Cache is the base interface for the cache object
type Cache interface {
	check(string) bool
	updatePolicy(filename string, size float32, hit bool) bool

	Init(...interface{})
	Clear()

	HitRate() float32
	HitOverMiss() float32
	WeightedHitRate() float32
	Size() float32
	Capacity() float32
	WrittenData() float32
	ReadOnHit() float32

	Get(filename string, size float32) bool

	SimGet(context.Context, *pb.SimCommonFile) (*pb.ActionResult, error)
	SimReset(context.Context, *empty.Empty) (*pb.SimCacheStatus, error)
	SimGetInfoCacheStatus(context.Context, *empty.Empty) (*pb.SimCacheStatus, error)
	SimGetInfoCacheFiles(*empty.Empty, pb.SimService_SimGetInfoCacheFilesServer) error
	SimGetInfoFilesWeights(*empty.Empty, pb.SimService_SimGetInfoFilesWeightsServer) error
	SimGetInfoFilesStats(*empty.Empty, pb.SimService_SimGetInfoFilesStatsServer) error
}
