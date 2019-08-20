package cache

import (
	"context"

	pb "./simService"
	google_protobuf "github.com/golang/protobuf/ptypes/empty"
)

// Cache is the base interface for the cache object
type Cache interface {
	check(string) bool
	updatePolicy(filename string, size float32, hit bool) bool

	Init()
	Clear()

	HitRate() float32
	Size() float32
	Capacity() float32
	WrittenData() float32

	Get(filename string, size float32) bool

	SimServiceGet(context.Context, *pb.SimCommonFile) (*pb.SimCacheStatus, error)
	SimServiceClear(ctx context.Context, in *google_protobuf.Empty) (*pb.SimCacheStatus, error)
	SimServiceInfo(ctx context.Context, in *google_protobuf.Empty) (*pb.SimCacheInfo, error)
}
