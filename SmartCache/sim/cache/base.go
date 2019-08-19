package cache

import (
	"context"

	pb "./simService"
)

// Cache is the base interface for the cache object
type Cache interface {
	check(string) bool
	updatePolicy(filename string, size float32, hit bool) bool

	Init()

	HitRate() float32
	Size() float32
	Capacity() float32
	WrittenData() float32

	Get(filename string, size float32) bool

	SimServiceGet(context.Context, *pb.SimCommonFile) (*pb.SimCacheStatus, error)
}
