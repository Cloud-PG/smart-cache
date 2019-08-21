package cache

import (
	"container/list"
	"context"

	pb "./simService"
	google_protobuf "github.com/golang/protobuf/ptypes/empty"
)

// LRU cache
type LRU struct {
	files                                 map[string]float32
	queue                                 *list.List
	hit, miss, writtenData, size, MaxSize float32
}

// Init the LRU struct
func (cache *LRU) Init() {
	cache.files = make(map[string]float32)
	cache.queue = list.New()
}

// Clear the LRU struct
func (cache *LRU) Clear() {
	cache.files = make(map[string]float32)
	tmpVal := cache.queue.Front()
	for {
		if tmpVal == nil {
			break
		} else if tmpVal.Next() == nil {
			cache.queue.Remove(tmpVal)
			break
		}
		tmpVal = tmpVal.Next()
		cache.queue.Remove(tmpVal.Prev())
	}
	cache.queue = list.New()
	cache.hit = 0.
	cache.miss = 0.
	cache.writtenData = 0.
	cache.size = 0.
}

// SimServiceGet updates the cache from a protobuf message
func (cache *LRU) SimServiceGet(ctx context.Context, commonFile *pb.SimCommonFile) (*pb.SimCacheStatus, error) {
	cache.Get(commonFile.Filename, commonFile.Size)
	return &pb.SimCacheStatus{
		HitRate:     cache.HitRate(),
		Size:        cache.Size(),
		WrittenData: cache.WrittenData(),
		Capacity:    cache.Capacity(),
	}, nil
}

// SimServiceClear deletes all cache content
func (cache *LRU) SimServiceClear(ctx context.Context, in *google_protobuf.Empty) (*pb.SimCacheStatus, error) {
	cache.Clear()
	return &pb.SimCacheStatus{
		HitRate:     cache.HitRate(),
		Size:        cache.Size(),
		WrittenData: cache.WrittenData(),
		Capacity:    cache.Capacity(),
	}, nil
}

// SimServiceInfo returns the current cache context
func (cache *LRU) SimServiceInfo(ctx context.Context, in *google_protobuf.Empty) (*pb.SimCacheInfo, error) {
	return &pb.SimCacheInfo{
		CacheFiles: cache.files,
	}, nil
}

func (cache *LRU) updatePolicy(filename string, size float32, hit bool) bool {
	var added = false
	if !hit {
		if cache.Size()+size > cache.MaxSize {
			var totalDeleted float32
			tmpVal := cache.queue.Front()
			for {
				if tmpVal == nil {
					break
				}
				fileSize := cache.files[tmpVal.Value.(string)]
				cache.size -= fileSize
				totalDeleted += fileSize
				delete(cache.files, tmpVal.Value.(string))

				tmpVal = tmpVal.Next()
				cache.queue.Remove(tmpVal.Prev())

				if totalDeleted >= size {
					break
				}
			}
		}
		cache.files[filename] = size
		cache.queue.PushBack(filename)
		cache.size += size
		added = true
	} else {
		var elm2move *list.Element
		for tmpVal := cache.queue.Front(); tmpVal != nil; tmpVal = tmpVal.Next() {
			if tmpVal.Value.(string) == filename {
				elm2move = tmpVal
				break
			}
		}
		if elm2move != nil {
			cache.queue.MoveToBack(elm2move)
		}
	}
	return added
}

// Get a file from the cache updating the statistics
func (cache *LRU) Get(filename string, size float32) bool {
	hit := cache.check(filename)
	added := cache.updatePolicy(filename, size, hit)

	if hit {
		cache.hit += 1.
	} else {
		cache.miss += 1.
	}

	if added {
		cache.writtenData += size
	}

	return added
}

// HitRate of the cache
func (cache LRU) HitRate() float32 {
	if cache.hit == 0. {
		return 0.
	} else {
		return (cache.hit / (cache.hit + cache.miss)) * 100.
	}
}

// Size of the cache
func (cache LRU) Size() float32 {
	return cache.size
}

// Capacity of the cache
func (cache LRU) Capacity() float32 {
	return (cache.Size() / cache.MaxSize) * 100.
}

// WrittenData of the cache
func (cache LRU) WrittenData() float32 {
	return cache.writtenData
}

func (cache LRU) check(key string) bool {
	_, ok := cache.files[key]
	return ok
}
