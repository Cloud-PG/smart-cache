package cache

import (
	"container/list"
	"context"

	pb "./simService"
)

// LRU cache
type LRU struct {
	files                                 map[string]float32
	queue                                 *list.List
	hit, miss, writtenData, size, MaxSize float32
}

// Init the LRU struct
func (cache *LRU) Init() {
	cache.files = make(map[string]float32, 0)
	cache.queue = list.New()
}

// SimServiceGet updates the cache from a protobuf message
func (cache *LRU) SimServiceGet(ctx context.Context, commonFile *pb.SimCommonFile) (*pb.SimCacheStatus, error) {
	cache.Get(commonFile.Filename, commonFile.Size)
	return &pb.SimCacheStatus{
		HitRate:     cache.HitRate(),
		Size:        cache.Size(),
		WrittenData: cache.HitRate(),
		Capacity:    cache.Capacity(),
	}, nil
}

func (cache *LRU) updatePolicy(filename string, size float32, hit bool) bool {
	var res bool
	if !hit {
		if cache.Size()+size > cache.MaxSize {
			for tmpVal := cache.queue.Front(); tmpVal != nil; tmpVal = tmpVal.Next() {
				cache.size -= cache.files[tmpVal.Value.(string)]
				delete(cache.files, tmpVal.Value.(string))
				cache.queue.Remove(tmpVal)
				if cache.Size()+size <= cache.MaxSize {
					break
				}
			}
		}
		cache.files[filename] = size
		cache.queue.PushBack(filename)
		cache.size += size
		res = true
	} else {
		res = false
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
	return res
}

// Get a file from the cache updating the statistics
func (cache *LRU) Get(filename string, size float32) bool {
	hit := cache.check(filename)
	res := cache.updatePolicy(filename, size, hit)

	if hit {
		cache.hit += 1.
	} else {
		cache.miss += 1.
	}

	if res {
		cache.writtenData += size
	}

	return res
}

// HitRate of the cache
func (cache LRU) HitRate() float32 {
	return cache.hit / (cache.hit + cache.miss)
}

// Size of the cache
func (cache LRU) Size() float32 {
	return cache.size
}

// Capacity of the cache
func (cache LRU) Capacity() float32 {
	return cache.Size() / cache.MaxSize
}

// WrittenData of the cache
func (cache LRU) WrittenData() float32 {
	return cache.writtenData
}

func (cache LRU) check(key string) bool {
	_, ok := cache.files[key]
	return ok
}
