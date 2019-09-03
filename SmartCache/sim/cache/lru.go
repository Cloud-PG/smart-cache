package cache

import (
	"container/list"
	"context"

	pb "./simService"
	empty "github.com/golang/protobuf/ptypes/empty"
)

// LRU cache
type LRU struct {
	files                                            map[string]float32
	queue                                            *list.List
	hit, miss, writtenData, readOnHit, size, MaxSize float32
}

// Init the LRU struct
func (cache *LRU) Init(vars ...interface{}) {
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

// SimGet updates the cache from a protobuf message
func (cache *LRU) SimGet(ctx context.Context, commonFile *pb.SimCommonFile) (*pb.ActionResult, error) {
	added := cache.Get(commonFile.Filename, commonFile.Size)
	return &pb.ActionResult{
		Filename: commonFile.Filename,
		Added:    added,
	}, nil
}

// SimReset deletes all cache content
func (cache *LRU) SimReset(ctx context.Context, _ *empty.Empty) (*pb.SimCacheStatus, error) {
	cache.Clear()
	return &pb.SimCacheStatus{
		HitRate:     cache.HitRate(),
		Size:        cache.Size(),
		Capacity:    cache.Capacity(),
		WrittenData: cache.WrittenData(),
		ReadOnHit:   cache.ReadOnHit(),
	}, nil
}

// SimGetInfoCacheStatus returns the current simulation status
func (cache *LRU) SimGetInfoCacheStatus(ctx context.Context, _ *empty.Empty) (*pb.SimCacheStatus, error) {
	return &pb.SimCacheStatus{
		HitRate:     cache.HitRate(),
		Size:        cache.Size(),
		Capacity:    cache.Capacity(),
		WrittenData: cache.WrittenData(),
		ReadOnHit:   cache.ReadOnHit(),
	}, nil
}

// SimGetInfoCacheFiles returns the content of the cache: filenames and sizes
func (cache *LRU) SimGetInfoCacheFiles(_ *empty.Empty, stream pb.SimService_SimGetInfoCacheFilesServer) error {
	for key, value := range cache.files {
		curFile := &pb.SimCommonFile{
			Filename: key,
			Size:     value,
		}
		if err := stream.Send(curFile); err != nil {
			return err
		}
	}
	return nil
}

// SimGetInfoFilesWeights returns the file weights
func (cache *LRU) SimGetInfoFilesWeights(_ *empty.Empty, stream pb.SimService_SimGetInfoFilesWeightsServer) error {
	return nil
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
		cache.readOnHit += size
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
	}
	return (cache.hit / (cache.hit + cache.miss)) * 100.
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

// ReadOnHit of the cache
func (cache LRU) ReadOnHit() float32 {
	return cache.readOnHit
}

func (cache LRU) check(key string) bool {
	_, ok := cache.files[key]
	return ok
}
