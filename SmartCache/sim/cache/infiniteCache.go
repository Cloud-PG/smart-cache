package cache

import (
	"simulator/v2/cache/files"
	"simulator/v2/cache/queue"
)

// InfiniteCache cache
type InfiniteCache struct {
	SimpleCache
}

// Init the InfiniteCache struct
func (cache *InfiniteCache) Init(params InitParameters) interface{} {
	params.QueueType = queue.NoQueue
	cache.SimpleCache.Init(params)

	return cache
}

// BeforeRequest of LRU cache
func (cache *InfiniteCache) BeforeRequest(request *Request, hit bool) (*files.Stats, bool) {
	// cache.prevTime = cache.curTime
	// cache.curTime = request.DayTime
	// if !cache.curTime.Equal(cache.prevTime) {}
	cache.numReq++

	curStats, _ := cache.stats.GetOrCreate(request.Filename, request.Size, request.DayTime, cache.tick)

	return curStats, hit
}

// UpdatePolicy of InfiniteCache cache
func (cache *InfiniteCache) UpdatePolicy(request *Request, fileStats *files.Stats, hit bool) (added bool) {
	requestedFileSize := request.Size

	if !hit {
		queue.Insert(cache.files, fileStats)

		added = true

		cache.size += requestedFileSize
		cache.MaxSize += requestedFileSize
	}

	return added
}
