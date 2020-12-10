package cache

import (
	"math/rand"
	"simulator/v2/cache/files"
	"simulator/v2/cache/queue"
)

const (
	randThreshold = 0.5
)

// RandomCache cache
type RandomCache struct {
	SimpleCache
	randomGenerator *rand.Rand
}

// Init the RandomCache struct
func (cache *RandomCache) Init(params InitParameters) interface{} {
	params.QueueType = queue.LRUQueue
	cache.SimpleCache.Init(params)

	cache.randomGenerator = rand.New(rand.NewSource(params.RandSeed))

	return cache
}

// BeforeRequest of LRU cache
func (cache *RandomCache) BeforeRequest(request *Request, hit bool) (*files.Stats, bool) {
	// cache.prevTime = cache.curTime
	// cache.curTime = request.DayTime
	// if !cache.curTime.Equal(cache.prevTime) {}
	cache.numReq++

	curStats, _ := cache.stats.GetOrCreate(request.Filename, request.Size, request.DayTime, cache.tick)

	return curStats, hit
}

// UpdatePolicy of RandomCache cache
func (cache *RandomCache) UpdatePolicy(request *Request, fileStats *files.Stats, hit bool) (added bool) {
	requestedFileSize := request.Size

	if !hit {
		if cache.randomGenerator.Float32() >= randThreshold {
			queue.Insert(cache.files, fileStats)

			added = true

			cache.size += requestedFileSize
			cache.MaxSize += requestedFileSize
		}
	}

	return added
}
