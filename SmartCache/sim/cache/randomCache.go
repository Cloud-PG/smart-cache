package cache

import (
	"fmt"
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
	cache.SimpleCache.Init(params)

	cache.randomGenerator = rand.New(rand.NewSource(params.RandSeed))

	return cache
}

// UpdatePolicy of LRU cache
func (cache *RandomCache) UpdatePolicy(request *Request, fileStats *files.Stats, hit bool) bool {
	var (
		added             = false
		requestedFileSize = request.Size
	)

	if !hit { //nolint:ignore,nestif
		if cache.randomGenerator.Float32() >= randThreshold {
			if cache.Size()+requestedFileSize > cache.MaxSize {
				cache.Free(requestedFileSize, false)
			}

			if cache.Size()+requestedFileSize <= cache.MaxSize {
				cache.size += requestedFileSize

				queue.Insert(cache.files, fileStats)

				if cache.logFile != nil {
					cache.toLogBuffer([]string{
						fmt.Sprintf("%d", cache.tick),
						ChoiceAdd,
						fmt.Sprintf("%0.2f", cache.size),
						fmt.Sprintf("%0.2f", cache.Capacity()),
						fmt.Sprintf("%d", fileStats.Filename),
						fmt.Sprintf("%0.2f", fileStats.Size),
						fmt.Sprintf("%d", fileStats.Frequency),
						fmt.Sprintf("%d", fileStats.DeltaLastRequest),
					})
				}

				added = true
			}
		}
	} else {
		queue.Update(cache.files, fileStats)
	}

	return added
}
