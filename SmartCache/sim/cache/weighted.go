package cache

import (
	"context"
	"math"
	"sort"
	"time"

	pb "./simService"
	empty "github.com/golang/protobuf/ptypes/empty"
)

// FunctionType is used to select the weight function
type FunctionType int

const (
	// StatsMemorySize indicates the size of fileStats memory
	StatsMemorySize int = 6
)

const (
	// FuncFileWeight indicates the simple function for weighted cache
	FuncFileWeight FunctionType = iota
	// FuncFileWeightAndTime indicates the function that uses time
	FuncFileWeightAndTime
	// FuncFileWeightOnlyTime indicates the function that uses time
	FuncFileWeightOnlyTime
	// FuncWeightedRequests has a small memory for request time
	FuncWeightedRequests
)

type weightedFile struct {
	filename string
	size     float32
	weight   float32
}

type weightedFileStats struct {
	size              float32
	totRequests       uint32
	nHits             uint32
	nMiss             uint32
	lastTimeRequested time.Time
	requestTicks      [StatsMemorySize]uint64
	requestLastIdx    int
}

func (stats *weightedFileStats) updateRequests(hit bool, curTick uint64, newTime time.Time) {
	stats.totRequests++

	if hit {
		stats.nHits++
	} else {
		stats.nMiss++
	}

	stats.lastTimeRequested = newTime

	stats.requestTicks[stats.requestLastIdx] = curTick
	stats.requestLastIdx = (stats.requestLastIdx + 1) % StatsMemorySize
}

func (stats weightedFileStats) getMeanTicks(curTick uint64) float32 {
	var timeMean uint64
	for idx := 0; idx < StatsMemorySize; idx++ {
		if stats.requestTicks[idx] != 0 {
			timeMean += curTick - stats.requestTicks[idx]
		}
	}
	timeMean /= uint64(StatsMemorySize)
	return float32(timeMean)
}

// WeightedCache cache
type WeightedCache struct {
	files                                                 map[string]float32
	stats                                                 map[string]*weightedFileStats
	queue                                                 []*weightedFile
	hit, miss, writtenData, readOnHit, size, MaxSize, exp float32
	tick                                                  uint64
	functionType                                          FunctionType
}

// Init the LRU struct
func (cache *WeightedCache) Init(vars ...interface{}) {
	if len(vars) < 2 {
		panic("ERROR: you need to specify the weighted function to use and the exponent...")
	}
	cache.files = make(map[string]float32)
	cache.stats = make(map[string]*weightedFileStats)
	cache.queue = make([]*weightedFile, 0)
	cache.functionType = vars[0].(FunctionType)
	cache.exp = vars[1].(float32)
}

// Clear the LRU struct
func (cache *WeightedCache) Clear() {
	cache.files = make(map[string]float32)
	cache.stats = make(map[string]*weightedFileStats)
	cache.queue = make([]*weightedFile, 0)
	cache.hit = 0.
	cache.miss = 0.
	cache.writtenData = 0.
	cache.size = 0.
	cache.tick = 0.
}

func fileWeight(size float32, totRequests uint32, exp float32) float32 {
	return float32(math.Pow(float64(size)/float64(totRequests), float64(exp)))
}

func fileWeightAndTime(size float32, totRequests uint32, exp float32, lastTimeRequested time.Time) float32 {
	deltaLastTimeRequested := float64(time.Now().Sub(lastTimeRequested) / time.Second)
	return (size / float32(math.Pow(float64(totRequests), float64(exp)))) + float32(math.Pow(deltaLastTimeRequested, float64(exp)))
}

func fileWeightOnlyTime(totRequests uint32, exp float32, lastTimeRequested time.Time) float32 {
	deltaLastTimeRequested := float64(time.Now().Sub(lastTimeRequested) / time.Second)
	return (1. / float32(math.Pow(float64(totRequests), float64(exp)))) + float32(math.Pow(deltaLastTimeRequested, float64(exp)))
}

func fileWeightedRequest(size float32, totRequests uint32, meanTicks float32, exp float32) float32 {
	return meanTicks + (size / float32(math.Pow(
		float64(totRequests),
		float64(exp))))
}

// SimGet updates the cache from a protobuf message
func (cache *WeightedCache) SimGet(ctx context.Context, commonFile *pb.SimCommonFile) (*pb.ActionResult, error) {
	added := cache.Get(commonFile.Filename, commonFile.Size)
	return &pb.ActionResult{
		Filename: commonFile.Filename,
		Added:    added,
	}, nil
}

// SimReset deletes all cache content
func (cache *WeightedCache) SimReset(ctx context.Context, _ *empty.Empty) (*pb.SimCacheStatus, error) {
	cache.Clear()
	return &pb.SimCacheStatus{
		HitRate:         cache.HitRate(),
		WeightedHitRate: cache.WeightedHitRate(),
		Size:            cache.Size(),
		Capacity:        cache.Capacity(),
		WrittenData:     cache.WrittenData(),
		ReadOnHit:       cache.ReadOnHit(),
	}, nil
}

// SimGetInfoCacheStatus returns the current simulation status
func (cache *WeightedCache) SimGetInfoCacheStatus(ctx context.Context, _ *empty.Empty) (*pb.SimCacheStatus, error) {
	return &pb.SimCacheStatus{
		HitRate:         cache.HitRate(),
		WeightedHitRate: cache.WeightedHitRate(),
		Size:            cache.Size(),
		Capacity:        cache.Capacity(),
		WrittenData:     cache.WrittenData(),
		ReadOnHit:       cache.ReadOnHit(),
	}, nil
}

// SimGetInfoCacheFiles returns the content of the cache: filenames and sizes
func (cache *WeightedCache) SimGetInfoCacheFiles(_ *empty.Empty, stream pb.SimService_SimGetInfoCacheFilesServer) error {
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
func (cache *WeightedCache) SimGetInfoFilesWeights(_ *empty.Empty, stream pb.SimService_SimGetInfoFilesWeightsServer) error {
	for filename, stats := range cache.stats {
		var weight float32

		switch cache.functionType {
		case FuncFileWeight:
			weight = fileWeight(
				stats.size,
				stats.totRequests,
				cache.exp,
			)
		case FuncFileWeightAndTime:
			weight = fileWeightAndTime(
				stats.size,
				stats.totRequests,
				cache.exp,
				stats.lastTimeRequested,
			)
		case FuncFileWeightOnlyTime:
			weight = fileWeightOnlyTime(
				stats.totRequests,
				cache.exp,
				stats.lastTimeRequested,
			)
		case FuncWeightedRequests:
			weight = fileWeightedRequest(
				stats.size,
				stats.totRequests,
				stats.getMeanTicks(cache.tick),
				cache.exp,
			)
		}

		curFile := &pb.SimFileWeight{
			Filename: filename,
			Weight:   weight,
		}
		if err := stream.Send(curFile); err != nil {
			return err
		}
	}

	return nil
}

func (cache *WeightedCache) getQueueSize() float32 {
	var size float32
	for _, curFile := range cache.queue {
		size += cache.stats[curFile.filename].size
	}
	return size
}

func (cache *WeightedCache) removeLast() *weightedFile {
	removedElm := cache.queue[len(cache.queue)-1]
	cache.queue = cache.queue[:len(cache.queue)-1]
	return removedElm
}

func (cache *WeightedCache) updatePolicy(filename string, size float32, hit bool) bool {
	var added = false
	var currentTime = time.Now()

	if _, inMap := cache.stats[filename]; !inMap {
		cache.stats[filename] = &weightedFileStats{
			size,
			0.,
			0,
			0,
			currentTime,
			[StatsMemorySize]uint64{},
			0,
		}
	}

	cache.stats[filename].updateRequests(hit, cache.tick, currentTime)

	if !hit {
		cache.queue = append(
			cache.queue,
			&weightedFile{
				filename,
				size,
				-1.,
			},
		)
		added = true
	}

	queueSize := cache.getQueueSize()
	if queueSize > cache.MaxSize {
		// Update weights
		for _, curFile := range cache.queue {
			curStats := cache.stats[curFile.filename]
			switch cache.functionType {
			case FuncFileWeight:
				curFile.weight = fileWeight(
					curStats.size,
					curStats.totRequests,
					cache.exp,
				)
			case FuncFileWeightAndTime:
				curFile.weight = fileWeightAndTime(
					curStats.size,
					curStats.totRequests,
					cache.exp,
					curStats.lastTimeRequested,
				)
			case FuncFileWeightOnlyTime:
				curFile.weight = fileWeightOnlyTime(
					curStats.totRequests,
					cache.exp,
					curStats.lastTimeRequested,
				)
			case FuncWeightedRequests:
				curFile.weight = fileWeightedRequest(
					curStats.size,
					curStats.totRequests,
					curStats.getMeanTicks(cache.tick),
					cache.exp,
				)
			}
		}
		// Sort queue
		sort.Slice(
			cache.queue,
			func(i, j int) bool {
				return cache.queue[i].weight < cache.queue[j].weight
			},
		)
		// Remove files
		for {
			if queueSize <= cache.MaxSize {
				break
			}
			elmRemoved := cache.removeLast()

			if elmRemoved.filename == filename {
				added = false
			} else {
				cache.size -= cache.files[elmRemoved.filename]
				delete(cache.files, elmRemoved.filename)
			}

			queueSize -= cache.stats[elmRemoved.filename].size
		}
	}

	if added {
		cache.files[filename] = size
		cache.size += size
	}

	cache.tick++

	return added
}

// Get a file from the cache updating the statistics
func (cache *WeightedCache) Get(filename string, size float32) bool {
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
func (cache WeightedCache) HitRate() float32 {
	if cache.hit == 0. {
		return 0.
	}
	return (cache.hit / (cache.hit + cache.miss)) * 100.
}

// WeightedHitRate of the cache
func (cache WeightedCache) WeightedHitRate() float32 {
	if cache.hit == 0. {
		return 0.
	}
	var sumHits float32
	var sumMiss float32
	for _, stats := range cache.stats {
		sumHits += float32(stats.nHits) * stats.size
		sumMiss += float32(stats.nMiss) * stats.size
	}
	return (sumHits / (sumHits + sumMiss)) * 100.
}

// Size of the cache
func (cache WeightedCache) Size() float32 {
	return cache.size
}

// Capacity of the cache
func (cache WeightedCache) Capacity() float32 {
	return (cache.Size() / cache.MaxSize) * 100.
}

// WrittenData of the cache
func (cache WeightedCache) WrittenData() float32 {
	return cache.writtenData
}

// ReadOnHit of the cache
func (cache WeightedCache) ReadOnHit() float32 {
	return cache.readOnHit
}

func (cache WeightedCache) check(key string) bool {
	_, ok := cache.files[key]
	return ok
}
