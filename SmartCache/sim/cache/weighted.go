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

type fileStats struct {
	size              float32
	totRequests       float32
	lastTimeRequested time.Time
	requestTicks      [StatsMemorySize]uint64
	requestLastIdx    int
}

func (stats *fileStats) updateRequests(curTick uint64, newTime time.Time) {
	stats.lastTimeRequested = newTime
	stats.totRequests += 1.

	stats.requestTicks[stats.requestLastIdx] = curTick
	stats.requestLastIdx = (stats.requestLastIdx + 1) % StatsMemorySize
}

func (stats fileStats) getMeanTicks(curTick uint64) float32 {
	var timeMean uint64
	for idx := 0; idx < StatsMemorySize; idx++ {
		if stats.requestTicks[idx] != 0 {
			timeMean += curTick - stats.requestTicks[idx]
		}
	}
	timeMean /= uint64(StatsMemorySize)
	return float32(timeMean)
}

// Weighted cache
type Weighted struct {
	files                                                 map[string]float32
	stats                                                 map[string]*fileStats
	queue                                                 []*weightedFile
	hit, miss, writtenData, readOnHit, size, MaxSize, exp float32
	tick                                                  uint64
	functionType                                          FunctionType
}

// Init the LRU struct
func (cache *Weighted) Init(vars ...interface{}) {
	cache.files = make(map[string]float32)
	cache.stats = make(map[string]*fileStats)
	cache.queue = make([]*weightedFile, 0)
	cache.functionType = vars[0].(FunctionType)
	cache.exp = vars[1].(float32)
}

// Clear the LRU struct
func (cache *Weighted) Clear() {
	cache.files = make(map[string]float32)
	cache.stats = make(map[string]*fileStats)
	cache.queue = make([]*weightedFile, 0)
	cache.hit = 0.
	cache.miss = 0.
	cache.writtenData = 0.
	cache.size = 0.
	cache.tick = 0.
}

func fileWeight(size float32, totRequests float32, exp float32) float32 {
	return float32(math.Pow(float64(size/totRequests), float64(exp)))
}

func fileWeightAndTime(size float32, totRequests float32, exp float32, lastTimeRequested time.Time) float32 {
	deltaLastTimeRequested := float64(time.Now().Sub(lastTimeRequested) / time.Second)
	return (size / float32(math.Pow(float64(totRequests), float64(exp)))) + float32(math.Pow(deltaLastTimeRequested, float64(exp)))
}

func fileWeightOnlyTime(totRequests float32, exp float32, lastTimeRequested time.Time) float32 {
	deltaLastTimeRequested := float64(time.Now().Sub(lastTimeRequested) / time.Second)
	return (1. / float32(math.Pow(float64(totRequests), float64(exp)))) + float32(math.Pow(deltaLastTimeRequested, float64(exp)))
}

func fileWeightedRequest(size float32, totRequests float32, meanTicks float32, exp float32) float32 {
	return float32(math.Pow(float64(meanTicks*(((size/totRequests)/size)*100.)), float64(exp)))
}

// SimServiceGet updates the cache from a protobuf message
func (cache *Weighted) SimServiceGet(ctx context.Context, commonFile *pb.SimCommonFile) (*pb.SimCacheStatus, error) {
	cache.Get(commonFile.Filename, commonFile.Size)
	return &pb.SimCacheStatus{
		HitRate:     cache.HitRate(),
		Size:        cache.Size(),
		Capacity:    cache.Capacity(),
		WrittenData: cache.WrittenData(),
		ReadOnHit:   cache.ReadOnHit(),
	}, nil
}

// SimServiceClear deletes all cache content
func (cache *Weighted) SimServiceClear(ctx context.Context, _ *empty.Empty) (*pb.SimCacheStatus, error) {
	cache.Clear()
	return &pb.SimCacheStatus{
		HitRate:     cache.HitRate(),
		Size:        cache.Size(),
		Capacity:    cache.Capacity(),
		WrittenData: cache.WrittenData(),
		ReadOnHit:   cache.ReadOnHit(),
	}, nil
}

// SimServiceGetInfoCacheFiles returns the content of the cache: filenames and sizes
func (cache *Weighted) SimServiceGetInfoCacheFiles(_ *empty.Empty, stream pb.SimService_SimServiceGetInfoCacheFilesServer) error {
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

// SimServiceGetInfoFilesWeights returns the file weights
func (cache *Weighted) SimServiceGetInfoFilesWeights(_ *empty.Empty, stream pb.SimService_SimServiceGetInfoFilesWeightsServer) error {
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

func (cache *Weighted) getQueueSize() float32 {
	var size float32
	for _, curFile := range cache.queue {
		size += cache.stats[curFile.filename].size
	}
	return size
}

func (cache *Weighted) removeLast() *weightedFile {
	removedElm := cache.queue[len(cache.queue)-1]
	cache.queue = cache.queue[:len(cache.queue)-1]
	return removedElm
}

func (cache *Weighted) updatePolicy(filename string, size float32, hit bool) bool {
	var added = false
	var currentTime = time.Now()

	if _, inMap := cache.stats[filename]; !inMap {
		cache.stats[filename] = &fileStats{
			size,
			0.,
			currentTime,
			[StatsMemorySize]uint64{},
			0,
		}
	}

	cache.stats[filename].updateRequests(cache.tick, currentTime)

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
func (cache *Weighted) Get(filename string, size float32) bool {
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
func (cache Weighted) HitRate() float32 {
	if cache.hit == 0. {
		return 0.
	}
	return (cache.hit / (cache.hit + cache.miss)) * 100.
}

// Size of the cache
func (cache Weighted) Size() float32 {
	return cache.size
}

// Capacity of the cache
func (cache Weighted) Capacity() float32 {
	return (cache.Size() / cache.MaxSize) * 100.
}

// WrittenData of the cache
func (cache Weighted) WrittenData() float32 {
	return cache.writtenData
}

// ReadOnHit of the cache
func (cache Weighted) ReadOnHit() float32 {
	return cache.readOnHit
}

func (cache Weighted) check(key string) bool {
	_, ok := cache.files[key]
	return ok
}
