package cache

import "math"

type FileStats struct {
	NumReq float64
	Size   float64
	Value  float64
}

// SimCache is the simulated cache object
type SimCache struct {
	MaxSize float64
	files   map[string]float64
	stats   map[string]*FileStats
}

// Init initializes the cache object
func (cache *SimCache) Init() {
	cache.files = make(map[string]float64)
	cache.stats = make(map[string]*FileStats, 0)
}

// UpdateStats change the file statistics
func (cache *SimCache) UpdateStats(fileName string, size float64) (float64, float64, float64) {
	curStat, inMap := cache.stats[fileName]

	if !inMap {
		cache.stats[fileName] = &FileStats{
			Size: size,
		}
		curStat, _ = cache.stats[fileName]
	}

	curStat.NumReq++
	curStat.Value = curStat.Size * curStat.NumReq

	return curStat.Size, curStat.NumReq, curStat.Value
}

// GetMinValue returns the minimum value file in the cache
func (cache SimCache) GetMinValue() (string, float64) {
	minValue := math.MaxFloat64
	minFileName := ""
	for fileName := range cache.files {
		if cache.stats[fileName].Value < minValue {
			minValue = cache.stats[fileName].Value
			minFileName = fileName
		}
	}
	return minFileName, minValue
}

// GetSize returns the current cache size
func (cache SimCache) GetSize() float64 {
	curSize := 0.0
	for fileName := range cache.files {
		curSize += cache.stats[fileName].Size
	}
	return curSize
}

// GetStats returns a selected file statistics
func (cache SimCache) GetStats(fileName string) *FileStats {
	return cache.stats[fileName]
}

// GetCapacity returns the current cache filling percentage
func (cache SimCache) GetCapacity() float64 {
	return (cache.GetSize() / cache.MaxSize) * 100.0
}

// GetPoints returns the current cache point amount
func (cache SimCache) GetPoints() float64 {
	points := 0.0
	for fileName := range cache.files {
		curStats := cache.stats[fileName]
		points += curStats.Size * curStats.NumReq
	}
	return points
}

// Insert add a file into the cache. If there is no space left, it starts to
// remove the files with less value and then adds it when there is space
func (cache *SimCache) Insert(fileName string, size float64) {
	if cache.GetSize()+size > cache.MaxSize {
		for {
			fileName, _ := cache.GetMinValue()
			cache.Remove(fileName)
			if cache.GetSize()+size <= cache.MaxSize {
				break
			}
		}
	}

	cache.files[fileName] = cache.stats[fileName].Size
}

// Remove deletes a file from the cache
func (cache *SimCache) Remove(fileName string) {
	delete(cache.files, fileName)
}
