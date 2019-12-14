package cache

// LRUStats collector
type LRUStats struct {
	stats map[string]*LRUFileStats
}

// GetOrCreate add the file into stats and returns it
func (statStruct *LRUStats) GetOrCreate(filename string, size float32) *LRUFileStats {
	curStats, inStats := statStruct.stats[filename]
	if !inStats {
		curStats = &LRUFileStats{
			size,
			0,
			0,
			0,
		}
		statStruct.stats[filename] = curStats
	}
	return curStats
}

// LRUFileStats contain file statistics collected by LRU cache
type LRUFileStats struct {
	size        float32
	totRequests uint32
	nHits       uint32
	nMiss       uint32
}

func (stats *LRUFileStats) updateRequests(hit bool) {
	stats.totRequests++

	if hit {
		stats.nHits++
	} else {
		stats.nMiss++
	}
}
