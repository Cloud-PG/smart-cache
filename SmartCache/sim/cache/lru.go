package cache

// LRU cache
type LRU struct {
	cache                           map[string]float32
	hit, miss, writtenData, MaxSize float32
}

// HitRate of the cache
func (cache LRU) HitRate() float32 {
	return cache.hit / (cache.hit + cache.miss)
}

// Size of the cache
func (cache LRU) Size() float32 {
	var totalSize float32
	for _, value := range cache.cache {
		totalSize += value
	}
	return totalSize
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
	_, ok := cache.cache[key]
	return ok
}
