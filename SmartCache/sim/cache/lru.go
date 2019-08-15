package cache

// LRU: the LRU cache
type LRU struct {
	hit, miss, MaxSize float32
}

func (cache LRU) hitRate() float32 {
	return cache.hit / (cache.hit + cache.miss)
}
func (cache LRU) capacity() float32 {
	return 0.0
}
