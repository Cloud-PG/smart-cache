package cache

// Cache: the base cache object interface
type Cache interface {
	hitRate() float32
	capacity() float32
	// writtenData() float32
}
