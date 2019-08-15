package cache

// Cache is the base interface for the cache object
type Cache interface {
	check(string) bool

	HitRate() float32
	Size() float32
	Capacity() float32
	WrittenData() float32
}
