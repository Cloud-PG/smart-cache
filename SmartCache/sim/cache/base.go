package cache

// Cache is the base interface for the cache object
type Cache interface {
	check(string) bool
	updatePolicy(filename string, size float32, hit bool) bool

	HitRate() float32
	Size() float32
	Capacity() float32
	WrittenData() float32

	Update(filename string, size float32) bool
}
