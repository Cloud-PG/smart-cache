package cache

import (
	"time"
)

// DumpRecord represents a record in the dump file
type DumpRecord struct {
	Info     string `json:"info"`
	Data     string `json:"data"`
	Filename int64  `json:"filename"`
}

// DumpInfo collects cache marshall info
type DumpInfo struct {
	Type string `json:"type"`
}

// FileDump represents the record of a dumped cache file
type FileDump struct {
	Filename int64   `json:"filename"`
	Size     float64 `json:"size"`
}

// Request represent an ingestable request for the cache
type Request struct {
	Filename int64
	Size     float64
	CPUEff   float64
	Day      int64
	DayTime  time.Time
	SiteName int64
	UserID   int64
	DataType int64
	FileType int64
	Protocol int64
}

// Cache is the base interface for the cache object
type Cache interface {
	Init(...interface{}) interface{}
	SetRegion(string)
	SetBandwidth(float64)

	Dumps(fileAndStats bool) [][]byte
	Dump(filename string, fileAndStats bool)
	Loads([][]byte, ...interface{})
	Load(filename string) [][]byte

	Clear()
	ClearFiles()
	ClearHitMissStats()
	Free(amount float64, percentage bool) float64

	ExtraStats() string
	ExtraOutput(string) string

	HitRate() float64
	HitOverMiss() float64
	WeightedHitRate() float64
	Size() float64
	Occupancy() float64
	DataWritten() float64
	DataRead() float64
	DataReadOnHit() float64
	DataReadOnMiss() float64
	DataDeleted() float64
	CPUEff() float64
	CPUHitEff() float64
	CPUMissEff() float64
	CPUEffUpperBound() float64
	CPUEffLowerBound() float64
	MeanSize() float64
	MeanFrequency() float64
	MeanRecency() float64
	BandwidthUsage() float64

	Check(int64) bool
	CheckWatermark() bool
	BeforeRequest(request *Request, hit bool) *FileStats
	UpdatePolicy(request *Request, fileStats *FileStats, hit bool) bool
	AfterRequest(request *Request, hit bool, added bool)
}

// GetFile requests a file to the cache
func GetFile(bandwidthManager bool, cache Cache, vars ...interface{}) (bool, bool) {
	/* vars:
	[0] -> filename int64
	[1] -> size     float64
	[2] -> wTime    float64
	[3] -> cpuTime  float64
	[4] -> day      int64
	[5] -> siteName int64
	[6] -> userID   int64
	[7] -> fileType   int64
	*/

	cacheRequest := Request{
		Filename: vars[0].(int64),
	}

	switch {
	case len(vars) > 7:
		cacheRequest.FileType = vars[7].(int64)
		fallthrough
	case len(vars) > 6:
		cacheRequest.UserID = vars[6].(int64)
		fallthrough
	case len(vars) > 5:
		cacheRequest.SiteName = vars[5].(int64)
		fallthrough
	case len(vars) > 4:
		cacheRequest.Day = vars[4].(int64)
		cacheRequest.DayTime = time.Unix(cacheRequest.Day, 0)
		fallthrough
	case len(vars) > 3:
		cacheRequest.CPUEff = vars[3].(float64)
		fallthrough
	case len(vars) > 2:
		cacheRequest.Protocol = vars[2].(int64)
		fallthrough
	case len(vars) > 1:
		cacheRequest.Size = vars[1].(float64)
	}

	hit := Check(cache, cacheRequest.Filename)
	if bandwidthManager && !hit && BandwidthUsage(cache) >= 95.0 {
		return false, true
	}
	fileStats := BeforeRequest(cache, &cacheRequest, hit)
	added := UpdatePolicy(cache, &cacheRequest, fileStats, hit)
	AfterRequest(cache, &cacheRequest, hit, added)
	CheckWatermark(cache)
	return added, false
}

// Init initializes the cache instance
func Init(cache Cache, args ...interface{}) interface{} {
	return cache.Init(args...)
}

// HitRate of the current cache instance
func HitRate(cache Cache) float64 {
	return cache.HitRate()
}

// HitOverMiss of the current cache instance
func HitOverMiss(cache Cache) float64 {
	return cache.HitOverMiss()
}

// WeightedHitRate of the current cache instance
func WeightedHitRate(cache Cache) float64 {
	return cache.WeightedHitRate()
}

// Size of the current cache instance
func Size(cache Cache) float64 {
	return cache.Size()
}

// Occupancy of the current cache instance
func Occupancy(cache Cache) float64 {
	return cache.Occupancy()
}

// DataWritten of the current cache instance
func DataWritten(cache Cache) float64 {
	return cache.DataWritten()
}

// DataRead of the current cache instance
func DataRead(cache Cache) float64 {
	return cache.DataRead()
}

// DataReadOnHit of the current cache instance
func DataReadOnHit(cache Cache) float64 {
	return cache.DataReadOnHit()
}

// DataReadOnMiss of the current cache instance
func DataReadOnMiss(cache Cache) float64 {
	return cache.DataReadOnMiss()
}

// DataDeleted of the current cache instance
func DataDeleted(cache Cache) float64 {
	return cache.DataDeleted()
}

// CPUEff of the current cache instance
func CPUEff(cache Cache) float64 {
	return cache.CPUEff()
}

// CPUHitEff of the current cache instance
func CPUHitEff(cache Cache) float64 {
	return cache.CPUHitEff()
}

// CPUMissEff of the current cache instance
func CPUMissEff(cache Cache) float64 {
	return cache.CPUMissEff()
}

// CPUEffUpperBound of the current cache instance
func CPUEffUpperBound(cache Cache) float64 {
	return cache.CPUEffUpperBound()
}

// CPUEffLowerBound of the current cache instance
func CPUEffLowerBound(cache Cache) float64 {
	return cache.CPUEffLowerBound()
}

// MeanSize of the current cache instance
func MeanSize(cache Cache) float64 {
	return cache.MeanSize()
}

// MeanFrequency of the current cache instance
func MeanFrequency(cache Cache) float64 {
	return cache.MeanFrequency()
}

// MeanRecency of the current cache instance
func MeanRecency(cache Cache) float64 {
	return cache.MeanRecency()
}

// BandwidthUsage of the current cache instance
func BandwidthUsage(cache Cache) float64 {
	return cache.BandwidthUsage()
}

// ExtraStats return the extra statistics of the current cache instance
func ExtraStats(cache Cache) string {
	return cache.ExtraStats()
}

// ExtraOutput return the extra info of the current cache instance
func ExtraOutput(cache Cache, key string) string {
	return cache.ExtraOutput(key)
}

// SetRegion set the region for the simulation of the current cache instance
func SetRegion(cache Cache, region string) {
	cache.SetRegion(region)
}

// SetBandwidth set the bandwidth for the simulation  of the current cache instance
func SetBandwidth(cache Cache, bandSize float64) {
	cache.SetBandwidth(bandSize)
}

// Dump the current cache instance
func Dump(cache Cache, filename string, fileAndStats bool) {
	cache.Dump(filename, fileAndStats)
}

// Loads a cache instance
func Loads(cache Cache, data [][]byte, args ...interface{}) {
	cache.Loads(data, args...)
}

// Load a current cache instance
func Load(cache Cache, filename string) [][]byte {
	return cache.Load(filename)
}

// Clear the current cache instance
func Clear(cache Cache) {
	cache.Clear()
}

// ClearFiles the current cache instance
func ClearFiles(cache Cache) {
	cache.ClearFiles()
}

// ClearHitMissStats the current cache instance
func ClearHitMissStats(cache Cache) {
	cache.ClearHitMissStats()
}

// Check the current cache instance
func Check(cache Cache, filename int64) bool {
	return cache.Check(filename)
}

// CheckWatermark of the current cache instance
func CheckWatermark(cache Cache) bool {
	return cache.CheckWatermark()
}

// BeforeRequest of the current cache instance
func BeforeRequest(cache Cache, request *Request, hit bool) *FileStats {
	return cache.BeforeRequest(request, hit)
}

// UpdatePolicy of the current cache instance
func UpdatePolicy(cache Cache, request *Request, fileStats *FileStats, hit bool) bool {
	return cache.UpdatePolicy(request, fileStats, hit)
}

// AfterRequest of the current cache instance
func AfterRequest(cache Cache, request *Request, hit bool, added bool) {
	cache.AfterRequest(request, hit, added)
}
