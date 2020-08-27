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
	Init(param InitParameters) interface{}
	SetRegion(string)
	SetBandwidth(float64)

	Dumps(fileAndStats bool) [][]byte
	Dump(filename string, fileAndStats bool)
	Loads([][]byte, ...interface{})
	Load(filename string) [][]byte

	Clear()
	ClearFiles()
	ClearStats()
	Free(amount float64, percentage bool) float64

	ExtraStats() string
	ExtraOutput(string) string

	HitRate() float64
	WeightedHitRate() float64
	Size() float64
	GetMaxSize() float64
	Occupancy() float64
	AvgFreeSpace() float64
	StdDevFreeSpace() float64
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
	Bandwidth() float64
	BandwidthUsage() float64
	NumRequests() int64
	NumHits() int64
	NumAdded() int64
	NumDeleted() int64
	NumRedirected() int64
	RedirectedSize() float64

	Check(int64) bool
	CheckWatermark() bool
	CheckRedirect(filename int64, size float64) bool
	BeforeRequest(request *Request, hit bool) (*FileStats, bool)
	UpdatePolicy(request *Request, fileStats *FileStats, hit bool) bool
	AfterRequest(request *Request, fileStats *FileStats, hit bool, added bool)
	Terminate() error
}

// GetFile requests a file to the cache
func GetFile(cache Cache, vars ...interface{}) (bool, bool) {
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

	// Check file
	hit := Check(cache, cacheRequest.Filename)

	// Check Redirect
	redirect := false
	if !hit {
		redirect = CheckRedirect(cache, cacheRequest.Filename, cacheRequest.Size)
	}
	if redirect {
		return false, redirect
	}
	// Manage request
	fileStats, hit := BeforeRequest(cache, &cacheRequest, hit)
	added := UpdatePolicy(cache, &cacheRequest, fileStats, hit)
	AfterRequest(cache, &cacheRequest, fileStats, hit, added)

	// Check watermarks
	CheckWatermark(cache)

	return added, redirect
}

// InitCache initializes the cache instance
func InitCache(cache Cache, params InitParameters) interface{} {
	return cache.Init(params)
}

// HitRate of the current cache instance
func HitRate(cache Cache) float64 {
	return cache.HitRate()
}

// WeightedHitRate of the current cache instance
func WeightedHitRate(cache Cache) float64 {
	return cache.WeightedHitRate()
}

// Size of the current cache instance
func Size(cache Cache) float64 {
	return cache.Size()
}

// GetMaxSize of the current cache instance
func GetMaxSize(cache Cache) float64 {
	return cache.GetMaxSize()
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

// Bandwidth of the current cache instance
func Bandwidth(cache Cache) float64 {
	return cache.Bandwidth()
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

// ClearStats the current cache instance
func ClearStats(cache Cache) {
	cache.ClearStats()
}

// Check the current cache instance
func Check(cache Cache, filename int64) bool {
	return cache.Check(filename)
}

// CheckRedirect the current cache instance
func CheckRedirect(cache Cache, filename int64, size float64) bool {
	return cache.CheckRedirect(filename, size)
}

// CheckWatermark of the current cache instance
func CheckWatermark(cache Cache) bool {
	return cache.CheckWatermark()
}

// BeforeRequest of the current cache instance
func BeforeRequest(cache Cache, request *Request, hit bool) (*FileStats, bool) {
	return cache.BeforeRequest(request, hit)
}

// UpdatePolicy of the current cache instance
func UpdatePolicy(cache Cache, request *Request, fileStats *FileStats, hit bool) bool {
	return cache.UpdatePolicy(request, fileStats, hit)
}

// AfterRequest of the current cache instance
func AfterRequest(cache Cache, request *Request, fileStats *FileStats, hit bool, added bool) {
	cache.AfterRequest(request, fileStats, hit, added)
}

// AvgFreeSpace of the current cache instance
func AvgFreeSpace(cache Cache) float64 {
	return cache.AvgFreeSpace()
}

// StdDevFreeSpace of the current cache instance
func StdDevFreeSpace(cache Cache) float64 {
	return cache.StdDevFreeSpace()
}

// NumRequests of the current cache instance
func NumRequests(cache Cache) int64 {
	return cache.NumRequests()
}

// NumRedirected of the current cache instance
func NumRedirected(cache Cache) int64 {
	return cache.NumRedirected()
}

// RedirectedSize of the current cache instance
func RedirectedSize(cache Cache) float64 {
	return cache.RedirectedSize()
}

// NumAdded of the current cache instance
func NumAdded(cache Cache) int64 {
	return cache.NumAdded()
}

// NumDeleted of the current cache instance
func NumDeleted(cache Cache) int64 {
	return cache.NumDeleted()
}

// NumHits of the current cache instance
func NumHits(cache Cache) int64 {
	return cache.NumHits()
}

// Terminate of the current cache instance
func Terminate(cache Cache) error {
	return cache.Terminate()
}
