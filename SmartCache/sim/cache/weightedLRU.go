package cache

import (
	"encoding/json"
)

// WeightedFunctionParameters are the input parameters of the weighted function
type WeightedFunctionParameters struct {
	Alpha float64
	Beta  float64
	Gamma float64
}

// WeightedLRU cache
type WeightedLRU struct {
	LRUCache
	Parameters      WeightedFunctionParameters
	SelFunctionType FunctionType
}

// Init the WeightedLRU struct
func (cache *WeightedLRU) Init(_ ...interface{}) interface{} {
	cache.LRUCache.Init()

	return cache
}

// Clear the WeightedLRU struct
func (cache *WeightedLRU) Clear() {
	cache.LRUCache.Clear()
	cache.LRUCache.Init()
}

// Dumps the WeightedLRU cache
func (cache *WeightedLRU) Dumps() [][]byte {
	outData := make([][]byte, 0)
	var newLine = []byte("\n")

	// ----- Files -----
	for filename, size := range cache.files {
		dumpInfo, _ := json.Marshal(DumpInfo{Type: "FILES"})
		dumpFile, _ := json.Marshal(FileDump{
			Filename: filename,
			Size:     size,
		})
		record, _ := json.Marshal(DumpRecord{
			Info: string(dumpInfo),
			Data: string(dumpFile),
		})
		record = append(record, newLine...)
		outData = append(outData, record)
	}
	// ----- Stats -----
	for _, stats := range cache.stats.fileStats {
		dumpInfo, _ := json.Marshal(DumpInfo{Type: "STATS"})
		dumpStats, _ := json.Marshal(stats)
		record, _ := json.Marshal(DumpRecord{
			Info: string(dumpInfo),
			Data: string(dumpStats),
		})
		record = append(record, newLine...)
		outData = append(outData, record)
	}
	return outData
}

// Loads the WeightedLRU cache
func (cache *WeightedLRU) Loads(inputString [][]byte) {
	var curRecord DumpRecord
	var curRecordInfo DumpInfo

	for _, record := range inputString {
		buffer := record[:len(record)-1]
		json.Unmarshal(buffer, &curRecord)
		json.Unmarshal([]byte(curRecord.Info), &curRecordInfo)
		switch curRecordInfo.Type {
		case "FILES":
			var curFile FileDump
			json.Unmarshal([]byte(curRecord.Data), &curFile)
			cache.files[curFile.Filename] = curFile.Size
			cache.size += curFile.Size
		case "STATS":
			json.Unmarshal([]byte(curRecord.Data), &cache.stats.fileStats)
		}
	}
}

// BeforeRequest of LRU cache
func (cache *WeightedLRU) BeforeRequest(request *Request, hit bool) *FileStats {
	curStats, newFile := cache.stats.GetOrCreate(request.Filename, request.Size, request.DayTime)
	curStats.updateStats(hit, request.Size, request.UserID, request.SiteName, request.DayTime)
	cache.stats.updateWeight(curStats, newFile, cache.SelFunctionType, cache.Parameters.Alpha, cache.Parameters.Beta, cache.Parameters.Gamma)
	return curStats
}

// UpdatePolicy of WeightedLRU cache
func (cache *WeightedLRU) UpdatePolicy(request *Request, fileStats *FileStats, hit bool) bool {
	var added = false

	requestedFileSize := request.Size
	requestedFilename := request.Filename

	if !hit {

		// If weight is higher exit and return added = false
		// and skip the file insertion
		if fileStats.Weight > cache.stats.GetWeightMedian() {
			return added
		}
		// Insert with LRU mechanism
		if cache.Size()+requestedFileSize > cache.MaxSize {
			cache.Free(requestedFileSize, false)
		}
		if cache.Size()+requestedFileSize <= cache.MaxSize {
			cache.files[requestedFilename] = requestedFileSize
			cache.queue = append(cache.queue, requestedFilename)
			cache.size += requestedFileSize
			fileStats.addInCache(nil)
			added = true
		}
	} else {
		cache.UpdateFileInQueue(requestedFilename)
	}
	return added
}
