package cache

import (
	"encoding/json"
)

// WeightFunctionParameters are the input parameters of the weighted function
type WeightFunctionParameters struct {
	Alpha float64
	Beta  float64
	Gamma float64
}

// WeightFunLRU cache
type WeightFunLRU struct {
	SimpleCache
	Parameters      WeightFunctionParameters
	SelFunctionType FunctionType
}

// Init the WeightFunLRU struct
func (cache *WeightFunLRU) Init(_ ...interface{}) interface{} {
	cache.SimpleCache.Init()
	return cache
}

// Dumps the WeightFunLRU cache
func (cache *WeightFunLRU) Dumps() [][]byte {
	logger.Info("Dump cache into byte string")
	outData := make([][]byte, 0)
	var newLine = []byte("\n")

	// ----- Files -----
	for file := range cache.files.Get(LRUQueue) {
		dumpInfo, _ := json.Marshal(DumpInfo{Type: "FILES"})
		dumpFile, _ := json.Marshal(file)
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

// Loads the WeightFunLRU cache
func (cache *WeightFunLRU) Loads(inputString [][]byte, _ ...interface{}) {
	logger.Info("Load cache dump string")
	var curRecord DumpRecord
	var curRecordInfo DumpInfo

	for _, record := range inputString {
		buffer := record[:len(record)-1]
		json.Unmarshal(buffer, &curRecord)
		json.Unmarshal([]byte(curRecord.Info), &curRecordInfo)
		switch curRecordInfo.Type {
		case "FILES":
			var curFile FileSupportData
			json.Unmarshal([]byte(curRecord.Data), &curFile)
			cache.files.Insert(curFile)
			cache.size += curFile.Size
		case "STATS":
			json.Unmarshal([]byte(curRecord.Data), &cache.stats.fileStats)
		}
	}
}

// BeforeRequest of LRU cache
func (cache *WeightFunLRU) BeforeRequest(request *Request, hit bool) *FileStats {
	cache.prevTime = cache.curTime
	cache.curTime = request.DayTime

	if !cache.curTime.Equal(cache.prevTime) {
		cache.numDailyHit = 0
		cache.numDailyMiss = 0
		cache.hitCPUEff = 0.
		cache.missCPUEff = 0.
		cache.upperCPUEff = 0.
		cache.lowerCPUEff = 0.
		cache.numLocal = 0
		cache.numRemote = 0
	}

	curStats, newFile := cache.stats.GetOrCreate(request.Filename, request.Size, request.DayTime)
	curStats.updateStats(hit, request.Size, request.UserID, request.SiteName, request.DayTime)
	cache.stats.updateWeight(curStats, newFile, cache.SelFunctionType, cache.Parameters.Alpha, cache.Parameters.Beta, cache.Parameters.Gamma)
	return curStats
}

// UpdatePolicy of WeightFunLRU cache
func (cache *WeightFunLRU) UpdatePolicy(request *Request, fileStats *FileStats, hit bool) bool {
	var added = false

	requestedFileSize := request.Size

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
			cache.files.Insert(FileSupportData{
				Filename:  request.Filename,
				Size:      request.Size,
				Frequency: fileStats.FrequencyInCache,
				Recency:   fileStats.Recency,
			})

			cache.size += requestedFileSize
			fileStats.addInCache(nil)
			added = true
		}
	} else {
		cache.files.Update(FileSupportData{
			Filename:  request.Filename,
			Size:      request.Size,
			Frequency: fileStats.FrequencyInCache,
			Recency:   fileStats.Recency,
		})
	}
	return added
}
