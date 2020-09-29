package cache

import (
	"encoding/json"
	"fmt"
)

// WeightFunctionParameters are the input parameters of the weighted function
type WeightFunctionParameters struct {
	Alpha float64
	Beta  float64
	Gamma float64
}

// WeightFun cache
type WeightFun struct {
	SimpleCache
	Parameters      WeightFunctionParameters
	SelFunctionType FunctionType
}

// Init the WeightFun struct
func (cache *WeightFun) Init(params InitParameters) interface{} {
	cache.SimpleCache.Init(params)

	return cache
}

// Dumps the WeightFun cache
func (cache *WeightFun) Dumps(fileAndStats bool) [][]byte {
	cache.logger.Info("Dump cache into byte string")
	outData := make([][]byte, 0)
	var newLine = []byte("\n")

	if fileAndStats {
		// ----- Files -----
		cache.logger.Info("Dump cache files")
		for _, file := range QueueGetQueue(cache.files) {
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
		cache.logger.Info("Dump cache stats")
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
	}
	return outData
}

// Loads the WeightFun cache
func (cache *WeightFun) Loads(inputString [][]byte, _ ...interface{}) {
	cache.logger.Info("Load cache dump string")
	var (
		curRecord     DumpRecord
		curRecordInfo DumpInfo
		unmarshalErr  error
	)
	for _, record := range inputString {
		buffer := record[:len(record)-1]
		json.Unmarshal(buffer, &curRecord)
		json.Unmarshal([]byte(curRecord.Info), &curRecordInfo)
		switch curRecordInfo.Type {
		case "FILES":
			var curFileStats FileStats
			unmarshalErr = json.Unmarshal([]byte(curRecord.Data), &curFileStats)
			if unmarshalErr != nil {
				panic(unmarshalErr)
			}
			QueueInsert(cache.files, &curFileStats)
			cache.size += curFileStats.Size
			cache.stats.fileStats[curRecord.Filename] = &curFileStats
		case "STATS":
			var curFileStats FileStats
			unmarshalErr = json.Unmarshal([]byte(curRecord.Data), &curFileStats)
			if unmarshalErr != nil {
				panic(unmarshalErr)
			}
			if _, inStats := cache.stats.fileStats[curRecord.Filename]; !inStats {
				cache.stats.fileStats[curRecord.Filename] = &curFileStats
			}
		}
	}
}

// BeforeRequest of LRU cache
func (cache *WeightFun) BeforeRequest(request *Request, hit bool) (*FileStats, bool) {
	// cache.prevTime = cache.curTime
	// cache.curTime = request.DayTime
	// if !cache.curTime.Equal(cache.prevTime) {}

	cache.numReq++

	curStats, newFile := cache.stats.GetOrCreate(request.Filename, request.Size, request.DayTime, cache.tick)
	curStats.updateStats(hit, request.Size, request.UserID, request.SiteName, request.DayTime)
	cache.stats.updateWeight(curStats, newFile,
		cache.SelFunctionType,
		cache.Parameters.Alpha, cache.Parameters.Beta, cache.Parameters.Gamma,
	)
	return curStats, hit
}

// UpdatePolicy of WeightFun cache
func (cache *WeightFun) UpdatePolicy(request *Request, fileStats *FileStats, hit bool) bool {
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
			cache.size += requestedFileSize

			QueueInsert(cache.files, fileStats)

			added = true
		}
	} else {
		QueueUpdate(cache.files, fileStats)
	}
	return added
}

// ExtraStats for output
func (cache *WeightFun) ExtraStats() string {
	return fmt.Sprintf(
		"a:%0.2f|b:%0.2f|g:%0.2f",
		cache.Parameters.Alpha, cache.Parameters.Beta, cache.Parameters.Gamma,
	)
}
