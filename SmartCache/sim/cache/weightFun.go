package cache

import (
	"encoding/json"
	"fmt"
	"os"

	"simulator/v2/cache/files"
	"simulator/v2/cache/functions"
	"simulator/v2/cache/queue"

	"github.com/rs/zerolog/log"
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
	weightFunction functions.WeightFun
	fParams        WeightFunctionParameters
}

// Init the WeightFun struct
func (cache *WeightFun) Init(params InitParameters) interface{} {
	cache.SimpleCache.Init(params)

	switch params.WfType {
	case functions.Additive:
		cache.weightFunction = functions.FileAdditiveWeight
	case functions.AdditiveExp:
		cache.weightFunction = functions.FileAdditiveExpWeight
	case functions.Multiplicative:
		cache.weightFunction = functions.FileMultiplicativeWeight
	default:
		fmt.Println("ERR: You need to specify a correct weight function.")
		os.Exit(-1)
	}

	cache.fParams = WeightFunctionParameters{
		Alpha: params.WfParams.Alpha,
		Beta:  params.WfParams.Beta,
		Gamma: params.WfParams.Gamma,
	}

	return cache
}

// Dumps the WeightFun cache
func (cache *WeightFun) Dumps(fileAndStats bool) [][]byte {
	log.Info().Msg("Dump cache into byte string")
	outData := make([][]byte, 0)
	var newLine = []byte("\n")

	if fileAndStats {
		// ----- Files -----
		log.Info().Msg("Dump cache files")
		for _, file := range queue.GetFromWorst(cache.files) {
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
		log.Info().Msg("Dump cache stats")
		for _, stats := range cache.stats.Data {
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
	log.Info().Msg("Load cache dump string")
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
			var curFileStats files.Stats
			unmarshalErr = json.Unmarshal([]byte(curRecord.Data), &curFileStats)
			if unmarshalErr != nil {
				panic(unmarshalErr)
			}
			queue.Insert(cache.files, &curFileStats)
			cache.size += curFileStats.Size
			cache.stats.Data[curRecord.Filename] = &curFileStats
		case "STATS":
			var curFileStats files.Stats
			unmarshalErr = json.Unmarshal([]byte(curRecord.Data), &curFileStats)
			if unmarshalErr != nil {
				panic(unmarshalErr)
			}
			if _, inStats := cache.stats.Data[curRecord.Filename]; !inStats {
				cache.stats.Data[curRecord.Filename] = &curFileStats
			}
		}
	}
}

// BeforeRequest of LRU cache
func (cache *WeightFun) BeforeRequest(request *Request, hit bool) (*files.Stats, bool) {
	// cache.prevTime = cache.curTime
	// cache.curTime = request.DayTime
	// if !cache.curTime.Equal(cache.prevTime) {}

	cache.numReq++

	curStats, newFile := cache.stats.GetOrCreate(request.Filename, request.Size, request.DayTime, cache.tick)
	curStats.UpdateStats(hit, request.Size, request.UserID, request.SiteName, request.DayTime)
	cache.stats.UpdateWeight(curStats, newFile,
		cache.weightFunction,
		cache.fParams.Alpha,
		cache.fParams.Beta,
		cache.fParams.Gamma,
	)
	return curStats, hit
}

// UpdatePolicy of WeightFun cache
func (cache *WeightFun) UpdatePolicy(request *Request, fileStats *files.Stats, hit bool) bool {
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

			queue.Insert(cache.files, fileStats)

			added = true
		}
	} else {
		queue.Update(cache.files, fileStats)
	}
	return added
}

// ExtraStats for output
func (cache *WeightFun) ExtraStats() string {
	return fmt.Sprintf(
		"a:%0.2f|b:%0.2f|g:%0.2f|wAVG:%0.2f",
		cache.fParams.Alpha, cache.fParams.Beta, cache.fParams.Gamma, cache.stats.GetWeightMedian(),
	)
}
