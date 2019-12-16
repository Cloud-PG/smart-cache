package cache

import (
	"container/list"
	"encoding/json"
	"time"
)

// WeightedLRU cache
type WeightedLRU struct {
	LRUCache
	WeightedStats
	Exp                     float32
	SelFunctionType         FunctionType
	SelUpdateStatPolicyType UpdateStatsPolicyType
	SelLimitStatsPolicyType LimitStatsPolicyType
}

// Init the WeightedLRU struct
func (cache *WeightedLRU) Init(_ ...interface{}) interface{} {
	cache.LRUCache.Init()
	cache.WeightedStats.Init()

	return cache
}

// Clear the WeightedLRU struct
func (cache *WeightedLRU) Clear() {
	cache.LRUCache.Init()
	cache.WeightedStats.Init()
}

// Dumps the WeightedLRU cache
func (cache *WeightedLRU) Dumps() *[][]byte {
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
	for _, stats := range cache.stats {
		dumpInfo, _ := json.Marshal(DumpInfo{Type: "STATS"})
		dumpStats, _ := json.Marshal(stats)
		record, _ := json.Marshal(DumpRecord{
			Info: string(dumpInfo),
			Data: string(dumpStats),
		})
		record = append(record, newLine...)
		outData = append(outData, record)
	}
	return &outData
}

// Loads the WeightedLRU cache
func (cache *WeightedLRU) Loads(inputString *[][]byte) {
	var curRecord DumpRecord
	var curRecordInfo DumpInfo

	for _, record := range *inputString {
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
			json.Unmarshal([]byte(curRecord.Data), &cache.stats)
		}
	}
}

func (cache *WeightedLRU) updatePolicy(filename string, size float32, hit bool, vars ...interface{}) bool {
	var (
		added       = false
		currentTime = vars[0].(time.Time)
		curStats    *WeightedFileStats
		newFile     bool
	)

	if cache.SelUpdateStatPolicyType == UpdateStatsOnRequest {
		curStats, newFile = cache.GetOrCreate(filename, size)
		curStats.updateStats(hit, size, &currentTime)
		cache.UpdateWeight(curStats, newFile, cache.SelFunctionType, cache.Exp)
	}

	if !hit {
		if cache.SelUpdateStatPolicyType == UpdateStatsOnMiss {
			curStats, newFile = cache.GetOrCreate(filename, size)
			curStats.updateStats(hit, size, &currentTime)
			cache.UpdateWeight(curStats, newFile, cache.SelFunctionType, cache.Exp)
		}

		// If weight is higher exit and return added = false
		// and skip the file insertion
		if curStats.Weight > cache.GetWeightMedian() {
			return added
		}
		// Insert with LRU mechanism
		if cache.Size()+size > cache.MaxSize {
			var totalDeleted float32
			tmpVal := cache.queue.Front()
			for {
				if tmpVal == nil {
					break
				}
				fileSize := cache.files[tmpVal.Value.(string)]
				cache.size -= fileSize
				cache.dataDeleted += size

				totalDeleted += fileSize
				delete(cache.files, tmpVal.Value.(string))

				tmpVal = tmpVal.Next()
				// Check if all files are deleted
				if tmpVal == nil {
					break
				}
				cache.queue.Remove(tmpVal.Prev())

				if totalDeleted >= size {
					break
				}
			}
		}
		if cache.Size()+size <= cache.MaxSize {
			cache.files[filename] = size
			cache.queue.PushBack(filename)
			cache.size += size
			added = true
		}
	} else {
		var elm2move *list.Element
		for tmpVal := cache.queue.Front(); tmpVal != nil; tmpVal = tmpVal.Next() {
			if tmpVal.Value.(string) == filename {
				elm2move = tmpVal
				break
			}
		}
		if elm2move != nil {
			cache.queue.MoveToBack(elm2move)
		}
	}
	return added
}
