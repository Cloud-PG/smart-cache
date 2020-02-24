package cache

import (
	"compress/gzip"
	"encoding/json"
	"fmt"
	"os"
	"simulator/v2/cache/ai/featuremap"
	qlearn "simulator/v2/cache/qLearn"
	"sort"
	"time"

	"go.uber.org/zap"
)

const (
	bandwidthLimit = (1000000. / 8.) * 60. * 60. * 24.
)

// AIRL cache
type AIRL struct {
	LRUCache
	prevTime          time.Time
	curTime           time.Time
	aiFeatureMap      map[string]featuremap.Obj
	aiFeatureMapOrder []string
	additionTable     *qlearn.QTable
	evictionTable     *qlearn.QTable
	qPrevState        map[int64]string
	qPrevAction       map[int64]qlearn.ActionType
	points            float64
	prevPoints        float64
	dailyReadOnHit    float64
	dailyReadOnMiss   float64
}

// Init the AIRL struct
func (cache *AIRL) Init(args ...interface{}) interface{} {
	logger = zap.L()

	cache.LRUCache.Init()

	featureMapFilePath := args[0].(string)

	cache.qPrevState = make(map[int64]string, 0)
	cache.qPrevAction = make(map[int64]qlearn.ActionType, 0)

	cache.aiFeatureMap = featuremap.Parse(featureMapFilePath)

	for key := range cache.aiFeatureMap {
		cache.aiFeatureMapOrder = append(cache.aiFeatureMapOrder, key)
	}
	sort.Strings(cache.aiFeatureMapOrder)

	cache.additionTable = &qlearn.QTable{}
	inputLengths := []int{}
	for _, featureName := range cache.aiFeatureMapOrder {
		curFeature, _ := cache.aiFeatureMap[featureName]
		curLen := len(curFeature.Values)
		if curFeature.UnknownValues {
			curLen++
		}
		inputLengths = append(inputLengths, curLen)
	}
	logger.Info("[Generate QTable]")
	cache.additionTable.Init(inputLengths, qlearn.AdditionTable)
	logger.Info("[Done]")

	return nil
}

// Clear the AIRL struct
func (cache *AIRL) Clear() {
	cache.LRUCache.Clear()
	cache.LRUCache.Init()
}

// Dumps the AIRL cache
func (cache *AIRL) Dumps() [][]byte {
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
	for filename, stats := range cache.Stats.fileStats {
		dumpInfo, _ := json.Marshal(DumpInfo{Type: "STATS"})
		dumpStats, _ := json.Marshal(stats)
		record, _ := json.Marshal(DumpRecord{
			Info:     string(dumpInfo),
			Data:     string(dumpStats),
			Filename: filename,
		})
		record = append(record, newLine...)
		outData = append(outData, record)
	}
	// ----- qtable -----
	dumpInfo, _ := json.Marshal(DumpInfo{Type: "ADDQTABLE"})
	dumpStats, _ := json.Marshal(cache.additionTable)
	record, _ := json.Marshal(DumpRecord{
		Info: string(dumpInfo),
		Data: string(dumpStats),
	})
	record = append(record, newLine...)
	outData = append(outData, record)

	return outData
}

// Dump the AIRL cache
func (cache *AIRL) Dump(filename string) {
	logger.Info("Dump cache", zap.String("filename", filename))
	outFile, osErr := os.Create(filename)
	if osErr != nil {
		panic(fmt.Sprintf("Error dump file creation: %s", osErr))
	}
	gwriter := gzip.NewWriter(outFile)

	for _, record := range cache.Dumps() {
		gwriter.Write(record)
	}

	gwriter.Close()
}

// Loads the AIRL cache
func (cache *AIRL) Loads(inputString [][]byte) {
	var (
		curRecord     DumpRecord
		curRecordInfo DumpInfo
		unmarshalErr  error
	)

	for _, record := range inputString {
		unmarshalErr = json.Unmarshal(record, &curRecord)
		if unmarshalErr != nil {
			panic(unmarshalErr)
		}
		unmarshalErr = json.Unmarshal([]byte(curRecord.Info), &curRecordInfo)
		if unmarshalErr != nil {
			panic(unmarshalErr)
		}
		switch curRecordInfo.Type {
		case "FILES":
			var curFile FileDump
			unmarshalErr = json.Unmarshal([]byte(curRecord.Data), &curFile)
			cache.files[curFile.Filename] = curFile.Size
			cache.size += curFile.Size
		case "STATS":
			var curFileStats FileStats
			unmarshalErr = json.Unmarshal([]byte(curRecord.Data), &curFileStats)
			cache.Stats.fileStats[curRecord.Filename] = &curFileStats
		case "ADDQTABLE":
			unmarshalErr = json.Unmarshal([]byte(curRecord.Data), cache.additionTable)
			cache.additionTable.ResetParams()
		}
		if unmarshalErr != nil {
			panic(fmt.Sprintf("%+v", unmarshalErr))
		}
	}

}

func (cache *AIRL) getCategory(catKey string, value interface{}) []bool {
	var (
		res         []bool
		inputValueI int64
		inputValueF float64
		inputValueS string
	)
	curCategory := cache.aiFeatureMap[catKey]

	if curCategory.UnknownValues == true || curCategory.BucketOpenRight == true {
		res = make([]bool, curCategory.GetLenKeys()+1)
	} else {
		res = make([]bool, curCategory.GetLenKeys())
	}

	if curCategory.Buckets == false {
		if curCategory.UnknownValues {
			oneHot, inMap := curCategory.Values[value.(string)]
			if inMap {
				res[oneHot] = true
			} else {
				res[0] = true
			}
		} else {
			res[curCategory.Values[string(value.(int64))]] = true
		}
		return res
	}

	switch curCategory.Type {
	case featuremap.TypeInt:
		inputValueI = int64(value.(float64))
	case featuremap.TypeFloat:
		inputValueF = value.(float64)
	case featuremap.TypeString:
		inputValueS = value.(string)
	}

	for curKey := range curCategory.GetKeys() {
		switch curCategory.Type {
		case featuremap.TypeInt:
			if inputValueI <= curKey.ValueI {
				res[curCategory.Values[fmt.Sprintf("%d", curKey.ValueI)]] = true
				return res
			}
		case featuremap.TypeFloat:
			if inputValueF <= curKey.ValueF {
				res[curCategory.Values[fmt.Sprintf("%0.2f", curKey.ValueF)]] = true
				return res
			}
		case featuremap.TypeString:
			if inputValueS <= curKey.ValueS {
				res[curCategory.Values[fmt.Sprintf("%s", curKey.ValueS)]] = true
				return res
			}
		}
	}

	if curCategory.BucketOpenRight == true {
		res[curCategory.Values["max"]] = true
		return res
	}

	panic(fmt.Sprintf("Cannot convert a value '%v' of category %s", value, catKey))
}

func (cache *AIRL) getState(request *Request, fileStats *FileStats) []bool {
	var (
		inputVector []bool
		tmpArr      []bool
	)

	dataType := request.DataType

	numReq, _, _ := fileStats.getStats()
	size := request.Size

	cacheCapacity := float64(cache.Capacity())
	deltaHighWatermark := float64(cache.HighWaterMark) - cacheCapacity

	for _, featureName := range cache.aiFeatureMapOrder {
		switch featureName {
		case "size":
			tmpArr = cache.getCategory(featureName, float64(size))
		case "numReq":
			tmpArr = cache.getCategory(featureName, float64(numReq))
		case "cacheUsage":
			tmpArr = cache.getCategory(featureName, cacheCapacity)
		case "dataType":
			tmpArr = cache.getCategory(featureName, dataType)
		case "deltaNumLastRequest":
			tmpArr = cache.getCategory(featureName, float64(fileStats.DeltaLastRequest))
		case "deltaHighWatermark":
			tmpArr = cache.getCategory(featureName, deltaHighWatermark)
		default:
			panic(fmt.Sprintf("Cannot prepare input %s", featureName))
		}
		inputVector = append(inputVector, tmpArr...)
	}

	return inputVector
}

// GetPoints returns the total amount of points for the files in cache
func (cache AIRL) GetPoints() float64 {
	points := 0.0
	for filename := range cache.files {
		points += cache.updateFilesPoints(filename, &cache.curTime)
	}
	return float64(points)
}

// BeforeRequest of LRU cache
func (cache *AIRL) BeforeRequest(request *Request, hit bool) *FileStats {
	fileStats, _ := cache.GetOrCreate(request.Filename, request.Size, request.DayTime)

	cache.prevTime = cache.curTime
	cache.curTime = request.DayTime

	if !cache.curTime.Equal(cache.prevTime) {
		cache.dailyReadOnHit = 0.0
		cache.dailyReadOnMiss = 0.0
	}

	// if !cache.curTime.Equal(cache.prevTime) {
	// 	cache.points = cache.GetPoints()
	// }

	// cache.prevPoints = cache.points

	// if !hit {
	// 	fileStats.updateStats(hit, request.Size, request.UserID, request.SiteName, request.DayTime)
	// 	fileStats.updateFilePoints(&cache.curTime)
	// } else {
	// 	cache.points -= fileStats.Points
	// 	fileStats.updateStats(hit, request.Size, request.UserID, request.SiteName, request.DayTime)
	// 	fileStats.updateFilePoints(&cache.curTime)
	// 	cache.points += fileStats.Points
	// }

	fileStats.updateStats(hit, request.Size, request.UserID, request.SiteName, request.DayTime)

	return fileStats
}

// UpdatePolicy of AIRL cache
func (cache *AIRL) UpdatePolicy(request *Request, fileStats *FileStats, hit bool) bool {
	var (
		added     = false
		curAction qlearn.ActionType
		curState  string

		requestedFilename = request.Filename
		requestedFileSize = request.Size
	)

	// Check learning phase or not
	expTradeoff := cache.additionTable.GetRandomFloat()

	if expTradeoff > cache.additionTable.Epsilon {
		//if cache.additionTable.Epsilon <= cache.additionTable.MinEpsilon { // Force learning until epsilon is > min epsilon
		// ########################
		// ##### Normal phase #####
		// ########################

		if !hit {
			// ########################
			// ##### MISS branch  #####
			// ########################

			curState = qlearn.State2String(cache.getState(request, fileStats))
			curAction = cache.additionTable.GetBestAction(curState)
			logger.Info("Normal MISS branch", zap.String("curState", curState), zap.Int("curAction", int(curAction)))
			// ----------------------------------
			// QLearn - Take the action NOT STORE
			// ----------------------------------
			if curAction == qlearn.ActionNotStore {
				logger.Info("Normal MISS branch NOT TO STORE ACTION")
				return added
			}
			logger.Info("Normal MISS branch STORE ACTION")
			// ------------------------------
			// QLearn - Take the action STORE
			// ------------------------------
			// Insert with LRU mechanism
			if cache.Size()+requestedFileSize > cache.MaxSize {
				cache.Free(requestedFileSize, false)
			}
			if cache.Size()+requestedFileSize <= cache.MaxSize {
				cache.files[requestedFilename] = requestedFileSize
				cache.queue = append(cache.queue, requestedFilename)
				cache.size += requestedFileSize
				added = true

				fileStats.addInCache(&request.DayTime)
				// fileStats.updateFilePoints(&cache.curTime)
				// cache.points += fileStats.Points
			}
		} else {
			// #######################
			// ##### HIT branch  #####
			// #######################
			logger.Info("Normal hit branch")
			cache.UpdateFileInQueue(requestedFilename)
		}
	} else {
		// ##########################
		// ##### Learning phase #####
		// ##########################

		if !hit {
			// ########################
			// ##### MISS branch  #####
			// ########################

			curState = qlearn.State2String(cache.getState(request, fileStats))

			// ----- Random choice -----
			if randomAction := cache.additionTable.GetRandomFloat(); randomAction > 0.5 {
				curAction = qlearn.ActionStore
			} else {
				curAction = qlearn.ActionNotStore
			}

			logger.Info("Learning MISS branch", zap.String("curState", curState), zap.Int("curAction", int(curAction)))

			// ----------------------------------
			// QLearn - Take the action NOT STORE
			if curAction == qlearn.ActionNotStore {
				// newScore := cache.points
				// diff := newScore - cache.prevPoints
				// reward := 0.
				// if diff >= 0 {
				// 	reward += 1.
				// } else {
				// 	reward -= 1.
				// }

				reward := 0.
				if cache.dataReadOnHit < cache.dataReadOnMiss/2.0 || cache.dailyReadOnHit < cache.dailyReadOnMiss/2.0 || cache.dailyReadOnMiss > bandwidthLimit {
					reward -= float64(request.Size)
				} else {
					reward += float64(request.Size)
				}

				// Update table
				cache.additionTable.Update(curState, curAction, reward)
				// Update epsilon
				cache.additionTable.UpdateEpsilon()
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
				added = true

				fileStats.addInCache(&request.DayTime)
				// fileStats.updateFilePoints(&cache.curTime)
				// cache.points += fileStats.Points
			}

			// ------------------------------
			// QLearn - Take the action STORE
			if curAction == qlearn.ActionStore {
				// newScore := cache.points
				// diff := newScore - cache.prevPoints
				// reward := 0.
				// if diff >= 0 {
				// 	reward += 1.
				// } else {
				// 	reward -= 1.
				// }

				reward := 0.
				if cache.dailyReadOnMiss >= bandwidthLimit {
					reward -= float64(request.Size)
				} else {
					reward += float64(request.Size)
				}
				cache.qPrevState[request.Filename] = curState
				cache.qPrevAction[request.Filename] = curAction

				// Update table
				cache.additionTable.Update(curState, curAction, reward)
				// Update epsilon
				cache.additionTable.UpdateEpsilon()
			}

		} else {
			// #######################
			// ##### HIT branch  #####
			// #######################
			cache.UpdateFileInQueue(requestedFilename)

			// ------------------------------
			// QLearn - hit reward on best action
			curState = cache.qPrevState[request.Filename]
			curAction = cache.qPrevAction[request.Filename]

			logger.Info("Learning HIT branch", zap.String("curState", curState), zap.Int("curAction", int(curAction)))

			if curState != "" { // Some action are not taken randomly
				reward := 0.0
				if cache.dataReadOnHit < cache.dataReadOnMiss/2.0 || cache.dailyReadOnHit < cache.dailyReadOnMiss/2.0 {
					reward -= float64(request.Size)
				} else {
					reward += float64(request.Size)
				}

				// Update table
				cache.additionTable.Update(curState, curAction, reward)
				// Update epsilon
				cache.additionTable.UpdateEpsilon()
			}

		}

	}

	return added
}

// AfterRequest of LRU cache
func (cache *AIRL) AfterRequest(request *Request, hit bool, added bool) {
	cache.LRUCache.AfterRequest(request, hit, added)
	if hit {
		cache.dailyReadOnHit += request.Size
	} else {
		cache.dailyReadOnMiss += request.Size
	}
}

// Free removes files from the cache
func (cache *AIRL) Free(amount float64, percentage bool) float64 {
	var (
		totalDeleted float64
		sizeToDelete float64
	)
	if percentage {
		sizeToDelete = amount * (cache.MaxSize / 100.)
	} else {
		sizeToDelete = amount
	}
	if sizeToDelete > 0. {
		var maxIdx2Delete int
		for idx, curFilename2Delete := range cache.queue {
			fileSize := cache.files[curFilename2Delete]
			curStats := cache.Stats.Get(curFilename2Delete)
			// Update sizes
			cache.size -= fileSize
			cache.dataDeleted += fileSize
			totalDeleted += fileSize

			// curFilePoints := curStats.Points
			// cache.points -= curFilePoints

			// Update sizes
			cache.size -= fileSize
			cache.dataDeleted += fileSize
			totalDeleted += fileSize
			curStats.removeFromCache()

			// Remove from queue
			delete(cache.files, curFilename2Delete)
			maxIdx2Delete = idx

			if totalDeleted >= sizeToDelete {
				break
			}
		}
		cache.queue = cache.queue[maxIdx2Delete+1:]
	}
	return totalDeleted
}

// CheckWatermark checks the watermark levels and resolve the situation
func (cache *AIRL) CheckWatermark() bool {
	goodStatus := cache.LRUCache.CheckWatermark()
	// if !goodStatus {
	// 	cache.points = cache.GetPoints()
	// }
	return goodStatus
}

// ExtraStats for output
func (cache *AIRL) ExtraStats() string {
	return fmt.Sprintf("SCov:%0.2f%%|ACov:%0.2f%%|Eps:%0.5f|P:%0.0f|HMRatio:%v|bandR:%v", cache.additionTable.GetStateCoverage(), cache.additionTable.GetActionCoverage(), cache.additionTable.Epsilon, cache.points, cache.dailyReadOnHit > cache.dailyReadOnMiss/2.0, cache.dailyReadOnMiss <= bandwidthLimit)
}

// ExtraOutput for output specific information
func (cache AIRL) ExtraOutput(info string) string {
	result := ""
	switch info {
	case "qtable":
		result = cache.GetQTable()
	default:
		result = "NONE"
	}
	return result
}

// GetQTable return a string of the qtable in csv format
func (cache AIRL) GetQTable() string {
	return cache.additionTable.ToString(&cache.aiFeatureMap, &cache.aiFeatureMapOrder)
}
