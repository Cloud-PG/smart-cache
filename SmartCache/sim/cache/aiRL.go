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
	SimpleCache
	prevTime                time.Time
	curTime                 time.Time
	additionTableOK         bool
	evictionTableOK         bool
	additionFeatureMap      map[string]featuremap.Obj
	additionFeatureMapOrder []string
	evictionFeatureMap      map[string]featuremap.Obj
	evictionFeatureMapOrder []string
	additionTable           qlearn.QTable
	evictionTable           qlearn.QTable
	qAdditionPrevState      map[int64]string
	qAdditionPrevAction     map[int64]qlearn.ActionType
	qEvictionPrevState      string
	qEvictionPrevAction     qlearn.ActionType
	points                  float64
	prevPoints              float64
	bufferCategory          []bool
	bufferInputVector       []bool
	chanCategory            chan bool
}

// Init the AIRL struct
func (cache *AIRL) Init(args ...interface{}) interface{} {
	logger = zap.L()

	cache.SimpleCache.Init()

	cache.qAdditionPrevState = make(map[int64]string, 0)
	cache.qAdditionPrevAction = make(map[int64]qlearn.ActionType, 0)

	additionFeatureMap := args[0].(string)
	evictionFeatureMap := args[1].(string)
	initEpsilon := args[2].(float64)

	logger.Info("Feature maps", zap.String("addition map", additionFeatureMap), zap.String("eviction map", evictionFeatureMap))

	if additionFeatureMap != "" {
		cache.additionFeatureMap = featuremap.Parse(additionFeatureMap)
		for key := range cache.additionFeatureMap {
			cache.additionFeatureMapOrder = append(cache.additionFeatureMapOrder, key)
		}
		sort.Strings(cache.additionFeatureMapOrder)
		cache.additionTable = makeQtable(cache.additionFeatureMap, cache.additionFeatureMapOrder, qlearn.AdditionTable, initEpsilon)
		cache.additionTableOK = true
	} else {
		cache.additionTableOK = false
	}

	if evictionFeatureMap != "" {
		cache.evictionFeatureMap = featuremap.Parse(evictionFeatureMap)
		for key := range cache.evictionFeatureMap {
			cache.evictionFeatureMapOrder = append(cache.evictionFeatureMapOrder, key)
		}
		sort.Strings(cache.evictionFeatureMapOrder)
		cache.evictionTable = makeQtable(cache.evictionFeatureMap, cache.evictionFeatureMapOrder, qlearn.EvictionTable, initEpsilon)
		cache.evictionTableOK = true
	} else {
		cache.evictionTableOK = false
	}

	logger.Info("Table creation done")

	return nil
}

func makeQtable(featureMap map[string]featuremap.Obj, featureOrder []string, role qlearn.QTableRole, initEpsilon float64) qlearn.QTable {
	curTable := qlearn.QTable{}
	inputLengths := []int{}
	for _, featureName := range featureOrder {
		curFeature, _ := featureMap[featureName]
		curLen := len(curFeature.Values)
		if curFeature.UnknownValues {
			curLen++
		}
		inputLengths = append(inputLengths, curLen)
	}
	logger.Info("[Generate QTable]")
	curTable.Init(inputLengths, role, initEpsilon)
	logger.Info("[Done]")
	return curTable
}

// Clear the AIRL struct
func (cache *AIRL) Clear() {
	cache.SimpleCache.Clear()
}

// Dumps the AIRL cache
func (cache *AIRL) Dumps() [][]byte {
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
	for filename, stats := range cache.stats.fileStats {
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
	if cache.additionTableOK {
		// ----- addition qtable -----
		dumpInfo, _ := json.Marshal(DumpInfo{Type: "ADDQTABLE"})
		dumpStats, _ := json.Marshal(cache.additionTable)
		record, _ := json.Marshal(DumpRecord{
			Info: string(dumpInfo),
			Data: string(dumpStats),
		})
		record = append(record, newLine...)
		outData = append(outData, record)
	}
	if cache.evictionTableOK {
		// ----- eviction qtable -----
		dumpInfo, _ := json.Marshal(DumpInfo{Type: "EVCQTABLE"})
		dumpStats, _ := json.Marshal(cache.evictionTable)
		record, _ := json.Marshal(DumpRecord{
			Info: string(dumpInfo),
			Data: string(dumpStats),
		})
		record = append(record, newLine...)
		outData = append(outData, record)
	}

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
	logger.Info("Loads cache dump string")
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
			var curFile FileSupportData
			json.Unmarshal([]byte(curRecord.Data), &curFile)
			cache.files.Insert(curFile)
			cache.size += curFile.Size
		case "STATS":
			var curFileStats FileStats
			unmarshalErr = json.Unmarshal([]byte(curRecord.Data), &curFileStats)
			cache.stats.fileStats[curRecord.Filename] = &curFileStats
		case "ADDQTABLE":
			unmarshalErr = json.Unmarshal([]byte(curRecord.Data), &cache.additionTable)
			cache.additionTable.ResetParams()
			cache.additionTableOK = true
		case "EVCQTABLE":
			unmarshalErr = json.Unmarshal([]byte(curRecord.Data), &cache.evictionTable)
			cache.evictionTable.ResetParams()
			cache.evictionTableOK = true
		}
		if unmarshalErr != nil {
			panic(fmt.Sprintf("%+v", unmarshalErr))
		}
	}

}

func (cache *AIRL) getCategory(featureMap map[string]featuremap.Obj, catKey string, value interface{}) chan bool {
	var (
		inputValueI int64
		inputValueF float64
		inputValueS string
		resPrepared bool = false
	)
	cache.chanCategory = make(chan bool)

	curCategory := featureMap[catKey]

	cache.bufferCategory = cache.bufferCategory[:0]

	if curCategory.UnknownValues == true || curCategory.BucketOpenRight == true {
		cache.bufferCategory = append(cache.bufferCategory, make([]bool, curCategory.GetLenKeys()+1)...)
	} else {
		cache.bufferCategory = append(cache.bufferCategory, make([]bool, curCategory.GetLenKeys())...)
	}

	if curCategory.Buckets == false {
		if curCategory.UnknownValues {
			oneHot, inMap := curCategory.Values[value.(string)]
			if inMap {
				cache.bufferCategory[oneHot] = true
			} else {
				cache.bufferCategory[0] = true
			}
		} else {
			cache.bufferCategory[curCategory.Values[string(value.(int64))]] = true
		}
		resPrepared = true
	}

	if !resPrepared {
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
				if inputValueI <= curKey.(int64) {
					cache.bufferCategory[curCategory.Values[fmt.Sprintf("%d", curKey.(int64))]] = true
					resPrepared = true
				}
			case featuremap.TypeFloat:
				if inputValueF <= curKey.(float64) {
					cache.bufferCategory[curCategory.Values[fmt.Sprintf("%0.2f", curKey.(float64))]] = true
					resPrepared = true
				}
			case featuremap.TypeString:
				if inputValueS <= curKey.(string) {
					cache.bufferCategory[curCategory.Values[fmt.Sprintf("%s", curKey.(string))]] = true
					resPrepared = true
				}
			}
			if resPrepared {
				break
			}
		}
		if !resPrepared {
			if curCategory.BucketOpenRight == true {
				cache.bufferCategory[curCategory.Values["max"]] = true
			} else {
				panic(fmt.Sprintf("Cannot convert a value '%v' of category %s", value, catKey))
			}
		}
	}

	go func() {
		defer close(cache.chanCategory)
		for _, value := range cache.bufferCategory {
			cache.chanCategory <- value
		}
	}()

	return cache.chanCategory
}

func (cache *AIRL) getState(request *Request, fileStats *FileStats, featureOrder []string, featureMap map[string]featuremap.Obj) []bool {
	var (
		size     float64
		numReq   float64
		dataType int64
		channel  chan bool
	)

	if fileStats != nil {
		numReq, _, _ = fileStats.getStats()
	}
	if request != nil {
		size = request.Size
		dataType = request.DataType
	}

	cacheCapacity := float64(cache.Capacity())
	deltaHighWatermark := float64(cache.HighWaterMark) - cacheCapacity

	cache.bufferInputVector = cache.bufferInputVector[:0]

	for _, featureName := range featureOrder {
		switch featureName {
		case "size":
			channel = cache.getCategory(featureMap, featureName, size)
		case "numReq":
			channel = cache.getCategory(featureMap, featureName, numReq)
		case "cacheUsage":
			channel = cache.getCategory(featureMap, featureName, cacheCapacity)
		case "dataType":
			channel = cache.getCategory(featureMap, featureName, dataType)
		case "deltaNumLastRequest":
			channel = cache.getCategory(featureMap, featureName, float64(fileStats.Recency))
		case "deltaHighWatermark":
			channel = cache.getCategory(featureMap, featureName, deltaHighWatermark)
		case "meanSize":
			channel = cache.getCategory(featureMap, featureName, cache.MeanSize())
		case "meanFrequency":
			channel = cache.getCategory(featureMap, featureName, cache.MeanFrequency())
		case "meanRecency":
			channel = cache.getCategory(featureMap, featureName, cache.MeanRecency())
		default:
			panic(fmt.Sprintf("Cannot prepare input %s", featureName))
		}
		for value := range channel {
			cache.bufferInputVector = append(cache.bufferInputVector, value)
		}
	}
	return cache.bufferInputVector
}

// GetPoints returns the total amount of points for the files in cache
func (cache *AIRL) GetPoints() float64 {
	points := 0.0
	for file := range cache.files.Get(LRUQueue) {
		points += cache.stats.updateFilesPoints(file.Filename, &cache.curTime)
	}
	return float64(points)
}

// BeforeRequest of LRU cache
func (cache *AIRL) BeforeRequest(request *Request, hit bool) *FileStats {
	fileStats, _ := cache.stats.GetOrCreate(request.Filename, request.Size, request.DayTime)

	cache.prevTime = cache.curTime
	cache.curTime = request.DayTime

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
		added             = false
		curAction         qlearn.ActionType
		curState          string
		requestedFileSize = request.Size
	)

	if cache.evictionTableOK {

		logger.Debug("EVICTION TABLE OK")

		// Check learning phase or not
		expEvictionTradeoff := cache.evictionTable.GetRandomFloat()
		if expEvictionTradeoff < cache.evictionTable.Epsilon {
			// ###################################
			// ##### Eviction Learning phase #####
			// ###################################
			if cache.qEvictionPrevAction != 0 && len(cache.qEvictionPrevState) != 0 {
				reward := 0.
				if hit {
					reward += request.Size
				} else {
					reward -= request.Size
				}
				// Update table
				cache.evictionTable.Update(cache.qEvictionPrevState, cache.qEvictionPrevAction, reward)
				// Update epsilon
				cache.evictionTable.UpdateEpsilon()
			}
		}
	}

	if cache.additionTableOK {

		logger.Debug("ADDITION TABLE OK")

		// Check learning phase or not
		expAdditionTradeoff := cache.additionTable.GetRandomFloat()

		if expAdditionTradeoff > cache.additionTable.Epsilon {
			//if cache.additionTable.Epsilon <= cache.additionTable.MinEpsilon { // Force learning until epsilon is > min epsilon
			// ########################
			// ##### Normal phase #####
			// ########################

			if !hit {
				// ########################
				// ##### MISS branch  #####
				// ########################

				curState = qlearn.State2String(cache.getState(request, fileStats, cache.additionFeatureMapOrder, cache.additionFeatureMap))
				curAction = cache.additionTable.GetBestAction(curState)
				logger.Debug("Normal MISS branch", zap.String("curState", curState), zap.Int("curAction", int(curAction)))
				// ----------------------------------
				// QLearn - Take the action NOT STORE
				// ----------------------------------
				if curAction == qlearn.ActionNotStore {
					logger.Debug("Normal MISS branch NOT TO STORE ACTION")
					return added
				}
				logger.Debug("Normal MISS branch STORE ACTION")
				// ------------------------------
				// QLearn - Take the action STORE
				// ------------------------------
				// Insert with LRU mechanism
				if cache.Size()+requestedFileSize > cache.MaxSize {
					cache.Free(requestedFileSize, false)
				}
				if cache.Size()+requestedFileSize <= cache.MaxSize {
					cache.files.Insert(FileSupportData{
						Filename:  request.Filename,
						Size:      request.Size,
						Frequency: fileStats.Frequency,
						Recency:   fileStats.Recency,
					})

					cache.size += requestedFileSize
					fileStats.addInCache(&request.DayTime)
					added = true
					// fileStats.updateFilePoints(&cache.curTime)
					// cache.points += fileStats.Points
				}
			} else {
				// #######################
				// ##### HIT branch  #####
				// #######################
				logger.Debug("Normal hit branch")
				cache.files.Update(FileSupportData{
					Filename:  request.Filename,
					Size:      request.Size,
					Frequency: fileStats.Frequency,
					Recency:   fileStats.Recency,
				})
			}
		} else {
			// ###################################
			// ##### Addition Learning phase #####
			// ###################################

			if !hit {
				// ########################
				// ##### MISS branch  #####
				// ########################

				// -----------------------------------------------------------------
				// QLearn - miss reward on best action
				curState = cache.qAdditionPrevState[request.Filename]
				curAction = cache.qAdditionPrevAction[request.Filename]

				logger.Debug("Learning MISS branch", zap.String("curState", curState), zap.Int("curAction", int(curAction)))

				if curState != "" { // Some action are not taken randomly
					reward := float64(request.Size)
					// Update table
					cache.additionTable.Update(curState, curAction, reward)
					// Update epsilon
					cache.additionTable.UpdateEpsilon()
				}
				// -----------------------------------------------------------------

				curState = qlearn.State2String(cache.getState(request, fileStats, cache.additionFeatureMapOrder, cache.additionFeatureMap))

				// ----- Random choice -----
				if randomAction := cache.additionTable.GetRandomFloat(); randomAction > 0.5 {
					curAction = qlearn.ActionStore
				} else {
					curAction = qlearn.ActionNotStore
				}

				logger.Debug("Learning MISS branch", zap.String("curState", curState), zap.Int("curAction", int(curAction)))

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

					cache.qAdditionPrevState[request.Filename] = curState
					cache.qAdditionPrevAction[request.Filename] = curAction

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
						Frequency: fileStats.Frequency,
						Recency:   fileStats.Recency,
					})

					cache.size += requestedFileSize
					fileStats.addInCache(&request.DayTime)
					added = true

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

					cache.qAdditionPrevState[request.Filename] = curState
					cache.qAdditionPrevAction[request.Filename] = curAction
				}

			} else {
				// #######################
				// ##### HIT branch  #####
				// #######################
				cache.files.Update(FileSupportData{
					Filename:  request.Filename,
					Size:      request.Size,
					Frequency: fileStats.Frequency,
					Recency:   fileStats.Recency,
				})

				// -----------------------------------------------------------------
				// QLearn - hit reward on best action
				curState = cache.qAdditionPrevState[request.Filename]
				curAction = cache.qAdditionPrevAction[request.Filename]

				logger.Debug("Learning HIT branch", zap.String("curState", curState), zap.Int("curAction", int(curAction)))

				if curState != "" { // Some action are not taken randomly
					reward := float64(request.Size)
					// Update table
					cache.additionTable.Update(curState, curAction, reward)
					// Update epsilon
					cache.additionTable.UpdateEpsilon()
				}
				// -----------------------------------------------------------------
			}
		}
	} else {
		if !hit {
			// ########################
			// ##### MISS branch  #####
			// ########################

			logger.Debug("NO ADDITION TABLE - Normal miss branch")

			// Insert with LRU mechanism
			if cache.Size()+requestedFileSize > cache.MaxSize {
				cache.Free(requestedFileSize, false)
			}
			if cache.Size()+requestedFileSize <= cache.MaxSize {
				cache.files.Insert(FileSupportData{
					Filename:  request.Filename,
					Size:      request.Size,
					Frequency: fileStats.Frequency,
					Recency:   fileStats.Recency,
				})

				cache.size += requestedFileSize
				fileStats.addInCache(&request.DayTime)
				added = true
				// fileStats.updateFilePoints(&cache.curTime)
				// cache.points += fileStats.Points
			}
		} else {
			// #######################
			// ##### HIT branch  #####
			// #######################
			logger.Debug("NO ADDITION TABLE - Normal hit branch")
			cache.files.Update(FileSupportData{
				Filename:  request.Filename,
				Size:      request.Size,
				Frequency: fileStats.Frequency,
				Recency:   fileStats.Recency,
			})
		}
	}

	return added
}

// Free removes files from the cache
func (cache *AIRL) Free(amount float64, percentage bool) float64 {
	logger.Debug(
		"Cache free",
		zap.Float64("mean size", cache.MeanSize()),
		zap.Float64("mean frequency", cache.MeanFrequency()),
		zap.Float64("mean recency", cache.MeanRecency()),
	)
	var (
		totalDeleted float64
		sizeToDelete float64
		curAction    qlearn.ActionType
		curState     string
	)
	if percentage {
		sizeToDelete = amount * (cache.MaxSize / 100.)
	} else {
		sizeToDelete = amount
	}

	if sizeToDelete > 0. {
		if cache.evictionTableOK {
			// Check learning phase or not
			expEvictionTradeoff := cache.evictionTable.GetRandomFloat()
			curState = qlearn.State2String(cache.getState(nil, nil, cache.evictionFeatureMapOrder, cache.evictionFeatureMap))

			if expEvictionTradeoff > cache.evictionTable.Epsilon {
				// ########################
				// ##### Normal phase #####
				// ########################
				curAction = cache.evictionTable.GetBestAction(curState)
			} else {
				// ##########################
				// ##### Learning phase #####
				// ##########################

				// ----- Random choice -----
				randomActionIdx := int(cache.evictionTable.GetRandomFloat() * float64(len(cache.evictionFeatureMap)))
				curAction = cache.evictionTable.Actions[randomActionIdx]
			}
		} else {
			curAction = qlearn.ActionRemoveWithLRU
		}

		cache.qEvictionPrevState = curState
		cache.qEvictionPrevAction = curAction
		var curPolicy queueType

		switch curAction {
		case qlearn.ActionRemoveWithLRU:
			curPolicy = LRUQueue
		case qlearn.ActionRemoveWithLFU:
			curPolicy = LFUQueue
		case qlearn.ActionRemoveWithSizeBig:
			curPolicy = SizeBigQueue
		case qlearn.ActionRemoveWithSizeSmall:
			curPolicy = SizeSmallQueue
		}

		deletedFiles := make([]int64, 0)
		for curFile := range cache.files.Get(curPolicy) {
			logger.Debug("delete", zap.Int64("filename", curFile.Filename))

			curFileStats := cache.stats.Get(curFile.Filename)
			curFileStats.removeFromCache()

			// Update sizes
			cache.size -= curFile.Size
			cache.dataDeleted += curFile.Size
			totalDeleted += curFile.Size

			deletedFiles = append(deletedFiles, curFile.Filename)

			if totalDeleted >= sizeToDelete {
				break
			}
		}
		cache.files.Remove(deletedFiles)

	}

	return totalDeleted
}

// CheckWatermark checks the watermark levels and resolve the situation
func (cache *AIRL) CheckWatermark() bool {
	ok := true
	if cache.Capacity() >= cache.HighWaterMark {
		ok = false
		cache.Free(
			cache.Capacity()-cache.LowWaterMark,
			true,
		)
	}
	return ok
}

// ExtraStats for output
func (cache *AIRL) ExtraStats() string {
	return fmt.Sprintf(
		"SCov:%0.2f%%|ACov:%0.2f%%||Eps:%0.5f|SCov:%0.2f%%|ACov:%0.2f%%|Eps:%0.5f",
		cache.additionTable.GetStateCoverage(), cache.additionTable.GetActionCoverage(), cache.additionTable.Epsilon,
		cache.evictionTable.GetStateCoverage(), cache.evictionTable.GetActionCoverage(), cache.evictionTable.Epsilon,
	)
}

// ExtraOutput for output specific information
func (cache *AIRL) ExtraOutput(info string) string {
	result := ""
	switch info {
	case "additionQtable":
		result = cache.additionTable.ToString(&cache.additionFeatureMap, &cache.additionFeatureMapOrder)
	case "evictionQtable":
		result = cache.evictionTable.ToString(&cache.evictionFeatureMap, &cache.evictionFeatureMapOrder)
	default:
		result = "NONE"
	}
	return result
}
