package cache

import (
	"compress/gzip"
	"encoding/json"
	"fmt"
	"os"
	"simulator/v2/cache/ai/featuremap"
	qlearn "simulator/v2/cache/qLearn"
	"sort"
	"strings"

	"go.uber.org/zap"
)

const (
	bandwidthLimit = (1000000. / 8.) * 60. * 60. * 24.
)

// AIRL cache
type AIRL struct {
	SimpleCache
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
	extendedEvictionTable   bool
	dailyReadOnHit          float64
	dailyReadOnMiss         float64
	points                  float64
	prevPoints              float64
	bufferCategory          []bool
	bufferInputVector       []string
	chanCategory            chan bool
	weightFunction          FunctionType
	weightAlpha             float64
	weightBeta              float64
	weightGamma             float64
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
	decayRateEpsilon := args[3].(float64)
	cache.extendedEvictionTable = args[4].(bool)

	if cache.extendedEvictionTable {
		cache.weightFunction = args[5].(FunctionType)
		cache.weightAlpha = args[6].(float64)
		cache.weightBeta = args[7].(float64)
		cache.weightGamma = args[8].(float64)
	}

	logger.Info("Feature maps", zap.String("addition map", additionFeatureMap), zap.String("eviction map", evictionFeatureMap))

	if additionFeatureMap != "" {
		logger.Info("Create addition map")
		cache.additionFeatureMap = featuremap.Parse(additionFeatureMap)
		for key := range cache.additionFeatureMap {
			cache.additionFeatureMapOrder = append(cache.additionFeatureMapOrder, key)
		}
		sort.Strings(cache.additionFeatureMapOrder)
		cache.additionTable = makeQtable(cache.additionFeatureMap, cache.additionFeatureMapOrder, qlearn.AdditionTable, initEpsilon, decayRateEpsilon)
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
		var evictionTable qlearn.QTableRole
		if cache.extendedEvictionTable {
			logger.Info("Create extended eviction map")
			evictionTable = qlearn.EvictionTableExtended
		} else {
			logger.Info("Create eviction map")
			evictionTable = qlearn.EvictionTable
		}
		cache.evictionTable = makeQtable(cache.evictionFeatureMap, cache.evictionFeatureMapOrder, evictionTable, initEpsilon, decayRateEpsilon)
		cache.evictionTableOK = true
	} else {
		cache.evictionTableOK = false
	}

	logger.Info("Table creation done")

	return nil
}

func makeQtable(featureMap map[string]featuremap.Obj, featureOrder []string, role qlearn.QTableRole, initEpsilon float64, decayRateEpsilon float64) qlearn.QTable {
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
	curTable.Init(inputLengths, role, initEpsilon, decayRateEpsilon)
	logger.Info("[Done]")
	return curTable
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
func (cache *AIRL) Loads(inputString [][]byte, vars ...interface{}) {
	logger.Info("Loads cache dump string")
	initEpsilon := vars[0].(float64)
	decayRateEpsilon := vars[1].(float64)
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
			cache.additionTable.ResetParams(initEpsilon, decayRateEpsilon)
			cache.additionTableOK = true
		case "EVCQTABLE":
			unmarshalErr = json.Unmarshal([]byte(curRecord.Data), &cache.evictionTable)
			cache.evictionTable.ResetParams(initEpsilon, decayRateEpsilon)
			cache.evictionTableOK = true
		}
		if unmarshalErr != nil {
			panic(fmt.Sprintf("%+v", unmarshalErr))
		}
	}

}

func (cache *AIRL) getState(request *Request, fileStats *FileStats, featureOrder []string, featureMap map[string]featuremap.Obj) string {
	var (
		size     float64
		numReq   float64
		dataType int64
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
		curObj, _ := featureMap[featureName]
		switch featureName {
		case "size":
			cache.bufferInputVector = append(cache.bufferInputVector, curObj.GetValue(size))
		case "numReq":
			cache.bufferInputVector = append(cache.bufferInputVector, curObj.GetValue(numReq))
		case "cacheUsage":
			cache.bufferInputVector = append(cache.bufferInputVector, curObj.GetValue(cacheCapacity))
		case "dataType":
			cache.bufferInputVector = append(cache.bufferInputVector, curObj.GetValue(dataType))
		case "deltaLastRequest":
			cache.bufferInputVector = append(cache.bufferInputVector, curObj.GetValue(float64(fileStats.DeltaLastRequest)))
		case "deltaHighWatermark":
			cache.bufferInputVector = append(cache.bufferInputVector, curObj.GetValue(deltaHighWatermark))
		case "meanSize":
			cache.bufferInputVector = append(cache.bufferInputVector, curObj.GetValue(cache.MeanSize()))
		case "meanFrequency":
			cache.bufferInputVector = append(cache.bufferInputVector, curObj.GetValue(cache.MeanFrequency()))
		case "meanRecency":
			cache.bufferInputVector = append(cache.bufferInputVector, curObj.GetValue(cache.MeanRecency()))
		default:
			panic(fmt.Sprintf("Cannot prepare input %s", featureName))
		}
	}
	return strings.Join(cache.bufferInputVector, "")
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

	if !cache.curTime.Equal(cache.prevTime) {
		cache.dailyReadOnHit = 0.0
		cache.dailyReadOnMiss = 0.0

		cache.numDailyHit = 0
		cache.numDailyMiss = 0
		cache.hitCPUEff = 0.
		cache.missCPUEff = 0.
		cache.upperCPUEff = 0.
		cache.lowerCPUEff = 0.
		cache.numLocal = 0
		cache.numRemote = 0
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
	if cache.extendedEvictionTable {
		fileStats.updateWeight(cache.weightFunction, cache.weightAlpha, cache.weightBeta, cache.weightGamma)
	}

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

	// fmt.Println(
	// 	fileStats.InCache,
	// 	"\t",
	// 	fileStats.Frequency,
	// 	"\t",
	// 	fileStats.FrequencyInCache,
	// 	"\t",
	// 	fileStats.Recency,
	// 	"\t",
	// 	fileStats.Weight,
	// 	"\t",
	// 	request.Filename,
	// 	"\t",
	// 	request.Size,
	// )

	if cache.evictionTableOK {

		logger.Debug("EVICTION TABLE OK")

		// Check learning phase or not
		expEvictionTradeoff := cache.evictionTable.GetRandomFloat()
		if expEvictionTradeoff < cache.evictionTable.Epsilon {
			// ###################################
			// ##### Eviction Learning phase #####
			// ###################################
			if cache.qEvictionPrevAction != 0 && len(cache.qEvictionPrevState) != 0 {
				reward := float64(request.Size)
				if !hit {
					reward = -reward
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

			// #################################################################
			// #                  ADDITION NORMAL PHASE                        #
			// #################################################################

			if !hit {
				// ########################
				// ##### MISS branch  #####
				// ########################

				curState = cache.getState(request, fileStats, cache.additionFeatureMapOrder, cache.additionFeatureMap)
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
						Frequency: fileStats.FrequencyInCache,
						Recency:   fileStats.Recency,
						Weight:    fileStats.Weight,
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
					Frequency: fileStats.FrequencyInCache,
					Recency:   fileStats.Recency,
					Weight:    fileStats.Weight,
				})
			}
		} else {
			// #################################################################
			// #                  ADDITION LEARNING PHASE                      #
			// #################################################################

			if !hit {
				// ########################
				// ##### MISS branch  #####
				// ########################

				curState = cache.getState(request, fileStats, cache.additionFeatureMapOrder, cache.additionFeatureMap)

				// ----- Random choice -----
				if randomAction := cache.additionTable.GetRandomFloat(); randomAction > 0.5 {
					curAction = qlearn.ActionStore
				} else {
					curAction = qlearn.ActionNotStore
				}

				logger.Debug("Learning MISS branch", zap.String("curState", curState), zap.Int("curAction", int(curAction)))

				// -------------------------------------------------------------
				//             QLearn - Take the action NOT STORE
				// -------------------------------------------------------------
				if curAction == qlearn.ActionNotStore {
					// newScore := cache.points
					// diff := newScore - cache.prevPoints
					// reward := 0.
					// if diff >= 0 {
					// 	reward += 1.
					// } else {
					// 	reward -= 1.
					// }
					reward := float64(request.Size)
					if cache.dataReadOnHit < (cache.dataReadOnMiss*0.5) || cache.dailyReadOnHit < (cache.dailyReadOnMiss*0.5) || cache.dailyReadOnMiss < bandwidthLimit {
						reward = -reward
					}
					// Update table
					cache.additionTable.Update(curState, curAction, reward)
					// Update epsilon
					cache.additionTable.UpdateEpsilon()

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
						Frequency: fileStats.FrequencyInCache,
						Recency:   fileStats.Recency,
						Weight:    fileStats.Weight,
					})

					cache.size += requestedFileSize
					fileStats.addInCache(&request.DayTime)
					added = true

					// fileStats.updateFilePoints(&cache.curTime)
					// cache.points += fileStats.Points
				}

				// -------------------------------------------------------------
				//               QLearn - Take the action STORE
				// -------------------------------------------------------------
				if curAction == qlearn.ActionStore {
					// newScore := cache.points
					// diff := newScore - cache.prevPoints
					// reward := 0.
					// if diff >= 0 {
					// 	reward += 1.
					// } else {
					// 	reward -= 1.
					// }
					reward := float64(request.Size)
					if cache.dailyReadOnMiss >= bandwidthLimit {
						reward = -reward
					}
					// Update table
					cache.additionTable.Update(curState, curAction, reward)
					// Update epsilon
					cache.additionTable.UpdateEpsilon()

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
					Frequency: fileStats.FrequencyInCache,
					Recency:   fileStats.Recency,
					Weight:    fileStats.Weight,
				})

				// -------------------------------------------------------------
				// QLearn - hit reward on best action
				curState = cache.qAdditionPrevState[request.Filename]
				curAction = cache.qAdditionPrevAction[request.Filename]

				logger.Debug("Learning HIT branch", zap.String("curState", curState), zap.Int("curAction", int(curAction)))

				if curState != "" { // Some action are not taken randomly
					reward := float64(request.Size)
					if cache.dataReadOnHit < (cache.dataReadOnMiss*0.5) || cache.dailyReadOnHit < (cache.dailyReadOnMiss*0.5) {
						reward = -reward
					}
					// Update table
					cache.additionTable.Update(curState, curAction, reward)
					// Update epsilon
					cache.additionTable.UpdateEpsilon()
				}
				// -------------------------------------------------------------
			}
		}
	} else {
		// #####################################################################
		// #                      NO ADDITION TABLE                            #
		// #####################################################################

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
					Frequency: fileStats.FrequencyInCache,
					Recency:   fileStats.Recency,
					Weight:    fileStats.Weight,
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
				Frequency: fileStats.FrequencyInCache,
				Recency:   fileStats.Recency,
				Weight:    fileStats.Weight,
			})
		}
	}

	return added
}

// AfterRequest of cache
func (cache *AIRL) AfterRequest(request *Request, hit bool, added bool) {
	cache.SimpleCache.AfterRequest(request, hit, added)
	if hit {
		cache.dailyReadOnHit += request.Size
	} else {
		cache.dailyReadOnMiss += request.Size
	}
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
			curState = cache.getState(nil, nil, cache.evictionFeatureMapOrder, cache.evictionFeatureMap)

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
		case qlearn.ActionRemoveWithWeight:
			curPolicy = WeightQueue
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
