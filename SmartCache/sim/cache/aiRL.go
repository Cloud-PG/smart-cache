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

// PrevChoice represents a choice in the past
type PrevChoice struct {
	Filename    int64
	Size        float64
	State       string
	Action      qlearn.ActionType
	GoodStrikes int
	BadStrikes  int
}

const (
	memorySize = 1000
	trace      = 10
)

// Memory represents the agent memory
type Memory struct {
	pastChoices []PrevChoice
	insertIdx   int
	traceIdx    int
}

// Init initializes the agent memory
func (mem *Memory) Init() {
	mem.pastChoices = make([]PrevChoice, memorySize)
	for idx := 0; idx < len(mem.pastChoices); idx++ {
		mem.pastChoices[idx].Filename = -1
	}
}

// Insert a new memory
func (mem *Memory) Insert(filename int64, size float64, state string, action qlearn.ActionType) {
	curCell := mem.pastChoices[mem.insertIdx]
	curCell.Filename = filename
	curCell.Size = size
	curCell.State = state
	curCell.Action = action
	mem.insertIdx = (mem.insertIdx + 1) % len(mem.pastChoices)
}

// Remember the past
func (mem *Memory) Remember() chan PrevChoice {
	iter := make(chan PrevChoice, trace)
	go func() {
		defer close(iter)
		diff := 0
		if mem.insertIdx > mem.traceIdx {
			diff = mem.insertIdx - mem.traceIdx
		} else {
			diff = len(mem.pastChoices) - mem.traceIdx + mem.insertIdx
		}
		if diff >= trace {
			for idx := 0; idx < trace; idx++ {
				// fmt.Println(idx, diff)
				curIdx := (mem.traceIdx + idx) % len(mem.pastChoices)
				if curIdx >= mem.insertIdx {
					break
				}
				if mem.pastChoices[curIdx].Filename != -1 {
					iter <- mem.pastChoices[curIdx]
					mem.pastChoices[curIdx].Filename = -1
				}
			}
			mem.traceIdx = (mem.traceIdx + diff) % len(mem.pastChoices)
		}
	}()
	return iter
}

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
	qAdditionPrevStates     Memory
	qEvictionPrevState      PrevChoice
	extendedEvictionTable   bool
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

	cache.qAdditionPrevStates.Init()

	additionFeatureMap := args[0].(string)
	evictionFeatureMap := args[1].(string)
	trainingEnabled := args[2].(bool)
	initEpsilon := args[3].(float64)
	decayRateEpsilon := args[4].(float64)
	cache.extendedEvictionTable = args[5].(bool)

	if cache.extendedEvictionTable {
		cache.weightFunction = args[6].(FunctionType)
		cache.weightAlpha = args[7].(float64)
		cache.weightBeta = args[8].(float64)
		cache.weightGamma = args[9].(float64)
	}

	logger.Info("Feature maps", zap.String("addition map", additionFeatureMap), zap.String("eviction map", evictionFeatureMap))

	if additionFeatureMap != "" {
		logger.Info("Create addition map")
		cache.additionFeatureMap = featuremap.Parse(additionFeatureMap)
		for key := range cache.additionFeatureMap {
			cache.additionFeatureMapOrder = append(cache.additionFeatureMapOrder, key)
		}
		sort.Strings(cache.additionFeatureMapOrder)
		cache.additionTable = makeQtable(cache.additionFeatureMap, cache.additionFeatureMapOrder, qlearn.AdditionTable, trainingEnabled, initEpsilon, decayRateEpsilon)
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
		cache.evictionTable = makeQtable(cache.evictionFeatureMap, cache.evictionFeatureMapOrder, evictionTable, trainingEnabled, initEpsilon, decayRateEpsilon)
		cache.evictionTableOK = true
	} else {
		cache.evictionTableOK = false
	}

	logger.Info("Table creation done")

	return nil
}

func makeQtable(featureMap map[string]featuremap.Obj, featureOrder []string, role qlearn.QTableRole, trainingEnabled bool, initEpsilon float64, decayRateEpsilon float64) qlearn.QTable {
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
	curTable.Init(inputLengths, role, trainingEnabled, initEpsilon, decayRateEpsilon)
	logger.Info("[Done]")
	return curTable
}

// Dumps the AIRL cache
func (cache *AIRL) Dumps(fileAndStats bool) [][]byte {
	logger.Info("Dump cache into byte string")
	outData := make([][]byte, 0)
	var newLine = []byte("\n")

	if fileAndStats {
		// ----- Files -----
		logger.Info("Dump cache files")
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
		logger.Info("Dump cache stats")
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
	}
	if cache.additionTableOK {
		// ----- addition qtable -----
		logger.Info("Dump cache addition table")
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
		logger.Info("Dump cache eviction table")
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
func (cache *AIRL) Dump(filename string, fileAndStats bool) {
	logger.Info("Dump cache", zap.String("filename", filename))
	outFile, osErr := os.Create(filename)
	if osErr != nil {
		panic(fmt.Sprintf("Error dump file creation: %s", osErr))
	}
	gwriter := gzip.NewWriter(outFile)

	for _, record := range cache.Dumps(fileAndStats) {
		gwriter.Write(record)
	}

	gwriter.Close()
}

// Loads the AIRL cache
func (cache *AIRL) Loads(inputString [][]byte, vars ...interface{}) {
	logger.Info("Loads cache dump string")
	trainingEnabled := vars[0].(bool)
	initEpsilon := vars[1].(float64)
	decayRateEpsilon := vars[2].(float64)
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
			cache.additionTable.ResetParams(trainingEnabled, initEpsilon, decayRateEpsilon)
			cache.additionTableOK = true
		case "EVCQTABLE":
			unmarshalErr = json.Unmarshal([]byte(curRecord.Data), &cache.evictionTable)
			cache.evictionTable.ResetParams(trainingEnabled, initEpsilon, decayRateEpsilon)
			cache.evictionTableOK = true
		}
		if unmarshalErr != nil {
			panic(fmt.Sprintf("%+v", unmarshalErr))
		}
	}

}

func (cache *AIRL) getState(fileStats *FileStats, featureOrder []string, featureMap map[string]featuremap.Obj) string {

	cacheCapacity := float64(cache.Capacity())
	deltaHighWatermark := float64(cache.HighWaterMark) - cacheCapacity

	cache.bufferInputVector = cache.bufferInputVector[:0]

	for _, featureName := range featureOrder {
		curObj, _ := featureMap[featureName]
		switch featureName {
		case "size":
			cache.bufferInputVector = append(cache.bufferInputVector, curObj.GetValue(fileStats.Size))
		case "numReq":
			cache.bufferInputVector = append(cache.bufferInputVector, curObj.GetValue(float64(fileStats.Frequency)))
		case "cacheUsage":
			cache.bufferInputVector = append(cache.bufferInputVector, curObj.GetValue(cacheCapacity))
		case "dataType":
			cache.bufferInputVector = append(cache.bufferInputVector, curObj.GetValue(fileStats.DataType))
		case "fileType":
			cache.bufferInputVector = append(cache.bufferInputVector, curObj.GetValue(fileStats.FileType))
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
		case "stdDevSize":
			cache.bufferInputVector = append(cache.bufferInputVector, curObj.GetValue(cache.StdDevSize()))
		case "stdDevFreq":
			cache.bufferInputVector = append(cache.bufferInputVector, curObj.GetValue(cache.StdDevFreq()))
		case "stdDevRec":
			cache.bufferInputVector = append(cache.bufferInputVector, curObj.GetValue(cache.StdDevRec()))
		case "numFiles":
			cache.bufferInputVector = append(cache.bufferInputVector, curObj.GetValue(float64(cache.NumFiles())))
		default:
			panic(fmt.Sprintf("ERROR: Cannot prepare state input '%s'", featureName))
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
		newState          string
		requestedFileSize = request.Size
	)

	// fmt.Println(
	// 	fileStats.InCache,
	// 	"\tFreq: ",
	// 	fileStats.Frequency,
	// 	"\tFreq in cache:",
	// 	fileStats.FrequencyInCache,
	// 	"\tRec:",
	// 	fileStats.Recency,
	// 	"\tDelta Rec:",
	// 	fileStats.DeltaLastRequest,
	// 	"\tweight:",
	// 	fileStats.Weight,
	// 	"\tname:",
	// 	request.Filename,
	// 	"\tsize:",
	// 	request.Size,
	// )

	// fmt.Println(
	// 	"Written data",
	// 	(cache.dataWritten/cache.dataRead)*100.,
	// 	"read on hit data",
	// 	(cache.dataReadOnHit/cache.dataRead)*100.,
	// 	"read on miss data",
	// 	(cache.dataReadOnMiss/cache.dataRead)*100.,
	// 	"read on miss data band",
	// 	(cache.dataReadOnMiss/cache.bandwidth)*100.,
	// )

	if cache.evictionTableOK && cache.evictionTable.TrainingEnabled {

		logger.Debug("EVICTION TABLE OK")

		// ###################################
		// ##### Eviction Learning phase #####
		// ###################################
		if cache.qEvictionPrevState.Action != 0 && len(cache.qEvictionPrevState.State) != 0 {
			reward := 0.
			// reward := request.Size

			// OLD POLICY
			// if !hit && (cache.dailyWrittenData >= cache.dailyReadOnHit) {
			// 	reward = -reward
			// }
			newState = cache.getState(nil, cache.evictionFeatureMapOrder, cache.evictionFeatureMap)

			// if cache.dataReadOnHit <= (cache.dataReadOnMiss*0.3) || cache.dataWritten >= (cache.dataReadOnHit*0.3) {
			if cache.dataWritten/cache.dataRead >= 0.3 {
				// cache.qEvictionPrevState.GoodStrikes = 0
				// cache.qEvictionPrevState.BadStrikes++
				// reward -= float64(cache.qEvictionPrevState.BadStrikes)
				if hit {
					reward -= request.Size / 1024.
				} else {
					reward -= 1.0
				}
			}
			if cache.dataReadOnHit/cache.dataRead < 0.3 {
				// cache.qEvictionPrevState.GoodStrikes = 0
				// cache.qEvictionPrevState.BadStrikes++
				// reward -= float64(cache.qEvictionPrevState.BadStrikes)
				if hit {
					reward -= request.Size / 1024.
				} else {
					reward -= 1.0
				}
			}

			if reward == 0. {
				// cache.qEvictionPrevState.BadStrikes = 0
				// cache.qEvictionPrevState.GoodStrikes++
				// reward += float64(cache.qEvictionPrevState.GoodStrikes)
				if hit {
					reward += request.Size / 1024.
				} else {
					reward += 1.0
				}
			}

			// Update table
			cache.evictionTable.Update(cache.qEvictionPrevState.State, cache.qEvictionPrevState.Action, reward, newState)
			// Update epsilon
			cache.evictionTable.UpdateEpsilon()
		}
	}

	if cache.additionTableOK {

		logger.Debug("ADDITION TABLE OK")

		curState = cache.getState(fileStats, cache.additionFeatureMapOrder, cache.additionFeatureMap)

		// newState = ""  // FOR LOD AND ECML

		// Check training
		if cache.additionTable.TrainingEnabled {

			for memory := range cache.qAdditionPrevStates.Remember() {
				curHit := cache.Check(memory.Filename)
				curFileStats := cache.stats.Get(memory.Filename)
				newState = cache.getState(curFileStats, cache.additionFeatureMapOrder, cache.additionFeatureMap)
				reward := 0.
				// reward := memory.Size
				if !curHit {
					if cache.dataReadOnMiss/cache.dataRead > 0.5 {
						reward -= memory.Size / 1024.
					}
					if reward == 0. {
						reward += memory.Size / 1024.
					}
				} else {
					if cache.dataReadOnHit/cache.dataRead < 0.3 {
						reward -= memory.Size / 1024.
					}
					if cache.dataWritten/cache.dataRead > 0.3 {
						reward -= memory.Size / 1024.
					}
					if reward == 0. {
						reward += memory.Size / 1024.
					}
				}
				// Update table
				cache.additionTable.Update(memory.State, memory.Action, reward, newState)
			}

			if !hit {
				// Check learning phase or not
				if expEvictionTradeoff := cache.additionTable.GetRandomFloat(); expEvictionTradeoff > cache.additionTable.Epsilon {
					// ########################
					// ##### Normal phase #####
					// ########################
					curAction = cache.additionTable.GetBestAction(curState)
				} else {
					// ##########################
					// ##### Learning phase #####
					// ##########################

					// ----- Random choice -----
					randomActionIdx := int(cache.additionTable.GetRandomFloat() * float64(len(cache.additionTable.Actions)))
					curAction = cache.additionTable.Actions[randomActionIdx]
				}

				logger.Debug("Learning MISS branch", zap.String("curState", curState), zap.Int("curAction", int(curAction)))

				// -------------------------------------------------------------
				//             QLearn - Take the action NOT STORE
				// -------------------------------------------------------------
				if curAction == qlearn.ActionNotStore {
					reward := 0.
					// reward := request.Size

					if cache.dataReadOnMiss/cache.bandwidth < 0.5 || cache.dataWritten/cache.dataRead < 0.1 {
						reward -= request.Size / 1024.
					}
					if reward == 0. {
						reward += request.Size / 1024.
					}

					// Update table
					cache.additionTable.Update(curState, curAction, reward, curState)

					cache.qAdditionPrevStates.Insert(request.Filename, request.Size, curState, curAction)

					// Update epsilon
					cache.additionTable.UpdateEpsilon()

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
				}

				// -------------------------------------------------------------
				//               QLearn - Take the action STORE
				// -------------------------------------------------------------
				if curAction == qlearn.ActionStore {
					reward := 0.
					// reward := request.Size

					if cache.dataReadOnMiss/cache.bandwidth > 0.75 || cache.dataWritten/cache.dataRead > 0.5 {
						reward -= request.Size / 1024.
					}
					if reward == 0. {
						reward += request.Size / 1024.
					}
					// Update table
					cache.additionTable.Update(curState, curAction, reward, curState)

					cache.qAdditionPrevStates.Insert(request.Filename, request.Size, curState, curAction)
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
			}

			// Update epsilon
			cache.additionTable.UpdateEpsilon()

		} else {
			// #########################
			// #####  NO TRAINING  #####
			// #########################
			if !hit {
				// ########################
				// ##### MISS branch  #####
				// ########################

				curState = cache.getState(fileStats, cache.additionFeatureMapOrder, cache.additionFeatureMap)
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
					cache.size += requestedFileSize
					fileStats.addInCache(&request.DayTime)

					cache.files.Insert(FileSupportData{
						Filename:  request.Filename,
						Size:      request.Size,
						Frequency: fileStats.FrequencyInCache,
						Recency:   fileStats.Recency,
						Weight:    fileStats.Weight,
					})

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

// Free removes files from the cache
func (cache *AIRL) Free(amount float64, percentage bool) float64 {
	logger.Debug(
		"Cache free",
		zap.Float64("mean size", cache.MeanSize()),
		zap.Float64("mean frequency", cache.MeanFrequency()),
		zap.Float64("mean recency", cache.MeanRecency()),
		zap.Int("num. files", cache.NumFiles()),
		zap.Float64("std.dev. freq.", cache.StdDevFreq()),
		zap.Float64("std.dev. rec.", cache.StdDevRec()),
		zap.Float64("std.dev. size", cache.StdDevSize()),
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
			curState = cache.getState(nil, cache.evictionFeatureMapOrder, cache.evictionFeatureMap)
			// Check training
			if cache.evictionTable.TrainingEnabled {
				// Check learning phase or not
				if expEvictionTradeoff := cache.evictionTable.GetRandomFloat(); expEvictionTradeoff > cache.evictionTable.Epsilon {
					// ########################
					// ##### Normal phase #####
					// ########################
					curAction = cache.evictionTable.GetBestAction(curState)
				} else {
					// ##########################
					// ##### Learning phase #####
					// ##########################

					// ----- Random choice -----
					randomActionIdx := int(cache.evictionTable.GetRandomFloat() * float64(len(cache.evictionTable.Actions)))
					curAction = cache.evictionTable.Actions[randomActionIdx]
				}

				cache.qEvictionPrevState = PrevChoice{
					State:  curState,
					Action: curAction,
				}

			} else {
				// #########################
				// #####  NO TRAINING  #####
				// #########################
				curAction = cache.evictionTable.GetBestAction(curState)
			}

		} else {
			curAction = qlearn.ActionRemoveWithLRU
		}

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
			logger.Debug("delete",
				zap.Int64("filename", curFile.Filename),
				zap.Float64("fileSize", curFile.Size),
				zap.Int64("frequency", curFile.Frequency),
				zap.Int64("recency", curFile.Recency),
				zap.Float64("cacheSize", cache.Size()),
			)

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
		"SCov:%0.2f%%|ACov:%0.2f%%|Eps:%0.5f||SCov:%0.2f%%|ACov:%0.2f%%|Eps:%0.5f",
		cache.additionTable.GetStateCoverage(), cache.additionTable.GetActionCoverage(), cache.additionTable.Epsilon,
		cache.evictionTable.GetStateCoverage(), cache.evictionTable.GetActionCoverage(), cache.evictionTable.Epsilon,
		// "%0.2f | %0.2f | %0.2f",
		// cache.StdDevSize(), cache.StdDevRec(), cache.StdDevFreq(),
	)
}

// ExtraOutput for output specific information
func (cache *AIRL) ExtraOutput(info string) string {
	result := ""
	switch info {
	case "additionQtable":
		result = cache.additionTable.ToString(cache.additionFeatureMap, cache.additionFeatureMapOrder)
	case "evictionQtable":
		result = cache.evictionTable.ToString(cache.evictionFeatureMap, cache.evictionFeatureMapOrder)
	default:
		result = "NONE"
	}
	return result
}
