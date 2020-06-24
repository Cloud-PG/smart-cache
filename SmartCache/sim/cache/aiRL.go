package cache

import (
	"compress/gzip"
	"encoding/json"
	"fmt"
	"os"
	"simulator/v2/cache/ai/featuremap"
	"simulator/v2/cache/ai/queue"
	qlearn "simulator/v2/cache/qLearn"

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

// AIRL cache
type AIRL struct {
	SimpleCache
	additionAgentOK        bool
	evictionAgentOK        bool
	additionFeatureManager featuremap.FeatureManager
	evictionFeatureManager featuremap.FeatureManager
	additionAgent          qlearn.Agent
	evictionAgent          qlearn.Agent
	evictionAgentStep      int64
	bufferCategory         []bool
	bufferIdxVector        []int
	chanCategory           chan bool
}

// Init the AIRL struct
func (cache *AIRL) Init(args ...interface{}) interface{} {
	logger = zap.L()

	cache.SimpleCache.Init(queue.NoQueue)

	additionFeatureMap := args[0].(string)
	evictionFeatureMap := args[1].(string)
	initEpsilon := args[2].(float64)
	decayRateEpsilon := args[3].(float64)

	cache.evictionAgentStep = 100

	logger.Info("Feature maps", zap.String("addition map", additionFeatureMap), zap.String("eviction map", evictionFeatureMap))

	if additionFeatureMap != "" {
		logger.Info("Create addition feature manager")
		cache.additionFeatureManager = featuremap.Parse(additionFeatureMap)
		logger.Info("Create addition agent")
		cache.additionAgent.Init(
			&cache.additionFeatureManager,
			qlearn.AdditionAgent,
			initEpsilon,
			decayRateEpsilon,
		)
		cache.additionAgentOK = true
	} else {
		cache.additionAgentOK = false
	}

	if evictionFeatureMap != "" {
		logger.Info("Create eviction feature manager")
		cache.evictionFeatureManager = featuremap.Parse(evictionFeatureMap)
		logger.Info("Create eviction agent")
		cache.evictionAgent.Init(
			&cache.evictionFeatureManager,
			qlearn.EvictionAgent,
			initEpsilon,
			decayRateEpsilon,
		)
		cache.evictionAgentOK = true
	} else {
		cache.evictionAgentOK = false
	}

	logger.Info("Table creation done")

	return nil
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
	if cache.additionAgentOK {
		// ----- addition agent -----
		logger.Info("Dump cache addition agent")
		dumpInfo, _ := json.Marshal(DumpInfo{Type: "ADDAGENT"})
		dumpStats, _ := json.Marshal(cache.additionAgent)
		record, _ := json.Marshal(DumpRecord{
			Info: string(dumpInfo),
			Data: string(dumpStats),
		})
		record = append(record, newLine...)
		outData = append(outData, record)
		// ----- addition feature map manager -----
		logger.Info("Dump cache addition feature map manager")
		dumpInfoFMM, _ := json.Marshal(DumpInfo{Type: "ADDFEATUREMAPMANAGER"})
		dumpStatsFMM, _ := json.Marshal(cache.additionFeatureManager)
		recordFMM, _ := json.Marshal(DumpRecord{
			Info: string(dumpInfoFMM),
			Data: string(dumpStatsFMM),
		})
		record = append(recordFMM, newLine...)
		outData = append(outData, record)
	}
	if cache.evictionAgentOK {
		// ----- eviction agent -----
		logger.Info("Dump cache eviction agent")
		dumpInfo, _ := json.Marshal(DumpInfo{Type: "EVCAGENT"})
		dumpStats, _ := json.Marshal(cache.evictionAgent)
		record, _ := json.Marshal(DumpRecord{
			Info: string(dumpInfo),
			Data: string(dumpStats),
		})
		record = append(record, newLine...)
		outData = append(outData, record)
		// ----- eviction feature map manager -----
		logger.Info("Dump cache eviction feature map manager")
		dumpInfoFMM, _ := json.Marshal(DumpInfo{Type: "EVCFEATUREMAPMANAGER"})
		dumpStatsFMM, _ := json.Marshal(cache.evictionFeatureManager)
		recordFMM, _ := json.Marshal(DumpRecord{
			Info: string(dumpInfoFMM),
			Data: string(dumpStatsFMM),
		})
		record = append(recordFMM, newLine...)
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
		case "ADDAGENT":
			unmarshalErr = json.Unmarshal([]byte(curRecord.Data), &cache.additionAgent)
			cache.additionAgent.ResetParams(initEpsilon, decayRateEpsilon)
			cache.additionAgentOK = true
		case "ADDFEATUREMAPMANAGER":
			unmarshalErr = json.Unmarshal([]byte(curRecord.Data), &cache.additionFeatureManager)
		case "EVCAGENT":
			unmarshalErr = json.Unmarshal([]byte(curRecord.Data), &cache.evictionAgent)
			cache.evictionAgent.ResetParams(initEpsilon, decayRateEpsilon)
			cache.evictionAgentOK = true
		case "EVCFEATUREMAPMANAGER":
			unmarshalErr = json.Unmarshal([]byte(curRecord.Data), &cache.evictionFeatureManager)
		}
		if unmarshalErr != nil {
			panic(fmt.Sprintf("%+v", unmarshalErr))
		}
	}

}

func (cache *AIRL) getState4AddAgent(curFileStats *FileStats) int {

	cache.bufferIdxVector = cache.bufferIdxVector[:0]

	for _, feature := range cache.additionFeatureManager.Features {
		switch feature.Name {
		case "size":
			cache.bufferIdxVector = append(cache.bufferIdxVector, feature.Index(curFileStats.Size))
		case "numReq":
			cache.bufferIdxVector = append(cache.bufferIdxVector, feature.Index(curFileStats.Frequency))
		case "deltaLastRequest":
			cache.bufferIdxVector = append(cache.bufferIdxVector, feature.Index(curFileStats.DeltaLastRequest))
		case "percOcc":
			cache.bufferIdxVector = append(cache.bufferIdxVector, feature.Index(cache.SimpleCache.Occupancy()))
		}
	}

	return cache.additionAgent.Table.FeatureIdxs2StateIdx(cache.bufferIdxVector...)
}

func (cache *AIRL) callEvictionAgent(forced bool) float64 {
	var totalDeleted float64
	return totalDeleted
}

// BeforeRequest of LRU cache
func (cache *AIRL) BeforeRequest(request *Request, hit bool) *FileStats {

	if cache.tick%cache.evictionAgentStep == 0 {
		if cache.evictionAgentOK {
			cache.callEvictionAgent(true)
		}
	}

	fileStats, _ := cache.stats.GetOrCreate(request.Filename, request.Size, request.DayTime, cache.tick)

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

	fileStats.updateStats(hit, request.Size, request.UserID, request.SiteName, request.DayTime)

	return fileStats
}

// UpdatePolicy of AIRL cache
func (cache *AIRL) UpdatePolicy(request *Request, fileStats *FileStats, hit bool) bool {
	var (
		added = false
		// curAction         qlearn.ActionType
		curState int
		// newState          string
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
	// 	(cache.dataWritten/cache.dataRead)*0.,
	// 	"read on hit data",
	// 	(cache.dataReadOnHit/cache.dataRead)*100.,
	// 	"read on miss data",
	// 	(cache.dataReadOnMiss/cache.dataRead)*100.,
	// 	"read on miss data band",
	// 	(cache.dataReadOnMiss/cache.bandwidth)*100.,
	// )

	if cache.additionAgentOK {

		logger.Debug("ADDITION TABLE OK")

		curState = cache.getState4AddAgent(fileStats)

		logger.Debug("cache", zap.Int("current state", curState))

		// // newState = ""  // FOR LOD AND ECML

		// // Check training

		// // if expEvictionTradeoff := cache.additionTable.GetRandomFloat(); expEvictionTradeoff <= cache.additionTable.Epsilon {
		// for memory := range cache.qAdditionPrevStates.Remember() {
		// 	curHit := cache.Check(memory.Filename)
		// 	curFileStats := cache.stats.Get(memory.Filename)
		// 	newState = cache.getState(curFileStats, cache.additionFeatureMapOrder, cache.additionFeatureMap)
		// 	reward := 0.
		// 	// reward := memory.Size
		// 	if !curHit {
		// 		if cache.dataReadOnMiss/cache.dataRead > 0.5 {
		// 			reward -= memory.Size / 1024.
		// 		}
		// 		if reward == 0. {
		// 			reward += memory.Size / 1024.
		// 		}
		// 	} else {
		// 		if cache.dataReadOnHit/cache.dataRead < 0.3 {
		// 			reward -= memory.Size / 1024.
		// 		}
		// 		if cache.dataWritten/cache.dataRead > 0.3 {
		// 			reward -= memory.Size / 1024.
		// 		}
		// 		if reward == 0. {
		// 			reward += memory.Size / 1024.
		// 		}
		// 	}
		// 	// Update table
		// 	cache.additionTable.Update(memory.State, memory.Action, reward, newState)
		// }
		// // }

		// if !hit {
		// 	// Check learning phase or not
		// 	if expEvictionTradeoff := cache.additionTable.GetRandomFloat(); expEvictionTradeoff > cache.additionTable.Epsilon {
		// 		// ########################
		// 		// ##### Normal phase #####
		// 		// ########################
		// 		curAction = cache.additionTable.GetBestAction(curState)
		// 	} else {
		// 		// ##########################
		// 		// ##### Learning phase #####
		// 		// ##########################

		// 		// ----- Random choice -----
		// 		randomActionIdx := int(cache.additionTable.GetRandomFloat() * float64(len(cache.additionTable.Actions)))
		// 		curAction = cache.additionTable.Actions[randomActionIdx]
		// 	}

		// 	logger.Debug("Learning MISS branch", zap.String("curState", curState), zap.Int("curAction", int(curAction)))

		// 	// -------------------------------------------------------------
		// 	//             QLearn - Take the action NOT STORE
		// 	// -------------------------------------------------------------
		// 	if curAction == qlearn.ActionNotStore {
		// 		reward := 0.
		// 		// reward := request.Size

		// 		if cache.dataReadOnMiss/cache.bandwidth < 0.5 || cache.dataWritten/cache.dataRead < 0.1 {
		// 			reward -= request.Size / 1024.
		// 		}
		// 		if reward == 0. {
		// 			reward += request.Size / 1024.
		// 		}

		// 		// Update table
		// 		cache.additionTable.Update(curState, curAction, reward, curState)

		// 		cache.qAdditionPrevStates.Insert(request.Filename, request.Size, curState, curAction)

		// 		// Update epsilon
		// 		cache.additionTable.UpdateEpsilon()

		// 		return added
		// 	}

		// 	// Insert with LRU mechanism
		// 	if cache.Size()+requestedFileSize > cache.MaxSize {
		// 		cache.Free(requestedFileSize, false)
		// 	}
		// 	if cache.Size()+requestedFileSize <= cache.MaxSize {
		// 		cache.files.Insert(FileSupportData{
		// 			Filename:  request.Filename,
		// 			Size:      request.Size,
		// 			Frequency: fileStats.FrequencyInCache,
		// 			Recency:   fileStats.Recency,
		// 			Weight:    fileStats.Weight,
		// 		})

		// 		cache.size += requestedFileSize
		// 		fileStats.addInCache(&request.DayTime)
		// 		added = true
		// 	}

		// 	// -------------------------------------------------------------
		// 	//               QLearn - Take the action STORE
		// 	// -------------------------------------------------------------
		// 	if curAction == qlearn.ActionStore {
		// 		reward := 0.
		// 		// reward := request.Size

		// 		if cache.dataReadOnMiss/cache.bandwidth > 0.75 || cache.dataWritten/cache.dataRead > 0.5 {
		// 			reward -= request.Size / 1024.
		// 		}
		// 		if reward == 0. {
		// 			reward += request.Size / 1024.
		// 		}
		// 		// Update table
		// 		cache.additionTable.Update(curState, curAction, reward, curState)

		// 		cache.qAdditionPrevStates.Insert(request.Filename, request.Size, curState, curAction)
		// 	}
		// } else {
		// 	// #######################
		// 	// ##### HIT branch  #####
		// 	// #######################
		// 	cache.files.Update(FileSupportData{
		// 		Filename:  request.Filename,
		// 		Size:      request.Size,
		// 		Frequency: fileStats.FrequencyInCache,
		// 		Recency:   fileStats.Recency,
		// 		Weight:    fileStats.Weight,
		// 	})
		// }

		// // Update epsilon
		// cache.additionTable.UpdateEpsilon()

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
	var totalDeleted float64
	if cache.evictionAgentOK {
		// TODO: manage the penalities before call eviction agent
		totalDeleted = cache.callEvictionAgent(true)
	} else {
		totalDeleted = cache.SimpleCache.Free(amount, percentage)
	}
	return totalDeleted
}

// CheckWatermark checks the watermark levels and resolve the situation
func (cache *AIRL) CheckWatermark() bool {
	ok := true
	if cache.SimpleCache.Occupancy() >= cache.HighWaterMark {
		ok = false
		cache.Free(
			cache.SimpleCache.Occupancy()-cache.LowWaterMark,
			true,
		)
	}
	return ok
}

// ExtraStats for output
func (cache *AIRL) ExtraStats() string {
	addActionCov, addStateCov := cache.additionAgent.GetCoverage()
	evcActionCov, evcStateCov := cache.evictionAgent.GetCoverage()
	return fmt.Sprintf(
		"SCov:%0.2f%%|ACov:%0.2f%%|Eps:%0.5f||SCov:%0.2f%%|ACov:%0.2f%%|Eps:%0.5f",
		addStateCov, addActionCov, cache.additionAgent.Epsilon,
		evcStateCov, evcActionCov, cache.additionAgent.Epsilon,
		// "%0.2f | %0.2f | %0.2f",
		// cache.StdDevSize(), cache.StdDevRec(), cache.StdDevFreq(),
	)
}

// ExtraOutput for output specific information
func (cache *AIRL) ExtraOutput(info string) string {
	result := ""
	switch info {
	case "additionQtable":
		result = cache.additionAgent.QTableToString()
	case "evictionQtable":
		result = cache.evictionAgent.QTableToString()
	case "valueFunctions":
		additionValueFunction := 0.
		evictionValueFunction := 0.
		if cache.additionAgentOK {
			additionValueFunction = cache.additionAgent.ValueFunction
		}
		if cache.evictionAgentOK {
			evictionValueFunction = cache.evictionAgent.ValueFunction
		}
		result = fmt.Sprintf("%0.2f,%0.2f", additionValueFunction, evictionValueFunction)
	default:
		result = "NONE"
	}
	return result
}
