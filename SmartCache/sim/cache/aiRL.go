package cache

import (
	"compress/gzip"
	"encoding/json"
	"fmt"
	"math/rand"
	"os"
	"simulator/v2/cache/ai/featuremap"
	"simulator/v2/cache/ai/qlearn"

	"go.uber.org/zap"
)

const (
	maxBadQValue = 6
)

// AIRL cache
type AIRL struct {
	SimpleCache
	additionAgentOK             bool
	evictionAgentOK             bool
	additionAgentBadQValue      int
	evictionAgentBadQValue      int
	additionAgentPrevQValue     float64
	evictionAgentPrevQValue     float64
	additionFeatureManager      featuremap.FeatureManager
	evictionFeatureManager      featuremap.FeatureManager
	additionAgent               qlearn.Agent
	evictionAgent               qlearn.Agent
	evictionAgentStep           int64
	evictionAgentK              int64
	evictionAgentNumCalls       int64
	evictionAgentNumForcedCalls int64
	evictionRO                  float64
	actionCounters              map[qlearn.ActionType]int
	bufferIdxVector             []int
	curCacheStates              map[int]qlearn.ActionType
	curCacheStatesFiles         map[int][]*FileSupportData
	numPrevDayReq               int64
	numServedReq                int64
}

// Init the AIRL struct
func (cache *AIRL) Init(args ...interface{}) interface{} {
	logger = zap.L()

	cache.curCacheStates = make(map[int]qlearn.ActionType)
	cache.curCacheStatesFiles = make(map[int][]*FileSupportData)

	cache.SimpleCache.Init(NoQueue)

	additionFeatureMap := args[0].(string)
	evictionFeatureMap := args[1].(string)
	initEpsilon := args[2].(float64)
	decayRateEpsilon := args[3].(float64)

	cache.evictionAgentK = 32
	cache.evictionAgentStep = cache.evictionAgentK
	cache.evictionRO = 0.42
	cache.numPrevDayReq = -1

	cache.actionCounters = make(map[qlearn.ActionType]int)

	cache.actionCounters[qlearn.ActionStore] = 0
	cache.actionCounters[qlearn.ActionNotStore] = 0
	cache.actionCounters[qlearn.ActionDeleteAll] = 0
	cache.actionCounters[qlearn.ActionDeleteHalf] = 0
	cache.actionCounters[qlearn.ActionDeleteQuarter] = 0
	cache.actionCounters[qlearn.ActionNotDelete] = 0

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

// ClearHitMissStats the cache stats
func (cache *AIRL) ClearHitMissStats() {
	cache.SimpleCache.ClearHitMissStats()
	cache.evictionAgentNumCalls = 0
	cache.evictionAgentNumForcedCalls = 0
	cache.actionCounters[qlearn.ActionStore] = 0
	cache.actionCounters[qlearn.ActionNotStore] = 0
	cache.actionCounters[qlearn.ActionDeleteAll] = 0
	cache.actionCounters[qlearn.ActionDeleteHalf] = 0
	cache.actionCounters[qlearn.ActionDeleteQuarter] = 0
	cache.actionCounters[qlearn.ActionNotDelete] = 0
}

// Dumps the AIRL cache
func (cache *AIRL) Dumps(fileAndStats bool) [][]byte {
	logger.Info("Dump cache into byte string")
	outData := make([][]byte, 0)
	var newLine = []byte("\n")

	if fileAndStats {
		// ----- Files -----
		logger.Info("Dump cache files")
		for _, file := range cache.files.Get() {
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
		_, writeErr := gwriter.Write(record)
		if writeErr != nil {
			panic(writeErr)
		}
	}

	writeErr := gwriter.Close()
	if writeErr != nil {
		panic(writeErr)
	}
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
			unmarshalErr = json.Unmarshal([]byte(curRecord.Data), &curFile)
			cache.files.Insert(&curFile)
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

func (cache *AIRL) getState4AdditionAgent(hit bool, curFileStats *FileStats) int {

	cache.bufferIdxVector = cache.bufferIdxVector[:0]

	for _, feature := range cache.additionFeatureManager.Features {
		switch feature.Name {
		case "hit":
			cache.bufferIdxVector = append(cache.bufferIdxVector, feature.Index(hit))
		case "size":
			cache.bufferIdxVector = append(cache.bufferIdxVector, feature.Index(curFileStats.Size))
		case "numReq":
			cache.bufferIdxVector = append(cache.bufferIdxVector, feature.Index(curFileStats.Frequency))
		case "deltaLastRequest":
			cache.bufferIdxVector = append(cache.bufferIdxVector, feature.Index(curFileStats.DeltaLastRequest))
		case "percOcc":
			cache.bufferIdxVector = append(cache.bufferIdxVector, feature.Index(cache.SimpleCache.Occupancy()))
		case "hitRate":
			cache.bufferIdxVector = append(cache.bufferIdxVector, feature.Index(cache.SimpleCache.HitRate()))
		}
	}

	return cache.additionAgent.QTable.FeatureIdxs2StateIdx(cache.bufferIdxVector...)
}

func (cache *AIRL) updateCategories() {

	// Remove previous content
	for key := range cache.curCacheStates {
		delete(cache.curCacheStates, key)
	}
	for key := range cache.curCacheStatesFiles {
		delete(cache.curCacheStatesFiles, key)
	}

	var curAction qlearn.ActionType
	catPercOcc := make(map[int]float64)
	catFiles := make(map[int][]*FileSupportData)
	catIdxMap := make(map[int][]int)

	idxWeights := cache.evictionFeatureManager.FileFeatureIdxWeights

	for _, file := range cache.files.Get() {
		// fmt.Println(file.Filename)
		cache.bufferIdxVector = cache.bufferIdxVector[:0]
		for _, feature := range cache.evictionFeatureManager.FileFeatures {
			// fmt.Println(feature.Name)
			switch feature.Name {
			case "catSize":
				cache.bufferIdxVector = append(cache.bufferIdxVector, feature.Index(file.Size))
			case "catNumReq":
				cache.bufferIdxVector = append(cache.bufferIdxVector, feature.Index(file.Frequency))
			case "catDeltaLastRequest":
				cache.bufferIdxVector = append(cache.bufferIdxVector, feature.Index(file.Recency))
			}
		}
		curCatIdx := 0
		for idx, value := range cache.bufferIdxVector {
			curCatIdx += value * idxWeights[idx]
		}
		catSize, inPercOcc := catPercOcc[curCatIdx]
		if !inPercOcc {
			catPercOcc[curCatIdx] = file.Size
			newSlice := make([]int, len(cache.bufferIdxVector))
			copy(newSlice, cache.bufferIdxVector)
			catIdxMap[curCatIdx] = newSlice
			// Create catFiles list
			catFiles[curCatIdx] = make([]*FileSupportData, 0)
		} else {
			catPercOcc[curCatIdx] = catSize + file.Size
		}

		catFiles[curCatIdx] = append(catFiles[curCatIdx], file)

		// fmt.Println(file, cache.bufferIdxVector, curCatIdx)
	}

	// fmt.Println(idxWeights)
	// fmt.Println(catPercOcc)
	// fmt.Println(catIdxMap)

	for catIdx, size := range catPercOcc {
		curCatPercOcc := (size / cache.MaxSize) * 100.
		catPercOcc[catIdx] = curCatPercOcc
	}

	// fmt.Println(catPercOcc)
	// fmt.Println(catIdxMap)

	for catIdx, curCat := range catIdxMap {
		cache.bufferIdxVector = cache.bufferIdxVector[:0]
		for _, feature := range cache.evictionFeatureManager.Features {
			switch feature.Name {
			case "catSize":
				cache.bufferIdxVector = append(cache.bufferIdxVector, curCat[cache.evictionFeatureManager.FileFeatureIdxMap["catSize"]])
			case "catNumReq":
				cache.bufferIdxVector = append(cache.bufferIdxVector, curCat[cache.evictionFeatureManager.FileFeatureIdxMap["catNumReq"]])
			case "catDeltaLastRequest":
				cache.bufferIdxVector = append(cache.bufferIdxVector, curCat[cache.evictionFeatureManager.FileFeatureIdxMap["catDeltaLastRequest"]])
			case "catPercOcc":
				cache.bufferIdxVector = append(cache.bufferIdxVector, feature.Index(catPercOcc[catIdx]))
			case "percOcc":
				cache.bufferIdxVector = append(cache.bufferIdxVector, feature.Index(cache.SimpleCache.Occupancy()))
			case "hitRate":
				cache.bufferIdxVector = append(cache.bufferIdxVector, feature.Index(cache.SimpleCache.HitRate()))
			}
		}
		// fmt.Println(cache.bufferIdxVector)
		curCatState := cache.evictionAgent.QTable.FeatureIdxs2StateIdx(cache.bufferIdxVector...)

		if expEvictionTradeoff := cache.evictionAgent.GetRandomFloat(); expEvictionTradeoff > cache.evictionAgent.Epsilon {
			// ########################
			// ### Exploiting phase ###
			// ########################
			curAction = cache.evictionAgent.GetBestAction(curCatState)
		} else {
			// ##########################
			// #### Exploring phase #####
			// ##########################

			// ----- Random choice -----
			randomActionIdx := int(cache.evictionAgent.GetRandomFloat() * float64(len(cache.evictionAgent.QTable.ActionTypes)))
			curAction = cache.evictionAgent.QTable.ActionTypes[randomActionIdx]
		}

		cache.actionCounters[curAction]++
		cache.curCacheStates[curCatState] = curAction

		cache.curCacheStatesFiles[curCatState] = make([]*FileSupportData, len(catFiles[catIdx]))
		copy(cache.curCacheStatesFiles[curCatState], catFiles[catIdx])
	}
}

func (cache *AIRL) callEvictionAgent(forced bool) (float64, []int64) {
	var (
		totalDeleted float64
		deletedFiles = make([]int64, 0)
	)

	cache.evictionAgentNumCalls++

	// fmt.Println("----- EVICTION -----")

	// fmt.Println("----- Update Category States -----")
	cache.updateCategories()

	// Forced event rewards
	if forced {
		cache.evictionAgentNumForcedCalls++
		cache.evictionAgentK = cache.evictionAgentK>>1 + 1
		if cache.evictionAgentK > cache.numPrevDayReq {
			cache.evictionAgentK = cache.numPrevDayReq
		}
		choicesList, inMemory := cache.evictionAgent.EventMemory["NotDelete"]
		if inMemory {
			for _, choice := range *choicesList {
				for catStateIdx := range cache.curCacheStates {
					if cache.checkEvictionNextState(choice.State, catStateIdx) {
						// Update table
						cache.evictionAgent.UpdateTable(choice.State, catStateIdx, choice.Action, -cache.evictionRO)
						// Update epsilon
						cache.evictionAgent.UpdateEpsilon()
					}
				}
			}
			delete(cache.evictionAgent.EventMemory, "NotDelete")
		}
	} else {
		cache.evictionAgentK = cache.evictionAgentK << 1
	}

	// fmt.Println(cache.curCacheStates)

	for catIdx, catAction := range cache.curCacheStates {
		switch catAction {
		case qlearn.ActionDeleteAll:
			curFileList := cache.curCacheStatesFiles[catIdx]
			for _, curFile := range curFileList {
				curFileStats := cache.stats.Get(curFile.Filename)
				// fmt.Println("REMOVE FREQ:", curFile.Frequency)
				curFileStats.removeFromCache()

				// Update sizes
				cache.size -= curFileStats.Size
				cache.dataDeleted += curFileStats.Size
				totalDeleted += curFileStats.Size

				deletedFiles = append(deletedFiles, curFile.Filename)

				cache.evictionAgent.UpdateFileMemory(curFile.Filename, qlearn.Choice{
					State:     catIdx,
					Action:    catAction,
					Tick:      cache.tick,
					ReadOnHit: cache.dataReadOnHit,
					Occupancy: cache.Occupancy(),
					Frequency: curFile.Frequency,
				})
			}
			cache.curCacheStatesFiles[catIdx] = make([]*FileSupportData, 0)
		case qlearn.ActionDeleteHalf, qlearn.ActionDeleteQuarter:
			curFileList := cache.curCacheStatesFiles[catIdx]
			rand.Shuffle(len(curFileList), func(i, j int) {
				curFileList[i], curFileList[j] = curFileList[j], curFileList[i]
			})
			// fmt.Println("REMOVE: ", maxDeleteNum, " OF ", len(curFileList), "|")
			maxIdx := 0
			if len(curFileList) == 1 {
				maxIdx = 1
			} else if catAction == qlearn.ActionDeleteHalf {
				maxIdx = len(curFileList) / 2
			} else {
				maxIdx = len(curFileList) / 4
			}
			for idx := 0; idx < maxIdx; idx++ {
				curFile := curFileList[idx]
				curFileStats := cache.stats.Get(curFile.Filename)
				// fmt.Println("REMOVE FREQ:", curFile.Frequency)
				curFileStats.removeFromCache()

				// Update sizes
				cache.size -= curFileStats.Size
				cache.dataDeleted += curFileStats.Size
				totalDeleted += curFileStats.Size

				deletedFiles = append(deletedFiles, curFile.Filename)

				cache.evictionAgent.UpdateFileMemory(curFile.Filename, qlearn.Choice{
					State:     catIdx,
					Action:    catAction,
					Tick:      cache.tick,
					ReadOnHit: cache.dataReadOnHit,
					Occupancy: cache.Occupancy(),
					Frequency: curFile.Frequency,
				})
			}
			newFileList := curFileList[maxIdx:]
			cache.curCacheStatesFiles[catIdx] = newFileList
		case qlearn.ActionNotDelete:
			for _, curFile := range cache.curCacheStatesFiles[catIdx] {
				cache.evictionAgent.UpdateFileMemory(curFile.Filename, qlearn.Choice{
					State:     catIdx,
					Action:    catAction,
					Tick:      cache.tick,
					ReadOnHit: cache.dataReadOnHit,
					Occupancy: cache.Occupancy(),
					Frequency: curFile.Frequency,
				})
				cache.evictionAgent.UpdateEventMemory("NotDelete", qlearn.Choice{
					State:     catIdx,
					Action:    catAction,
					Tick:      cache.tick,
					ReadOnHit: cache.dataReadOnHit,
					Occupancy: cache.Occupancy(),
					Frequency: curFile.Frequency,
				})
			}
		}
	}

	// fmt.Println("deleted", deletedFiles)
	cache.files.Remove(deletedFiles, false)

	return totalDeleted, deletedFiles
}

func (cache *AIRL) checkEvictionNextState(oldStateIdx int, newStateIdx int) bool {
	oldState := cache.evictionAgent.QTable.States[oldStateIdx]
	newState := cache.evictionAgent.QTable.States[newStateIdx]
	catSizeIdx := cache.evictionFeatureManager.FeatureIdxMap["catSize"]
	catNumReqIdx := cache.evictionFeatureManager.FeatureIdxMap["catNumReq"]
	catDeltaLastRequestIdx := cache.evictionFeatureManager.FeatureIdxMap["catDeltaLastRequest"]
	// catPercOccIdx := cache.evictionFeatureManager.FeatureIdxMap["catPercOcc"]
	return newState[catSizeIdx] == oldState[catSizeIdx] && newState[catDeltaLastRequestIdx] == oldState[catDeltaLastRequestIdx] && newState[catNumReqIdx] >= oldState[catNumReqIdx]
}

func (cache *AIRL) delayedRewardEvictionAgent(fileStats *FileStats, hit bool) {
	prevChoice, inMemory := cache.evictionAgent.FileMemory[fileStats.Filename]

	if inMemory {
		reward := 0.0
		if cache.dataReadOnHit > prevChoice.ReadOnHit {
			reward += 1.
		} else {
			reward += -1.
		}
		if hit {
			reward += 1.
		} else {
			reward += -1.
		}
		if cache.dataReadOnHit > cache.dataReadOnMiss {
			reward += 1.
		} else {
			reward += -1.
		}
		switch prevChoice.Action {
		/*case qlearn.ActionNotDelete:
		if cache.Occupancy() >= 98. {
			reward += -1.
		}*/
		case qlearn.ActionDeleteAll, qlearn.ActionDeleteHalf, qlearn.ActionDeleteQuarter:
			if cache.Occupancy() < 75. {
				reward += -1.
			}
		}
		for catStateIdx := range cache.curCacheStates {
			if cache.checkEvictionNextState(prevChoice.State, catStateIdx) {
				// Update table
				cache.evictionAgent.UpdateTable(prevChoice.State, catStateIdx, prevChoice.Action, reward)
				// Update epsilon
				cache.evictionAgent.UpdateEpsilon()
			}
		}
	}
}

func (cache *AIRL) delayedRewardAdditionAgent(curState int, fileStats *FileStats, hit bool) {
	prevChoice, inMemory := cache.additionAgent.FileMemory[fileStats.Filename]

	if inMemory {
		reward := 0.0

		if cache.dataReadOnHit > prevChoice.ReadOnHit {
			reward += 1.
		} else {
			reward += -1.
		}

		switch prevChoice.Action {
		case qlearn.ActionStore:
			if !prevChoice.Hit == hit {
				reward += 1.
			}
			if cache.Occupancy() >= 98. {
				reward += -1.
			}
		case qlearn.ActionNotStore:
			if !prevChoice.Hit == !hit {
				reward += 1.
			} else {
				reward += -1.
			}
			if cache.Occupancy() < 100. {
				reward += -1.
			}
		}
		// Update table
		cache.additionAgent.UpdateTable(prevChoice.State, curState, prevChoice.Action, reward)
		// Update epsilon
		cache.additionAgent.UpdateEpsilon()
	}

}

func (cache *AIRL) rewardEvictionAfterForcedCall(added bool) {
	for state, action := range cache.curCacheStates {
		if !added && action == qlearn.ActionNotDelete {
			// Update table
			cache.evictionAgent.UpdateTable(state, state, action, -cache.evictionRO)
			// Update epsilon
			cache.evictionAgent.UpdateEpsilon()
		} else {
			// Update table
			cache.evictionAgent.UpdateTable(state, state, action, cache.evictionRO)
			// Update epsilon
			cache.evictionAgent.UpdateEpsilon()
		}
	}
}

// BeforeRequest of LRU cache
func (cache *AIRL) BeforeRequest(request *Request, hit bool) (*FileStats, bool) {

	if cache.evictionAgentOK {
		if cache.evictionAgentStep <= 0 {
			_, deletedFiles := cache.callEvictionAgent(false)
			if hit {
				for _, filename := range deletedFiles {
					if filename == request.Filename {
						hit = false
						break
					}
				}
			}
			cache.evictionAgentStep = cache.evictionAgentK
		} else {
			cache.evictionAgentStep--
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
		cache.numPrevDayReq = cache.numServedReq
		cache.numServedReq = 0

		if cache.additionAgent.Epsilon <= .2 {
			if cache.additionAgentPrevQValue == 0. {
				cache.additionAgentPrevQValue = cache.additionAgent.QValue
			} else {
				if cache.additionAgent.QValue < cache.additionAgentPrevQValue {
					cache.additionAgentBadQValue++
				} else {
					cache.additionAgentBadQValue--
				}
				if cache.additionAgentBadQValue < 0 {
					cache.additionAgentBadQValue = 0
				}
				cache.additionAgentPrevQValue = cache.additionAgent.QValue
			}
		}
		if cache.evictionAgent.Epsilon <= .2 {
			if cache.evictionAgentPrevQValue == 0. {
				cache.evictionAgentPrevQValue = cache.evictionAgent.QValue
			} else {
				if cache.evictionAgent.QValue < cache.evictionAgentPrevQValue {
					cache.evictionAgentBadQValue++
				} else {
					cache.evictionAgentBadQValue--
				}
				if cache.evictionAgentBadQValue < 0 {
					cache.evictionAgentBadQValue = 0
				}
				cache.evictionAgentPrevQValue = cache.evictionAgent.QValue
			}
		}

		// fmt.Println(cache.additionAgentBadQValue, cache.evictionAgentBadQValue)

		if cache.additionAgentBadQValue >= maxBadQValue {
			cache.additionAgentBadQValue = 0
			cache.additionAgent.UnleashEpsilon()
		}
		if cache.evictionAgentBadQValue >= maxBadQValue {
			cache.evictionAgentBadQValue = 0
			cache.evictionAgent.UnleashEpsilon()
			cache.evictionAgentK = cache.evictionAgentK>>1 + 1
			cache.evictionAgentStep = cache.evictionAgentK
		}
	}

	fileStats.updateStats(hit, request.Size, request.UserID, request.SiteName, request.DayTime)

	cache.numServedReq++

	return fileStats, hit
}

// UpdatePolicy of AIRL cache
func (cache *AIRL) UpdatePolicy(request *Request, fileStats *FileStats, hit bool) bool {
	var (
		added             = false
		curAction         qlearn.ActionType
		curState          int
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

		curState = cache.getState4AdditionAgent(hit, fileStats)

		logger.Debug("cache", zap.Int("current state", curState))

		cache.delayedRewardAdditionAgent(curState, fileStats, hit)

		if cache.evictionAgentOK {
			cache.delayedRewardEvictionAgent(fileStats, hit)
		}

		if !hit {

			if expAdditionTradeoff := cache.additionAgent.GetRandomFloat(); expAdditionTradeoff > cache.additionAgent.Epsilon {
				// ########################
				// ### Exploiting phase ###
				// ########################
				curAction = cache.additionAgent.GetBestAction(curState)
			} else {
				// ##########################
				// #### Exploring phase #####
				// ##########################

				// ----- Random choice -----
				randomActionIdx := int(cache.additionAgent.GetRandomFloat() * float64(len(cache.additionAgent.QTable.ActionTypes)))
				curAction = cache.additionAgent.QTable.ActionTypes[randomActionIdx]
			}
			cache.actionCounters[curAction]++

			cache.additionAgent.UpdateFileMemory(request.Filename, qlearn.Choice{
				State:     curState,
				Action:    curAction,
				Tick:      cache.tick,
				Hit:       hit,
				ReadOnHit: cache.dataReadOnHit,
				Occupancy: cache.Occupancy(),
				Frequency: fileStats.Frequency,
			})

			switch curAction {
			case qlearn.ActionNotStore:
				return false
			case qlearn.ActionStore:
				forced := false
				if cache.Size()+requestedFileSize > cache.MaxSize {
					if cache.evictionAgentOK {
						forced = true
						cache.callEvictionAgent(forced)
					} else {
						cache.Free(requestedFileSize, false)
					}
				}
				if cache.Size()+requestedFileSize > cache.MaxSize {
					if cache.evictionAgentOK && forced {
						cache.rewardEvictionAfterForcedCall(false)
					}
					return false
				}
				cache.files.Insert(&FileSupportData{
					Filename:  request.Filename,
					Size:      request.Size,
					Frequency: fileStats.Frequency,
					Recency:   fileStats.Recency,
					Weight:    fileStats.Weight,
				})

				cache.size += requestedFileSize
				fileStats.addInCache(cache.tick, &request.DayTime)
				added = true
				if cache.evictionAgentOK && forced {
					cache.rewardEvictionAfterForcedCall(added)
				}
			}

		} else {
			// #######################
			// ##### HIT branch  #####
			// #######################
			cache.files.Update(&FileSupportData{
				Filename:  request.Filename,
				Size:      request.Size,
				Frequency: fileStats.Frequency,
				Recency:   fileStats.Recency,
				Weight:    fileStats.Weight,
			})
		}

	} else {
		// #####################################################################
		// #                      NO ADDITION TABLE                            #
		// #####################################################################

		if cache.evictionAgentOK {
			cache.delayedRewardEvictionAgent(fileStats, hit)
		}

		if !hit {
			// ########################
			// ##### MISS branch  #####
			// ########################

			logger.Debug("NO ADDITION TABLE - Normal miss branch")

			forced := false

			// Insert with LRU mechanism
			if cache.Size()+requestedFileSize > cache.MaxSize {
				if cache.evictionAgentOK {
					forced = true
					cache.callEvictionAgent(true)
				} else {
					cache.Free(requestedFileSize, false)
				}
			}
			if cache.Size()+requestedFileSize <= cache.MaxSize {
				cache.files.Insert(&FileSupportData{
					Filename:  request.Filename,
					Size:      request.Size,
					Frequency: fileStats.Frequency,
					Recency:   fileStats.Recency,
					Weight:    fileStats.Weight,
				})

				cache.size += requestedFileSize
				fileStats.addInCache(cache.tick, &request.DayTime)
				added = true
				if cache.evictionAgentOK && forced {
					cache.rewardEvictionAfterForcedCall(added)
				}
			}
		} else {
			// #######################
			// ##### HIT branch  #####
			// #######################
			logger.Debug("NO ADDITION TABLE - Normal hit branch")
			cache.files.Update(&FileSupportData{
				Filename:  request.Filename,
				Size:      request.Size,
				Frequency: fileStats.Frequency,
				Recency:   fileStats.Recency,
				Weight:    fileStats.Weight,
			})
		}
	}

	return added
}

// Free removes files from the cache
func (cache *AIRL) Free(amount float64, percentage bool) float64 {
	return cache.SimpleCache.Free(amount, percentage)
}

// ExtraStats for output
func (cache *AIRL) ExtraStats() string {
	addActionCov, addStateCov := cache.additionAgent.GetCoverage()
	evcActionCov, evcStateCov := cache.evictionAgent.GetCoverage()
	return fmt.Sprintf(
		"SCov:%0.2f%%|ACov:%0.2f%%|Eps:%0.5f||SCov:%0.2f%%|ACov:%0.2f%%|Eps:%0.5f",
		addStateCov, addActionCov, cache.additionAgent.Epsilon,
		evcStateCov, evcActionCov, cache.evictionAgent.Epsilon,
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
			additionValueFunction = cache.additionAgent.QValue
		}
		if cache.evictionAgentOK {
			evictionValueFunction = cache.evictionAgent.QValue
		}
		result = fmt.Sprintf("%0.2f,%0.2f",
			additionValueFunction,
			evictionValueFunction,
		)
	case "evictionStats":
		result = fmt.Sprintf("%d,%d,%d",
			cache.evictionAgentNumCalls,
			cache.evictionAgentNumForcedCalls,
			cache.evictionAgentK,
		)
	case "epsilonStats":
		result = fmt.Sprintf("%0.6f,%0.6f",
			cache.additionAgent.Epsilon, cache.evictionAgent.Epsilon,
		)
	case "actionStats":
		result = fmt.Sprintf("%d,%d,%d,%d,%d,%d",
			cache.actionCounters[qlearn.ActionStore],
			cache.actionCounters[qlearn.ActionNotStore],
			cache.actionCounters[qlearn.ActionDeleteAll],
			cache.actionCounters[qlearn.ActionDeleteHalf],
			cache.actionCounters[qlearn.ActionDeleteQuarter],
			cache.actionCounters[qlearn.ActionNotDelete],
		)
	default:
		result = "NONE"
	}
	return result
}
