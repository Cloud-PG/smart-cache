package cache

import (
	"compress/gzip"
	"encoding/binary"
	"encoding/json"
	"fmt"
	"math"
	"math/rand"
	"os"
	"simulator/v2/cache/ai/featuremap"
	"simulator/v2/cache/ai/qlearn"

	"go.uber.org/zap"
)

type aiRLType int
type evictionType int

const (
	maxBadQValueInRow = 8
	// SCDL type
	SCDL aiRLType = iota - 2
	// SCDL2 type
	SCDL2

	// EvictionOnK agent uses uses k to call eviction decisions
	EvictionOnK = iota - 3
	// EvictionOnDayEnd agent waits the end of the day to call eviction decisions
	EvictionOnDayEnd
	// EvictionOnFree agent call eviction decisions on free
	EvictionOnFree
)

// AIRL cache
type AIRL struct {
	SimpleCache
	rlType                            aiRLType
	additionAgentOK                   bool
	evictionAgentOK                   bool
	evictionType                      evictionType
	additionAgentBadQValue            int
	evictionAgentBadQValue            int
	additionAgentPrevQValue           float64
	evictionAgentPrevQValue           float64
	evictionAgentStep                 int64
	evictionAgentK                    int64
	evictionAgentNumCalls             int64
	evictionAgentNumForcedCalls       int64
	evictionRO                        float64
	sumNumDailyCategories             int
	additionFeatureManager            featuremap.FeatureManager
	evictionFeatureManager            featuremap.FeatureManager
	additionAgent                     qlearn.Agent
	evictionAgent                     qlearn.Agent
	additionAgentChoicesLogFile       *OutputCSV
	evictionAgentChoicesLogFile       *OutputCSV
	additionAgentChoicesLogFileBuffer [][]string
	evictionAgentChoicesLogFileBuffer [][]string
	evictionCheckNextStateMap         map[[8]byte]bool
	evictionCategoryManager           CategoryManager
	actionCounters                    map[qlearn.ActionType]int
	bufferIdxVector                   []int
	numDailyCategories                []int
}

// Init the AIRL struct
func (cache *AIRL) Init(params InitParameters) interface{} { //nolint:ignore,funlen

	additionFeatureMap := params.AIRLAdditionFeatureMap
	evictionFeatureMap := params.AIRLEvictionFeatureMap

	initEpsilon := params.AIRLEpsilonStart
	decayRateEpsilon := params.AIRLEpsilonDecay

	cache.actionCounters = make(map[qlearn.ActionType]int)

	if cache.logger == nil {
		cache.logger = zap.L()
	}

	cache.logger.Info("Feature maps",
		zap.String("addition map", additionFeatureMap),
		zap.String("eviction map", evictionFeatureMap),
	)

	switch params.AIRLType {
	case "scdl", "SCDL":
		cache.rlType = SCDL

		params.QueueType = LRUQueue
		cache.SimpleCache.Init(params)

		if additionFeatureMap == "" {
			panic("ERROR: SCDL needs the addition feature map...")
		}
	case "scdl2", "SCDL2":
		cache.rlType = SCDL2

		evictionAgetType := params.EvictionAgentType
		evictionk := params.AIRLEvictionK

		switch evictionAgetType {
		case "onK", "on_k", "ONK":
			cache.evictionType = EvictionOnK
		case "onDayEnd", "on_day_end", "ONDAYEND":
			cache.evictionType = EvictionOnDayEnd
		case "onFree", "on_free", "ONFREE":
			cache.evictionType = EvictionOnFree
		default:
			panic("ERROR: no valid eviction type...")
		}
		cache.evictionAgentK = evictionk
		cache.evictionAgentStep = cache.evictionAgentK
		cache.evictionRO = 1.0

		if evictionFeatureMap != "" {
			params.QueueType = NoQueue
			cache.SimpleCache.Init(params)

			cache.logger.Info("Create eviction feature manager")
			cache.evictionFeatureManager = featuremap.Parse(evictionFeatureMap)

			cache.logger.Info("Create eviction agent")
			cache.evictionAgent.Init(
				&cache.evictionFeatureManager,
				qlearn.EvictionAgent,
				initEpsilon,
				decayRateEpsilon,
			)
			cache.evictionCategoryManager = CategoryManager{}
			cache.evictionCategoryManager.Init(
				cache.evictionFeatureManager.Features,
				cache.evictionFeatureManager.FeatureIdxWeights,
				cache.evictionFeatureManager.FileFeatures,
				cache.evictionFeatureManager.FileFeatureIdxWeights,
				cache.evictionFeatureManager.FileFeatureIdxMap,
			)
			if cache.logSimulation {
				cache.evictionAgentChoicesLogFile = &OutputCSV{}
				cache.evictionAgentChoicesLogFile.Create("evictionAgentChoiceLog.csv", true)
				cache.evictionAgentChoicesLogFile.Write(ChoiceLogHeader)
				cache.evictionAgentChoicesLogFileBuffer = make([][]string, 0)
			}
			cache.evictionCheckNextStateMap = make(map[[8]byte]bool)
			cache.evictionAgentOK = true
			// Eviction agent action counters
			cache.actionCounters[qlearn.ActionDeleteAll] = 0
			cache.actionCounters[qlearn.ActionDeleteHalf] = 0
			cache.actionCounters[qlearn.ActionDeleteQuarter] = 0
			cache.actionCounters[qlearn.ActionDeleteOne] = 0
			cache.actionCounters[qlearn.ActionNotDelete] = 0
			cache.numDailyCategories = make([]int, 0)
		} else {
			cache.evictionAgentOK = false

			params.QueueType = LRUQueue
			cache.SimpleCache.Init(params)
		}
	default:
		panic("ERROR: RL type is not valid...")
	}

	if additionFeatureMap != "" {
		cache.logger.Info("Create addition feature manager")
		cache.additionFeatureManager = featuremap.Parse(additionFeatureMap)

		cache.logger.Info("Create addition agent")
		cache.additionAgent.Init(
			&cache.additionFeatureManager,
			qlearn.AdditionAgent,
			initEpsilon,
			decayRateEpsilon,
		)
		if cache.logSimulation {
			cache.additionAgentChoicesLogFile = &OutputCSV{}
			cache.additionAgentChoicesLogFile.Create("additionAgentChoiceLog.csv", true)
			cache.additionAgentChoicesLogFile.Write(ChoiceLogHeader)
			cache.additionAgentChoicesLogFileBuffer = make([][]string, 0)
		}
		cache.additionAgentOK = true
		// Addition agent action counters
		cache.actionCounters[qlearn.ActionStore] = 0
		cache.actionCounters[qlearn.ActionNotStore] = 0
	} else {
		cache.additionAgentOK = false
	}

	cache.logger.Info("Table creation done")

	return nil
}

// ClearStats the cache stats
func (cache *AIRL) ClearStats() {
	cache.SimpleCache.ClearStats()
	cache.evictionAgentNumCalls = 0
	cache.evictionAgentNumForcedCalls = 0
	cache.actionCounters[qlearn.ActionStore] = 0
	cache.actionCounters[qlearn.ActionNotStore] = 0
	cache.actionCounters[qlearn.ActionDeleteAll] = 0
	cache.actionCounters[qlearn.ActionDeleteHalf] = 0
	cache.actionCounters[qlearn.ActionDeleteQuarter] = 0
	cache.actionCounters[qlearn.ActionDeleteOne] = 0
	cache.actionCounters[qlearn.ActionNotDelete] = 0

	if cache.evictionAgentOK {
		cache.numDailyCategories = cache.numDailyCategories[:0]
		cache.sumNumDailyCategories = 0
		if cache.evictionType == EvictionOnDayEnd {
			cache.callEvictionAgent()
		}
	}
}

// Dumps the AIRL cache
func (cache *AIRL) Dumps(fileAndStats bool) [][]byte { //nolint:funlen
	cache.logger.Info("Dump cache into byte string")
	outData := make([][]byte, 0)
	var newLine = []byte("\n")

	if fileAndStats {
		// ----- Files -----
		cache.logger.Info("Dump cache files")
		for _, file := range cache.files.GetQueue() {
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
		cache.logger.Info("Dump cache addition agent")
		dumpInfo, _ := json.Marshal(DumpInfo{Type: "ADDAGENT"})
		dumpStats, _ := json.Marshal(cache.additionAgent)
		record, _ := json.Marshal(DumpRecord{
			Info: string(dumpInfo),
			Data: string(dumpStats),
		})
		record = append(record, newLine...)
		outData = append(outData, record)
		// ----- addition feature map manager -----
		cache.logger.Info("Dump cache addition feature map manager")
		dumpInfoFMM, _ := json.Marshal(DumpInfo{Type: "ADDFEATUREMAPMANAGER"})
		dumpStatsFMM, _ := json.Marshal(cache.additionFeatureManager)
		recordFMM, _ := json.Marshal(DumpRecord{
			Info: string(dumpInfoFMM),
			Data: string(dumpStatsFMM),
		})
		record = append(recordFMM, newLine...) //nolint:gocritic
		outData = append(outData, record)
	}
	if cache.evictionAgentOK {
		// ----- eviction agent -----
		cache.logger.Info("Dump cache eviction agent")
		dumpInfo, _ := json.Marshal(DumpInfo{Type: "EVCAGENT"})
		dumpStats, _ := json.Marshal(cache.evictionAgent)
		record, _ := json.Marshal(DumpRecord{
			Info: string(dumpInfo),
			Data: string(dumpStats),
		})
		record = append(record, newLine...)
		outData = append(outData, record)
		// ----- eviction feature map manager -----
		cache.logger.Info("Dump cache eviction feature map manager")
		dumpInfoFMM, _ := json.Marshal(DumpInfo{Type: "EVCFEATUREMAPMANAGER"})
		dumpStatsFMM, _ := json.Marshal(cache.evictionFeatureManager)
		recordFMM, _ := json.Marshal(DumpRecord{
			Info: string(dumpInfoFMM),
			Data: string(dumpStatsFMM),
		})
		record = append(recordFMM, newLine...) //nolint:gocritic
		outData = append(outData, record)
	}
	return outData
}

// Dump the AIRL cache
func (cache *AIRL) Dump(filename string, fileAndStats bool) {
	cache.logger.Info("Dump cache", zap.String("filename", filename))
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
	cache.logger.Info("Loads cache dump string")
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
			var curFileStats FileStats
			unmarshalErr = json.Unmarshal([]byte(curRecord.Data), &curFileStats)
			cache.files.Insert(&curFileStats)
			cache.size += curFileStats.Size
			cache.stats.fileStats[curRecord.Filename] = &curFileStats
		case "STATS":
			var curFileStats FileStats
			unmarshalErr = json.Unmarshal([]byte(curRecord.Data), &curFileStats)
			if _, inStats := cache.stats.fileStats[curRecord.Filename]; !inStats {
				cache.stats.fileStats[curRecord.Filename] = &curFileStats
			}
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
			cache.bufferIdxVector = append(cache.bufferIdxVector, feature.Index(cache.SimpleCache.Capacity()))
		case "hitRate":
			cache.bufferIdxVector = append(cache.bufferIdxVector, feature.Index(cache.SimpleCache.HitRate()))
		}
	}

	return cache.additionAgent.QTable.FeatureIdxs2StateIdx(cache.bufferIdxVector...)
}

func (cache *AIRL) punishEvictionAgentOnForcedCall() {
	cache.evictionAgentNumForcedCalls++
	choicesList, inMemory := cache.evictionAgent.Memory["NotDelete"]
	if inMemory {
		for _, choice := range choicesList {
			for catState := range cache.evictionCategoryManager.GetStateFromCategories(
				false,
				cache.evictionAgent,
				cache.Capacity(),
				cache.HitRate(),
				cache.MaxSize,
			) {
				if cache.checkEvictionNextState(choice.State, catState.Idx) {
					// Update table
					cache.evictionAgent.UpdateTable(choice.State, catState.Idx, choice.Action, -cache.evictionRO)
					// Update epsilon
					cache.evictionAgent.UpdateEpsilon()
				}
			}
		}

		delete(cache.evictionAgent.Memory, "NotDelete")
	}
}

func (cache *AIRL) callEvictionAgent() (float64, []int64) { //nolint:funlen
	var (
		totalDeleted float64
		deletedFiles = make([]int64, 0)
		files2delete = make([]DelCatFile, 0)
	)

	cache.evictionAgentNumCalls++

	// fmt.Println("----- EVICTION ----- Forced[", forced, "]")

	for catState := range cache.evictionCategoryManager.GetStateFromCategories(
		true,
		cache.evictionAgent,
		cache.Capacity(),
		cache.HitRate(),
		cache.MaxSize,
	) {
		// fmt.Printf("[CATMANAGER]: Category -> %#v\n", catState)
		cache.actionCounters[catState.Action]++

		switch catState.Action {
		case qlearn.ActionDeleteAll:
			curFileList := catState.Files
			for idx := len(curFileList) - 1; idx > -1; idx-- {
				curFile := curFileList[idx]
				curFileStats := cache.stats.Get(curFile.Filename)
				// fmt.Println("REMOVE FREQ:", curFile.Frequency)
				curFileStats.removeFromCache()

				// Update sizes
				cache.size -= curFileStats.Size
				cache.dataDeleted += curFileStats.Size
				totalDeleted += curFileStats.Size

				deletedFiles = append(deletedFiles, curFile.Filename)

				files2delete = append(files2delete, DelCatFile{
					Category: catState.Category,
					File:     curFile,
				})

				cache.evictionAgent.SaveMemoryWithNoLimits(curFile.Filename, qlearn.Choice{
					State:     catState.Idx,
					Action:    catState.Action,
					Tick:      cache.tick,
					DeltaT:    curFileStats.DeltaLastRequest,
					Capacity:  cache.Capacity(),
					Size:      curFile.Size,
					Frequency: curFileStats.Frequency,
				})
				cache.toEvictionChoiceBuffer([]string{
					fmt.Sprintf("%d", cache.tick),
					fmt.Sprintf("%d", curFileStats.Filename),
					fmt.Sprintf("%0.2f", curFileStats.Size),
					fmt.Sprintf("%d", curFileStats.Frequency),
					fmt.Sprintf("%d", curFileStats.DeltaLastRequest),
					"DeleteAll",
				})
				cache.toChoiceBuffer([]string{
					fmt.Sprintf("%d", cache.tick),
					fmt.Sprintf("%d", curFileStats.Filename),
					fmt.Sprintf("%0.2f", curFileStats.Size),
					fmt.Sprintf("%d", curFileStats.Frequency),
					fmt.Sprintf("%d", curFileStats.DeltaLastRequest),
					ChoiceDelete,
				})
			}
		case qlearn.ActionDeleteHalf, qlearn.ActionDeleteQuarter:
			curFileList := catState.Files
			rand.Shuffle(len(curFileList), func(i, j int) {
				curFileList[i], curFileList[j] = curFileList[j], curFileList[i]
			})
			numDeletes := 0
			actionString := ""
			if catState.Action == qlearn.ActionDeleteHalf {
				actionString = "DeleteHalf"
			} else {
				actionString = "DeleteQuarter"
			}

			switch {
			case len(curFileList) == 1:
				numDeletes = 1
			case catState.Action == qlearn.ActionDeleteHalf:
				numDeletes = len(curFileList) / 2
			default:
				numDeletes = len(curFileList) / 4
			}

			for idx := len(curFileList) - 1; idx > -1; idx-- {
				curFile := curFileList[idx]
				curFileStats := cache.stats.Get(curFile.Filename)
				// fmt.Println("REMOVE FREQ:", curFile.Frequency)
				curFileStats.removeFromCache()

				// Update sizes
				cache.size -= curFileStats.Size
				cache.dataDeleted += curFileStats.Size
				totalDeleted += curFileStats.Size

				deletedFiles = append(deletedFiles, curFile.Filename)

				files2delete = append(files2delete, DelCatFile{
					Category: catState.Category,
					File:     curFile,
				})

				cache.evictionAgent.SaveMemoryWithNoLimits(curFile.Filename, qlearn.Choice{
					State:     catState.Idx,
					Action:    catState.Action,
					Tick:      cache.tick,
					DeltaT:    curFileStats.DeltaLastRequest,
					Capacity:  cache.Capacity(),
					Size:      curFile.Size,
					Frequency: curFileStats.Frequency,
				})
				cache.toEvictionChoiceBuffer([]string{
					fmt.Sprintf("%d", cache.tick),
					fmt.Sprintf("%d", curFileStats.Filename),
					fmt.Sprintf("%0.2f", curFileStats.Size),
					fmt.Sprintf("%d", curFileStats.Frequency),
					fmt.Sprintf("%d", curFileStats.DeltaLastRequest),
					actionString,
				})
				cache.toChoiceBuffer([]string{
					fmt.Sprintf("%d", cache.tick),
					fmt.Sprintf("%d", curFileStats.Filename),
					fmt.Sprintf("%0.2f", curFileStats.Size),
					fmt.Sprintf("%d", curFileStats.Frequency),
					fmt.Sprintf("%d", curFileStats.DeltaLastRequest),
					ChoiceDelete,
				})
				numDeletes--
				if numDeletes <= 0 {
					break
				}
			}
		case qlearn.ActionDeleteOne:
			curFileList := catState.Files
			delIdx := rand.Intn(len(curFileList))
			curFile := curFileList[delIdx]
			curFileStats := cache.stats.Get(curFile.Filename)
			// fmt.Println("REMOVE FREQ:", curFile.Frequency)
			curFileStats.removeFromCache()

			// Update sizes
			cache.size -= curFileStats.Size
			cache.dataDeleted += curFileStats.Size
			totalDeleted += curFileStats.Size

			deletedFiles = append(deletedFiles, curFile.Filename)

			files2delete = append(files2delete, DelCatFile{
				Category: catState.Category,
				File:     curFile,
			})

			cache.evictionAgent.SaveMemoryWithNoLimits(curFile.Filename, qlearn.Choice{
				State:     catState.Idx,
				Action:    catState.Action,
				Tick:      cache.tick,
				DeltaT:    curFileStats.DeltaLastRequest,
				Capacity:  cache.Capacity(),
				Size:      curFile.Size,
				Frequency: curFileStats.Frequency,
			})
			cache.toEvictionChoiceBuffer([]string{
				fmt.Sprintf("%d", cache.tick),
				fmt.Sprintf("%d", curFileStats.Filename),
				fmt.Sprintf("%0.2f", curFileStats.Size),
				fmt.Sprintf("%d", curFileStats.Frequency),
				fmt.Sprintf("%d", curFileStats.DeltaLastRequest),
				"DeleteOne",
			})
			cache.toChoiceBuffer([]string{
				fmt.Sprintf("%d", cache.tick),
				fmt.Sprintf("%d", curFileStats.Filename),
				fmt.Sprintf("%0.2f", curFileStats.Size),
				fmt.Sprintf("%d", curFileStats.Frequency),
				fmt.Sprintf("%d", curFileStats.DeltaLastRequest),
				ChoiceDelete,
			})
		case qlearn.ActionNotDelete:
			for _, curFile := range catState.Files {
				curFileStats := cache.stats.Get(curFile.Filename)

				cache.evictionAgent.SaveMemoryWithNoLimits(curFile.Filename, qlearn.Choice{
					State:     catState.Idx,
					Action:    catState.Action,
					Tick:      cache.tick,
					DeltaT:    curFileStats.DeltaLastRequest,
					Capacity:  cache.Capacity(),
					Size:      curFile.Size,
					Frequency: curFileStats.Frequency,
				})
				cache.evictionAgent.SaveMemoryWithNoLimits("NotDelete", qlearn.Choice{
					State:     catState.Idx,
					Action:    catState.Action,
					Tick:      cache.tick,
					DeltaT:    curFileStats.DeltaLastRequest,
					Capacity:  cache.Capacity(),
					Size:      curFile.Size,
					Frequency: curFileStats.Frequency,
				})
				cache.toEvictionChoiceBuffer([]string{
					fmt.Sprintf("%d", cache.tick),
					fmt.Sprintf("%d", curFileStats.Filename),
					fmt.Sprintf("%0.2f", curFileStats.Size),
					fmt.Sprintf("%d", curFileStats.Frequency),
					fmt.Sprintf("%d", curFileStats.DeltaLastRequest),
					"NotDelete",
				})
			}
		}
	}

	// fmt.Printf("[CATMANAGER] files 2 delete -> %#v\n", files2delete)
	for _, file2Delete := range files2delete {
		cache.numDeleted++
		cache.evictionCategoryManager.deleteFileFromCategory(file2Delete.Category, file2Delete.File)
	}

	// fmt.Println("[CATMANAGER] Deleted files -> ", deletedFiles)
	cache.files.Remove(deletedFiles)

	return totalDeleted, deletedFiles
}

func (cache *AIRL) checkEvictionNextState(oldStateIdx int, newStateIdx int) bool {
	curArgs := [8]byte{}

	binary.BigEndian.PutUint32(curArgs[:4], uint32(oldStateIdx))
	binary.BigEndian.PutUint32(curArgs[4:], uint32(newStateIdx))

	isNext, inMap := cache.evictionCheckNextStateMap[curArgs]
	if !inMap {
		oldState := cache.evictionAgent.QTable.States[oldStateIdx]
		newState := cache.evictionAgent.QTable.States[newStateIdx]
		catSizeIdx := cache.evictionFeatureManager.FeatureIdxMap["catSize"]
		catNumReqIdx := cache.evictionFeatureManager.FeatureIdxMap["catNumReq"]
		catDeltaLastRequestIdx := cache.evictionFeatureManager.FeatureIdxMap["catDeltaLastRequest"]
		// catPercOccIdx := cache.evictionFeatureManager.FeatureIdxMap["catPercOcc"]
		isNext = newState[catSizeIdx] == oldState[catSizeIdx]
		isNext = isNext && newState[catDeltaLastRequestIdx] == oldState[catDeltaLastRequestIdx]
		isNext = isNext && newState[catNumReqIdx] >= oldState[catNumReqIdx]
	}

	cache.evictionCheckNextStateMap[curArgs] = isNext
	return isNext
}

func (cache *AIRL) delayedRewardEvictionAgent(filename int64, hit bool) { //nolint:ignore,gocognit
	memories, inMemory := cache.evictionAgent.Memory[filename]

	if inMemory { //nolint:ignore,nestif
		for idx := 0; idx < len(memories)-1; idx++ {
			var (
				prevMemory, nextMemory qlearn.Choice
			)
			prevMemory = memories[idx]

			if idx == len(memories)-1 {
				for catState := range cache.evictionCategoryManager.GetStateFromCategories(
					false,
					cache.evictionAgent,
					cache.Capacity(),
					cache.HitRate(),
					cache.MaxSize,
				) {
					if cache.checkEvictionNextState(prevMemory.State, catState.Idx) {
						nextMemory.State = catState.Idx
					}
				}

				continue // No next state found
			} else {
				nextMemory = memories[idx+1]
			}

			reward := 0.0
			if hit {
				reward += 1.
				if prevMemory.Action == qlearn.ActionNotDelete {
					reward += 1.
					if prevMemory.Capacity >= nextMemory.Capacity {
						reward += 1.
					}
				}
			} else { // MISS
				reward += -1.
				if prevMemory.Action != qlearn.ActionNotDelete {
					reward += -1.
					if prevMemory.Capacity < nextMemory.Capacity {
						reward += -1.
					}
				}
			}

			// Update table
			cache.evictionAgent.UpdateTable(prevMemory.State, nextMemory.State, prevMemory.Action, reward)
			// Update epsilon
			cache.evictionAgent.UpdateEpsilon()
		}
	}
}

func (cache *AIRL) delayedRewardAdditionAgent(filename int64, hit bool) { //nolint:ignore,funlen
	switch cache.rlType {
	case SCDL:
		lastMemories := cache.additionAgent.Remember(filename)
		for _, memory := range lastMemories {
			reward := 0.0

			// MISS
			if !memory.Hit { // nolint:ignore,nestif
				if memory.Action == qlearn.ActionNotStore {
					if cache.dataReadOnMiss/cache.bandwidth < 0.5 || cache.dataWritten/cache.dataRead < 0.1 {
						reward -= memory.Size / 1024.
					}
				} else if memory.Action == qlearn.ActionStore {
					if cache.dataReadOnMiss/cache.bandwidth > 0.75 || cache.dataWritten/cache.dataRead > 0.5 {
						reward -= memory.Size / 1024.
					}
				}
				if cache.dataReadOnMiss/cache.dataRead > 0.5 {
					reward -= memory.Size / 1024.
				}
			} else { // HIT
				if cache.dataReadOnHit/cache.dataRead < 0.3 {
					reward -= memory.Size / 1024.
				}
				if cache.dataWritten/cache.dataRead > 0.3 {
					reward -= memory.Size / 1024.
				}
			}

			if reward == 0. {
				if hit {
					reward += memory.Size / 1024.
				} else {
					reward -= memory.Size / 1024.
				}
			}

			// Update table
			cache.additionAgent.UpdateTable(memory.State, memory.State, memory.Action, reward)
			// Update epsilon
			cache.additionAgent.UpdateEpsilon()
		}
	case SCDL2:
		memories, inMemory := cache.additionAgent.Memory[filename]
		if inMemory { //nolint:ignore,nestif
			for idx := 0; idx < len(memories)-2; idx++ {
				prevMemory, nextMemory := memories[idx], memories[idx+1]
				reward := 0.0

				if prevMemory.Action != qlearn.ActionNONE {
					if hit { // HIT
						reward += 1.
						if !prevMemory.Hit && nextMemory.Hit {
							reward += 1.
						}
					} else { // MISS
						reward += -1.
						if prevMemory.Action == qlearn.ActionNotStore {
							if !prevMemory.Hit && !nextMemory.Hit {
								reward += -1.
							} else if prevMemory.Hit && !nextMemory.Hit {
								reward += -1.
							}
						}
					}
					// Update table
					cache.additionAgent.UpdateTable(prevMemory.State, nextMemory.State, prevMemory.Action, reward)
					// Update epsilon
					cache.additionAgent.UpdateEpsilon()
				}
			}
		}
	}
}

func (cache *AIRL) rewardEvictionAfterForcedCall(added bool) {
	for catState := range cache.evictionCategoryManager.GetStateFromCategories(
		false,
		cache.evictionAgent,
		cache.Capacity(),
		cache.HitRate(),
		cache.MaxSize,
	) {
		if !added && catState.Action == qlearn.ActionNotDelete {
			// Update table
			cache.evictionAgent.UpdateTable(catState.Idx, catState.Idx, catState.Action, -cache.evictionRO)
			// Update epsilon
			cache.evictionAgent.UpdateEpsilon()
		} else {
			// Update table
			cache.evictionAgent.UpdateTable(catState.Idx, catState.Idx, catState.Action, cache.evictionRO)
			// Update epsilon
			cache.evictionAgent.UpdateEpsilon()
		}
	}
}

// BeforeRequest of LRU cache
func (cache *AIRL) BeforeRequest(request *Request, hit bool) (*FileStats, bool) { //nolint:ignore,funlen
	// fmt.Println("+++ REQUESTED FILE +++-> ", request.Filename)

	fileStats, _ := cache.stats.GetOrCreate(request.Filename, request.Size, request.DayTime, cache.tick)

	cache.prevTime = cache.curTime
	cache.curTime = request.DayTime

	if !cache.curTime.Equal(cache.prevTime) { //nolint:ignore,nestif
		if cache.additionAgent.Epsilon <= cache.additionAgent.MinEpsilon+0.01 {
			if cache.additionAgentPrevQValue == 0. {
				cache.additionAgentPrevQValue = cache.additionAgent.QValue
			} else {
				if cache.additionAgent.QValue < cache.additionAgentPrevQValue {
					cache.additionAgentBadQValue++
				} else {
					cache.additionAgentBadQValue = 0
				}
				cache.additionAgentPrevQValue = cache.additionAgent.QValue
			}
		}
		if cache.evictionAgent.Epsilon <= cache.evictionAgent.MinEpsilon+0.01 {
			if cache.evictionAgentPrevQValue == 0. {
				cache.evictionAgentPrevQValue = cache.evictionAgent.QValue
			} else {
				if cache.evictionAgent.QValue < cache.evictionAgentPrevQValue {
					cache.evictionAgentBadQValue++
				} else {
					cache.evictionAgentBadQValue = 0
				}
				cache.evictionAgentPrevQValue = cache.evictionAgent.QValue
			}
		}

		// fmt.Println(cache.additionAgentBadQValue, cache.evictionAgentBadQValue)

		if cache.additionAgentBadQValue >= maxBadQValueInRow {
			if cache.additionAgentOK {
				cache.additionAgentBadQValue = 0
				if cache.additionAgent.QValue < 0. {
					cache.additionAgent.UnleashEpsilon(nil)
					// cache.additionAgent.ResetTableAction()  // Clean completely the actions
					cache.additionAgent.ResetMemories()
				} else {
					cache.additionAgent.UnleashEpsilon(0.5)
				}
			}
		}
		if cache.evictionAgentBadQValue >= maxBadQValueInRow {
			if cache.evictionAgentOK {
				cache.evictionAgentBadQValue = 0
				if cache.evictionAgent.QValue < 0. {
					cache.evictionAgent.UnleashEpsilon(nil)
					// cache.evictionAgent.ResetTableAction()  // Clean completely the actions
					cache.evictionAgent.ResetMemories()
				} else {
					cache.evictionAgent.UnleashEpsilon(0.5)
				}
			}
		}
	}

	cache.numReq++

	fileStats.updateStats(hit, request.Size, request.UserID, request.SiteName, request.DayTime)

	return fileStats, hit
}

// UpdatePolicy of AIRL cache
func (cache *AIRL) UpdatePolicy(request *Request, fileStats *FileStats, hit bool) bool { //nolint:ignore,funlen
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

	if cache.additionAgentOK { //nolint:ignore,nestif

		cache.logger.Debug("ADDITION AGENT OK")

		curState = cache.getState4AdditionAgent(hit, fileStats)

		cache.logger.Debug("cache", zap.Int("current state", curState))

		if cache.rlType == SCDL {
			cache.delayedRewardAdditionAgent(request.Filename, hit)
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

			switch curAction {
			case qlearn.ActionNotStore:
				cache.actionCounters[curAction]++
				curChoice := qlearn.Choice{
					State:     curState,
					Action:    curAction,
					Tick:      cache.tick,
					DeltaT:    fileStats.DeltaLastRequest,
					Hit:       hit,
					Capacity:  cache.Capacity(),
					Size:      request.Size,
					Frequency: fileStats.Frequency,
				}

				switch cache.rlType {
				case SCDL:
					cache.additionAgent.SaveMemory(request.Filename, curChoice)
				case SCDL2:
					cache.additionAgent.SaveMemoryWithNoLimits(request.Filename, curChoice)
				}

				cache.toAdditionChoiceBuffer([]string{
					fmt.Sprintf("%d", cache.tick),
					fmt.Sprintf("%d", fileStats.Filename),
					fmt.Sprintf("%0.2f", fileStats.Size),
					fmt.Sprintf("%d", fileStats.Frequency),
					fmt.Sprintf("%d", fileStats.DeltaLastRequest),
					"NotStore",
				})
				return false
			case qlearn.ActionStore:
				forced := false

				if cache.Size()+requestedFileSize > cache.MaxSize {
					if cache.evictionAgentOK {
						forced = true
						switch cache.evictionType {
						case EvictionOnDayEnd, EvictionOnK:
							cache.punishEvictionAgentOnForcedCall()
							cache.callEvictionAgent()
						case EvictionOnFree:
							cache.callEvictionAgent()
						}

						cache.callEvictionAgent()
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

				cache.files.Insert(fileStats)

				if cache.evictionAgentOK {
					fileCategory := cache.evictionCategoryManager.GetFileCategory(fileStats)
					cache.evictionCategoryManager.AddOrUpdateCategoryFile(fileCategory, fileStats)
				}

				cache.size += requestedFileSize
				fileStats.addInCache(cache.tick, &request.DayTime)
				added = true
				if cache.evictionAgentOK && forced {
					cache.rewardEvictionAfterForcedCall(added)
				}

				cache.actionCounters[curAction]++
				curChoice := qlearn.Choice{
					State:     curState,
					Action:    curAction,
					Tick:      cache.tick,
					DeltaT:    fileStats.DeltaLastRequest,
					Hit:       hit,
					Capacity:  cache.Capacity(),
					Size:      request.Size,
					Frequency: fileStats.Frequency,
				}

				switch cache.rlType {
				case SCDL:
					cache.additionAgent.SaveMemory(request.Filename, curChoice)
				case SCDL2:
					cache.additionAgent.SaveMemoryWithNoLimits(request.Filename, curChoice)
				}

				cache.toAdditionChoiceBuffer([]string{
					fmt.Sprintf("%d", cache.tick),
					fmt.Sprintf("%d", fileStats.Filename),
					fmt.Sprintf("%0.2f", fileStats.Size),
					fmt.Sprintf("%d", fileStats.Frequency),
					fmt.Sprintf("%d", fileStats.DeltaLastRequest),
					"Store",
				})
				cache.toChoiceBuffer([]string{
					fmt.Sprintf("%d", cache.tick),
					fmt.Sprintf("%d", fileStats.Filename),
					fmt.Sprintf("%0.2f", fileStats.Size),
					fmt.Sprintf("%d", fileStats.Frequency),
					fmt.Sprintf("%d", fileStats.DeltaLastRequest),
					ChoiceAdd,
				})
			}
		} else {
			// #######################
			// ##### HIT branch  #####
			// #######################
			cache.files.Update(fileStats)
			curChoice := qlearn.Choice{
				State:     curState,
				Action:    qlearn.ActionNONE,
				Tick:      cache.tick,
				DeltaT:    fileStats.DeltaLastRequest,
				Hit:       hit,
				Capacity:  cache.Capacity(),
				Size:      request.Size,
				Frequency: fileStats.Frequency,
			}
			switch cache.rlType {
			case SCDL2:
				cache.additionAgent.SaveMemoryWithNoLimits(request.Filename, curChoice)
			}

			if cache.evictionAgentOK {
				fileCategory := cache.evictionCategoryManager.GetFileCategory(fileStats)
				cache.evictionCategoryManager.AddOrUpdateCategoryFile(fileCategory, fileStats)
			}
		}
	} else {
		// #####################################################################
		// #                      NO ADDITION AGENT                            #
		// #####################################################################

		if !hit {
			// ########################
			// ##### MISS branch  #####
			// ########################

			cache.logger.Debug("NO ADDITION AGENT - Normal miss branch")

			forced := false

			// Insert with LRU mechanism
			if cache.Size()+requestedFileSize > cache.MaxSize {
				if cache.evictionAgentOK {
					forced = true
					switch cache.evictionType {
					case EvictionOnDayEnd, EvictionOnK:
						cache.punishEvictionAgentOnForcedCall()
						cache.callEvictionAgent()
					case EvictionOnFree:
						cache.callEvictionAgent()
					}
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

			cache.files.Insert(fileStats)

			if cache.evictionAgentOK {
				fileCategory := cache.evictionCategoryManager.GetFileCategory(fileStats)
				cache.evictionCategoryManager.AddOrUpdateCategoryFile(fileCategory, fileStats)
			}

			cache.size += requestedFileSize
			fileStats.addInCache(cache.tick, &request.DayTime)
			added = true
			if cache.evictionAgentOK && forced {
				cache.rewardEvictionAfterForcedCall(added)
			}

			cache.toChoiceBuffer([]string{
				fmt.Sprintf("%d", cache.tick),
				fmt.Sprintf("%d", fileStats.Filename),
				fmt.Sprintf("%0.2f", fileStats.Size),
				fmt.Sprintf("%d", fileStats.Frequency),
				fmt.Sprintf("%d", fileStats.DeltaLastRequest),
				ChoiceAdd,
			})
		} else {
			// #######################
			// ##### HIT branch  #####
			// #######################
			cache.logger.Debug("NO ADDITION AGENT - Normal hit branch")
			cache.files.Update(fileStats)

			if cache.evictionAgentOK {
				fileCategory := cache.evictionCategoryManager.GetFileCategory(fileStats)
				cache.evictionCategoryManager.AddOrUpdateCategoryFile(fileCategory, fileStats)
			}
		}
	}

	return added
}

// AfterRequest of the cache
func (cache *AIRL) AfterRequest(request *Request, fileStats *FileStats, hit bool, added bool) {
	if cache.rlType == SCDL2 { //nolint:ignore,nestif
		if cache.additionAgentOK { //nolint:ignore,nestif
			cache.delayedRewardAdditionAgent(request.Filename, hit)
		}
		if cache.evictionAgentOK {
			cache.delayedRewardEvictionAgent(request.Filename, hit)
			curNumCat := cache.evictionCategoryManager.GetNumCategories()
			cache.numDailyCategories = append(cache.numDailyCategories, curNumCat)
			cache.sumNumDailyCategories += curNumCat
		}
		if cache.evictionAgentOK && cache.evictionType == EvictionOnK {
			// fmt.Println(cache.evictionAgentStep)
			if cache.evictionAgentStep <= 0 {
				cache.callEvictionAgent()
				cache.evictionAgentStep = cache.evictionAgentK
			} else {
				cache.evictionAgentStep--
			}
		}
	}

	cache.SimpleCache.AfterRequest(request, fileStats, hit, added)
}

// Free removes files from the cache
func (cache *AIRL) Free(amount float64, percentage bool) float64 {
	return cache.SimpleCache.Free(amount, percentage)
}

// CheckWatermark checks the watermark levels and resolve the situation
func (cache *AIRL) CheckWatermark() bool {
	if cache.rlType == SCDL {
		return cache.SimpleCache.CheckWatermark()
	}
	return true
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

func writeQTable(outFilename string, data string) {
	qtableAdditionFile, errCreateQTablecsv := os.Create(outFilename)

	defer func() {
		closeErr := qtableAdditionFile.Close()
		if closeErr != nil {
			panic(closeErr)
		}
	}()

	if errCreateQTablecsv != nil {
		panic(errCreateQTablecsv)
	}

	_, writeErr := qtableAdditionFile.WriteString(data)
	if writeErr != nil {
		panic(writeErr)
	}
}

func (cache *AIRL) meanNumCategories() int {
	return cache.sumNumDailyCategories / len(cache.numDailyCategories)
}

func (cache *AIRL) stdDevNumCategories() float64 {
	var sum int

	mean := cache.meanNumCategories()

	for _, value := range cache.numDailyCategories {
		curDiff := value - mean
		sum += curDiff * curDiff
	}

	return math.Sqrt(float64(sum) / float64(len(cache.numDailyCategories)-1))
}

// ExtraOutput for output specific information
func (cache *AIRL) ExtraOutput(info string) string { //nolint:ignore,funlen
	result := ""
	switch info {
	case "evictionCategoryStats":
		if cache.evictionAgentOK {
			result = fmt.Sprintf("%d,%0.2f",
				cache.meanNumCategories(),
				cache.stdDevNumCategories(),
			)
		} else {
			// cache.logger.Info("No Category stats...")
			result = fmt.Sprintf("%d,%0.2f",
				-1,
				-1.,
			)
		}
	case "additionQTable":
		if cache.additionAgentOK {
			result = cache.additionAgent.QTableToString()
			writeQTable("additionQTable.csv", result)
		} else {
			cache.logger.Info("No Addition Table...")
			result = ""
		}
	case "evictionQTable":
		if cache.evictionAgentOK {
			result = cache.evictionAgent.QTableToString()
			writeQTable("evictionQTable.csv", result)
		} else {
			cache.logger.Info("No Eviction Table...")
			result = ""
		}
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
		result = fmt.Sprintf("%d,%d",
			cache.evictionAgentNumCalls,
			cache.evictionAgentNumForcedCalls,
		)
	case "epsilonStats":
		result = fmt.Sprintf("%0.6f,%0.6f",
			cache.additionAgent.Epsilon, cache.evictionAgent.Epsilon,
		)
	case "actionStats":
		result = fmt.Sprintf("%d,%d,%d,%d,%d,%d,%d",
			cache.actionCounters[qlearn.ActionStore],
			cache.actionCounters[qlearn.ActionNotStore],
			cache.actionCounters[qlearn.ActionDeleteAll],
			cache.actionCounters[qlearn.ActionDeleteHalf],
			cache.actionCounters[qlearn.ActionDeleteQuarter],
			cache.actionCounters[qlearn.ActionDeleteOne],
			cache.actionCounters[qlearn.ActionNotDelete],
		)
	default:
		result = "NONE"
	}
	return result
}

// Terminate pending things of the cache
func (cache *AIRL) Terminate() error {
	if cache.logSimulation {
		if cache.additionAgentChoicesLogFile != nil {
			cache.flushAdditionChoices()
			cache.additionAgentChoicesLogFile.Close()
		}
		if cache.evictionAgentChoicesLogFile != nil {
			cache.flushEvictionChoices()
			cache.evictionAgentChoicesLogFile.Close()
		}
	}
	_ = cache.SimpleCache.Terminate()
	return nil
}

func (cache *AIRL) toAdditionChoiceBuffer(curChoice []string) {
	if cache.logSimulation {
		cache.additionAgentChoicesLogFileBuffer = append(cache.additionAgentChoicesLogFileBuffer, curChoice)
		if len(cache.choicesBuffer) > 9999 {
			cache.flushChoices()
		}
	}
}

func (cache *AIRL) flushAdditionChoices() {
	for _, choice := range cache.additionAgentChoicesLogFileBuffer {
		cache.additionAgentChoicesLogFile.Write(choice)
	}
	cache.additionAgentChoicesLogFileBuffer = cache.additionAgentChoicesLogFileBuffer[:0]
}

func (cache *AIRL) toEvictionChoiceBuffer(curChoice []string) {
	if cache.logSimulation {
		cache.evictionAgentChoicesLogFileBuffer = append(cache.evictionAgentChoicesLogFileBuffer, curChoice)
		if len(cache.choicesBuffer) > 9999 {
			cache.flushChoices()
		}
	}
}

func (cache *AIRL) flushEvictionChoices() {
	for _, choice := range cache.evictionAgentChoicesLogFileBuffer {
		cache.evictionAgentChoicesLogFile.Write(choice)
	}
	cache.evictionAgentChoicesLogFileBuffer = cache.evictionAgentChoicesLogFileBuffer[:0]
}
