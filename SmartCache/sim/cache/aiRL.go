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
	evictionCategoryManager     CategoryManager
	actionCounters              map[qlearn.ActionType]int
	bufferIdxVector             []int
	numPrevDayReq               int64
	numServedReq                int64
}

// Init the AIRL struct
func (cache *AIRL) Init(args ...interface{}) interface{} {
	logger = zap.L()

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
		cache.evictionCategoryManager = CategoryManager{}
		cache.evictionCategoryManager.Init(
			cache.evictionFeatureManager.Features,
			cache.evictionFeatureManager.FeatureIdxWeights,
			cache.evictionFeatureManager.FileFeatures,
			cache.evictionFeatureManager.FileFeatureIdxWeights,
			cache.evictionFeatureManager.FileFeatureIdxMap,
		)
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

// CatState is a struct to manage the state of eviction agent starting from categories
type CatState struct {
	Idx      int
	Category int
	Files    []*FileSupportData
	Action   qlearn.ActionType
}

// DelCatFile stores the files to be deleted in eviction call
type DelCatFile struct {
	Category int
	File     *FileSupportData
}

// CategoryManager helps the category management in the eviction agent
type CategoryManager struct {
	buffer                 []int
	featureIdxWeights      []int
	fileFeatureIdxWeights  []int
	fileFeatures           []featuremap.Obj
	features               []featuremap.Obj
	fileFeatureIdxMap      map[string]int
	categoryFileListMap    map[int][]*FileSupportData
	categoryFileFeatureIdx map[int][]int
	fileSupportDataSizeMap map[*FileSupportData]float64
	filesCategoryMap       map[int64]int
	categorySizesMap       map[int]float64
	categoryStatesMap      map[int]int
	generatorChan          chan CatState
}

// Init initialize the Category Manager
func (catMan *CategoryManager) Init(features []featuremap.Obj, featureWeights []int, fileFeatures []featuremap.Obj, fileFeatureWeights []int, fileFeatureIdxMap map[string]int) {
	catMan.buffer = make([]int, 0)
	catMan.featureIdxWeights = featureWeights
	catMan.fileFeatureIdxWeights = fileFeatureWeights
	catMan.fileFeatures = fileFeatures
	catMan.features = features

	catMan.categoryFileListMap = make(map[int][]*FileSupportData)
	catMan.categoryFileFeatureIdx = make(map[int][]int)
	catMan.fileSupportDataSizeMap = make(map[*FileSupportData]float64)
	catMan.filesCategoryMap = make(map[int64]int)
	catMan.categorySizesMap = make(map[int]float64)
	catMan.fileFeatureIdxMap = make(map[string]int)
	catMan.categoryStatesMap = make(map[int]int)

	catMan.fileFeatureIdxMap = fileFeatureIdxMap
}

func (catMan *CategoryManager) deleteFileFromCategory(category int, file2Remove *FileSupportData) {
	// fmt.Println("DELETE FILE FROM CATEGORY [", category, "]-> ", file2Remove.Filename)
	delete(catMan.filesCategoryMap, file2Remove.Filename)
	catMan.categorySizesMap[category] -= catMan.fileSupportDataSizeMap[file2Remove]
	categoryFiles := catMan.categoryFileListMap[category]
	deleteIdx := -1
	for idx, file := range categoryFiles {
		if file.Filename == file2Remove.Filename {
			deleteIdx = idx
			break
		}
	}
	if deleteIdx == -1 {
		panic("ERROR: Cannot delete file from category...")
	}
	copy(categoryFiles[deleteIdx:], categoryFiles[deleteIdx+1:])
	categoryFiles = categoryFiles[:len(categoryFiles)-1]
	if len(categoryFiles) > 0 {
		catMan.categoryFileListMap[category] = categoryFiles
	} else {
		delete(catMan.categoryFileListMap, category)
		delete(catMan.categoryFileFeatureIdx, category)
		delete(catMan.categorySizesMap, category)
	}
}

func (catMan *CategoryManager) insertFileInCategory(category int, file *FileSupportData) {
	// fmt.Println("INSERT FILE IN CATEGORY [", category, "]-> ", file.Filename)
	_, inMemory := catMan.categoryFileListMap[category]
	if !inMemory {
		catMan.categoryFileListMap[category] = make([]*FileSupportData, 0)
	}
	catMan.categoryFileListMap[category] = append(catMan.categoryFileListMap[category], file)
	catMan.fileSupportDataSizeMap[file] = file.Size
	catMan.filesCategoryMap[file.Filename] = category
	catMan.categorySizesMap[category] += file.Size
}

// AddOrUpdateCategoryFile inserts or update a file associated to its category
func (catMan *CategoryManager) AddOrUpdateCategoryFile(category int, file *FileSupportData) {
	// fmt.Println("ADD OR UPDATE FILE CATEGORY [", category, "]-> ", file.Filename)
	oldFileCategory, inMemory := catMan.filesCategoryMap[file.Filename]
	if inMemory {
		if oldFileCategory != category {
			// Delete from category
			catMan.deleteFileFromCategory(oldFileCategory, file)
			// Add to category
			catMan.insertFileInCategory(category, file)
		}
	} else {
		// Add to category
		catMan.insertFileInCategory(category, file)
	}
}

// GetFileCategory returns the category of a specific file
func (catMan CategoryManager) GetFileCategory(file *FileSupportData) int {
	catMan.buffer = catMan.buffer[:0]
	for _, feature := range catMan.features {
		// fmt.Println(feature.Name)
		switch feature.Name {
		case "catSize":
			catMan.buffer = append(catMan.buffer, feature.Index(file.Size))
		case "catNumReq":
			catMan.buffer = append(catMan.buffer, feature.Index(file.Frequency))
		case "catDeltaLastRequest":
			catMan.buffer = append(catMan.buffer, feature.Index(file.Recency))
		}
	}
	curCatIdx := 0
	for idx, value := range catMan.buffer {
		curCatIdx += value * catMan.fileFeatureIdxWeights[idx]
	}

	_, present := catMan.categoryFileListMap[curCatIdx]
	if !present {
		catMan.categoryFileListMap[curCatIdx] = make([]*FileSupportData, 0)
		catMan.categoryFileFeatureIdx[curCatIdx] = make([]int, len(catMan.buffer))
		catMan.categorySizesMap[curCatIdx] = 0.0
		copy(catMan.categoryFileFeatureIdx[curCatIdx], catMan.buffer)
	}

	return curCatIdx
}

// GetStateFromCategories generates all the states from the current categories
func (catMan CategoryManager) GetStateFromCategories(agent qlearn.Agent, occupancy float64, hitRate float64, maxSize float64) chan CatState {
	catMan.generatorChan = make(chan CatState, len(catMan.categoryFileListMap))
	go func() {
		defer close(catMan.generatorChan)
		for catID := range catMan.categoryFileListMap {
			curCat := catMan.categoryFileFeatureIdx[catID]
			catMan.buffer = catMan.buffer[:0]
			for _, feature := range catMan.features {
				switch feature.Name {
				case "catSize":
					catMan.buffer = append(catMan.buffer, curCat[catMan.fileFeatureIdxMap["catSize"]])
				case "catNumReq":
					catMan.buffer = append(catMan.buffer, curCat[catMan.fileFeatureIdxMap["catNumReq"]])
				case "catDeltaLastRequest":
					catMan.buffer = append(catMan.buffer, curCat[catMan.fileFeatureIdxMap["catDeltaLastRequest"]])
				case "catPercOcc":
					percSize := (catMan.categorySizesMap[catID] / maxSize) * 100.
					catMan.buffer = append(catMan.buffer, feature.Index(percSize))
				case "percOcc":
					catMan.buffer = append(catMan.buffer, feature.Index(occupancy))
				case "hitRate":
					catMan.buffer = append(catMan.buffer, feature.Index(hitRate))
				}
			}
			// fmt.Println(catMan.buffer)
			curState := agent.QTable.FeatureIdxs2StateIdx(catMan.buffer...)
			var curAction qlearn.ActionType
			if expEvictionTradeoff := agent.GetRandomFloat(); expEvictionTradeoff > agent.Epsilon {
				// ########################
				// ### Exploiting phase ###
				// ########################
				curAction = agent.GetBestAction(curState)
			} else {
				// ##########################
				// #### Exploring phase #####
				// ##########################

				// ----- Random choice -----
				randomActionIdx := int(agent.GetRandomFloat() * float64(len(agent.QTable.ActionTypes)))
				curAction = agent.QTable.ActionTypes[randomActionIdx]
			}
			catMan.generatorChan <- CatState{
				Idx:      curState,
				Category: catID,
				Files:    catMan.categoryFileListMap[catID],
				Action:   curAction,
			}
		}
	}()
	return catMan.generatorChan
}

func (cache *AIRL) callEvictionAgent(forced bool) (float64, []int64) {
	var (
		totalDeleted float64
		deletedFiles = make([]int64, 0)
	)

	cache.evictionAgentNumCalls++

	// fmt.Println("----- EVICTION -----")

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
				for catState := range cache.evictionCategoryManager.GetStateFromCategories(
					cache.evictionAgent,
					cache.Occupancy(),
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
			delete(cache.evictionAgent.EventMemory, "NotDelete")
		}
	} else {
		cache.evictionAgentK = cache.evictionAgentK << 1
	}

	// fmt.Println(cache.curCacheStates)

	files2delete := make([]DelCatFile, 0)

	for catState := range cache.evictionCategoryManager.GetStateFromCategories(
		cache.evictionAgent,
		cache.Occupancy(),
		cache.HitRate(),
		cache.MaxSize,
	) {
		switch catState.Action {
		case qlearn.ActionDeleteAll:
			curFileList := catState.Files
			for idx := len(curFileList) - 1; idx > 0; idx-- {
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
				cache.evictionAgent.UpdateFileMemory(curFile.Filename, qlearn.Choice{
					State:     catState.Idx,
					Action:    catState.Action,
					Tick:      cache.tick,
					ReadOnHit: cache.dataReadOnHit,
					Occupancy: cache.Occupancy(),
					Frequency: curFile.Frequency,
				})
			}
		case qlearn.ActionDeleteHalf, qlearn.ActionDeleteQuarter:
			curFileList := catState.Files
			rand.Shuffle(len(curFileList), func(i, j int) {
				curFileList[i], curFileList[j] = curFileList[j], curFileList[i]
			})
			numDeletes := 0
			if len(curFileList) == 1 {
				numDeletes = 1
			} else if catState.Action == qlearn.ActionDeleteHalf {
				numDeletes = len(curFileList) / 2
			} else {
				numDeletes = len(curFileList) / 4
			}
			for idx := len(curFileList) - 1; idx > 0; idx-- {
				if numDeletes == 0 {
					break
				}
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
				cache.evictionAgent.UpdateFileMemory(curFile.Filename, qlearn.Choice{
					State:     catState.Idx,
					Action:    catState.Action,
					Tick:      cache.tick,
					ReadOnHit: cache.dataReadOnHit,
					Occupancy: cache.Occupancy(),
					Frequency: curFile.Frequency,
				})
				numDeletes--
			}
		case qlearn.ActionNotDelete:
			for _, curFile := range catState.Files {
				cache.evictionAgent.UpdateFileMemory(curFile.Filename, qlearn.Choice{
					State:     catState.Idx,
					Action:    catState.Action,
					Tick:      cache.tick,
					ReadOnHit: cache.dataReadOnHit,
					Occupancy: cache.Occupancy(),
					Frequency: curFile.Frequency,
				})
				cache.evictionAgent.UpdateEventMemory("NotDelete", qlearn.Choice{
					State:     catState.Idx,
					Action:    catState.Action,
					Tick:      cache.tick,
					ReadOnHit: cache.dataReadOnHit,
					Occupancy: cache.Occupancy(),
					Frequency: curFile.Frequency,
				})
			}
		}
	}

	for _, file2Delete := range files2delete {
		cache.evictionCategoryManager.deleteFileFromCategory(file2Delete.Category, file2Delete.File)
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
		for catState := range cache.evictionCategoryManager.GetStateFromCategories(
			cache.evictionAgent,
			cache.Occupancy(),
			cache.HitRate(),
			cache.MaxSize,
		) {
			if cache.checkEvictionNextState(prevChoice.State, catState.Idx) {
				// Update table
				cache.evictionAgent.UpdateTable(prevChoice.State, catState.Idx, prevChoice.Action, reward)
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
	for catState := range cache.evictionCategoryManager.GetStateFromCategories(
		cache.evictionAgent,
		cache.Occupancy(),
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
func (cache *AIRL) BeforeRequest(request *Request, hit bool) (*FileStats, bool) {
	// fmt.Println("+++ REQUESTED FILE -> ", request.Filename)
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
				curFileSupportData := FileSupportData{
					Filename:  request.Filename,
					Size:      request.Size,
					Frequency: fileStats.Frequency,
					Recency:   fileStats.Recency,
					Weight:    fileStats.Weight,
				}
				cache.files.Insert(&curFileSupportData)

				if cache.evictionAgentOK {
					fileCategory := cache.evictionCategoryManager.GetFileCategory(&curFileSupportData)
					cache.evictionCategoryManager.AddOrUpdateCategoryFile(fileCategory, &curFileSupportData)
				}

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
			curFileSupportData := FileSupportData{
				Filename:  request.Filename,
				Size:      request.Size,
				Frequency: fileStats.Frequency,
				Recency:   fileStats.Recency,
				Weight:    fileStats.Weight,
			}
			cache.files.Update(&curFileSupportData)
			if cache.evictionAgentOK {
				fileCategory := cache.evictionCategoryManager.GetFileCategory(&curFileSupportData)
				cache.evictionCategoryManager.AddOrUpdateCategoryFile(fileCategory, &curFileSupportData)
			}
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
				curFileSupportData := FileSupportData{
					Filename:  request.Filename,
					Size:      request.Size,
					Frequency: fileStats.Frequency,
					Recency:   fileStats.Recency,
					Weight:    fileStats.Weight,
				}
				cache.files.Insert(&curFileSupportData)

				if cache.evictionAgentOK {
					fileCategory := cache.evictionCategoryManager.GetFileCategory(&curFileSupportData)
					cache.evictionCategoryManager.AddOrUpdateCategoryFile(fileCategory, &curFileSupportData)
				}

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
			curFileSupportData := FileSupportData{
				Filename:  request.Filename,
				Size:      request.Size,
				Frequency: fileStats.Frequency,
				Recency:   fileStats.Recency,
				Weight:    fileStats.Weight,
			}
			cache.files.Update(&curFileSupportData)
			if cache.evictionAgentOK {
				fileCategory := cache.evictionCategoryManager.GetFileCategory(&curFileSupportData)
				cache.evictionCategoryManager.AddOrUpdateCategoryFile(fileCategory, &curFileSupportData)
			}
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
