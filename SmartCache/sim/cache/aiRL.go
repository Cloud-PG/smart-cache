package cache

import (
	"compress/gzip"
	"encoding/binary"
	"encoding/json"
	"fmt"
	"math/rand"
	"os"
	"simulator/v2/cache/ai/featuremap"
	"simulator/v2/cache/ai/qlearn"
	"strings"

	"go.uber.org/zap"
)

type aiRLType int

const (
	maxBadQValueInRow = 8
	// SCDL type
	SCDL aiRLType = iota - 2
	// SCDL2 type
	SCDL2
)

// AIRL cache
type AIRL struct {
	SimpleCache
	rlType                            aiRLType
	additionAgentOK                   bool
	evictionAgentOK                   bool
	additionAgentBadQValue            int
	evictionAgentBadQValue            int
	additionAgentPrevQValue           float64
	evictionAgentPrevQValue           float64
	additionFeatureManager            featuremap.FeatureManager
	evictionFeatureManager            featuremap.FeatureManager
	additionAgent                     qlearn.Agent
	evictionAgent                     qlearn.Agent
	additionAgentChoicesLogFile       *OutputCSV
	evictionAgentChoicesLogFile       *OutputCSV
	additionAgentChoicesLogFileBuffer [][]string
	evictionAgentChoicesLogFileBuffer [][]string
	evictionCheckNextStateMap         map[[8]byte]bool
	evictionAgentStep                 int64
	evictionAgentK                    int64
	evictionUseK                      bool
	evictionAgentNumCalls             int64
	evictionAgentNumForcedCalls       int64
	evictionRO                        float64
	evictionCategoryManager           CategoryManager
	actionCounters                    map[qlearn.ActionType]int
	bufferIdxVector                   []int
}

// Init the AIRL struct
func (cache *AIRL) Init(args ...interface{}) interface{} {
	logger = zap.L()

	useK := args[5].(bool)
	evictionk := args[6].(int64)
	rlType := args[7].(string)
	additionFeatureMap := args[8].(string)
	evictionFeatureMap := args[9].(string)
	initEpsilon := args[10].(float64)
	decayRateEpsilon := args[11].(float64)

	cache.actionCounters = make(map[qlearn.ActionType]int)

	logger.Info("Feature maps", zap.String("addition map", additionFeatureMap), zap.String("eviction map", evictionFeatureMap))

	switch strings.ToLower(rlType) {
	case "scdl":
		cache.rlType = SCDL

		cache.SimpleCache.Init(LRUQueue,
			args[0], // log
			args[1], // redirect
			args[2], // watermarks
			args[3], // watermarks
			args[4], // watermarks
		)

		if additionFeatureMap == "" {
			panic("ERROR: SCDL needs the addition feature map...")
		}
	case "scdl2":
		cache.rlType = SCDL2

		cache.SimpleCache.Init(NoQueue,
			args[0], // log
			args[1], // redirect
			args[2], // watermarks
			args[3], // watermarks
			args[4], // watermarks
		)

		cache.evictionUseK = useK
		cache.evictionAgentK = evictionk
		cache.evictionAgentStep = cache.evictionAgentK
		cache.evictionRO = 0.42

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
				cache.evictionAgentChoicesLogFile.Write(choicesLogHeader)
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
		} else {
			cache.evictionAgentOK = false
		}
	default:
		panic("ERROR: RL type is not valid...")
	}

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
		if cache.logSimulation {
			cache.additionAgentChoicesLogFile = &OutputCSV{}
			cache.additionAgentChoicesLogFile.Create("additionAgentChoiceLog.csv", true)
			cache.additionAgentChoicesLogFile.Write(choicesLogHeader)
			cache.additionAgentChoicesLogFileBuffer = make([][]string, 0)
		}
		cache.additionAgentOK = true
		// Addition agent action counters
		cache.actionCounters[qlearn.ActionStore] = 0
		cache.actionCounters[qlearn.ActionNotStore] = 0
	} else {
		cache.additionAgentOK = false
	}

	logger.Info("Table creation done")

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
	if cache.evictionAgentOK && !cache.evictionUseK {
		cache.callEvictionAgent(false)
	}
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
	lastStateAction        map[int]CatState
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
	catMan.lastStateAction = make(map[int]CatState)

	catMan.fileFeatureIdxMap = fileFeatureIdxMap
}

func (catMan *CategoryManager) deleteFileFromCategory(category int, file2Remove *FileSupportData) {
	// fmt.Println("[CATMANAGER] DELETE FILE FROM CATEGORY [", category, "]-> ", file2Remove.Filename)
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
	// fmt.Println("[CATMANAGER] INSERT FILE IN CATEGORY [", category, "]-> ", file.Filename)
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
	// fmt.Println("[CATMANAGER] ADD OR UPDATE FILE CATEGORY [", category, "]-> ", file.Filename)
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
func (catMan *CategoryManager) GetStateFromCategories(newStates bool, agent qlearn.Agent, occupancy float64, hitRate float64, maxSize float64) chan CatState {
	catMan.generatorChan = make(chan CatState, len(catMan.categoryFileListMap))
	go func() {
		defer close(catMan.generatorChan)
		if newStates {
			catMan.lastStateAction = make(map[int]CatState)
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
				curCatState := CatState{
					Idx:      curState,
					Category: catID,
					Files:    catMan.categoryFileListMap[catID],
					Action:   curAction,
				}
				catMan.generatorChan <- curCatState
				catMan.lastStateAction[curState] = curCatState
			}
		} else {
			for _, curCatState := range catMan.lastStateAction {
				catMan.generatorChan <- curCatState
			}
		}
	}()
	return catMan.generatorChan
}

func (cache *AIRL) callEvictionAgent(forced bool) (float64, []int64) {
	var (
		totalDeleted float64
		deletedFiles = make([]int64, 0)
		files2delete = make([]DelCatFile, 0)
	)

	cache.evictionAgentNumCalls++

	// fmt.Println("----- EVICTION ----- Forced[", forced, "]")

	// Forced event rewards
	if forced {
		cache.evictionAgentNumForcedCalls++
		choicesList, inMemory := cache.evictionAgent.Memory["NotDelete"]
		if inMemory {
			for _, choice := range choicesList {
				for catState := range cache.evictionCategoryManager.GetStateFromCategories(
					false,
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
			delete(cache.evictionAgent.Memory, "NotDelete")
		}
	}

	for catState := range cache.evictionCategoryManager.GetStateFromCategories(
		true,
		cache.evictionAgent,
		cache.Occupancy(),
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
				cache.evictionAgent.SaveMemory(curFile.Filename, qlearn.Choice{
					State:     catState.Idx,
					Action:    catState.Action,
					Tick:      cache.tick,
					DeltaT:    curFileStats.DeltaLastRequest,
					Occupancy: cache.Occupancy(),
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
					"Delete",
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
			if len(curFileList) == 1 {
				numDeletes = 1
			} else if catState.Action == qlearn.ActionDeleteHalf {
				numDeletes = len(curFileList) / 2
			} else {
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
				cache.evictionAgent.SaveMemory(curFile.Filename, qlearn.Choice{
					State:     catState.Idx,
					Action:    catState.Action,
					Tick:      cache.tick,
					DeltaT:    curFileStats.DeltaLastRequest,
					Occupancy: cache.Occupancy(),
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
					"Delete",
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
			cache.evictionAgent.SaveMemory(curFile.Filename, qlearn.Choice{
				State:     catState.Idx,
				Action:    catState.Action,
				Tick:      cache.tick,
				DeltaT:    curFileStats.DeltaLastRequest,
				Occupancy: cache.Occupancy(),
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
				"Delete",
			})
		case qlearn.ActionNotDelete:
			for _, curFile := range catState.Files {
				curFileStats := cache.stats.Get(curFile.Filename)
				cache.evictionAgent.SaveMemory(curFile.Filename, qlearn.Choice{
					State:     catState.Idx,
					Action:    catState.Action,
					Tick:      cache.tick,
					DeltaT:    curFileStats.DeltaLastRequest,
					Occupancy: cache.Occupancy(),
					Size:      curFile.Size,
					Frequency: curFileStats.Frequency,
				})
				cache.evictionAgent.SaveMemory("NotDelete", qlearn.Choice{
					State:     catState.Idx,
					Action:    catState.Action,
					Tick:      cache.tick,
					DeltaT:    curFileStats.DeltaLastRequest,
					Occupancy: cache.Occupancy(),
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
	cache.files.Remove(deletedFiles, false)

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
		isNext = newState[catSizeIdx] == oldState[catSizeIdx] && newState[catDeltaLastRequestIdx] == oldState[catDeltaLastRequestIdx] && newState[catNumReqIdx] >= oldState[catNumReqIdx]
	}

	cache.evictionCheckNextStateMap[curArgs] = isNext
	return isNext
}

func (cache *AIRL) delayedRewardEvictionAgent(filename int64, hit bool) {
	memories, inMemory := cache.evictionAgent.Memory[filename]

	if inMemory {
		for idx := 0; idx < len(memories)-1; idx++ {
			var (
				prevMemory, nextMemory qlearn.Choice
			)
			prevMemory = memories[idx]

			if idx == len(memories)-1 {
				for catState := range cache.evictionCategoryManager.GetStateFromCategories(
					false,
					cache.evictionAgent,
					cache.Occupancy(),
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
				}
				if prevMemory.Occupancy >= nextMemory.Occupancy && prevMemory.Action == qlearn.ActionNotDelete {
					reward += 1.
				}
			} else { // MISS
				reward += -1.
				if prevMemory.Occupancy > nextMemory.Occupancy && prevMemory.Action != qlearn.ActionNotDelete {
					reward += -1.
				}
			}

			// Update table
			cache.evictionAgent.UpdateTable(prevMemory.State, nextMemory.State, prevMemory.Action, reward)
			// Update epsilon
			cache.evictionAgent.UpdateEpsilon()
		}
	}
}

func (cache *AIRL) delayedRewardAdditionAgent(filename int64, hit bool) {
	switch cache.rlType {
	case SCDL:
		lastMemories := cache.additionAgent.Remember(SCDL)
		for _, memory := range lastMemories {
			reward := 0.0
			if !memory.Hit { // MISS
				if memory.Action == qlearn.ActionNotStore {
					if cache.dataReadOnMiss/cache.bandwidth < 0.5 || cache.dataWritten/cache.dataRead < 0.1 {
						reward -= memory.Size / 1024.
					}
					if reward == 0. {
						reward += memory.Size / 1024.
					}
				} else if memory.Action == qlearn.ActionStore {
					if cache.dataReadOnMiss/cache.bandwidth > 0.75 || cache.dataWritten/cache.dataRead > 0.5 {
						reward -= memory.Size / 1024.
					}
					if reward == 0. {
						reward += memory.Size / 1024.
					}
				}
				if cache.dataReadOnMiss/cache.dataRead > 0.5 {
					reward -= memory.Size / 1024.
				}
				if reward == 0. {
					reward += memory.Size / 1024.
				}
			} else { // HIT
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
			cache.additionAgent.UpdateTable(memory.State, memory.State, memory.Action, reward)
			// Update epsilon
			cache.additionAgent.UpdateEpsilon()
		}
	case SCDL2:
		memories, inMemory := cache.additionAgent.Memory[filename]
		if inMemory {
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
	//fmt.Println("+++ REQUESTED FILE +++-> ", request.Filename)
	if cache.evictionAgentOK && cache.evictionUseK {
		// fmt.Println(cache.evictionAgentStep)
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

		if cache.additionAgent.Epsilon <= .2 {
			if cache.additionAgentPrevQValue == 0. {
				cache.additionAgentPrevQValue = cache.additionAgent.QValue
			} else {
				if cache.additionAgent.QValue <= cache.additionAgentPrevQValue {
					cache.additionAgentBadQValue++
				} else {
					cache.additionAgentBadQValue = 0
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
				if cache.evictionAgent.QValue <= cache.evictionAgentPrevQValue {
					cache.evictionAgentBadQValue++
				} else {
					cache.evictionAgentBadQValue = 0
				}
				if cache.evictionAgentBadQValue < 0 {
					cache.evictionAgentBadQValue = 0
				}
				cache.evictionAgentPrevQValue = cache.evictionAgent.QValue
			}
		}

		// fmt.Println(cache.additionAgentBadQValue, cache.evictionAgentBadQValue)

		if cache.additionAgentBadQValue >= maxBadQValueInRow {
			if cache.additionAgentOK {
				cache.additionAgentBadQValue = 0
				cache.additionAgent.UnleashEpsilon(nil)
			}
			if cache.evictionAgentOK {
				cache.evictionAgentBadQValue = 0
				cache.evictionAgent.UnleashEpsilon(0.25)
			}
		}
		if cache.evictionAgentBadQValue >= maxBadQValueInRow {
			if cache.additionAgentOK {
				cache.additionAgentBadQValue = 0
				cache.additionAgent.UnleashEpsilon(0.25)
			}
			if cache.evictionAgentOK {
				cache.evictionAgentBadQValue = 0
				cache.evictionAgent.UnleashEpsilon(nil)
			}
		}
	}

	cache.numReq++

	fileStats.updateStats(hit, request.Size, request.UserID, request.SiteName, request.DayTime)

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

		logger.Debug("ADDITION AGENT OK")

		curState = cache.getState4AdditionAgent(hit, fileStats)

		logger.Debug("cache", zap.Int("current state", curState))

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
					Occupancy: cache.Occupancy(),
					Size:      request.Size,
					Frequency: fileStats.Frequency,
				}
				switch cache.rlType {
				case SCDL:
					cache.additionAgent.SaveMemory(SCDL, curChoice)
				case SCDL2:
					cache.additionAgent.SaveMemory(request.Filename, curChoice)
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
				} else {
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

					cache.actionCounters[curAction]++
					curChoice := qlearn.Choice{
						State:     curState,
						Action:    curAction,
						Tick:      cache.tick,
						DeltaT:    fileStats.DeltaLastRequest,
						Hit:       hit,
						Occupancy: cache.Occupancy(),
						Size:      request.Size,
						Frequency: fileStats.Frequency,
					}
					switch cache.rlType {
					case SCDL:
						cache.additionAgent.SaveMemory(SCDL, curChoice)
					case SCDL2:
						cache.additionAgent.SaveMemory(request.Filename, curChoice)
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
						"Add",
					})
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
			curChoice := qlearn.Choice{
				State:     curState,
				Action:    qlearn.ActionNONE,
				Tick:      cache.tick,
				DeltaT:    fileStats.DeltaLastRequest,
				Hit:       hit,
				Occupancy: cache.Occupancy(),
				Size:      request.Size,
				Frequency: fileStats.Frequency,
			}
			switch cache.rlType {
			case SCDL:
				cache.additionAgent.SaveMemory(SCDL, curChoice)
			case SCDL2:
				cache.additionAgent.SaveMemory(request.Filename, curChoice)
			}

			if cache.evictionAgentOK {
				fileCategory := cache.evictionCategoryManager.GetFileCategory(&curFileSupportData)
				cache.evictionCategoryManager.AddOrUpdateCategoryFile(fileCategory, &curFileSupportData)
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

			logger.Debug("NO ADDITION AGENT - Normal miss branch")

			forced := false

			// Insert with LRU mechanism
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
			} else {
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

				cache.toChoiceBuffer([]string{
					fmt.Sprintf("%d", cache.tick),
					fmt.Sprintf("%d", fileStats.Filename),
					fmt.Sprintf("%0.2f", fileStats.Size),
					fmt.Sprintf("%d", fileStats.Frequency),
					fmt.Sprintf("%d", fileStats.DeltaLastRequest),
					"Add",
				})
			}
		} else {
			// #######################
			// ##### HIT branch  #####
			// #######################
			logger.Debug("NO ADDITION AGENT - Normal hit branch")
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

// AfterRequest of the cache
func (cache *AIRL) AfterRequest(request *Request, fileStats *FileStats, hit bool, added bool) {
	if cache.rlType == SCDL2 {
		if cache.additionAgentOK {
			cache.delayedRewardAdditionAgent(request.Filename, hit)
		}
		if cache.evictionAgentOK {
			cache.delayedRewardEvictionAgent(request.Filename, hit)
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
	switch cache.rlType {
	case SCDL:
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

// ExtraOutput for output specific information
func (cache *AIRL) ExtraOutput(info string) string {
	result := ""
	switch info {
	case "additionQTable":
		if cache.additionAgentOK {
			result = cache.additionAgent.QTableToString()
			writeQTable("additionQTable.csv", result)
		} else {
			logger.Info("No Addition Table...")
			result = ""
		}
	case "evictionQTable":
		if cache.evictionAgentOK {
			result = cache.evictionAgent.QTableToString()
			writeQTable("evictionQTable.csv", result)
		} else {
			logger.Info("No Eviction Table...")
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
