package cache

import (
	"simulator/v2/cache/ai/featuremap"
	"simulator/v2/cache/ai/qlearn"
)

// CatState is a struct to manage the state of eviction agent starting from categories
type CatState struct {
	Idx      int
	Category int
	Files    []*FileStats
	Action   qlearn.ActionType
}

// DelCatFile stores the files to be deleted in eviction call
type DelCatFile struct {
	Category int
	File     *FileStats
}

// CategoryManager helps the category management in the eviction agent
type CategoryManager struct {
	buffer                 []int
	featureIdxWeights      []int
	fileFeatureIdxWeights  []int
	fileFeatures           []featuremap.Obj
	features               []featuremap.Obj
	fileFeatureIdxMap      map[string]int
	categoryFileListMap    map[int][]*FileStats
	categoryFileFeatureIdx map[int][]int
	fileSupportDataSizeMap map[*FileStats]float64
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

	catMan.categoryFileListMap = make(map[int][]*FileStats)
	catMan.categoryFileFeatureIdx = make(map[int][]int)
	catMan.fileSupportDataSizeMap = make(map[*FileStats]float64)
	catMan.filesCategoryMap = make(map[int64]int)
	catMan.categorySizesMap = make(map[int]float64)
	catMan.fileFeatureIdxMap = make(map[string]int)
	catMan.categoryStatesMap = make(map[int]int)
	catMan.lastStateAction = make(map[int]CatState)

	catMan.fileFeatureIdxMap = fileFeatureIdxMap
}

// GetNumCategories returns the current number of categories in the cache
func (catMan CategoryManager) GetNumCategories() int {
	return len(catMan.categorySizesMap)
}

func (catMan *CategoryManager) deleteFileFromCategory(category int, file2Remove *FileStats) {
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

func (catMan *CategoryManager) insertFileInCategory(category int, file *FileStats) {
	// fmt.Println("[CATMANAGER] INSERT FILE IN CATEGORY [", category, "]-> ", file.Filename)
	_, inMemory := catMan.categoryFileListMap[category]
	if !inMemory {
		catMan.categoryFileListMap[category] = make([]*FileStats, 0)
	}

	catMan.categoryFileListMap[category] = append(catMan.categoryFileListMap[category], file)
	catMan.fileSupportDataSizeMap[file] = file.Size
	catMan.filesCategoryMap[file.Filename] = category
	catMan.categorySizesMap[category] += file.Size
}

// AddOrUpdateCategoryFile inserts or update a file associated to its category
func (catMan *CategoryManager) AddOrUpdateCategoryFile(category int, file *FileStats) {
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
func (catMan CategoryManager) GetFileCategory(file *FileStats) int {
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
		catMan.categoryFileListMap[curCatIdx] = make([]*FileStats, 0)
		catMan.categoryFileFeatureIdx[curCatIdx] = make([]int, len(catMan.buffer))
		catMan.categorySizesMap[curCatIdx] = 0.0
		copy(catMan.categoryFileFeatureIdx[curCatIdx], catMan.buffer)
	}

	return curCatIdx
}

// GetStateFromCategories generates all the states from the current categories
func (catMan *CategoryManager) GetStateFromCategories(newStates bool, agent qlearn.Agent, capacity float64, hitRate float64, maxSize float64) chan CatState {
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
						catMan.buffer = append(catMan.buffer, feature.Index(capacity))
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
