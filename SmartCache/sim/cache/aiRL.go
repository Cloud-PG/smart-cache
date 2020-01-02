package cache

import (
	"container/list"
	"encoding/json"
	"fmt"
	"strconv"
	"strings"
	"time"

	"simulator/v2/cache/ai/featuremap"
	qlearn "simulator/v2/cache/qLearn"
)

// AIRL cache
type AIRL struct {
	LRUCache
	WeightedStats
	prevTime          time.Time
	curTime           time.Time
	stats             map[string]*WeightedFileStats
	Exp               float32
	aiFeatureMap      map[string]featuremap.Obj
	aiFeatureMapOrder []string
	qTable            *qlearn.QTable
	points            float64
}

// Init the AIRL struct
func (cache *AIRL) Init(args ...interface{}) interface{} {
	cache.LRUCache.Init()
	cache.WeightedStats.Init()

	featureMapFilePath := args[0].(string)

	cache.aiFeatureMap = featuremap.Parse(featureMapFilePath)

	for key := range cache.aiFeatureMap {
		cache.aiFeatureMapOrder = append(cache.aiFeatureMapOrder, key)
	}

	cache.qTable = &qlearn.QTable{}
	inputLenghts := []int{}
	for _, featureName := range cache.aiFeatureMapOrder {
		curFeature, _ := cache.aiFeatureMap[featureName]
		curLen := len(curFeature.Values)
		if curFeature.UnknownValues {
			curLen++
		}
		inputLenghts = append(inputLenghts, curLen)
	}
	fmt.Print("[Generate QTable]")
	cache.qTable.Init(inputLenghts)
	fmt.Println("[Done]")

	return nil
}

// Clear the AIRL struct
func (cache *AIRL) Clear() {
	cache.LRUCache.Clear()
	cache.LRUCache.Init()
	cache.WeightedStats.Init()
}

// Dumps the AIRL cache
func (cache *AIRL) Dumps() *[][]byte {
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
	for _, stats := range cache.stats {
		dumpInfo, _ := json.Marshal(DumpInfo{Type: "STATS"})
		dumpStats, _ := json.Marshal(stats)
		record, _ := json.Marshal(DumpRecord{
			Info: string(dumpInfo),
			Data: string(dumpStats),
		})
		record = append(record, newLine...)
		outData = append(outData, record)
	}
	// ----- qtable -----
	dumpInfo, _ := json.Marshal(DumpInfo{Type: "QTABLE"})
	dumpStats, _ := json.Marshal(cache.qTable)
	record, _ := json.Marshal(DumpRecord{
		Info: string(dumpInfo),
		Data: string(dumpStats),
	})
	record = append(record, newLine...)
	outData = append(outData, record)

	return &outData
}

// Loads the AIRL cache
func (cache *AIRL) Loads(inputString *[][]byte) {
	var curRecord DumpRecord
	var curRecordInfo DumpInfo

	for _, record := range *inputString {
		buffer := record[:len(record)-1]
		json.Unmarshal(buffer, &curRecord)
		json.Unmarshal([]byte(curRecord.Info), &curRecordInfo)
		switch curRecordInfo.Type {
		case "FILES":
			var curFile FileDump
			json.Unmarshal([]byte(curRecord.Data), &curFile)
			cache.files[curFile.Filename] = curFile.Size
			cache.size += curFile.Size
		case "STATS":
			json.Unmarshal([]byte(curRecord.Data), &cache.stats)
		case "QTABLE":
			json.Unmarshal([]byte(curRecord.Data), cache.qTable)
		}
	}
}

func (cache *AIRL) getCategory(catKey string, value interface{}) []bool {
	var res []bool
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
			res[curCategory.Values[value.(string)]] = true
		}
		return res

	}

	for curKey := range curCategory.GetKeys() {
		switch curCategory.Type {
		case featuremap.TypeInt:
			inputValue := int64(value.(float64))
			if inputValue <= curKey.ValueI {
				res[curCategory.Values[fmt.Sprintf("%d", curKey.ValueI)]] = true
				return res
			}
		case featuremap.TypeFloat:
			inputValue := value.(float64)
			if inputValue <= curKey.ValueF {
				res[curCategory.Values[fmt.Sprintf("%0.2f", curKey.ValueF)]] = true
				return res
			}
		case featuremap.TypeString:
			inputValue := value.(string)
			if inputValue <= curKey.ValueS {
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

func (cache *AIRL) composeFeatures(vars ...interface{}) []bool {
	var inputVector []bool
	var tmpArr []bool

	siteName := vars[0].(string)
	userID := strconv.Itoa(vars[1].(int))
	fileType := vars[2].(string)
	dataType := vars[3].(string)
	campain := vars[4].(string)
	process := vars[5].(string)
	totRequests := float64(vars[6].(uint32))
	avgTime := float64(vars[7].(float32))
	size := float64(vars[8].(float32))

	curInputs := []interface{}{
		siteName,
		userID,
		fileType,
		dataType,
		campain,
		process,
		totRequests,
		avgTime,
		size,
	}

	for idx, featureName := range cache.aiFeatureMapOrder {
		_, inFeatureMap := cache.aiFeatureMap[featureName]
		if inFeatureMap {
			tmpArr = cache.getCategory(featureName, curInputs[idx])
			inputVector = append(inputVector, tmpArr...)
			continue
		}
		inputVector = append(inputVector, curInputs[idx].(bool))

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

// UpdatePolicy of AIRL cache
func (cache *AIRL) UpdatePolicy(filename string, size float32, hit bool, vars ...interface{}) bool {
	var (
		added      = false
		curAction  qlearn.ActionType
		prevPoints float64
		curState   []bool
	)

	day := vars[0].(int64)
	currentTime := time.Unix(day, 0)

	cache.prevTime = cache.curTime
	cache.curTime = currentTime

	if !cache.curTime.Equal(cache.prevTime) {
		cache.points = cache.GetPoints()
	}

	curStats, _ := cache.GetOrCreate(filename, size, &currentTime)

	prevPoints = cache.points
	if !hit {
		curStats.updateStats(hit, size, nil)
		curStats.updateFilePoints(&cache.curTime)
	} else {
		cache.points -= curStats.Points
		curStats.updateStats(hit, size, nil)
		curStats.updateFilePoints(&cache.curTime)
		cache.points += curStats.Points
	}

	if !hit {
		siteName := vars[1].(string)
		userID := vars[2].(int)
		tmpSplit := strings.Split(filename, "/")
		dataType := tmpSplit[2]
		campain := tmpSplit[3]
		process := tmpSplit[4]
		fileType := tmpSplit[5]

		featureVector := cache.composeFeatures(
			siteName,
			userID,
			fileType,
			dataType,
			campain,
			process,
			curStats.TotRequests,
			curStats.RequestTicksMean,
			size,
		)

		curState = featureVector

		// QLearn - Check action
		expTradeoff := cache.qTable.GetRandomTradeOff()
		if expTradeoff > cache.qTable.Epsilon {
			// action
			curAction = cache.qTable.GetBestAction(curState)
		} else {
			// random choice
			if expTradeoff > 0.5 {
				curAction = qlearn.ActionStore
			} else {
				curAction = qlearn.ActionNotStore
			}
		}

		// QLearn - Take the action NOT STORE
		if curAction == qlearn.ActionNotStore {
			reward := -curStats.Points
			// Update table
			cache.qTable.Update(curState, curAction, reward)
			// Update epsilon
			cache.qTable.UpdateEpsilon()
			return added
		}

		// Insert with LRU mechanism
		if cache.Size()+size > cache.MaxSize {
			var totalDeleted float32
			tmpVal := cache.queue.Front()
			for {
				if tmpVal == nil {
					break
				}
				curFilename2Delete := tmpVal.Value.(string)
				cache.points -= cache.getPoints(curFilename2Delete) // Q-Learning
				fileSize := cache.files[curFilename2Delete]
				cache.size -= fileSize
				cache.dataDeleted += size

				totalDeleted += fileSize
				delete(cache.files, curFilename2Delete)

				tmpVal = tmpVal.Next()
				// Check if all files are deleted
				if tmpVal == nil {
					break
				}
				cache.queue.Remove(tmpVal.Prev())

				if totalDeleted >= size {
					break
				}
			}
		}
		if cache.Size()+size <= cache.MaxSize {
			cache.files[filename] = size
			cache.queue.PushBack(filename)
			cache.size += size
			curStats.addInCache(&currentTime)
			curStats.updateFilePoints(&cache.curTime)
			cache.points += curStats.Points // Q-Learning
			added = true
		}

		// QLearn - Take the action STORE
		if cache.qTable != nil && curAction == qlearn.ActionStore {
			newScore := cache.points
			reward := newScore - prevPoints
			// Update table
			cache.qTable.Update(curState, curAction, reward)
			// Update epsilon
			cache.qTable.UpdateEpsilon()
		}

	} else {
		var elm2move *list.Element
		for tmpVal := cache.queue.Front(); tmpVal != nil; tmpVal = tmpVal.Next() {
			if tmpVal.Value.(string) == filename {
				elm2move = tmpVal
				break
			}
		}
		if elm2move != nil {
			cache.queue.MoveToBack(elm2move)
		}
	}

	return added
}

// ExtraStats for output
func (cache *AIRL) ExtraStats() string {
	return fmt.Sprintf("Cov: %0.2f%%, Epsilon: %0.2f", cache.qTable.GetCoveragePercentage(), cache.qTable.Epsilon)
}
