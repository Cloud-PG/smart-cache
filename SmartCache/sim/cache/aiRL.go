package cache

import (
	"encoding/json"
	"fmt"
	"log"
	"os"
	"simulator/v2/cache/ai/featuremap"
	qlearn "simulator/v2/cache/qLearn"
	"sort"
	"strings"
	"time"

	"github.com/fatih/color"
)

var (
	logger = log.New(os.Stderr, color.GreenString("[AIRL] "), log.Lshortfile|log.LstdFlags)
)

// AIRL cache
type AIRL struct {
	LRUCache
	prevTime          time.Time
	curTime           time.Time
	Exp               float32
	aiFeatureMap      map[string]featuremap.Obj
	aiFeatureMapOrder []string
	qTable            *qlearn.QTable
	qPrevState        map[string]string
	qPrevAction       map[string]qlearn.ActionType
	points            float64
	prevPoints        float64
}

// Init the AIRL struct
func (cache *AIRL) Init(args ...interface{}) interface{} {
	cache.LRUCache.Init()

	featureMapFilePath := args[0].(string)

	cache.qPrevState = make(map[string]string, 0)
	cache.qPrevAction = make(map[string]qlearn.ActionType, 0)

	cache.aiFeatureMap = featuremap.Parse(featureMapFilePath)

	for key := range cache.aiFeatureMap {
		cache.aiFeatureMapOrder = append(cache.aiFeatureMapOrder, key)
	}
	sort.Strings(cache.aiFeatureMapOrder)

	cache.qTable = &qlearn.QTable{}
	inputLengths := []int{}
	for _, featureName := range cache.aiFeatureMapOrder {
		curFeature, _ := cache.aiFeatureMap[featureName]
		curLen := len(curFeature.Values)
		if curFeature.UnknownValues {
			curLen++
		}
		inputLengths = append(inputLengths, curLen)
	}
	logger.Println("[Generate QTable]")
	cache.qTable.Init(inputLengths)
	logger.Println("[Done]")

	return nil
}

// Clear the AIRL struct
func (cache *AIRL) Clear() {
	cache.LRUCache.Clear()
	cache.LRUCache.Init()
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
	for _, stats := range cache.Stats.fileStats {
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
			json.Unmarshal([]byte(curRecord.Data), &cache.Stats.fileStats)
		case "QTABLE":
			json.Unmarshal([]byte(curRecord.Data), cache.qTable)
		}
	}
}

func (cache *AIRL) getCategory(catKey string, value interface{}) []bool {
	var (
		res         []bool
		inputValueI int64
		inputValueF float64
		inputValueS string
	)
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
			if inputValueI <= curKey.ValueI {
				res[curCategory.Values[fmt.Sprintf("%d", curKey.ValueI)]] = true
				return res
			}
		case featuremap.TypeFloat:
			if inputValueF <= curKey.ValueF {
				res[curCategory.Values[fmt.Sprintf("%0.2f", curKey.ValueF)]] = true
				return res
			}
		case featuremap.TypeString:
			if inputValueS <= curKey.ValueS {
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

func (cache *AIRL) getState(request *Request, fileStats *FileStats) []bool {
	var (
		inputVector []bool
		tmpArr      []bool
	)

	tmpSplit := strings.Split(request.Filename, "/")
	dataType := tmpSplit[2]

	numReq, _, _ := fileStats.getRealTimeStats(&request.DayTime)
	size := request.Size

	cacheCapacity := float64(cache.Capacity())

	for _, featureName := range cache.aiFeatureMapOrder {
		switch featureName {
		case "size":
			tmpArr = cache.getCategory(featureName, float64(size))
		case "numReq":
			tmpArr = cache.getCategory(featureName, float64(numReq))
		case "cacheUsage":
			tmpArr = cache.getCategory(featureName, cacheCapacity)
		case "dataType":
			tmpArr = cache.getCategory(featureName, dataType)
		case "deltaNumLastRequest":
			tmpArr = cache.getCategory(featureName, float64(fileStats.DeltaLastRequest))
		default:
			panic(fmt.Sprintf("Cannot prepare input %s", featureName))
		}
		inputVector = append(inputVector, tmpArr...)
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

// BeforeRequest of LRU cache
func (cache *AIRL) BeforeRequest(request *Request, hit bool) *FileStats {
	fileStats, _ := cache.GetOrCreate(request.Filename, request.Size, request.DayTime)

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

	return fileStats
}

// UpdatePolicy of AIRL cache
func (cache *AIRL) UpdatePolicy(request *Request, fileStats *FileStats, hit bool) bool {
	var (
		added     = false
		curAction qlearn.ActionType
		curState  string

		requestedFilename = request.Filename
		requestedFileSize = request.Size
	)

	// Check learning phase or not
	expTradeoff := cache.qTable.GetRandomFloat()

	if expTradeoff > cache.qTable.Epsilon {
		// ########################
		// ##### Normal phase #####
		// ########################

		if !hit {
			// ########################
			// ##### MISS branch  #####
			// ########################

			curState = qlearn.State2String(cache.getState(request, fileStats))
			curAction = cache.qTable.GetBestAction(curState)
			// ----------------------------------
			// QLearn - Take the action NOT STORE
			// ----------------------------------
			if curAction == qlearn.ActionNotStore {
				return added
			}
			// ------------------------------
			// QLearn - Take the action STORE
			// ------------------------------
			// Insert with LRU mechanism
			if cache.Size()+requestedFileSize > cache.MaxSize {
				cache.Free(requestedFileSize, false)
			}
			if cache.Size()+requestedFileSize <= cache.MaxSize {
				cache.files[requestedFilename] = requestedFileSize
				cache.queue.PushBack(requestedFilename)
				cache.size += requestedFileSize
				added = true

				fileStats.addInCache(&request.DayTime)
				fileStats.updateFilePoints(&cache.curTime)
				cache.points += fileStats.Points
			}
		} else {
			// #######################
			// ##### HIT branch  #####
			// #######################
			cache.UpdateFileInQueue(requestedFilename)
		}
	} else {
		// ##########################
		// ##### Learning phase #####
		// ##########################

		if !hit {
			// ########################
			// ##### MISS branch  #####
			// ########################

			curState = qlearn.State2String(cache.getState(request, fileStats))

			// ----- Random choice -----
			randomAction := cache.qTable.GetRandomFloat()
			if randomAction > 0.5 {
				curAction = qlearn.ActionStore
			} else {
				curAction = qlearn.ActionNotStore
			}

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

				reward := 0.
				if fileStats.TotRequests() > 1 || fileStats.DeltaLastRequest < 1000 {
					reward -= 1.0
				} else {
					reward += 1.0
				}
				cache.qPrevState[request.Filename] = curState
				cache.qPrevAction[request.Filename] = curAction

				// Update table
				cache.qTable.Update(curState, curAction, reward)
				// Update epsilon
				cache.qTable.UpdateEpsilon()
				return added
			}

			// Insert with LRU mechanism
			if cache.Size()+requestedFileSize > cache.MaxSize {
				cache.Free(requestedFileSize, false)
			}
			if cache.Size()+requestedFileSize <= cache.MaxSize {
				cache.files[requestedFilename] = requestedFileSize
				cache.queue.PushBack(requestedFilename)
				cache.size += requestedFileSize
				added = true

				fileStats.addInCache(&request.DayTime)
				fileStats.updateFilePoints(&cache.curTime)
				cache.points += fileStats.Points
			}

			// ------------------------------
			// QLearn - Take the action STORE
			if cache.qTable != nil && curAction == qlearn.ActionStore {
				// newScore := cache.points
				// diff := newScore - cache.prevPoints
				// reward := 0.
				// if diff >= 0 {
				// 	reward += 1.
				// } else {
				// 	reward -= 1.
				// }

				reward := 0.
				if fileStats.TotRequests() > 1 || fileStats.DeltaLastRequest < 1000 {
					reward += 1.0
				} else {
					reward -= 1.0
				}
				cache.qPrevState[request.Filename] = curState
				cache.qPrevAction[request.Filename] = curAction

				// Update table
				cache.qTable.Update(curState, curAction, reward)
				// Update epsilon
				cache.qTable.UpdateEpsilon()
			}

		} else {
			// #######################
			// ##### HIT branch  #####
			// #######################
			cache.UpdateFileInQueue(requestedFilename)

			// ------------------------------
			// QLearn - hit reward on best action
			curState = cache.qPrevState[request.Filename]
			curAction = cache.qPrevAction[request.Filename]

			if curState != "" { // Some action are not taken randomly
				reward := 1.0

				// Update table
				cache.qTable.Update(curState, curAction, reward)
				// Update epsilon
				cache.qTable.UpdateEpsilon()
			}

		}

	}

	return added
}

// Free removes files from the cache
func (cache *AIRL) Free(amount float32, percentage bool) float32 {
	var (
		totalDeleted float32
		sizeToDelete float32
	)
	if percentage {
		sizeToDelete = amount * (cache.MaxSize / 100.)
	} else {
		sizeToDelete = amount
	}
	tmpVal := cache.queue.Front()
	for {
		if tmpVal == nil {
			break
		}
		curFilename2Delete := tmpVal.Value.(string)
		fileSize := cache.files[curFilename2Delete]
		curStats, added := cache.GetOrCreate(curFilename2Delete, fileSize)
		if added {
			panic("File in cache was removed from stats...")
		}
		curFilePoints := curStats.Points
		cache.points -= curFilePoints

		// Update stats
		cache.size -= fileSize
		cache.dataDeleted += fileSize
		totalDeleted += fileSize
		curStats.removeFromCache()

		// Remove from queue
		delete(cache.files, tmpVal.Value.(string))
		tmpVal = tmpVal.Next()

		// Check if all files are deleted
		if tmpVal == nil {
			break
		}
		cache.queue.Remove(tmpVal.Prev())

		if totalDeleted >= sizeToDelete {
			break
		}
	}
	return totalDeleted
}

// CheckWatermark checks the watermark levels and resolve the situation
func (cache *AIRL) CheckWatermark() bool {
	goodStatus := cache.LRUCache.CheckWatermark()
	if !goodStatus {
		cache.points = cache.GetPoints()
	}
	return goodStatus
}

// ExtraStats for output
func (cache *AIRL) ExtraStats() string {
	return fmt.Sprintf("Cov:%0.2f%%|Eps:%0.2f|P:%0.0f", cache.qTable.GetCoveragePercentage(), cache.qTable.Epsilon, cache.points)
}
