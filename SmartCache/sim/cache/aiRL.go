package cache

import (
	"encoding/json"
	"fmt"
	"math"
	"simulator/v2/cache/ai/featuremap"
	qlearn "simulator/v2/cache/qLearn"
	"sort"
	"strings"
	"time"
)

// AIRL cache
type AIRL struct {
	LRUCache
	Stats
	prevTime          time.Time
	curTime           time.Time
	Exp               float32
	aiFeatureMap      map[string]featuremap.Obj
	aiFeatureMapOrder []string
	qTable            *qlearn.QTable
	points            float64
	minFilePoints     float64
}

// Init the AIRL struct
func (cache *AIRL) Init(args ...interface{}) interface{} {
	cache.LRUCache.Init()
	cache.Stats.Init()

	cache.minFilePoints = math.Inf(1)

	featureMapFilePath := args[0].(string)

	cache.aiFeatureMap = featuremap.Parse(featureMapFilePath)

	for key := range cache.aiFeatureMap {
		cache.aiFeatureMapOrder = append(cache.aiFeatureMapOrder, key)
	}
	sort.Strings(cache.aiFeatureMapOrder)

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
	cache.Stats.Init()
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
	for _, stats := range cache.Stats.data {
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
			json.Unmarshal([]byte(curRecord.Data), &cache.Stats.data)
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

func (cache *AIRL) getState(vars ...interface{}) []bool {
	var inputVector []bool
	var tmpArr []bool

	size := float64(vars[0].(float32))
	dataType := vars[1].(string)
	numReq := vars[2].(float64)
	numUsers := vars[3].(float64)
	numSites := vars[4].(float64)

	cacheCapacity := float64(cache.Capacity())

	for _, featureName := range cache.aiFeatureMapOrder {
		switch featureName {
		case "size":
			tmpArr = cache.getCategory(featureName, size)
		case "numReq":
			tmpArr = cache.getCategory(featureName, numReq)
		case "numUsers":
			tmpArr = cache.getCategory(featureName, numUsers)
		case "numSites":
			tmpArr = cache.getCategory(featureName, numSites)
		case "cacheUsage":
			tmpArr = cache.getCategory(featureName, cacheCapacity)
		case "dataType":
			tmpArr = cache.getCategory(featureName, dataType)
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

	if !hit {
		fileStats.updateStats(hit, request.Size, request.UserID, request.SiteName, nil)
		fileStats.updateFilePoints(&cache.curTime)
	} else {
		cache.points -= fileStats.Points
		fileStats.updateStats(hit, request.Size, request.UserID, request.SiteName, nil)
		fileStats.updateFilePoints(&cache.curTime)
		cache.points += fileStats.Points
	}

	return fileStats
}

// UpdatePolicy of AIRL cache
func (cache *AIRL) UpdatePolicy(request *Request, fileStats *FileStats, hit bool) bool {
	var (
		added      = false
		curAction  qlearn.ActionType
		prevPoints float64
		curState   string

		requestedFilename = request.Filename
		requestedFileSize = request.Size
	)

	cache.prevTime = cache.curTime
	cache.curTime = request.DayTime

	if !cache.curTime.Equal(cache.prevTime) {
		cache.points = cache.GetPoints()
	}

	prevPoints = cache.points

	if !hit {
		tmpSplit := strings.Split(requestedFilename, "/")
		dataType := tmpSplit[2]

		numReq, numUsers, numSites := fileStats.getRealTimeStats(&request.DayTime)

		curState = qlearn.State2String(
			cache.getState(
				requestedFileSize,
				dataType,
				numReq,
				numUsers,
				numSites,
			),
		)

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

		// ----------------------------------
		// QLearn - Take the action NOT STORE
		if curAction == qlearn.ActionNotStore {
			newScore := cache.points
			diff := newScore - prevPoints
			reward := 0.
			if diff >= 0. {
				if cache.CheckWatermark() {
					reward -= 1.
				} else {
					reward += 1.
				}
			} else {
				reward -= 1.
			}
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

			// Q-Learning
			fileStats.addInCache(&request.DayTime)
			fileStats.updateFilePoints(&cache.curTime)
			cache.points += fileStats.Points
			if fileStats.Points < cache.minFilePoints {
				cache.minFilePoints = fileStats.Points
			}
		}

		// ------------------------------
		// QLearn - Take the action STORE
		if cache.qTable != nil && curAction == qlearn.ActionStore {
			newScore := cache.points
			diff := newScore - prevPoints
			reward := 0.
			if diff >= 0. {
				if !cache.CheckWatermark() {
					reward -= 1.
				} else {
					reward += 1.
				}
			} else {
				reward -= 1.
			}
			// Update table
			cache.qTable.Update(curState, curAction, reward)
			// Update epsilon
			cache.qTable.UpdateEpsilon()
		}

	} else {
		cache.UpdateFileInQueue(requestedFilename)
		if fileStats.Points < cache.minFilePoints {
			cache.minFilePoints = fileStats.Points
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
		curFilePoints := cache.getPoints(curFilename2Delete)
		cache.points -= curFilePoints
		if curFilePoints <= cache.minFilePoints {
			cache.minFilePoints = math.Inf(1)
		}
		fileSize := cache.files[curFilename2Delete]
		// Update sizes
		cache.size -= fileSize
		cache.dataDeleted += fileSize
		totalDeleted += fileSize

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
	return fmt.Sprintf("Cov: %0.2f%%, Epsilon: %0.2f", cache.qTable.GetCoveragePercentage(), cache.qTable.Epsilon)
}
