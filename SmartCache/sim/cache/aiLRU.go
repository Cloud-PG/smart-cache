package cache

import (
	"compress/gzip"
	"container/list"
	"context"
	"encoding/json"
	"fmt"
	"log"
	"os"
	"strconv"
	"strings"
	"time"

	aiPb "simulator/v2/cache/aiService"
	"simulator/v2/cache/neuralnet"
	qlearn "simulator/v2/cache/qLearn"

	"gonum.org/v1/gonum/mat"
	"google.golang.org/grpc"
)

type mapType int

const (
	typeInt mapType = iota
	typeFloat
	typeString
	typeBool
)

type featureMapObj struct {
	Feature         string
	Type            mapType
	Keys            []interface{}
	KeysB           []bool
	KeysI           []int64
	KeysF           []float64
	KeysS           []string
	Values          map[string]int
	UnknownValues   bool
	Buckets         bool
	BucketOpenRight bool
}

type featureMapKey struct {
	ValueI int64
	ValueF float64
	ValueS string
}

func (curMap featureMapObj) GetLenKeys() int {
	var lenght int
	switch curMap.Type {
	case typeInt:
		lenght = len(curMap.KeysI)
	case typeFloat:
		lenght = len(curMap.KeysF)
	case typeString:
		lenght = len(curMap.KeysS)
	}
	return lenght
}

func (curMap featureMapObj) GetKeys() chan featureMapKey {
	channel := make(chan featureMapKey)
	go func() {
		defer close(channel)
		numKeys := curMap.GetLenKeys()
		for idx := 0; idx < numKeys; idx++ {
			curKey := featureMapKey{}
			switch curMap.Type {
			case typeInt:
				curKey.ValueI = curMap.KeysI[idx]
			case typeFloat:
				curKey.ValueF = curMap.KeysF[idx]
			case typeString:
				curKey.ValueS = curMap.KeysS[idx]
			}
			channel <- curKey
		}
	}()
	return channel
}

// AILRU cache
type AILRU struct {
	LRUCache
	WeightedStats
	curTime            time.Time
	stats              map[string]*WeightedFileStats
	Exp                float32
	aiClientHost       string
	aiClientPort       string
	aiClient           aiPb.AIServiceClient
	aiFeatureMap       map[string]featureMapObj
	aiFeatureOrder     []string
	aiFeatureSelection []bool
	aiModel            *neuralnet.AIModel
	grpcConn           *grpc.ClientConn
	grpcContext        context.Context
	grpcCxtCancel      context.CancelFunc
	qTable             *qlearn.QTable
	curCachePoints     float64
}

// Init the AILRU struct
func (cache *AILRU) Init(args ...interface{}) interface{} {
	cache.LRUCache.Init()
	cache.WeightedStats.Init()

	cache.aiClientHost = args[0].(string)
	cache.aiClientPort = args[1].(string)
	featureMapFilePath := args[2].(string)
	modelFilePath := args[3].(string)
	enableQLearn := args[4].(bool)

	cache.aiFeatureOrder = []string{
		"siteName",
		"userID",
		"fileType",
		"dataType",
		"campain",
		"process",
		"numReq",
		"avgTime",
		"size",
	}

	if !enableQLearn {
		cache.aiFeatureSelection = []bool{
			true,
			true,
			true,
			true,
			true,
			true,
			true,
			true,
			true,
		}
	} else {
		cache.aiFeatureSelection = []bool{
			false,
			false,
			false,
			false,
			false,
			false,
			true,
			false,
			true,
		}
	}

	cache.aiFeatureMap = make(map[string]featureMapObj, 0)

	featureMapFile, errOpenFile := os.Open(featureMapFilePath)
	if errOpenFile != nil {
		log.Fatalf("Cannot open file '%s'\n", errOpenFile)
	}

	featureMapFileGz, errOpenZipFile := gzip.NewReader(featureMapFile)
	if errOpenZipFile != nil {
		log.Fatalf("Cannot open zip stream from file '%s'\nError: %s\n", featureMapFilePath, errOpenZipFile)
	}

	var tmpMap interface{}
	errJSONUnmarshal := json.NewDecoder(featureMapFileGz).Decode(&tmpMap)
	if errJSONUnmarshal != nil {
		log.Fatalf("Cannot unmarshal json from file '%s'\nError: %s\n", featureMapFilePath, errJSONUnmarshal)
	}

	// Parse feature map
	lvl0 := tmpMap.(map[string]interface{})
	for k0, v0 := range lvl0 {
		curObj := v0.(map[string]interface{})
		curStruct := featureMapObj{}
		for objK, objV := range curObj {
			switch objK {
			case "feature":
				curStruct.Feature = objV.(string)
			case "type":
				curType := objV.(string)
				switch curType {
				case "int":
					curStruct.Type = typeInt
				case "float":
					curStruct.Type = typeFloat
				case "string":
					curStruct.Type = typeString
				case "bool":
					curStruct.Type = typeBool
				}
			case "keys":
				curStruct.Keys = objV.([]interface{})
			case "values":
				curValues := objV.(map[string]interface{})
				curStruct.Values = make(map[string]int)
				for vK, vV := range curValues {
					curStruct.Values[vK] = int(vV.(float64))
				}
			case "unknown_values":
				curStruct.UnknownValues = objV.(bool)
			case "buckets":
				curStruct.Buckets = objV.(bool)
			case "bucket_open_right":
				curStruct.BucketOpenRight = objV.(bool)
			}
		}

		for _, elm := range curStruct.Keys {
			switch curStruct.Type {
			case typeInt:
				curStruct.KeysI = append(curStruct.KeysI, int64(elm.(float64)))
			case typeFloat:
				curStruct.KeysF = append(curStruct.KeysF, elm.(float64))
			case typeString:
				curStruct.KeysS = append(curStruct.KeysS, elm.(string))
			case typeBool:
				curStruct.KeysB = append(curStruct.KeysB, elm.(bool))
			}
		}

		// Output the structure
		// fmt.Println(curStruct)

		cache.aiFeatureMap[k0] = curStruct
	}

	if modelFilePath == "" && !enableQLearn && cache.aiClientHost != "" && cache.aiClientPort != "" {
		var opts []grpc.DialOption
		opts = append(opts, grpc.WithInsecure())
		opts = append(opts, grpc.WithBlock())

		conn, err := grpc.Dial(fmt.Sprintf("%s:%s",
			cache.aiClientHost, cache.aiClientPort,
		), opts...)

		cache.grpcConn = conn
		if err != nil {
			log.Fatalf("ERROR: Fail to dial wit AI Client: %v", err)
		}

		cache.aiClient = aiPb.NewAIServiceClient(cache.grpcConn)

		return cache.grpcConn
	} else if modelFilePath != "" && !enableQLearn {
		cache.aiModel = neuralnet.LoadModel(modelFilePath)
	} else if enableQLearn {
		cache.qTable = &qlearn.QTable{}
		inputLenghts := []int{}
		for idx, featureName := range cache.aiFeatureOrder {
			if cache.aiFeatureSelection[idx] {
				curFeature, _ := cache.aiFeatureMap[featureName]
				curLen := len(curFeature.Values)
				if curFeature.UnknownValues {
					curLen++
				}
				inputLenghts = append(inputLenghts, curLen)
			}
		}
		fmt.Print("[Generate QTable]")
		cache.qTable.Init(inputLenghts)
		fmt.Println("[Done]")
	}

	return nil
}

// Clear the AILRU struct
func (cache *AILRU) Clear() {
	cache.LRUCache.Init()
	cache.WeightedStats.Init()
}

// Dumps the AILRU cache
func (cache *AILRU) Dumps() *[][]byte {
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

// Loads the AILRU cache
func (cache *AILRU) Loads(inputString *[][]byte) {
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

func (cache *AILRU) getCategory(catKey string, value interface{}) []float64 {
	var res []float64
	curCategory := cache.aiFeatureMap[catKey]

	if curCategory.UnknownValues == true || curCategory.BucketOpenRight == true {
		res = make([]float64, curCategory.GetLenKeys()+1)
	} else {
		res = make([]float64, curCategory.GetLenKeys())
	}

	if curCategory.Buckets == false {
		if curCategory.UnknownValues {
			oneHot, inMap := curCategory.Values[value.(string)]
			if inMap {
				res[oneHot] = 1.0
			} else {
				res[0] = 1.0
			}
		} else {
			res[curCategory.Values[value.(string)]] = 1.0
		}
		return res

	}

	for curKey := range curCategory.GetKeys() {
		switch curCategory.Type {
		case typeInt:
			inputValue := int64(value.(float64))
			if inputValue <= curKey.ValueI {
				res[curCategory.Values[fmt.Sprintf("%d", curKey.ValueI)]] = 1.0
				return res
			}
		case typeFloat:
			inputValue := value.(float64)
			if inputValue <= curKey.ValueF {
				res[curCategory.Values[fmt.Sprintf("%0.2f", curKey.ValueF)]] = 1.0
				return res
			}
		case typeString:
			inputValue := value.(string)
			if inputValue <= curKey.ValueS {
				res[curCategory.Values[fmt.Sprintf("%s", curKey.ValueS)]] = 1.0
				return res
			}
		}
	}

	if curCategory.BucketOpenRight == true {
		res[curCategory.Values["max"]] = 1.0
		return res
	}

	panic(fmt.Sprintf("Cannot convert a value '%v' of category %s", value, catKey))
}

func (cache *AILRU) composeFeatures(vars ...interface{}) []float64 {
	var inputVector []float64
	var tmpArr []float64

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

	for idx, featureName := range cache.aiFeatureOrder {
		if cache.aiFeatureSelection[idx] {
			_, inFeatureMap := cache.aiFeatureMap[featureName]
			if inFeatureMap {
				tmpArr = cache.getCategory(featureName, curInputs[idx])
				inputVector = append(inputVector, tmpArr...)
				continue
			}
			inputVector = append(inputVector, curInputs[idx].(float64))
		}

	}

	return inputVector
}

// GetPoints returns the total amount of points for the files in cache
func (cache AILRU) GetPoints(curTime *time.Time) float64 {
	points := 0.0
	for filename := range cache.files {
		points += cache.getFilePoints(filename, curTime)
	}
	return float64(points)
}

// UpdatePolicy of AILRU cache
func (cache *AILRU) UpdatePolicy(filename string, size float32, hit bool, vars ...interface{}) bool {
	var (
		added      = false
		curAction  qlearn.ActionType
		prevPoints float64
		curState   []float64
	)

	day := vars[0].(int64)
	currentTime := time.Unix(day, 0)
	cache.curTime = currentTime

	curStats, _ := cache.GetOrCreate(filename, size, &currentTime)
	if cache.qTable == nil {
		curStats.updateStats(hit, size, &currentTime)
	} else {
		curStats.updateStats(hit, size, nil)
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

		if cache.aiModel == nil && cache.qTable == nil {
			ctx, ctxCancel := context.WithTimeout(context.Background(), 24*time.Hour)
			defer ctxCancel()

			req := &aiPb.AIInput{
				InputVector: featureVector,
			}

			result, errGRPC := cache.aiClient.AIPredictOne(ctx, req)

			if errGRPC != nil {
				fmt.Println()
				fmt.Println(filename)
				fmt.Println(siteName)
				fmt.Println(userID)
				fmt.Println(curStats.TotRequests)
				fmt.Println(curStats.RequestTicksMean)
				fmt.Println(size)
				fmt.Println(featureVector)
				log.Fatalf("ERROR: %v.AIPredictOne(_) = _, %v", cache.aiClient, errGRPC)
			}
			if result.Store == false {
				return added
			}
		} else if cache.aiModel != nil {
			inputVector := mat.NewDense(len(featureVector), 1, featureVector)
			result := cache.aiModel.Predict(inputVector)
			store := neuralnet.GetPredictionArgMax(result)
			// PrintTensor(result)
			if store == 0 {
				return added
			}
		} else if cache.qTable != nil {
			curState = featureVector
			if cache.curCachePoints == 0.0 {
				prevPoints = cache.GetPoints(&currentTime)
			} else {
				prevPoints = cache.curCachePoints
			}

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
				newScore := cache.GetPoints(&currentTime)
				cache.curCachePoints = newScore
				reward := newScore - prevPoints
				// Update table
				cache.qTable.Update(curState, curAction, reward)
				// Update epsilon
				cache.qTable.UpdateEpsilon()
				return added
			}
		}

		// Insert with LRU mechanism
		if cache.Size()+size > cache.MaxSize {
			var totalDeleted float32
			tmpVal := cache.queue.Front()
			for {
				if tmpVal == nil {
					break
				}
				fileSize := cache.files[tmpVal.Value.(string)]
				cache.size -= fileSize
				cache.dataDeleted += size

				totalDeleted += fileSize
				delete(cache.files, tmpVal.Value.(string))

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
			added = true
		}

		// QLearn - Take the action STORE
		if cache.qTable != nil && curAction == qlearn.ActionStore {
			newScore := cache.GetPoints(&currentTime)
			cache.curCachePoints = newScore
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
