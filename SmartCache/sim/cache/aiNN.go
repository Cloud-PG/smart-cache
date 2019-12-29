package cache

import (
	"container/list"
	"context"
	"encoding/json"
	"fmt"
	"log"
	"strconv"
	"strings"
	"time"

	"simulator/v2/cache/ai/featuremap"
	"simulator/v2/cache/ai/neuralnet"
	aiPb "simulator/v2/cache/aiService"
	qlearn "simulator/v2/cache/qLearn"

	"gonum.org/v1/gonum/mat"
	"google.golang.org/grpc"
)

// AINN cache
type AINN struct {
	LRUCache
	WeightedStats
	prevTime           time.Time
	curTime            time.Time
	stats              map[string]*WeightedFileStats
	Exp                float32
	aiClientHost       string
	aiClientPort       string
	aiClient           aiPb.AIServiceClient
	aiFeatureMap       map[string]featuremap.Obj
	aiFeatureOrder     []string
	aiFeatureSelection []bool
	aiModel            *neuralnet.AIModel
	grpcConn           *grpc.ClientConn
	grpcContext        context.Context
	grpcCxtCancel      context.CancelFunc
}

// Init the AINN struct
func (cache *AINN) Init(args ...interface{}) interface{} {
	cache.LRUCache.Init()
	cache.WeightedStats.Init()

	cache.aiClientHost = args[0].(string)
	cache.aiClientPort = args[1].(string)
	featureMapFilePath := args[2].(string)
	modelFilePath := args[3].(string)

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

	cache.aiFeatureMap = make(map[string]featuremap.Obj, 0)

	for entry := range featuremap.Parse(featureMapFilePath) {
		cache.aiFeatureMap[entry.Key] = entry.Value
	}

	if modelFilePath == "" && cache.aiClientHost != "" && cache.aiClientPort != "" {
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
	} else if modelFilePath != "" {
		cache.aiModel = neuralnet.LoadModel(modelFilePath)
	}

	return nil
}

// Clear the AINN struct
func (cache *AINN) Clear() {
	cache.LRUCache.Init()
	cache.WeightedStats.Init()
}

// Dumps the AINN cache
func (cache *AINN) Dumps() *[][]byte {
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

	return &outData
}

// Loads the AINN cache
func (cache *AINN) Loads(inputString *[][]byte) {
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
		}
	}
}

func (cache *AINN) getCategory(catKey string, value interface{}) []float64 {
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
		case featuremap.TypeInt:
			inputValue := int64(value.(float64))
			if inputValue <= curKey.ValueI {
				res[curCategory.Values[fmt.Sprintf("%d", curKey.ValueI)]] = 1.0
				return res
			}
		case featuremap.TypeFloat:
			inputValue := value.(float64)
			if inputValue <= curKey.ValueF {
				res[curCategory.Values[fmt.Sprintf("%0.2f", curKey.ValueF)]] = 1.0
				return res
			}
		case featuremap.TypeString:
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

func (cache *AINN) composeFeatures(vars ...interface{}) []float64 {
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
func (cache AINN) GetPoints() float64 {
	points := 0.0
	for filename := range cache.files {
		points += cache.updateFilesPoints(filename, &cache.curTime)
	}
	return float64(points)
}

// UpdatePolicy of AINN cache
func (cache *AINN) UpdatePolicy(filename string, size float32, hit bool, vars ...interface{}) bool {
	var (
		added      = false
		curAction  qlearn.ActionType
		prevPoints float64
		curState   []float64
	)

	day := vars[0].(int64)
	currentTime := time.Unix(day, 0)

	cache.prevTime = cache.curTime
	cache.curTime = currentTime

	curStats, _ := cache.GetOrCreate(filename, size, &currentTime)
	curStats.updateStats(hit, size, &currentTime)
	

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

		if cache.aiModel == nil {
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
			added = true
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
