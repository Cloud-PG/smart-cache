package cache

import (
	"context"
	"encoding/json"
	"time"

	"simulator/v2/cache/ai/featuremap"
	"simulator/v2/cache/ai/neuralnet"
	aiPb "simulator/v2/cache/aiService"

	"google.golang.org/grpc"
)

// AINN cache
type AINN struct {
	SimpleCache
	prevTime           time.Time
	curTime            time.Time
	Exp                float32
	aiClientHost       string
	aiClientPort       string
	aiClient           aiPb.AIServiceClient
	aiFeatureMap       featuremap.FeatureManager
	aiFeatureOrder     []string
	aiFeatureSelection []bool
	aiModel            *neuralnet.AIModel
	grpcConn           *grpc.ClientConn
	grpcContext        context.Context
	grpcCxtCancel      context.CancelFunc
}

// Init the AINN struct
func (cache *AINN) Init(params InitParameters) interface{} {
	params.QueueType = LRUQueue
	cache.SimpleCache.Init(params)

	featureMapFilePath := params.AIFeatureMap
	modelFilePath := params.AIModel

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

	cache.aiFeatureMap = featuremap.Parse(featureMapFilePath)

	if modelFilePath != "" {
		cache.aiModel = neuralnet.LoadModel(modelFilePath)
	}

	return nil
}

// Dumps the AINN cache
func (cache *AINN) Dumps(fileAndStats bool) [][]byte {
	outData := make([][]byte, 0)
	var newLine = []byte("\n")

	if fileAndStats {
		// ----- Files -----
		logger.Info("Dump cache files")
		for file := range cache.files.Get() {
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
		for _, stats := range cache.stats.fileStats {
			dumpInfo, _ := json.Marshal(DumpInfo{Type: "STATS"})
			dumpStats, _ := json.Marshal(stats)
			record, _ := json.Marshal(DumpRecord{
				Info: string(dumpInfo),
				Data: string(dumpStats),
			})
			record = append(record, newLine...)
			outData = append(outData, record)
		}
	}

	return outData
}

// Loads the AINN cache
func (cache *AINN) Loads(inputString [][]byte, _ ...interface{}) {
	var curRecord DumpRecord
	var curRecordInfo DumpInfo

	for _, record := range inputString {
		buffer := record[:len(record)-1]
		json.Unmarshal(buffer, &curRecord)
		json.Unmarshal([]byte(curRecord.Info), &curRecordInfo)
		switch curRecordInfo.Type {
		case "FILES":
			var curFile FileSupportData
			json.Unmarshal([]byte(curRecord.Data), &curFile)
			cache.files.Insert(&curFile)
			cache.size += curFile.Size
		case "STATS":
			json.Unmarshal([]byte(curRecord.Data), &cache.stats.fileStats)
		}
	}
}

// func (cache *AINN) getCategory(catKey string, value interface{}) []float64 {
// 	var res []float64
// 	curCategory := cache.aiFeatureMap[catKey]

// 	if curCategory.UnknownValues == true || curCategory.BucketOpenRight == true {
// 		res = make([]float64, curCategory.GetLenKeys()+1)
// 	} else {
// 		res = make([]float64, curCategory.GetLenKeys())
// 	}

// 	if curCategory.Buckets == false {
// 		if curCategory.UnknownValues {
// 			oneHot, inMap := curCategory.Values[value.(string)]
// 			if inMap {
// 				res[oneHot] = 1.0
// 			} else {
// 				res[0] = 1.0
// 			}
// 		} else {
// 			res[curCategory.Values[value.(string)]] = 1.0
// 		}
// 		return res

// 	}

// 	switch curCategory.Type {
// 	case featuremap.TypeInt:
// 		inputValue := int64(value.(float64))
// 		for _, curKey := range curCategory.KeysI {
// 			if inputValue <= curKey {
// 				res[curCategory.Values[fmt.Sprintf("%d", curKey)]] = 1.0
// 				return res
// 			}
// 		}
// 	case featuremap.TypeFloat:
// 		inputValue := value.(float64)
// 		for _, curKey := range curCategory.KeysF {
// 			if inputValue <= curKey {
// 				res[curCategory.Values[fmt.Sprintf("%0.2f", curKey)]] = 1.0
// 				return res
// 			}
// 		}
// 	case featuremap.TypeString:
// 		inputValue := value.(string)
// 		for _, curKey := range curCategory.KeysS {
// 			if inputValue <= curKey {
// 				res[curCategory.Values[fmt.Sprintf("%s", curKey)]] = 1.0
// 				return res
// 			}
// 		}
// 	}

// 	if curCategory.BucketOpenRight == true {
// 		res[curCategory.Values["max"]] = 1.0
// 		return res
// 	}

// 	panic(fmt.Sprintf("Cannot convert a value '%v' of category %s", value, catKey))
// }

// func (cache *AINN) composeFeatures(vars ...interface{}) []float64 {
// 	var inputVector []float64
// 	var tmpArr []float64

// 	siteName := vars[0].(string)
// 	userID := strconv.Itoa(vars[1].(int))
// 	fileType := vars[2].(string)
// 	dataType := vars[3].(string)
// 	campain := vars[4].(string)
// 	process := vars[5].(string)
// 	Frequency := float64(vars[6].(uint32))
// 	avgTime := float64(vars[7].(float32))
// 	size := float64(vars[8].(float32))

// 	curInputs := []interface{}{
// 		siteName,
// 		userID,
// 		fileType,
// 		dataType,
// 		campain,
// 		process,
// 		Frequency,
// 		avgTime,
// 		size,
// 	}

// 	for idx, featureName := range cache.aiFeatureOrder {
// 		if cache.aiFeatureSelection[idx] {
// 			_, inFeatureMap := cache.aiFeatureMap[featureName]
// 			if inFeatureMap {
// 				tmpArr = cache.getCategory(featureName, curInputs[idx])
// 				inputVector = append(inputVector, tmpArr...)
// 				continue
// 			}
// 			inputVector = append(inputVector, curInputs[idx].(float64))
// 		}

// 	}

// 	return inputVector
// }

// // GetPoints returns the total amount of points for the files in cache
// func (cache *AINN) GetPoints() float64 {
// 	points := 0.0
// 	for file := range cache.files.Get(LRUQueue) {
// 		points += cache.stats.updateFilesPoints(file.Filename, &cache.curTime)
// 	}
// 	return float64(points)
// }

// UpdatePolicy of AINN cache
// func (cache *AINN) UpdatePolicy(request *Request, fileStats *FileStats, hit bool) bool {
// 	var (
// 		added = false

// 		requestedFilename = request.Filename
// 		requestedFileSize = request.Size
// 	)

// 	if !hit {
// 		siteName := request.SiteName
// 		userID := request.UserID
// 		dataType := request.DataType
// 		campain := 0
// 		process := 0
// 		fileType := request.FileType

// 		featureVector := cache.composeFeatures(
// 			siteName,
// 			userID,
// 			fileType,
// 			dataType,
// 			campain,
// 			process,
// 			fileStats.Frequency,
// 			fileStats.RequestTicksMean,
// 			requestedFileSize,
// 		)

// 		if cache.aiModel == nil {
// 			ctx, ctxCancel := context.WithTimeout(context.Background(), 24*time.Hour)
// 			defer ctxCancel()

// 			req := &aiPb.AIInput{
// 				InputVector: featureVector,
// 			}

// 			result, errGRPC := cache.aiClient.AIPredictOne(ctx, req)

// 			if errGRPC != nil {
// 				fmt.Println()
// 				fmt.Println(requestedFilename)
// 				fmt.Println(siteName)
// 				fmt.Println(userID)
// 				fmt.Println(fileStats.Frequency)
// 				fmt.Println(fileStats.RequestTicksMean)
// 				fmt.Println(requestedFileSize)
// 				fmt.Println(featureVector)
// 				log.Fatalf("ERROR: %v.AIPredictOne(_) = _, %v", cache.aiClient, errGRPC)
// 			}
// 			if result.Store == false {
// 				return added
// 			}
// 		} else if cache.aiModel != nil {
// 			inputVector := mat.NewDense(len(featureVector), 1, featureVector)
// 			result := cache.aiModel.Predict(inputVector)
// 			store := neuralnet.GetPredictionArgMax(result)
// 			// PrintTensor(result)
// 			if store == 0 {
// 				return added
// 			}
// 		}

// 		// Insert with LRU mechanism
// 		if cache.Size()+requestedFileSize > cache.MaxSize {
// 			cache.Free(requestedFileSize, false)
// 		}
// 		if cache.Size()+requestedFileSize <= cache.MaxSize {
// 			cache.files.Insert(FileSupportData{
// 				Filename:  request.Filename,
// 				Size:      request.Size,
// 				Frequency: fileStats.Frequency,
// 				Recency:   fileStats.DeltaLastRequest,
// 			})

// 			cache.size += requestedFileSize
// 			fileStats.addInCache(nil)
// 			added = true
// 		}
// 	} else {
// 		cache.files.Update(FileSupportData{
// 			Filename:  request.Filename,
// 			Size:      request.Size,
// 			Frequency: fileStats.Frequency,
// 			Recency:   fileStats.DeltaLastRequest,
// 		})
// 	}

// 	return added
// }
