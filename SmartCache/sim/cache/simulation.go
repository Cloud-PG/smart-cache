package cache

import (
	"compress/gzip"
	"encoding/csv"
	"fmt"
	"io"
	"log"
	"os"
	"path"
	"sort"
	"strconv"
	"strings"

	"go.uber.org/zap"
)

const (
	csvHeader        = "Filename,SiteName,UserID,TaskID,TaskMonitorID,JobID,Protocol,JobExecExitCode,JobStart,JobEnd,NumCPU,WrapWC,WrapCPU,Size,DataType,FileType,JobLengthH,JobLengthM,JobSuccess,CPUTime,IOTime,reqDay,Region,Campain,Process"
	readerBufferSize = 1024
)

var (
	/*
		CSV HEADER:
			- [0] Filename (int64)
			- [1] SiteName (int64)
			- [2] UserID (int64)
			- [3] TaskID (int64)
			- [4] TaskMonitorID (int64)
			- [5] JobID (int64)
			- [6] Protocol (int64)
			- [7] JobExecExitCode (int64)
			- [8] JobStart (int64)
			- [9] JobEnd (int64)
			- [10] NumCPU (int64)
			- [11] WrapWC (float64)
			- [12] WrapCPU (float64)
			- [13] Size (float64)
			- [14] DataType (int64)
			- [15] FileType (int64)
			- [16] JobLengthH (float64)
			- [17] JobLengthM (float64)
			- [18] JobSuccess (bool)
			- [19] CPUTime (float64)
			- [20] IOTime (float64)
			- [21] reqDay (int64)
			- [22] Region (int64)
			- [23] Campain (int64)
			- [24] Process (int64)
	*/
	csvHeaderIndexes = []int{0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16, 17, 18, 19, 20, 21, 22, 23, 24}
)

// SimulationStats is used to output the simulation statistics
type SimulationStats struct {
	TimeElapsed           string  `json:"timeElapsed"`
	Extra                 string  `json:"extra"`
	TotNumRecords         int64   `json:"totNumRecords"`
	TotJumpedRecords      int64   `json:"totJumpedRecords"`
	TotInvalidRecords     int64   `json:"totInvalidRecords"`
	TotFilteredRecords    int64   `json:"totFilteredRecords"`
	TotRedirectedRecords  int64   `json:"totRedirectedRecords"`
	SizeRedirectedRecords float64 `json:"sizeRedirectedRecords"`
	AvgSpeed              string  `json:"avgSpeed"`
}

// CSVRecord is the base record composition readed from the logs
type CSVRecord struct {
	Day             int64   `json:"day"`
	Region          int64   `json:"region"`
	Filename        int64   `json:"filename"`
	FileType        int64   `json:"fileType"`
	DataType        int64   `json:"dataType"`
	Campain         int64   `json:"campain"`
	Process         int64   `json:"process"`
	Protocol        int64   `json:"protocol"`
	TaskMonitorID   int64   `json:"taskMonitorID"`
	TaskID          int64   `json:"taskID"`
	JobID           int64   `json:"jobID"`
	SiteName        int64   `json:"siteName"`
	JobExecExitCode int64   `json:"jobExecExitCode"`
	JobStart        int64   `json:"jobStart"`
	JobEnd          int64   `json:"jobEnd"`
	JobSuccess      bool    `json:"jobSuccess"`
	JobLengthH      float64 `json:"jobLengthH"`
	JobLengthM      float64 `json:"jobLengthM"`
	UserID          int64   `json:"user"`
	NumCPU          int64   `json:"numCPU"`
	WrapWC          float64 `json:"WrapWC"`
	WrapCPU         float64 `json:"WrapCPU"`
	CPUTime         float64 `json:"CPUTime"`
	IOTime          float64 `json:"IOTime"`
	Size            float64 `json:"size"`
	SizeM           float64 `json:"sizeM"`
}

func recordGenerator(csvReader *csv.Reader, curFile *os.File, headerMap []int) chan CSVRecord {
	channel := make(chan CSVRecord, readerBufferSize)
	go func() {
		defer close(channel)
		defer curFile.Close()
		for {
			record, err := csvReader.Read()
			if err == io.EOF {
				break
			}
			if err != nil {
				log.Fatal(err)
			}

			// fmt.Println(record)

			filename, _ := strconv.ParseInt(record[headerMap[0]], 10, 64)
			siteName, _ := strconv.ParseInt(record[headerMap[1]], 10, 64)
			userID, _ := strconv.ParseInt(record[headerMap[2]], 10, 64)
			taskID, _ := strconv.ParseInt(record[headerMap[3]], 10, 64)
			taskMonitorID, _ := strconv.ParseInt(record[headerMap[4]], 10, 64)
			jobID, _ := strconv.ParseInt(record[headerMap[5]], 10, 64)
			protocol, _ := strconv.ParseInt(record[headerMap[6]], 10, 64)
			size, _ := strconv.ParseFloat(record[headerMap[13]], 64)
			dataType, _ := strconv.ParseInt(record[headerMap[14]], 10, 64)
			fileType, _ := strconv.ParseInt(record[headerMap[15]], 10, 64)
			joblengthh, _ := strconv.ParseFloat(record[headerMap[16]], 64)
			joblengthm, _ := strconv.ParseFloat(record[headerMap[17]], 64)
			jobSuccess, _ := strconv.ParseBool(record[headerMap[18]])
			cputime, _ := strconv.ParseFloat(record[headerMap[19]], 64)
			iotime, _ := strconv.ParseFloat(record[headerMap[20]], 64)
			day, _ := strconv.ParseFloat(record[headerMap[21]], 64)
			region, _ := strconv.ParseInt(record[headerMap[22]], 10, 64)
			campain, _ := strconv.ParseInt(record[headerMap[23]], 10, 64)
			process, _ := strconv.ParseInt(record[headerMap[24]], 10, 64)

			sizeInMegabytes := size / (1024. * 1024.)

			curRecord := CSVRecord{
				Day:           int64(day),
				Region:        region,
				Filename:      filename,
				FileType:      fileType,
				DataType:      dataType,
				Protocol:      protocol,
				TaskMonitorID: taskMonitorID,
				TaskID:        taskID,
				JobID:         jobID,
				SiteName:      siteName,
				JobSuccess:    jobSuccess,
				JobLengthH:    joblengthh,
				JobLengthM:    joblengthm,
				UserID:        userID,
				CPUTime:       cputime,
				IOTime:        iotime,
				Size:          size,
				SizeM:         sizeInMegabytes,
				Campain:       campain,
				Process:       process,
			}

			// fmt.Println(curRecord)

			channel <- curRecord
		}
	}()
	return channel
}

// OpenSimFile opens a simulation file
func OpenSimFile(filePath string) chan CSVRecord {
	logger = zap.L()

	fileExt := path.Ext(filePath)
	var iterator chan CSVRecord

	logger.Debug("Open Data File", zap.String("filename", filePath))
	curFile, errOpenFile := os.Open(filePath)
	if errOpenFile != nil {
		panic(errOpenFile)
	}

	headerMap := csvHeaderIndexes

	switch fileExt {
	case ".gz", ".gzip":
		// Create new reader to decompress gzip.
		curCsv, errReadGz := gzip.NewReader(curFile)
		if errReadGz != nil {
			panic(errReadGz)
		}
		csvReader := csv.NewReader(curCsv)
		// Discar header
		header, errCSVRead := csvReader.Read()
		headerStr := strings.Join(header, ",")
		logger.Debug("File header", zap.String("CSV header", headerStr), zap.String("file", filePath))
		if headerStr != csvHeader {
			headerMap = getHeaderIndexes(header)
		}
		if errCSVRead != nil {
			panic(errCSVRead)
		}
		iterator = recordGenerator(csvReader, curFile, headerMap)
	default:
		csvReader := csv.NewReader(curFile)
		// Discar header
		header, errCSVRead := csvReader.Read()
		headerStr := strings.Join(header, ",")
		logger.Debug("FIle header", zap.String("CSV header", headerStr), zap.String("file", filePath))
		if headerStr != csvHeader {
			headerMap = getHeaderIndexes(header)
		}
		if errCSVRead != nil {
			panic(errCSVRead)
		}
		iterator = recordGenerator(csvReader, curFile, headerMap)
	}

	return iterator
}

func getHeaderIndexes(header []string) []int {
	var indexes []int
	for _, name := range strings.Split(csvHeader, ",") {
		for idx, curHeaderName := range header {
			if name == curHeaderName {
				indexes = append(indexes, idx)
			}
		}
	}
	return indexes
}

// OpenSimFolder opens a simulation folder
func OpenSimFolder(dirPath *os.File) chan CSVRecord {

	channel := make(chan CSVRecord)

	fileStats, _ := dirPath.Readdir(0)
	var fileList []string

	for _, file := range fileStats {
		fileList = append(fileList, path.Join(dirPath.Name(), file.Name()))
	}
	sort.Slice(fileList, func(i, j int) bool { return fileList[i] < fileList[j] })

	go func() {
		defer close(channel)
		for _, name := range fileList {
			fileExt := path.Ext(name)
			switch fileExt {
			case ".csv":
			case ".gz":
				for record := range OpenSimFile(name) {
					channel <- record
				}
			}

		}
	}()

	return channel
}

// OutputCSV is an utility to output CSV
type OutputCSV struct {
	filename         string
	file             *os.File
	compressedWriter *gzip.Writer
	csvWriter        *csv.Writer
}

// Create an output file in CSV format
func (output *OutputCSV) Create(filename string, compressed bool) {
	if compressed {
		output.filename = filename + ".gz"
	} else {
		output.filename = filename
	}

	outputFile, errCreateFile := os.Create(output.filename)
	if errCreateFile != nil {
		panic(errCreateFile)
	}
	output.file = outputFile

	if compressed {
		output.compressedWriter = gzip.NewWriter(output.file)
		output.csvWriter = csv.NewWriter(output.compressedWriter)
	} else {
		output.csvWriter = csv.NewWriter(output.file)
	}

}

// Close the output file after flush the buffer
func (output OutputCSV) Write(record []string) {
	if errWriter := output.csvWriter.Write(record); errWriter != nil {
		panic(errWriter)
	}
	output.csvWriter.Flush()
}

// Close the output file after flush the buffer
func (output OutputCSV) Close() {
	output.csvWriter.Flush()
	if output.compressedWriter != nil {
		output.compressedWriter.Close()
	}
	output.file.Close()
}

// Filter interface
type Filter interface {
	Check(CSVRecord) bool
}

// UsDataMcTypes for USA
type UsDataMcTypes struct {
}

// Check if the record have to be sent to the cache
func (filter UsDataMcTypes) Check(record CSVRecord) bool {
	// Check if data type == data, mc
	if record.DataType == 0 || record.DataType == 3 {
		return true
	}
	return false
}

// ItDataMcTypes for IT
type ItDataMcTypes struct {
}

// Check if the record have to be sent to the cache
func (filter ItDataMcTypes) Check(record CSVRecord) bool {
	// Check if data type == data, mc
	if record.DataType == 0 || record.DataType == 1 {
		return true
	}
	return false
}

// SuccessJob filter
type SuccessJob struct {
}

// Check if the record is with a success or not
func (filter SuccessJob) Check(record CSVRecord) bool {
	if record.JobSuccess && record.JobExecExitCode == 0 {
		return true
	}
	return false
}

// UsMINIAODNOT1andT3 for USA MINIAOD records without fnalpc and T1
type UsMINIAODNOT1andT3 struct {
}

// Check if the record have to be sent to the cache
func (filter UsMINIAODNOT1andT3) Check(record CSVRecord) bool {
	// Check if file type == MINIAOD, MINIAODSIM
	// Check if site != T1_US_FNAL, T3_US_FNALLPC
	if (record.FileType == 2 || record.FileType == 9) && record.SiteName < 9 {
		return true
	}
	return false
}

// UsMINIAODNOFNALLPCNOT1FNALLFilter for USA MINIAOD records without fnalpc and T1
type UsMINIAODNOFNALLPCNOT1FNALLFilter struct {
}

// Check if the record have to be sent to the cache
func (filter UsMINIAODNOFNALLPCNOT1FNALLFilter) Check(record CSVRecord) bool {
	// Check if file type == MINIAOD, MINIAODSIM
	// Check if site != T1_US_FNAL, T3_US_FNALLPC
	if (record.FileType == 2 || record.FileType == 9) && record.SiteName != 9 && record.SiteName != 16 {
		return true
	}
	return false
}

// UsMINIAODandNOFNALLPCFilter for USA MINIAOD records without fnalpc
type UsMINIAODandNOFNALLPCFilter struct {
}

// Check if the record have to be sent to the cache
func (filter UsMINIAODandNOFNALLPCFilter) Check(record CSVRecord) bool {
	// Check if file type == MINIAOD, MINIAODSIM
	// Check if site != T1_US_FNAL, T3_US_FNALLPC
	if (record.FileType == 2 || record.FileType == 9) && record.SiteName != 16 {
		return true
	}
	return false
}

// GetCacheSize returns the cache size in megabytes
func GetCacheSize(cacheSize float64, cacheSizeUnit string) float64 {
	res := -1.
	switch cacheSizeUnit {
	case "M", "m":
		res = cacheSize
	case "G", "g":
		res = cacheSize * 1024.
	case "T", "t":
		res = cacheSize * 1024. * 1024.
	}
	return res
}

func Create(cacheType string, cacheSize float64, cacheSizeUnit string, log bool, weightFunc string, weightFuncParams WeightFunctionParameters) Cache {
	logger := zap.L()
	var cacheInstance Cache
	cacheSizeMegabytes := GetCacheSize(cacheSize, cacheSizeUnit)
	switch cacheType {
	case "lru":
		logger.Info("Create LRU Cache",
			zap.Float64("cacheSize", cacheSizeMegabytes),
		)
		cacheInstance = &SimpleCache{
			MaxSize: cacheSizeMegabytes,
		}
	case "lfu":
		logger.Info("Create LFU Cache",
			zap.Float64("cacheSize", cacheSizeMegabytes),
		)
		cacheInstance = &SimpleCache{
			MaxSize: cacheSizeMegabytes,
		}
	case "sizeBig":
		logger.Info("Create Size Big Cache",
			zap.Float64("cacheSize", cacheSizeMegabytes),
		)
		cacheInstance = &SimpleCache{
			MaxSize: cacheSizeMegabytes,
		}
	case "sizeSmall":
		logger.Info("Create Size Small Cache",
			zap.Float64("cacheSize", cacheSizeMegabytes),
		)
		cacheInstance = &SimpleCache{
			MaxSize: cacheSizeMegabytes,
		}
	case "lruDatasetVerifier":
		logger.Info("Create lruDatasetVerifier Cache",
			zap.Float64("cacheSize", cacheSizeMegabytes),
		)
		cacheInstance = &LRUDatasetVerifier{
			SimpleCache: SimpleCache{
				MaxSize: cacheSizeMegabytes,
			},
		}
	case "aiNN":
		logger.Info("Create aiNN Cache",
			zap.Float64("cacheSize", cacheSizeMegabytes),
		)
		cacheInstance = &AINN{
			SimpleCache: SimpleCache{
				MaxSize: cacheSizeMegabytes,
			},
		}
	case "aiRL":
		logger.Info("Create aiRL Cache",
			zap.Float64("cacheSize", cacheSizeMegabytes),
		)
		cacheInstance = &AIRL{
			SimpleCache: SimpleCache{
				MaxSize: cacheSizeMegabytes,
			},
		}
	case "weightFunLRU":
		logger.Info("Create Weight Function Cache",
			zap.Float64("cacheSize", cacheSizeMegabytes),
		)

		var (
			selFunctionType FunctionType
		)

		switch weightFunc {
		case "FuncAdditive":
			selFunctionType = FuncAdditive
		case "FuncAdditiveExp":
			selFunctionType = FuncAdditiveExp
		case "FuncMultiplicative":
			selFunctionType = FuncMultiplicative
		case "FuncWeightedRequests":
			selFunctionType = FuncWeightedRequests
		default:
			fmt.Println("ERR: You need to specify a correct weight function.")
			os.Exit(-1)
		}

		cacheInstance = &WeightFun{
			SimpleCache: SimpleCache{
				MaxSize: cacheSizeMegabytes,
			},
			Parameters:      weightFuncParams,
			SelFunctionType: selFunctionType,
		}
	default:
		fmt.Printf("ERR: '%s' is not a valid cache type...\n", cacheType)
		os.Exit(-2)
	}
	return cacheInstance
}

type InitParameters struct {
	Log                    bool
	RedirectReq            bool
	Watermarks             bool
	Dataset2TestPath       string
	AIFeatureMap           string
	AIModel                string
	FunctionType           string
	WeightAlpha            float64
	WeightBeta             float64
	WeightGamma            float64
	SimUseK                bool
	AIRLEvictionK          int64
	AIRLAdditionFeatureMap string
	AIRLEvictionFeatureMap string
	AIRLEpsilonStart       float64
	AIRLEpsilonDecay       float64
}

func InitInstance(cacheType string, cacheInstance Cache, param InitParameters) {
	logger := zap.L()
	switch cacheType {
	case "lru":
		logger.Info("Init LRU Cache")
		InitCache(cacheInstance, LRUQueue, param.Log, param.RedirectReq, param.Watermarks)
	case "lfu":
		logger.Info("Init LFU Cache")
		InitCache(cacheInstance, LFUQueue, param.Log, param.RedirectReq, param.Watermarks)
	case "sizeBig":
		logger.Info("Init Size Big Cache")
		InitCache(cacheInstance, SizeBigQueue, param.Log, param.RedirectReq, param.Watermarks)
	case "sizeSmall":
		InitCache(cacheInstance, SizeSmallQueue, param.Log, param.RedirectReq, param.Watermarks)
	case "lruDatasetVerifier":
		logger.Info("Init lruDatasetVerifier Cache")
		InitCache(cacheInstance, param.Log, param.RedirectReq, param.Watermarks, param.Dataset2TestPath)
	case "aiNN":
		logger.Info("Init aiNN Cache")
		if param.AIFeatureMap == "" {
			fmt.Println("ERR: No feature map indicated...")
			os.Exit(-1)
		}
		InitCache(cacheInstance, param.Log, param.RedirectReq, param.Watermarks, param.AIFeatureMap, param.AIModel)
	case "aiRL":
		logger.Info("Init aiRL Cache")
		if param.AIRLAdditionFeatureMap == "" {
			logger.Info("No addition feature map indicated...")
		}
		if param.AIRLEvictionFeatureMap == "" {
			logger.Info("No eviction feature map indicated...")
		}

		var selFunctionType FunctionType
		switch param.FunctionType {
		case "FuncAdditive":
			selFunctionType = FuncAdditive
		case "FuncAdditiveExp":
			selFunctionType = FuncAdditiveExp
		case "FuncMultiplicative":
			selFunctionType = FuncMultiplicative
		case "FuncWeightedRequests":
			selFunctionType = FuncWeightedRequests
		default:
			fmt.Println("ERR: You need to specify a correct weight function.")
			os.Exit(-1)
		}

		InitCache(
			cacheInstance,
			param.Log,
			param.RedirectReq,
			param.Watermarks,
			param.SimUseK,
			param.AIRLEvictionK,
			param.AIRLAdditionFeatureMap,
			param.AIRLEvictionFeatureMap,
			param.AIRLEpsilonStart,
			param.AIRLEpsilonDecay,
			selFunctionType,
			param.WeightAlpha,
			param.WeightBeta,
			param.WeightGamma,
		)
	case "weightFunLRU":
		logger.Info("Init Weight Function Cache")
		InitCache(cacheInstance, LRUQueue, param.Log, param.RedirectReq, param.Watermarks)
	default:
		fmt.Printf("ERR: '%s' is not a valid cache type...\n", cacheType)
		os.Exit(-2)
	}
}
