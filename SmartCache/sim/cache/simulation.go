package cache

import (
	"compress/gzip"
	"encoding/csv"
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
	csvHeader = "Filename,SiteName,UserID,TaskID,TaskMonitorID,JobID,Protocol,JobExecExitCode,JobStart,JobEnd,NumCPU,WrapWC,WrapCPU,Size,DataType,FileType,JobLengthH,JobLengthM,JobSuccess,CPUTime,IOTime,reqDay,Region,Campain,Process"
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
	TimeElapsed        string `json:"timeElapsed"`
	Extra              string `json:"extra"`
	TotNumRecords      int64  `json:"totNumRecords"`
	TotJumpedRecords   int64  `json:"totJumpedRecords"`
	TotInvalidRecords  int64  `json:"totInvalidRecords"`
	TotFilteredRecords int64  `json:"totFilteredRecords"`
	AvgSpeed           string `json:"avgSpeed"`
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
	JobSuccess      int64   `json:"jobSuccess"`
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
	channel := make(chan CSVRecord)
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
			jobSuccess, _ := strconv.ParseInt(record[headerMap[18]], 10, 64)
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
	filename  string
	file      *os.File
	csvWriter *csv.Writer
}

// Create an output file in CSV format
func (output *OutputCSV) Create(filename string) {
	output.filename = filename
	outputFile, errCreateFile := os.Create(output.filename)
	if errCreateFile != nil {
		panic(errCreateFile)
	}
	output.file = outputFile

	output.csvWriter = csv.NewWriter(output.file)
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
	output.file.Close()
}

// Filter interface
type Filter interface {
	Check(CSVRecord) bool
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
