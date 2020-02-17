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
	csvHeader = "Filename,SiteName,UserID,TaskID,TaskMonitorID,JobID,Protocol,JobExecExitCode,JobStart,JobEnd,NumCPU,WrapWC,WrapCPU,Size,DataType,FileType,JobLengthH,JobLengthM,JobSuccess,CPUTime,IOTime,reqDay"
)

var (
	/*
		CSV HEADER:
			-[0] Filename
			-[1] SiteName
			-[2] UserID
			-[3] TaskID
			-[4] TaskMonitorID
			-[5] JobID
			-[6] Protocol
			-[7] JobExecExitCode
			-[8] JobStart
			-[9] JobEnd
			-[10] NumCPU
			-[11] WrapWC
			-[12] WrapCPU
			-[13] Size
			-[14] DataType
			-[15] FileType
			-[16] JobLengthH
			-[17] JobLengthM
			-[18] JobSuccess
			-[19] CPUTime
			-[20] IOTime
			-[21] reqDay
	*/
	csvHeaderIndexes = []int{0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16, 17, 18, 19, 20, 21}
)

// SimulationStats is used to output the simulation statistics
type SimulationStats struct {
	TimeElapsed   string `json:"timeElapsed"`
	Extra         string `json:"extra"`
	TotNumRecords int    `json:"totNumRecords"`
	AvgSpeed      string `json:"avgSpeed"`
}

// CSVRecord is the base record composition readed from the logs
type CSVRecord struct {
	Day             int64   `json:"day"`
	Filename        int64   `json:"filename"`
	FileType        int     `json:"fileType"`
	DataType        int     `json:"dataType"`
	Protocol        int64   `json:"protocol"`
	TaskMonitorID   int64   `json:"taskMonitorID"`
	TaskID          int     `json:"taskID"`
	JobID           int     `json:"jobID"`
	SiteName        int     `json:"siteName"`
	JobExecExitCode int     `json:"jobExecExitCode"`
	JobStart        int64   `json:"jobStart"`
	JobEnd          int64   `json:"jobEnd"`
	JobSuccess      int64   `json:"jobSuccess"`
	JobLengthH      float32 `json:"jobLengthH"`
	JobLengthM      float32 `json:"jobLengthM"`
	UserID          int     `json:"user"`
	NumCPU          int     `json:"numCPU"`
	WrapWC          float32 `json:"WrapWC"`
	WrapCPU         float32 `json:"WrapCPU"`
	CPUTime         float32 `json:"CPUTime"`
	IOTime          float32 `json:"IOTime"`
	Size            float32 `json:"size"`
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
			siteName, _ := strconv.ParseInt(record[headerMap[1]], 10, 32)
			userID, _ := strconv.ParseInt(record[headerMap[2]], 10, 32)
			taskID, _ := strconv.ParseInt(record[headerMap[3]], 10, 32)
			taskMonitorID, _ := strconv.ParseInt(record[headerMap[4]], 10, 64)
			jobID, _ := strconv.ParseInt(record[headerMap[5]], 10, 32)
			protocol, _ := strconv.ParseInt(record[headerMap[6]], 10, 64)
			size, _ := strconv.ParseFloat(record[headerMap[13]], 32)
			dataType, _ := strconv.ParseInt(record[headerMap[14]], 10, 64)
			fileType, _ := strconv.ParseInt(record[headerMap[15]], 10, 64)
			joblengthh, _ := strconv.ParseFloat(record[headerMap[16]], 32)
			joblengthm, _ := strconv.ParseFloat(record[headerMap[17]], 32)
			jobSuccess, _ := strconv.ParseInt(record[headerMap[18]], 10, 64)
			cputime, _ := strconv.ParseFloat(record[headerMap[19]], 32)
			iotime, _ := strconv.ParseFloat(record[headerMap[20]], 32)
			day, _ := strconv.ParseFloat(record[headerMap[21]], 64)

			curRecord := CSVRecord{
				Day:           int64(day),
				Filename:      filename,
				FileType:      int(fileType),
				DataType:      int(dataType),
				Protocol:      protocol,
				TaskMonitorID: taskMonitorID,
				TaskID:        int(taskID),
				JobID:         int(jobID),
				SiteName:      int(siteName),
				JobSuccess:    jobSuccess,
				JobLengthH:    float32(joblengthh),
				JobLengthM:    float32(joblengthm),
				UserID:        int(userID),
				CPUTime:       float32(cputime),
				IOTime:        float32(iotime),
				Size:          float32(size),
			}

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
		logger.Info("FIle header", zap.String("CSV header", headerStr), zap.String("file", filePath))
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
		logger.Info("FIle header", zap.String("CSV header", headerStr), zap.String("file", filePath))
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
