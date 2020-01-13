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
	Day           int64   `json:"day"`
	Filename      string  `json:"filename"`
	FileType      string  `json:"fileType"`
	Protocol      string  `json:"protocol"`
	TaskMonitorID string  `json:"taskMonitorID"`
	TaskID        int     `json:"taskID"`
	JobID         int     `json:"jobID"`
	SiteName      string  `json:"siteName"`
	JobSuccess    string  `json:"jobSuccess"`
	JobLengthH    float32 `json:"jobLengthH"`
	JobLengthM    float32 `json:"jobLengthM"`
	UserID        int     `json:"user"`
	CPUTime       float32 `json:"CPUTime"`
	IOTime        float32 `json:"IOTime"`
	Size          float32 `json:"size"`
}

func recordGenerator(csvReader *csv.Reader, curFile *os.File) chan CSVRecord {
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

			day, _ := strconv.ParseFloat(record[0], 64)
			taskID, _ := strconv.ParseInt(record[4], 10, 32)
			jobID, _ := strconv.ParseInt(record[5], 10, 32)
			joblengthh, _ := strconv.ParseFloat(record[8], 32)
			joblengthm, _ := strconv.ParseFloat(record[9], 32)
			userID, _ := strconv.ParseInt(record[10], 10, 32)
			cputime, _ := strconv.ParseFloat(record[11], 32)
			iotime, _ := strconv.ParseFloat(record[12], 32)
			size, _ := strconv.ParseFloat(record[13], 32)

			filename := record[1]
			tmpSplit := strings.Split(filename, "/")
			// dataType := tmpSplit[2]
			// campain := tmpSplit[3]
			// process := tmpSplit[4]
			fileType := tmpSplit[5]

			curRecord := CSVRecord{
				Day:           int64(day),
				Filename:      filename,
				FileType:      fileType,
				Protocol:      record[2],
				TaskMonitorID: record[3],
				TaskID:        int(taskID),
				JobID:         int(jobID),
				SiteName:      record[6],
				JobSuccess:    record[7],
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
	fileExt := path.Ext(filePath)
	var iterator chan CSVRecord

	curFile, errOpenFile := os.Open(filePath)
	if errOpenFile != nil {
		panic(errOpenFile)
	}

	switch fileExt {
	case ".gz", ".gzip":
		// Create new reader to decompress gzip.
		curCsv, errReadGz := gzip.NewReader(curFile)
		if errReadGz != nil {
			panic(errReadGz)
		}
		csvReader := csv.NewReader(curCsv)
		// Discar header
		csvReader.Read()
		iterator = recordGenerator(csvReader, curFile)
	default:
		csvReader := csv.NewReader(curFile)
		// Discar header
		csvReader.Read()
		iterator = recordGenerator(csvReader, curFile)
	}

	return iterator
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

// SimulationOutputCSV
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
