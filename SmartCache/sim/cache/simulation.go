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
)

// CSVRecord is the base record composition readed from the logs
type CSVRecord struct {
	Index         int     `json:"index"`
	Day           int64   `json:"day"`
	Filename      string  `json:"filename"`
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

			index, _ := strconv.ParseInt(record[0], 10, 32)
			day, _ := strconv.ParseFloat(record[1], 64)
			taskID, _ := strconv.ParseInt(record[5], 10, 32)
			jobID, _ := strconv.ParseInt(record[6], 10, 32)
			joblengthh, _ := strconv.ParseFloat(record[9], 32)
			joblengthm, _ := strconv.ParseFloat(record[10], 32)
			userID, _ := strconv.ParseInt(record[11], 10, 32)
			cputime, _ := strconv.ParseFloat(record[12], 32)
			iotime, _ := strconv.ParseFloat(record[13], 32)
			size, _ := strconv.ParseFloat(record[14], 32)

			channel <- CSVRecord{
				Index:         int(index),
				Day:           int64(day),
				Filename:      record[2],
				Protocol:      record[3],
				TaskMonitorID: record[4],
				TaskID:        int(taskID),
				JobID:         int(jobID),
				SiteName:      record[7],
				JobSuccess:    record[8],
				JobLengthH:    float32(joblengthh),
				JobLengthM:    float32(joblengthm),
				UserID:        int(userID),
				CPUTime:       float32(cputime),
				IOTime:        float32(iotime),
				Size:          float32(size),
			}
		}
	}()
	return channel
}

// OpenSimFile opens a simulation file
func OpenSimFile(filePath string) chan CSVRecord {
	fileExt := path.Ext(filePath)
	var iterator chan CSVRecord

	switch fileExt {
	case ".csv":
		println("CSV")
	case ".gz":
		gzFile, _ := os.Open(filePath)
		// Create new reader to decompress gzip.
		gzReader, _ := gzip.NewReader(gzFile)
		csvReader := csv.NewReader(gzReader)
		// Discar header
		csvReader.Read()
		iterator = recordGenerator(csvReader, gzFile)
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
