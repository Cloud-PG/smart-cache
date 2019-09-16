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
	TaskID        string  `json:"taskID"`
	JobID         string  `json:"jobID"`
	SiteName      string  `json:"siteName"`
	JobSuccess    string  `json:"jobSuccess"`
	JobLengthH    float32 `json:"jobLengthH"`
	JobLengthM    float32 `json:"jobLengthM"`
	User          string  `json:"user"`
	CPUTime       float32 `json:"CPUTime"`
	IOTime        float32 `json:"IOTime"`
	Size          float32 `json:"size"`
}

func recordGenerator(csvReader *csv.Reader, curFile *os.File) chan CSVRecord {
	channel := make(chan CSVRecord)
	go func() {
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
			joblengthh, _ := strconv.ParseFloat(record[9], 32)
			joblengthm, _ := strconv.ParseFloat(record[10], 32)
			cputime, _ := strconv.ParseFloat(record[12], 32)
			iotime, _ := strconv.ParseFloat(record[13], 32)
			size, _ := strconv.ParseFloat(record[14], 32)

			channel <- CSVRecord{
				int(index),
				int64(day),
				record[2],
				record[3],
				record[4],
				record[5],
				record[6],
				record[7],
				record[8],
				float32(joblengthh),
				float32(joblengthm),
				record[11],
				float32(cputime),
				float32(iotime),
				float32(size),
			}
		}
		close(channel)
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
		for _, name := range fileList {
			for record := range OpenSimFile(name) {
				channel <- record
			}
		}
		close(channel)
	}()

	return channel
}
