package cache

import (
	"compress/gzip"
	"encoding/json"
	"log"
	"os"
)

// DatasetFiles represents the dataset file composition
type DatasetFiles struct {
	SelectedFiles []int64 `json:"selected_files"`
}

// LRUDatasetVerifier cache
type LRUDatasetVerifier struct {
	SimpleCache
	datasetFileMap map[int64]bool
}

// Init the LRU struct
func (cache *LRUDatasetVerifier) Init(args ...interface{}) interface{} {
	cache.SimpleCache.Init()

	cache.files = Manager{}
	cache.stats.fileStats = make(map[int64]*FileStats)

	cache.datasetFileMap = make(map[int64]bool)
	datasetFilePath := args[0].(string)

	datasetFile, errOpenFile := os.Open(datasetFilePath)
	if errOpenFile != nil {
		log.Fatalf("Cannot open file '%s'\n", errOpenFile)
	}

	datasetFileGz, errOpenZipFile := gzip.NewReader(datasetFile)
	if errOpenZipFile != nil {
		log.Fatalf("Cannot open zip stream from file '%s'\nError: %s\n", datasetFilePath, errOpenZipFile)
	}

	var datasetFiles *DatasetFiles
	errJSONUnmarshal := json.NewDecoder(datasetFileGz).Decode(&datasetFiles)
	if errJSONUnmarshal != nil {
		log.Fatalf("Cannot unmarshal json from file '%s'\nError: %s\n", datasetFilePath, errJSONUnmarshal)
	}

	for _, fileName := range datasetFiles.SelectedFiles {
		cache.datasetFileMap[fileName] = true
	}

	return cache
}

// UpdatePolicy of LRUDatasetVerifier cache
func (cache *LRUDatasetVerifier) UpdatePolicy(request *Request, fileStats *FileStats, hit bool) bool {
	var (
		added = false

		requestedFileSize = request.Size
		requestedFilename = request.Filename
	)
	_, inDataset := cache.datasetFileMap[requestedFilename]

	if !hit {
		if inDataset {
			if cache.Size()+requestedFileSize > cache.MaxSize {
				cache.Free(requestedFileSize, false)
			}
			if cache.Size()+requestedFileSize <= cache.MaxSize {
				cache.files.Insert(&FileSupportData{
					Filename:  request.Filename,
					Size:      request.Size,
					Frequency: fileStats.Frequency,
					Recency:   fileStats.DeltaLastRequest,
				})

				cache.size += requestedFileSize
				fileStats.addInCache(cache.tick, nil)
				added = true
			}
		}
	} else {
		cache.files.Update(&FileSupportData{
			Filename:  request.Filename,
			Size:      request.Size,
			Frequency: fileStats.Frequency,
			Recency:   fileStats.DeltaLastRequest,
		})
	}

	return added
}
