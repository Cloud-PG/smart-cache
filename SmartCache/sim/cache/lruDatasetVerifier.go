package cache

import (
	"compress/gzip"
	"encoding/json"
	"log"
	"os"

	"simulator/v2/cache/files"
	"simulator/v2/cache/queue"
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
func (cache *LRUDatasetVerifier) Init(param InitParameters) interface{} {
	param.QueueType = queue.LRUQueue
	cache.SimpleCache.Init(param)

	cache.files = &queue.LRU{}
	cache.stats.Data = make(map[int64]*files.Stats)

	cache.datasetFileMap = make(map[int64]bool)
	datasetFilePath := param.Dataset2TestPath

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
func (cache *LRUDatasetVerifier) UpdatePolicy(request *Request, fileStats *files.Stats, hit bool) bool {
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
				queue.Insert(cache.files, fileStats)

				cache.size += requestedFileSize
				added = true
			}
		}
	} else {
		queue.Update(cache.files, fileStats)
	}

	return added
}
