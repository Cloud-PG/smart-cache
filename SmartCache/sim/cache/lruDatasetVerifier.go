package cache

import (
	"compress/gzip"
	"container/list"
	"encoding/json"
	"log"
	"os"
)

// DatasetFiles represents the dataset file composition
type DatasetFiles struct {
	SelectedFiles []string `json:"selected_files"`
}

// LRUDatasetVerifier cache
type LRUDatasetVerifier struct {
	LRUCache
	datasetFileMap map[string]bool
}

// Init the LRU struct
func (cache *LRUDatasetVerifier) Init(args ...interface{}) interface{} {
	cache.LRUCache.Init()

	cache.files = make(map[string]float32)
	cache.stats = make(map[string]*LRUFileStats)
	cache.queue = list.New()

	cache.datasetFileMap = make(map[string]bool)
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

func (cache *LRUDatasetVerifier) updatePolicy(filename string, size float32, hit bool, _ ...interface{}) bool {
	var added = false
	_, inDataset := cache.datasetFileMap[filename]
	if inDataset {
		if !hit {
			if cache.Size()+size > cache.MaxSize {
				var totalDeleted float32
				tmpVal := cache.queue.Front()
				for {
					if tmpVal == nil {
						break
					}
					fileSize := cache.files[tmpVal.Value.(string)]
					cache.size -= fileSize
					cache.dataDeleted += size

					totalDeleted += fileSize
					delete(cache.files, tmpVal.Value.(string))

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
	}
	return added
}
