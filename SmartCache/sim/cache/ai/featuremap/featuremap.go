package featuremap

import (
	"compress/gzip"
	"encoding/json"
	"fmt"
	"math"
	"os"
	"path/filepath"
	"reflect"

	"go.uber.org/zap"
)

type mapType int

// Obj represents a map object
type Obj struct {
	Name            string       `json:"name"`
	Type            string       `json:"type"`
	ReflectType     reflect.Kind `json:"reflectType"`
	Int64Values     []int64      `json:"int64Values"`
	Float64Values   []float64    `json:"float64Values"`
	Buckets         bool         `json:"buckets"`
	BucketOpenRight bool         `json:"bucketOpenRight"`
	FileFeature     bool         `json:"fileFeature"`
}

// ObjVal used in range cycles
type ObjVal struct {
	Idx int
	Val interface{}
}

// FeatureManager collects and manages the features
type FeatureManager struct {
	Features        []Obj `json:"feature"`
	FileFeatureIdxs []int `json:"fileFeatureIdxs"`
}

// FileFeatureIter returns all the file feature objects
func (manager FeatureManager) FileFeatureIter() chan Obj {
	outChan := make(chan Obj, len(manager.FileFeatureIdxs))
	go func() {
		defer close(outChan)
		for _, objIdx := range manager.FileFeatureIdxs {
			outChan <- manager.Features[objIdx]
		}
	}()
	return outChan
}

// FileFeatureIdxWeights returns the weight for each index of file features
func (manager FeatureManager) FileFeatureIdxWeights() []int {
	weights := make([]int, len(manager.FileFeatureIdxs))
	for idx := 0; idx < len(weights); idx++ {
		weights[idx] = 1
		for inIdx := idx + 1; inIdx < len(weights); inIdx++ {
			weights[idx] *= manager.Features[manager.FileFeatureIdxs[inIdx]].Size()
		}
	}
	return weights
}

// FeatureIdxWeights returns the weight for each index of the features
func (manager FeatureManager) FeatureIdxWeights() []int {
	weights := make([]int, len(manager.Features))
	for idx := 0; idx < len(weights); idx++ {
		weights[idx] = 1
		for inIdx := idx + 1; inIdx < len(weights); inIdx++ {
			weights[idx] *= manager.Features[inIdx].Size()
		}
	}
	return weights
}

// FeatureIter returns all the feature objects
func (manager FeatureManager) FeatureIter() chan Obj {
	outChan := make(chan Obj, len(manager.Features))
	go func() {
		defer close(outChan)
		for _, curObj := range manager.Features {
			outChan <- curObj
		}
	}()
	return outChan
}

// FeatureIdexMap returns a map of the feature indexes
func (manager FeatureManager) FeatureIdexMap() map[string]int {
	indexes := make(map[string]int, 0)
	for idx, curObj := range manager.Features {
		indexes[curObj.Name] = idx
	}
	return indexes
}

// FileFeatureIdexMap returns a map of the file feature indexes
func (manager FeatureManager) FileFeatureIdexMap() map[string]int {
	indexes := make(map[string]int, 0)
	for idx, curObjIdx := range manager.FileFeatureIdxs {
		indexes[manager.Features[curObjIdx].Name] = idx
	}
	return indexes
}

// Parse a feature map file and returns the map of keys and objects
func Parse(featureMapFilePath string) FeatureManager {
	manager := FeatureManager{}
	manager.Features = make([]Obj, 0)
	manager.Populate(featureMapFilePath)
	return manager
}

// Populate reads the feature map files and populates the manager
func (manager *FeatureManager) Populate(featureMapFilePath string) {
	logger := zap.L()

	var tmpMap interface{}
	fileExtension := filepath.Ext(featureMapFilePath)

	featureMapFile, errOpenFile := os.Open(featureMapFilePath)
	if errOpenFile != nil {
		logger.Error("Cannot open file", zap.Error(errOpenFile))
		os.Exit(-1)
	}

	if fileExtension == ".gzip" || fileExtension == ".gz" {
		featureMapFileGz, errOpenZipFile := gzip.NewReader(featureMapFile)
		if errOpenZipFile != nil {
			logger.Error("Cannot open zip stream from file", zap.String("filename", featureMapFilePath), zap.Error(errOpenZipFile))
			os.Exit(-1)
		}

		errJSONUnmarshal := json.NewDecoder(featureMapFileGz).Decode(&tmpMap)
		if errJSONUnmarshal != nil {
			logger.Error("Cannot unmarshal gzipped json from file", zap.String("filename", featureMapFilePath), zap.Error(errJSONUnmarshal))
			os.Exit(-1)
		}
	} else if fileExtension == ".json" {
		errJSONUnmarshal := json.NewDecoder(featureMapFile).Decode(&tmpMap)
		if errJSONUnmarshal != nil {
			logger.Error("Cannot unmarshal plain json from file", zap.String("filename", featureMapFilePath), zap.Error(errJSONUnmarshal))
			os.Exit(-1)
		}
	} else {
		logger.Error("Cannot unmarshal", zap.String("filename", featureMapFilePath), zap.String("extension", fileExtension))
		os.Exit(-1)
	}

	defer featureMapFile.Close()

	if mainType := reflect.TypeOf(tmpMap).Kind(); mainType == reflect.Map {
		mapIter := reflect.ValueOf(tmpMap).MapRange()
		for mapIter.Next() {
			feature := mapIter.Key().String()
			curFeature := mapIter.Value()

			if featureType := curFeature.Elem().Kind(); featureType == reflect.Map {
				featureIter := curFeature.Elem().MapRange()
				curStruct := Obj{
					Name: feature,
				}

				var bucketValues []interface{}

				for featureIter.Next() {
					curFeatureKey := featureIter.Key().String()
					curFeatureValue := featureIter.Value()

					switch curFeatureKey {
					case "buckets":
						if curFeatureValue.Elem().Kind() == reflect.Slice {
							bucketValues = curFeatureValue.Elem().Interface().([]interface{})
							curStruct.Buckets = true
						} else {
							logger.Error(
								"Feature entries",
								zap.String(
									"error",
									fmt.Sprintf("bucket of %s is not a slice", feature),
								),
							)
							os.Exit(-1)
						}
					case "openRight":
						if curFeatureValue.Elem().Bool() {
							curStruct.BucketOpenRight = true
						}
					case "fileFeature":
						if curFeatureValue.Elem().Bool() {
							curStruct.FileFeature = true
						}
					case "type":
						curStruct.Type = curFeatureValue.Elem().String()
					default:
						logger.Error(
							"Feature entries",
							zap.String(
								"error",
								fmt.Sprintf("entry %s of  %s is not allowed", curFeatureKey, feature),
							),
						)
						os.Exit(-1)
					}
				}

				switch curStruct.Type {
				case "int":
					// fmt.Println("int", reflect.TypeOf(bucketValues[0]).Kind())
					curStruct.ReflectType = reflect.Int64
					curStruct.Int64Values = make([]int64, len(bucketValues))
					for idx, val := range bucketValues {
						curStruct.Int64Values[idx] = int64(val.(float64)) // numbers from JSON are always floats
					}
					curStruct.Int64Values = append(curStruct.Int64Values, math.MaxInt64)
				case "float":
					// fmt.Println("float", reflect.TypeOf(bucketValues[0]).Kind())
					curStruct.ReflectType = reflect.Float64
					curStruct.Float64Values = make([]float64, len(bucketValues))
					for idx, val := range bucketValues {
						curStruct.Float64Values[idx] = val.(float64)
					}
					curStruct.Float64Values = append(curStruct.Float64Values, math.MaxFloat64)
				}

				// fmt.Println("struct", curStruct)

				manager.Features = append(manager.Features, curStruct)

				if curStruct.FileFeature {
					manager.FileFeatureIdxs = append(manager.FileFeatureIdxs, len(manager.Features)-1)
				}

			} else {
				logger.Error(
					"Feature entries",
					zap.String(
						"error",
						fmt.Sprintf("feature %s is not a valid map", feature),
					),
				)
				os.Exit(-1)
			}
		}
	} else {
		logger.Error("Feature entries", zap.String("error", "Not a valid feature JSON"))
		os.Exit(-1)
	}
}

// Size returns the number of possible elements
func (obj Obj) Size() int {
	if obj.Buckets {
		switch obj.ReflectType {
		case reflect.Int64:
			return len(obj.Int64Values)
		case reflect.Float64:
			return len(obj.Float64Values)
		}
	}
	return -1
}

// Value returns the value (as interface) of a specific index of a feature
func (obj Obj) Value(idx int) interface{} {
	if obj.Buckets {
		switch obj.ReflectType {
		case reflect.Int64:
			if obj.BucketOpenRight {
				return math.MaxInt64
			}
			return obj.Int64Values[idx]

		case reflect.Float64:
			if obj.BucketOpenRight {
				return math.MaxFloat64
			}
			return obj.Float64Values[idx]

		}
	}
	return nil
}

// Values returns all the values (as interface generator) of the feature
func (obj Obj) Values() chan ObjVal {
	outChan := make(chan ObjVal, obj.Size())
	if obj.Buckets {
		switch obj.ReflectType {
		case reflect.Int64:
			go func() {
				defer close(outChan)
				for idx, elm := range obj.Int64Values {
					outChan <- ObjVal{
						Idx: idx,
						Val: elm,
					}
				}
			}()
		case reflect.Float64:
			go func() {
				defer close(outChan)
				for idx, elm := range obj.Float64Values {
					outChan <- ObjVal{
						Idx: idx,
						Val: elm,
					}
				}
			}()
		}
	}
	return outChan
}

// Index returns the index of the value for the selected feature
func (obj Obj) Index(value interface{}) int {
	if obj.Buckets {
		switch obj.ReflectType {
		case reflect.Int64:
			curVal := value.(int64)
			for idx, val := range obj.Int64Values {
				if curVal <= val {
					return idx
				}
			}
		case reflect.Float64:
			curVal := value.(float64)
			for idx, val := range obj.Float64Values {
				if curVal <= val {
					return idx
				}
			}
		}
	}
	return -1
}
