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
	Name            string
	Type            reflect.Kind
	Int64Values     []int64
	Float64Values   []float64
	Buckets         bool
	BucketOpenRight bool
}

// FeatureManager collects and manages the features
type FeatureManager struct {
	Features []Obj
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

// FeatureI returns the feature at requested position
func (manager FeatureManager) FeatureI(idx int) Obj {
	return manager.Features[idx]
}

// FeatureIdexes returns a map of the feature indexes
func (manager FeatureManager) FeatureIdexes(idx int) map[string]int {
	indexes := make(map[string]int, 0)
	for idx, curObj := range manager.Features {
		indexes[curObj.Name] = idx
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
			curFeature := mapIter.Value().Interface().(map[string]interface{})

			if featureType := reflect.TypeOf(curFeature).Kind(); featureType == reflect.Map {
				featureIter := reflect.ValueOf(curFeature).MapRange()
				curStruct := Obj{
					Name: feature,
				}

				for featureIter.Next() {
					curFeatureKey := featureIter.Key().String()
					curFeatureValue := featureIter.Value()

					switch curFeatureKey {
					case "buckets":
						if curFeatureValue.Kind() == reflect.Slice {
							curSlice := curFeatureValue.Slice(0, curFeatureValue.Len())
							curStruct.Buckets = true
							switch curSlice.Type().Elem().Kind() {
							case reflect.Int64:
								curStruct.Type = reflect.Int64
								curStruct.Int64Values = curSlice.Interface().([]int64)
							case reflect.Float64:
								curStruct.Type = reflect.Float64
								curStruct.Float64Values = curSlice.Interface().([]float64)
							}
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
							switch curStruct.Type {
							case reflect.Int64:
								curStruct.Type = reflect.Int64
								curStruct.Int64Values = append(curStruct.Int64Values, math.MaxInt64)
							case reflect.Float64:
								curStruct.Type = reflect.Float64
								curStruct.Float64Values = append(curStruct.Float64Values, math.MaxFloat64)
							}
						}
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
				// Output the structure
				// fmt.Println(curStruct)

				manager.Features = append(manager.Features, curStruct)

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
		switch obj.Type {
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
		switch obj.Type {
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
func (obj Obj) Values() chan interface{} {
	outChan := make(chan interface{}, obj.Size())
	if obj.Buckets {
		switch obj.Type {
		case reflect.Int64:
			go func() {
				defer close(outChan)
				for _, elm := range obj.Int64Values {
					outChan <- elm
				}
			}()
		case reflect.Float64:
			go func() {
				defer close(outChan)
				for _, elm := range obj.Int64Values {
					outChan <- elm
				}
			}()
		}
	}
	return outChan
}

// Index returns the index of the value for the selected feature
func (obj Obj) Index(value interface{}) int {
	if obj.Buckets {
		switch obj.Type {
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
