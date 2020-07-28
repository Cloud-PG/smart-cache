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

// Obj represents a map object
type Obj struct {
	Name            string       `json:"name"`
	Type            string       `json:"type"`
	ReflectType     reflect.Kind `json:"reflectType"`
	StringValues    []string     `json:"stringValues"`
	BoolValues      []bool       `json:"boolValues"`
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
	Features              []Obj          `json:"feature"`
	FileFeatures          []Obj          `json:"fileFeature"`
	FeatureIdxMap         map[string]int `json:"featureIdxMap"`
	FileFeatureIdxMap     map[string]int `json:"fileFeatureIdxMap"`
	FeatureIdxWeights     []int          `json:"featureIdxWeights"`
	FileFeatureIdxWeights []int          `json:"fileFeatureIdxWeights"`
}

func (manager *FeatureManager) makeFileFeatureIdxWeights() {
	manager.FileFeatureIdxWeights = make([]int, len(manager.FileFeatures))
	for idx := 0; idx < len(manager.FileFeatureIdxWeights); idx++ {
		manager.FileFeatureIdxWeights[idx] = 1
		for inIdx := idx + 1; inIdx < len(manager.FileFeatureIdxWeights); inIdx++ {
			manager.FileFeatureIdxWeights[idx] *= manager.FileFeatures[inIdx].Size()
		}
	}
}

func (manager *FeatureManager) makeFeatureIdxWeights() {
	manager.FeatureIdxWeights = make([]int, len(manager.Features))
	for idx := 0; idx < len(manager.FeatureIdxWeights); idx++ {
		manager.FeatureIdxWeights[idx] = 1
		for inIdx := idx + 1; inIdx < len(manager.FeatureIdxWeights); inIdx++ {
			manager.FeatureIdxWeights[idx] *= manager.Features[inIdx].Size()
		}
	}
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

	defer func() {
		closeErr := featureMapFile.Close()
		if closeErr != nil {
			panic(closeErr)
		}
	}()

	manager.Features = make([]Obj, 0)
	manager.FileFeatures = make([]Obj, 0)
	manager.FeatureIdxMap = make(map[string]int)
	manager.FileFeatureIdxMap = make(map[string]int)

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

				var itemValues []interface{}

				for featureIter.Next() {
					curFeatureKey := featureIter.Key().String()
					curFeatureValue := featureIter.Value()

					switch curFeatureKey {
					case "buckets", "values":
						if curFeatureValue.Elem().Kind() == reflect.Slice {
							itemValues = curFeatureValue.Elem().Interface().([]interface{})
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
				case "bool":
					// fmt.Println("int", reflect.TypeOf(itemValues[0]).Kind())
					curStruct.ReflectType = reflect.Bool
					curStruct.BoolValues = make([]bool, len(itemValues))
					for idx, val := range itemValues {
						curStruct.BoolValues[idx] = val.(bool)
					}
				case "string":
					// fmt.Println("int", reflect.TypeOf(itemValues[0]).Kind())
					curStruct.ReflectType = reflect.String
					curStruct.StringValues = make([]string, len(itemValues))
					for idx, val := range itemValues {
						curStruct.StringValues[idx] = val.(string)
					}
				case "int":
					// fmt.Println("int", reflect.TypeOf(itemValues[0]).Kind())
					curStruct.ReflectType = reflect.Int64
					curStruct.Int64Values = make([]int64, len(itemValues))
					for idx, val := range itemValues {
						curStruct.Int64Values[idx] = int64(val.(float64)) // numbers from JSON are always floats
					}
					if curStruct.BucketOpenRight {
						curStruct.Int64Values = append(curStruct.Int64Values, math.MaxInt64)
					}
				case "float":
					// fmt.Println("float", reflect.TypeOf(itemValues[0]).Kind())
					curStruct.ReflectType = reflect.Float64
					curStruct.Float64Values = make([]float64, len(itemValues))
					for idx, val := range itemValues {
						curStruct.Float64Values[idx] = val.(float64)
					}
					if curStruct.BucketOpenRight {
						curStruct.Float64Values = append(curStruct.Float64Values, math.MaxFloat64)
					}
				}

				// fmt.Println("struct", curStruct)

				curFeatureIdx := len(manager.Features)
				manager.FeatureIdxMap[curStruct.Name] = curFeatureIdx
				manager.Features = append(manager.Features, curStruct)

				if curStruct.FileFeature {
					curFileFeatureIdx := len(manager.FileFeatures)
					manager.FileFeatureIdxMap[curStruct.Name] = curFileFeatureIdx
					manager.FileFeatures = append(manager.FileFeatures, curStruct)
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

	manager.makeFeatureIdxWeights()
	manager.makeFileFeatureIdxWeights()
}

// Size returns the number of possible elements
func (obj Obj) Size() int {
	if obj.Buckets {
		switch obj.ReflectType {
		case reflect.String:
			return len(obj.StringValues)
		case reflect.Bool:
			return len(obj.BoolValues)
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
		case reflect.String:
			return obj.StringValues[idx]
		case reflect.Bool:
			return obj.BoolValues[idx]
		case reflect.Int64:
			return obj.Int64Values[idx]
		case reflect.Float64:
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
		case reflect.String:
			go func() {
				defer close(outChan)
				for idx, elm := range obj.StringValues {
					outChan <- ObjVal{
						Idx: idx,
						Val: elm,
					}
				}
			}()
		case reflect.Bool:
			go func() {
				defer close(outChan)
				for idx, elm := range obj.BoolValues {
					outChan <- ObjVal{
						Idx: idx,
						Val: elm,
					}
				}
			}()
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
		case reflect.String:
			curVal := value.(string)
			for idx, val := range obj.StringValues {
				if curVal == val {
					return idx
				}
			}
		case reflect.Bool:
			curVal := value.(bool)
			for idx, val := range obj.BoolValues {
				if curVal == val {
					return idx
				}
			}
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
