package featuremap

import (
	"compress/gzip"
	"encoding/json"
	"fmt"
	"math"
	"os"
	"path/filepath"
	"reflect"

	"github.com/rs/zerolog/log"
)

// Obj represents a map object.
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

// ObjVal used in range cycles.
type ObjVal struct {
	Idx int
	Val interface{}
}

// FeatureManager collects and manages the features.
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

// Parse a feature map file and returns the map of keys and objects.
func Parse(featureMapFilePath string) FeatureManager {
	manager := FeatureManager{}
	manager.Features = make([]Obj, 0)
	manager.Populate(featureMapFilePath)

	return manager
}

// Populate reads the feature map files and populates the manager.
func (manager *FeatureManager) Populate(featureMapFilePath string) { //nolint:ignore,funlen
	var tmpMap interface{}

	fileExtension := filepath.Ext(featureMapFilePath)

	featureMapFile, errOpenFile := os.Open(featureMapFilePath)
	if errOpenFile != nil {
		log.Err(errOpenFile).Msg("Cannot open file")
		os.Exit(-1)
	}

	switch {
	case fileExtension == ".gzip" || fileExtension == ".gz":
		featureMapFileGz, errOpenZipFile := gzip.NewReader(featureMapFile)
		if errOpenZipFile != nil {
			log.Err(errOpenZipFile).Str("filename", featureMapFilePath).Msg("Cannot open zip stream from file")
			os.Exit(-1)
		}

		errJSONUnmarshal := json.NewDecoder(featureMapFileGz).Decode(&tmpMap)
		if errJSONUnmarshal != nil {
			log.Err(errJSONUnmarshal).Str("filename", featureMapFilePath).Msg("Cannot unmarshal gzipped json from file")
			os.Exit(-1)
		}
	case fileExtension == ".json":
		errJSONUnmarshal := json.NewDecoder(featureMapFile).Decode(&tmpMap)
		if errJSONUnmarshal != nil {
			log.Err(errJSONUnmarshal).Str("filename", featureMapFilePath).Msg("Cannot unmarshal plain json from file")
			os.Exit(-1)
		}
	default:
		log.Err(nil).Str("filename", featureMapFilePath).Msg("Cannot unmarshal")
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

	if mainType := reflect.TypeOf(tmpMap).Kind(); mainType == reflect.Map { //nolint:ignore,nestif
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
							log.Err(nil).Str(
								"error",
								fmt.Sprintf("bucket of %s is not a slice", feature),
							).Msg(
								"Feature entries",
							)

							panic("Error: deconding features")
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
						log.Err(nil).Str(
							"error",
							fmt.Sprintf("entry %s of  %s is not allowed", curFeatureKey, feature),
						).Msg(
							"Feature entries",
						)

						panic("Error: deconding features")
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
				log.Err(nil).Str("error",
					fmt.Sprintf("feature %s is not a valid map", feature),
				).Msg("Feature entries")
				panic("ERROR: decoding features")
			}
		}
	} else {
		log.Err(nil).Str("error", "Not a valid feature JSON").Msg("Feature entries")
		panic("ERROR: decoding features")
	}

	manager.makeFeatureIdxWeights()
	manager.makeFileFeatureIdxWeights()
}

// Size returns the number of possible elements.
func (obj Obj) Size() int {
	if obj.Buckets {
		switch obj.ReflectType { //nolint:ignore,exaustive
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

// Value returns the value (as interface) of a specific index of a feature.
func (obj Obj) Value(idx int) interface{} {
	if obj.Buckets {
		switch obj.ReflectType { //nolint:ignore,exaustive
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

// Values returns all the values (as interface generator) of the feature.
func (obj Obj) Values() chan ObjVal {
	outChan := make(chan ObjVal, obj.Size())

	if obj.Buckets {
		switch obj.ReflectType { //nolint:ignore,exaustive
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

// Index returns the index of the value for the selected feature.
func (obj Obj) Index(value interface{}) int { //nolint:ignore,gocognit
	if obj.Buckets {
		switch obj.ReflectType { //nolint:ignore,exaustive
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

// ToString transform the given index feature object value to a string.
func (obj Obj) ToString(valIdx int) string {
	outString := ""

	switch obj.ReflectType { //nolint:ignore,exaustive
	case reflect.String:
		outString = obj.StringValues[valIdx]
	case reflect.Bool:
		curFeatureVal := obj.BoolValues[valIdx]
		outString = fmt.Sprintf("%t", curFeatureVal)
	case reflect.Int64:
		curFeatureVal := obj.Int64Values[valIdx]
		if curFeatureVal == math.MaxInt64 {
			outString = "max"
		} else {
			outString = fmt.Sprintf("%d", curFeatureVal)
		}
	case reflect.Float64:
		curFeatureVal := obj.Float64Values[valIdx]
		if curFeatureVal == math.MaxFloat64 {
			outString = "max"
		} else {
			outString = fmt.Sprintf("%09.2f", curFeatureVal)
		}
	}

	return outString
}
