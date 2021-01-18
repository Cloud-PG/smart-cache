package cmd

import (
	"fmt"
	"os"
	"path/filepath"
	"simulator/v2/cache"
	"strings"

	"github.com/rs/zerolog/log"
)

// Create simulation cache
func CreateCache(conf interface{}) (baseName string, cacheInstance cache.Cache) { //nolint:ignore,funlen
	var (
		runType             string
		simOverwrite        bool
		outputFolder        string
		cacheType           string
		cacheSizeValue      uint
		cacheSizeUnit       string
		cacheBandwidthValue uint
		weightFunctionName  string
		weightFunctionAlpha float64
		weightFunctionBeta  float64
		weightFunctionGamma float64
		aiRLType            string
		aiRLEvictionType    string
		region              string
	)

	switch curConf := conf.(type) {
	case SimConfig:
		cacheType = curConf.Sim.Cache.Type
		cacheSizeValue = curConf.Sim.Cache.Size.Value
		cacheSizeUnit = curConf.Sim.Cache.Size.Unit
		cacheBandwidthValue = curConf.Sim.Cache.Bandwidth.Value
		weightFunctionName = curConf.Sim.WeightFunction.Name
		weightFunctionAlpha = curConf.Sim.WeightFunction.Alpha
		weightFunctionBeta = curConf.Sim.WeightFunction.Beta
		weightFunctionGamma = curConf.Sim.WeightFunction.Gamma
		aiRLType = curConf.Sim.AI.RL.Type
		aiRLEvictionType = curConf.Sim.AI.RL.Eviction.Type
		region = curConf.Sim.Region
		runType = curConf.Sim.Type
		simOverwrite = curConf.Sim.Overwrite
		outputFolder = curConf.Sim.OutputFolder
	case ServiceConfig:
		cacheType = curConf.Service.Cache.Type
		cacheSizeValue = curConf.Service.Cache.Size.Value
		cacheSizeUnit = curConf.Service.Cache.Size.Unit
		cacheBandwidthValue = curConf.Service.Cache.Bandwidth.Value
		weightFunctionName = curConf.Service.WeightFunction.Name
		weightFunctionAlpha = curConf.Service.WeightFunction.Alpha
		weightFunctionBeta = curConf.Service.WeightFunction.Beta
		weightFunctionGamma = curConf.Service.WeightFunction.Gamma
		aiRLType = curConf.Service.AI.RL.Type
		aiRLEvictionType = curConf.Service.AI.RL.Eviction.Type
		outputFolder = curConf.Service.OutputFolder
	}

	// Generate simulation file output basename
	cacheSizeString := fmt.Sprintf("%d%s",
		cacheSizeValue,
		strings.ToUpper(cacheSizeUnit),
	)
	cacheBandwidthString := fmt.Sprintf("%dGbit", cacheBandwidthValue)

	switch cacheType {
	case "weightFunLRU":
		parameters := strings.Join([]string{
			fmt.Sprintf("%0.2f", weightFunctionAlpha),
			fmt.Sprintf("%0.2f", weightFunctionBeta),
			fmt.Sprintf("%0.2f", weightFunctionGamma),
		}, "_")
		baseName = strings.Join([]string{
			cacheType,
			weightFunctionName,
			parameters,
		}, "_")
	case "aiRL", "airl", "aiRl", "AIRL":
		subAIType := aiRLType
		if aiRLType == "SCDL2" {
			subAIType += "-" + aiRLEvictionType
		}
		baseName = strings.Join([]string{
			cacheType,
			subAIType,
			cacheSizeString,
			cacheBandwidthString,
			region,
		}, "_")
	default:
		baseName = strings.Join([]string{
			cacheType,
			cacheSizeString,
			cacheBandwidthString,
			region,
		}, "_")
	}

	// Output files
	resultFileName := "simulation_results.csv"

	// Create output folder and move working dir
	switch runType { //nolint:ignore,nestif
	case "normal":
		finalOutputFolder := filepath.Join(outputFolder, "run_full_normal", baseName)

		errMkdir := os.MkdirAll(finalOutputFolder, 0755)
		if errMkdir != nil && !os.IsExist(errMkdir) {
			panic(errMkdir)
		}

		errChdir := os.Chdir(finalOutputFolder)
		if errChdir != nil {
			panic(errChdir)
		}

		curWd, _ := os.Getwd()
		log.Info().Str("path", curWd).Msg("Current Working Dir")
	}

	switch conf.(type) {
	case SimConfig:
		// Check previous simulation results
		if !simOverwrite { //nolint:ignore,nestif
			fileStat, errStat := os.Stat(resultFileName)
			if errStat != nil {
				if !os.IsNotExist(errStat) {
					panic(errStat)
				}
			} else {
				if fileStat.Size() > 600 {
					// TODO: check if the configuration is the same
					log.Info().Msg("Simulation already DONE! NO OVERWRITE...")

					os.Exit(0)
				} else {
					log.Info().Msg("Simulation results is empty... OVERWRITE...")
				}
			}
		}
	}

	cacheSizeMegabytes := cache.GetCacheSize(float64(cacheSizeValue), cacheSizeUnit)

	switch cacheType {
	case "infinite":
		log.Info().Float64("cacheSize", cacheSizeMegabytes).Msg("Create infinite Cache")
		cacheInstance = &cache.InfiniteCache{
			SimpleCache: cache.SimpleCache{
				MaxSize: cacheSizeMegabytes,
			},
		}
	case "random":
		log.Info().Float64("cacheSize", cacheSizeMegabytes).Msg("Create random Cache")
		cacheInstance = &cache.RandomCache{
			SimpleCache: cache.SimpleCache{
				MaxSize: cacheSizeMegabytes,
			},
		}
	case "random_lru":
		log.Info().Float64("cacheSize", cacheSizeMegabytes).Msg("Create random lru Cache")
		cacheInstance = &cache.RandomCache{
			SimpleCache: cache.SimpleCache{
				MaxSize: cacheSizeMegabytes,
			},
		}
	case "lru":
		log.Info().Float64("cacheSize", cacheSizeMegabytes).Msg("Create LRU Cache")
		cacheInstance = &cache.SimpleCache{
			MaxSize: cacheSizeMegabytes,
		}
	case "lfu":
		log.Info().Float64("cacheSize", cacheSizeMegabytes).Msg("Create LFU Cache")
		cacheInstance = &cache.SimpleCache{
			MaxSize: cacheSizeMegabytes,
		}
	case "sizeBig":
		log.Info().Float64("cacheSize", cacheSizeMegabytes).Msg("Create Size Big Cache")
		cacheInstance = &cache.SimpleCache{
			MaxSize: cacheSizeMegabytes,
		}
	case "sizeSmall":
		log.Info().Float64("cacheSize", cacheSizeMegabytes).Msg("Create Size Small Cache")
		cacheInstance = &cache.SimpleCache{
			MaxSize: cacheSizeMegabytes,
		}
	case "lruDatasetVerifier":
		log.Info().Float64("cacheSize", cacheSizeMegabytes).Msg("Create lruDatasetVerifier Cache")
		cacheInstance = &cache.LRUDatasetVerifier{
			SimpleCache: cache.SimpleCache{
				MaxSize: cacheSizeMegabytes,
			},
		}
	case "aiNN":
		log.Info().Float64("cacheSize", cacheSizeMegabytes).Msg("Create aiNN Cache")
		cacheInstance = &cache.AINN{
			SimpleCache: cache.SimpleCache{
				MaxSize: cacheSizeMegabytes,
			},
		}
	case "aiRL":
		log.Info().Float64("cacheSize", cacheSizeMegabytes).Msg("Create aiRL Cache")
		cacheInstance = &cache.AIRL{
			SimpleCache: cache.SimpleCache{
				MaxSize: cacheSizeMegabytes,
			},
		}
	case "weightFunLRU":
		log.Info().Float64("cacheSize", cacheSizeMegabytes).Msg("Create Weight Function Cache")

		cacheInstance = &cache.WeightFun{
			SimpleCache: cache.SimpleCache{
				MaxSize: cacheSizeMegabytes,
			},
		}
	default:
		fmt.Printf("ERR: '%s' is not a valid cache type...\n", cacheType)
		os.Exit(-2)
	}

	return baseName, cacheInstance
}
