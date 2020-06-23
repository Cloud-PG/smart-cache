package main

import (
	"encoding/json"
	"fmt"
	"log"
	"math"
	"os"
	"path/filepath"
	"runtime/pprof"
	"strings"
	"time"

	"simulator/v2/cache"

	"github.com/fatih/color"
	"go.uber.org/zap"
	"go.uber.org/zap/zapcore"

	"github.com/spf13/cobra"
)

func initZapLog(level zapcore.Level) *zap.Logger {
	config := zap.NewDevelopmentConfig()
	config.EncoderConfig.EncodeLevel = zapcore.CapitalColorLevelEncoder
	config.Level = zap.NewAtomicLevelAt(level)
	logger, _ := config.Build()
	return logger
}

var (
	aiFeatureMap           string
	aiModel                string
	aiRLAdditionFeatureMap string
	aiRLEvictionFeatureMap string
	aiRLExtTable           bool
	aiRLEpsilonStart       float64
	aiRLEpsilonDecay       float64
	buildstamp             string
	cacheSize              float64
	cacheSizeUnit          string
	cpuprofile             string
	dataset2TestPath       string
	githash                string
	logger                 = log.New(os.Stderr, color.MagentaString("[SIM] "), log.Lshortfile|log.LstdFlags)
	logLevel               string
	memprofile             string
	outputUpdateDelay      float64
	simColdStart           bool
	simColdStartNoStats    bool
	simDump                bool
	simDumpFilesAndStats   bool
	simDumpFileName        string
	simFileType            string
	simLoadDump            bool
	simLoadDumpFileName    string
	simOutFile             string
	simRegion              string
	simStartFromWindow     uint32
	simStopWindow          uint32
	simWindowSize          uint32
	simBandwidth           float64
	simBandwidthManager    bool
	weightAlpha            float64
	weightBeta             float64
	weightFunc             string
	weightGamma            float64
)

type simDetailCmd int

const (
	normalSimulationCmd simDetailCmd = iota
	aiSimCmd
	testDatasetCmd
)

func main() {
	rootCmd := &cobra.Command{}
	rootCmd.AddCommand(commandSimulate())
	rootCmd.AddCommand(commandSimulateAI())
	rootCmd.AddCommand(testDataset())

	rootCmd.AddCommand(&cobra.Command{
		Use:   "version",
		Short: "Print the version number",
		Long:  "Print the version number of the executable",
		Run: func(cmd *cobra.Command, args []string) {
			fmt.Printf("Build time:\t%s\nGit hash:\t%s\n", buildstamp, githash)
		},
	})
	rootCmd.PersistentFlags().StringVar(
		&cpuprofile, "cpuprofile", "",
		"[Profiling] Profile the CPU during the simulation. If a file is specified the CPU will be profiled on that file",
	)
	rootCmd.PersistentFlags().StringVar(
		&memprofile, "memprofile", "",
		"[Profiling] Profile the Memory during the simulation. If a file is specified the Memory will be profiled on that file. Note that memprofile will stop the simulation after 1 iteration.",
	)
	rootCmd.PersistentFlags().Float64Var(
		&cacheSize, "size", 100., // 100TB
		"[Simulation] cache size",
	)
	rootCmd.PersistentFlags().StringVar(
		&cacheSizeUnit, "sizeUnit", "T", // Terabytes
		"[Simulation] cache size unit",
	)
	rootCmd.PersistentFlags().Float64Var(
		&outputUpdateDelay, "outputUpdateDelay", 2.4,
		"[Simulation] time delay for cmd output",
	)
	rootCmd.PersistentFlags().StringVar(
		&weightFunc, "weightFunc", "FuncAdditiveExp",
		"[WeightFunLRU] function to use with weight cache",
	)
	rootCmd.PersistentFlags().Float64Var(
		&weightAlpha, "weightAlpha", 1.0,
		"[Simulation] Parameter Alpha of the weight function",
	)
	rootCmd.PersistentFlags().Float64Var(
		&weightBeta, "weightBeta", 1.0,
		"[Simulation] Parameter Beta of the weight function",
	)
	rootCmd.PersistentFlags().Float64Var(
		&weightGamma, "weightGamma", 1.0,
		"[Simulation] Parameter Gamma of the weight function",
	)
	rootCmd.PersistentFlags().StringVar(
		&logLevel, "logLevel", "INFO",
		"[Debugging] Enable or not a level of logging",
	)

	if err := rootCmd.Execute(); err != nil {
		fmt.Println(err.Error())
		os.Exit(-1)
	}
}

func addSimFlags(cmd *cobra.Command) {
	cmd.PersistentFlags().StringVar(
		&simRegion, "simRegion", "all",
		"indicate the filter for record region",
	)
	cmd.PersistentFlags().StringVar(
		&simFileType, "simFileType", "all",
		"indicate the filter for record file type",
	)
	cmd.PersistentFlags().StringVar(
		&simOutFile, "simOutFile", "",
		"the output file name",
	)
	cmd.PersistentFlags().BoolVar(
		&simDump, "simDump", false,
		"indicates if to dump the cache status after the simulation",
	)
	cmd.PersistentFlags().BoolVar(
		&simDumpFilesAndStats, "simDumpFilesAndStats", true,
		"indicates if to dump the cache files and stats after the simulation",
	)
	cmd.PersistentFlags().StringVar(
		&simDumpFileName, "simDumpFileName", "",
		"the dump output file name",
	)
	cmd.PersistentFlags().BoolVar(
		&simLoadDump, "simLoadDump", false,
		"indicates if the simulator have to search a dump of previous session",
	)
	cmd.PersistentFlags().StringVar(
		&simLoadDumpFileName, "simLoadDumpFileName", "",
		"the dump input file name",
	)
	cmd.PersistentFlags().Uint32Var(
		&simWindowSize, "simWindowSize", 7,
		"size of the simulation window",
	)
	cmd.PersistentFlags().Uint32Var(
		&simStartFromWindow, "simStartFromWindow", 0,
		"number of the window to start with the simulation",
	)
	cmd.PersistentFlags().Uint32Var(
		&simStopWindow, "simStopWindow", 0,
		"number of the window to stop with the simulation",
	)
	cmd.PersistentFlags().BoolVar(
		&simColdStart, "simColdStart", false,
		"indicates if the cache have to be empty after a dump load",
	)
	cmd.PersistentFlags().BoolVar(
		&simColdStartNoStats, "simColdStartNoStats", false,
		"indicates if the cache have to be empty and without any stats after a dump load",
	)
	cmd.PersistentFlags().Float64Var(
		&aiRLEpsilonStart, "aiRLEpsilonStart", 1.0,
		"indicates the initial value of Epsilon in the RL method",
	)
	cmd.PersistentFlags().Float64Var(
		&aiRLEpsilonDecay, "aiRLEpsilonDecay", 0.0000042,
		"indicates the decay rate value of Epsilon in the RL method",
	)
	cmd.PersistentFlags().Float64Var(
		&simBandwidth, "simBandwidth", 10.0,
		"indicates the network bandwidth available in Gbit",
	)
	cmd.PersistentFlags().BoolVar(
		&simBandwidthManager, "simBandwidthManager", false,
		"enable the file redirection to another cache when bandwidth is over 95%",
	)

}

func simulationCmd(typeCmd simDetailCmd) *cobra.Command {
	var useDesc, shortDesc, longDesc string

	switch typeCmd {
	case normalSimulationCmd:
		useDesc = `sim cacheType fileOrFolderPath`
		shortDesc = "Simulate a session"
		longDesc = "Simulate a session from data input"
	case aiSimCmd:
		useDesc = `simAI cacheType fileOrFolderPath`
		shortDesc = "Simulate a session with AI"
		longDesc = "Simulate a session from data input using an AI model"
	case testDatasetCmd:
		useDesc = `testDataset cacheType fileOrFolderPath`
		shortDesc = "Simulate a cache with the given dataset"
		longDesc = "Simulate a cache that accept only the file in the dataset"
	}

	cmd := &cobra.Command{
		Run: func(cmd *cobra.Command, args []string) {
			// Get logger
			logger := zap.L()

			if len(args) != 2 {
				fmt.Println("ERR: You need to specify the cache type and a file or a folder")
				os.Exit(-1)
			}

			// CHECK DEBUG MODE
			switch logLevel {
			case "INFO", "info":
				logger.Info("ENABLE INFO LOG")
				loggerMgr := initZapLog(zap.InfoLevel)
				zap.ReplaceGlobals(loggerMgr)
				defer loggerMgr.Sync() // flushes buffer, if any
			case "DEBUG", "debug":
				logger.Info("ENABLE DEBUG LOG")
				loggerMgr := initZapLog(zap.DebugLevel)
				zap.ReplaceGlobals(loggerMgr)
				defer loggerMgr.Sync() // flushes buffer, if any
			}
			// Update logger
			logger = zap.L()

			cacheType := args[0]
			pathString := args[1]
			copy(args, args[2:])
			args = args[:len(args)-1]

			var (
				numDailyRecords    int64
				numInvalidRecords  int64
				numJumpedRecords   int64
				numFilteredRecords int64
				totNumRecords      int64
				totIterations      uint32
				numIterations      uint32
				windowStepCounter  uint32
				windowCounter      uint32
				recordFilter       cache.Filter
				dataTypeFilter     cache.Filter
				succesJobFilter    = cache.SuccessJob{}
				cacheSizeString    string
				redirectedData     float64
				numRedirected      int64
			)

			cacheSizeString = fmt.Sprintf("%0.0f%s", cacheSize, strings.ToUpper(cacheSizeUnit))

			baseName := strings.Join([]string{
				cacheType,
				cacheSizeString,
				simRegion,
			}, "_")

			if cacheType == "weightedLRU" {
				parameters := strings.Join([]string{
					fmt.Sprintf("%0.2f", weightAlpha),
					fmt.Sprintf("%0.2f", weightBeta),
					fmt.Sprintf("%0.2f", weightGamma),
				}, "_")
				baseName = strings.Join([]string{
					baseName,
					weightFunc,
					parameters,
				}, "_")
			}

			// Output files
			dumpFileName := baseName + ".json.gz"
			resultFileName := baseName + "_results.csv"
			resultRunStatsName := baseName + "_run_stats.json"
			resultReportStatsName := baseName + "_report_stats.csv"
			resultAdditionQTableName := baseName + "_additionQtable.csv"
			resultEvictionQTableName := baseName + "_evictionQtable.csv"

			if simOutFile == "" {
				simOutFile = resultFileName
			}

			// ------------------------- Create cache --------------------------
			curCacheInstance := genCache(cacheType)

			// ----------------------- Configure cache -------------------------
			cache.SetBandwidth(curCacheInstance, simBandwidth)
			// selectedRegion := fmt.Sprintf("_%s_", strings.ToLower(simRegion))
			switch simRegion {
			// TODO: add filter as a parameter
			case "us":
				recordFilter = cache.UsMINIAODNOT1andT3{}
				dataTypeFilter = cache.UsDataMcTypes{}
				cache.SetRegion(curCacheInstance, "us")
			case "it":
				dataTypeFilter = cache.ItDataMcTypes{}
				cache.SetRegion(curCacheInstance, "it")
			}

			switch typeCmd {
			case aiSimCmd:
				switch cacheType {
				case "aiNN":
					if aiFeatureMap == "" {
						fmt.Println("ERR: No feature map indicated...")
						os.Exit(-1)
					}
					cache.Init(curCacheInstance, aiFeatureMap, aiModel)
				case "aiRL":
					if aiRLAdditionFeatureMap == "" {
						logger.Info("No addition feature map indicated...")
					}
					if aiRLEvictionFeatureMap == "" {
						logger.Info("No eviction feature map indicated...")
					}

					var selFunctionType cache.FunctionType
					switch weightFunc {
					case "FuncAdditive":
						selFunctionType = cache.FuncAdditive
					case "FuncAdditiveExp":
						selFunctionType = cache.FuncAdditiveExp
					case "FuncMultiplicative":
						selFunctionType = cache.FuncMultiplicative
					case "FuncWeightedRequests":
						selFunctionType = cache.FuncWeightedRequests
					default:
						fmt.Println("ERR: You need to specify a correct weight function.")
						os.Exit(-1)
					}

					cache.Init(
						curCacheInstance,
						aiRLAdditionFeatureMap,
						aiRLEvictionFeatureMap,
						aiRLEpsilonStart,
						aiRLEpsilonDecay,
						aiRLExtTable,
						selFunctionType,
						weightAlpha,
						weightBeta,
						weightGamma,
					)
				}
			case testDatasetCmd:
				cache.Init(curCacheInstance, dataset2TestPath)
			}

			if simDumpFileName == "" {
				simDumpFileName = dumpFileName
			}
			if simLoadDumpFileName == "" {
				simLoadDumpFileName = dumpFileName
			}

			if simLoadDump {
				logger.Info("Loading cache dump", zap.String("filename", simLoadDumpFileName))

				latestCacheRun := cache.GetSimulationRunNum(filepath.Dir(simLoadDumpFileName))

				os.Rename(
					simOutFile,
					fmt.Sprintf("%s_run-%02d.csv",
						strings.Split(simOutFile, ".")[0],
						latestCacheRun,
					),
				)

				loadedDump := cache.Load(curCacheInstance, simLoadDumpFileName)

				if cacheType == "aiRL" {
					cache.Loads(curCacheInstance, loadedDump, aiRLEpsilonStart, aiRLEpsilonDecay)
				} else {
					cache.Loads(curCacheInstance, loadedDump)
				}

				logger.Info("Cache dump loaded!")
				if simColdStart {
					if simColdStartNoStats {
						cache.Clear(curCacheInstance)
						logger.Info("Cache Files deleted... COLD START with NO STATISTICS")
					} else {
						cache.ClearFiles(curCacheInstance)
						logger.Info("Cache Files deleted... COLD START")
					}
				} else {
					logger.Info("Cache Files stored... HOT START")
				}
			}

			// Open simulation files
			fileStats, statErr := os.Stat(pathString)
			if statErr != nil {
				fmt.Printf("ERR: Cannot open source %s.\n", pathString)
				os.Exit(-1)
			}

			var iterator chan cache.CSVRecord

			switch mode := fileStats.Mode(); {
			case mode.IsRegular():
				iterator = cache.OpenSimFile(pathString)
			case mode.IsDir():
				curFolder, _ := os.Open(pathString)
				defer curFolder.Close()
				iterator = cache.OpenSimFolder(curFolder)
			}

			csvSimOutput := cache.OutputCSV{}
			csvSimOutput.Create(simOutFile)
			defer csvSimOutput.Close()

			csvHeaderColumns := []string{"date",
				"size",
				"hit rate",
				"hit over miss",
				"weighted hit rate",
				"written data",
				"read data",
				"read on hit data",
				"read on miss data",
				"deleted data",
				"CPU efficiency",
				"CPU hit efficiency",
				"CPU miss efficiency",
				"CPU efficiency upper bound",
				"CPU efficiency lower bound",
			}
			if cacheType == "aiRL" {
				csvHeaderColumns = append(csvHeaderColumns, "Addition value function")
				csvHeaderColumns = append(csvHeaderColumns, "Eviction value function")
			}
			csvSimOutput.Write(csvHeaderColumns)

			csvSimReport := cache.OutputCSV{}
			csvSimReport.Create(resultReportStatsName)
			defer csvSimReport.Close()

			csvSimReport.Write([]string{"numFiles",
				"avgSize",
				"avgNumUsers",
				"avgNumSites",
				"avgNumRequests",
				"avgNumHits",
				"avgNumMiss",
			})

			simBeginTime := time.Now()
			start := time.Now()
			var latestTime time.Time

			if cpuprofile != "" {
				profileOut, err := os.Create(cpuprofile)
				if err != nil {
					fmt.Printf("ERR: Can not create CPU profile file %s.\n", err)
					os.Exit(-1)
				}
				logger.Info("Enable CPU profiliing", zap.String("filename", cpuprofile))
				pprof.StartCPUProfile(profileOut)
				defer pprof.StopCPUProfile()
			}

			logger.Info("Simulation START")

			for record := range iterator {

				numIterations++

				// --------------------- Make daily output ---------------------
				if latestTime.IsZero() {
					latestTime = time.Unix(record.Day, 0.)
				}

				curTime := time.Unix(record.Day, 0.)

				if curTime.Sub(latestTime).Hours() >= 24. {
					if windowCounter >= simStartFromWindow {
						csvRow := []string{
							fmt.Sprintf("%s", latestTime),
							fmt.Sprintf("%f", cache.Size(curCacheInstance)),
							fmt.Sprintf("%0.2f", cache.HitRate(curCacheInstance)),
							fmt.Sprintf("%0.2f", cache.HitOverMiss(curCacheInstance)),
							fmt.Sprintf("%0.2f", cache.WeightedHitRate(curCacheInstance)),
							fmt.Sprintf("%f", cache.DataWritten(curCacheInstance)),
							fmt.Sprintf("%f", cache.DataRead(curCacheInstance)),
							fmt.Sprintf("%f", cache.DataReadOnHit(curCacheInstance)),
							fmt.Sprintf("%f", cache.DataReadOnMiss(curCacheInstance)),
							fmt.Sprintf("%f", cache.DataDeleted(curCacheInstance)),
							fmt.Sprintf("%f", cache.CPUEff(curCacheInstance)),
							fmt.Sprintf("%f", cache.CPUHitEff(curCacheInstance)),
							fmt.Sprintf("%f", cache.CPUMissEff(curCacheInstance)),
							fmt.Sprintf("%f", cache.CPUEffUpperBound(curCacheInstance)),
							fmt.Sprintf("%f", cache.CPUEffLowerBound(curCacheInstance)),
						}
						if cacheType == "aiRL" {
							csvRow = append(csvRow, strings.Split(cache.ExtraOutput(curCacheInstance, "valueFunctions"), ",")...)
						}
						csvSimOutput.Write(csvRow)
					}
					cache.ClearHitMissStats(curCacheInstance)
					// Update time window
					latestTime = curTime
					windowStepCounter++
				}

				if windowStepCounter == simWindowSize {
					windowCounter++
					windowStepCounter = 0
					numDailyRecords = 0
				}
				if windowCounter == simStopWindow {
					break
				}

				totNumRecords++

				if windowCounter >= simStartFromWindow {
					if succesJobFilter.Check(record) == false {
						numFilteredRecords++
						continue
					}
					if dataTypeFilter != nil {
						if dataTypeFilter.Check(record) == false {
							numFilteredRecords++
							continue
						}
					}
					if recordFilter != nil {
						if recordFilter.Check(record) == false {
							numFilteredRecords++
							continue
						}
					}

					sizeInMbytes := record.SizeM // Size in Megabytes

					cpuEff := (record.CPUTime / (record.CPUTime + record.IOTime)) * 100.
					// Filter records with invalid CPU efficiency
					if cpuEff < 0. {
						numInvalidRecords++
						continue
					} else if math.IsInf(cpuEff, 0) {
						numInvalidRecords++
						continue
					} else if math.IsNaN(cpuEff) {
						numInvalidRecords++
						continue
					} else if cpuEff > 100. {
						numInvalidRecords++
						continue
					}

					_, redirected := cache.GetFile(
						simBandwidthManager,
						curCacheInstance,
						record.Filename,
						sizeInMbytes,
						record.Protocol,
						cpuEff,
						record.Day,
						record.SiteName,
						record.UserID,
						record.FileType,
					)

					if redirected {
						redirectedData += sizeInMbytes
						numRedirected++
						continue
					}

					numDailyRecords++

					if time.Now().Sub(start).Seconds() >= outputUpdateDelay {
						elapsedTime := time.Now().Sub(simBeginTime)
						logger.Info("Simulation",
							zap.String("cache", baseName),
							zap.String("elapsedTime", fmt.Sprintf("%02d:%02d:%02d",
								int(elapsedTime.Hours()),
								int(elapsedTime.Minutes())%60,
								int(elapsedTime.Seconds())%60,
							)),
							zap.Uint32("window", windowCounter),
							zap.Uint32("step", windowStepCounter),
							zap.Uint32("windowSize", simWindowSize),
							zap.Int64("numDailyRecords", numDailyRecords),
							zap.Float64("hitRate", cache.HitRate(curCacheInstance)),
							zap.Float64("capacity", cache.Occupancy(curCacheInstance)),
							zap.Float64("redirectedData", redirectedData),
							zap.Int64("numRedirected", numRedirected),
							zap.String("extra", cache.ExtraStats(curCacheInstance)),
							zap.Float64("it/s", float64(numIterations)/time.Now().Sub(start).Seconds()),
						)
						totIterations += numIterations
						numIterations = 0
						start = time.Now()
					}

				} else {
					numJumpedRecords++
					if time.Now().Sub(start).Seconds() >= outputUpdateDelay {
						logger.Info("Jump records",
							zap.Int64("numDailyRecords", numDailyRecords),
							zap.Int64("numJumpedRecords", numJumpedRecords),
							zap.Int64("numFilteredRecords", numFilteredRecords),
							zap.Int64("numInvalidRecords", numInvalidRecords),
							zap.Uint32("window", windowCounter),
						)
						start = time.Now()
					}
				}
			}

			if memprofile != "" {
				profileOut, err := os.Create(memprofile)
				if err != nil {
					logger.Error("Cannot create Memory profile file",
						zap.Error(err),
						zap.String("filename", memprofile),
					)
					os.Exit(-1)
				}
				logger.Info("Write memprofile", zap.String("filename", memprofile))
				pprof.WriteHeapProfile(profileOut)
				profileOut.Close()
				return
			}

			elapsedTime := time.Now().Sub(simBeginTime)
			elTH := int(elapsedTime.Hours())
			elTM := int(elapsedTime.Minutes()) % 60
			elTS := int(elapsedTime.Seconds()) % 60
			avgSpeed := float64(totIterations) / elapsedTime.Seconds()
			logger.Info("Simulation end...",
				zap.String("elapsedTime", fmt.Sprintf("%02d:%02d:%02d", elTH, elTM, elTS)),
				zap.Float64("avg it/s", avgSpeed),
				zap.Int64("totRecords", totNumRecords),
				zap.Int64("numJumpedRecords", numJumpedRecords),
				zap.Int64("numFilteredRecords", numFilteredRecords),
				zap.Int64("numInvalidRecords", numInvalidRecords),
			)
			// Save run statistics
			statFile, errCreateStat := os.Create(resultRunStatsName)
			defer statFile.Close()
			if errCreateStat != nil {
				panic(errCreateStat)
			}
			jsonBytes, errMarshal := json.Marshal(cache.SimulationStats{
				TimeElapsed:           fmt.Sprintf("%02d:%02d:%02d", elTH, elTM, elTS),
				Extra:                 cache.ExtraStats(curCacheInstance),
				TotNumRecords:         totNumRecords,
				TotFilteredRecords:    numFilteredRecords,
				TotJumpedRecords:      numJumpedRecords,
				TotInvalidRecords:     numInvalidRecords,
				AvgSpeed:              fmt.Sprintf("Num.Records/s = %0.2f", avgSpeed),
				TotRedirectedRecords:  numRedirected,
				SizeRedirectedRecords: redirectedData,
			})
			if errMarshal != nil {
				panic(errMarshal)
			}
			statFile.Write(jsonBytes)

			if simDump {
				cache.Dump(curCacheInstance, simDumpFileName, simDumpFilesAndStats)
			}

			if cacheType == "aiRL" {
				// Save tables
				logger.Info("Save addition table...")
				writeQTable(resultAdditionQTableName, cache.ExtraOutput(curCacheInstance, "additionQtable"))
				logger.Info("Save eviction table...")
				writeQTable(resultEvictionQTableName, cache.ExtraOutput(curCacheInstance, "evictionQtable"))
			}

			logger.Info("Simulation DONE!")
			logger.Sync()
		},
		Use:   useDesc,
		Short: shortDesc,
		Long:  longDesc,
		Args:  cobra.MaximumNArgs(2),
	}
	addSimFlags(cmd)
	switch typeCmd {
	case aiSimCmd:
		cmd.PersistentFlags().StringVar(
			&aiFeatureMap, "aiFeatureMap", "",
			"the feature map file for data conversions",
		)
		cmd.PersistentFlags().BoolVar(
			&aiRLExtTable, "aiRLExtTable", false,
			"use the extended eviction table",
		)
		cmd.PersistentFlags().StringVar(
			&aiRLAdditionFeatureMap, "aiRLAdditionFeatureMap", "",
			"the RL addition feature map file for data conversions",
		)
		cmd.PersistentFlags().StringVar(
			&aiRLEvictionFeatureMap, "aiRLEvictionFeatureMap", "",
			"the RL eviction feature map file for data conversions",
		)
		cmd.PersistentFlags().StringVar(
			&aiModel, "aiModel", "",
			"the model to load into the simulator",
		)
	case testDatasetCmd:
		cmd.PersistentFlags().StringVar(
			&dataset2TestPath, "dataset2TestPath", "",
			"the dataset to use as reference for the lru choices",
		)
	}
	return cmd
}

func writeQTable(outFilename string, data string) {
	qtableAdditionFile, errCreateQTablecsv := os.Create(outFilename)
	defer qtableAdditionFile.Close()
	if errCreateQTablecsv != nil {
		panic(errCreateQTablecsv)
	}
	qtableAdditionFile.WriteString(data)
}

func commandSimulate() *cobra.Command {
	return simulationCmd(normalSimulationCmd)
}

func commandSimulateAI() *cobra.Command {
	return simulationCmd(aiSimCmd)
}

func testDataset() *cobra.Command {
	return simulationCmd(testDatasetCmd)
}

func genCache(cacheType string) cache.Cache {
	logger := zap.L()
	var cacheInstance cache.Cache
	cacheSizeMegabytes := cache.GetCacheSize(cacheSize, cacheSizeUnit)
	switch cacheType {
	case "lru":
		logger.Info("Create LRU Cache",
			zap.Float64("cacheSize", cacheSizeMegabytes),
		)
		cacheInstance = &cache.SimpleCache{
			MaxSize: cacheSizeMegabytes,
		}
		cache.Init(cacheInstance)
	case "lfu":
		logger.Info("Create LFU Cache",
			zap.Float64("cacheSize", cacheSizeMegabytes),
		)
		cacheInstance = &cache.SimpleCache{
			MaxSize: cacheSizeMegabytes,
		}
		cacheInstance.Init(cache.LFUQueue)
	case "sizeBig":
		logger.Info("Create Size Big Cache",
			zap.Float64("cacheSize", cacheSizeMegabytes),
		)
		cacheInstance = &cache.SimpleCache{
			MaxSize: cacheSizeMegabytes,
		}
		cacheInstance.Init(cache.SizeBigQueue)
	case "sizeSmall":
		logger.Info("Create Size Small Cache",
			zap.Float64("cacheSize", cacheSizeMegabytes),
		)
		cacheInstance = &cache.SimpleCache{
			MaxSize: cacheSizeMegabytes,
		}
		cacheInstance.Init(cache.SizeSmallQueue)
	case "lruDatasetVerifier":
		logger.Info("Create lruDatasetVerifier Cache",
			zap.Float64("cacheSize", cacheSizeMegabytes),
		)
		cacheInstance = &cache.LRUDatasetVerifier{
			SimpleCache: cache.SimpleCache{
				MaxSize: cacheSizeMegabytes,
			},
		}
	case "aiNN":
		logger.Info("Create aiNN Cache",
			zap.Float64("cacheSize", cacheSizeMegabytes),
		)
		cacheInstance = &cache.AINN{
			SimpleCache: cache.SimpleCache{
				MaxSize: cacheSizeMegabytes,
			},
		}
	case "aiRL":
		logger.Info("Create aiRL Cache",
			zap.Float64("cacheSize", cacheSizeMegabytes),
		)
		cacheInstance = &cache.AIRL{
			SimpleCache: cache.SimpleCache{
				MaxSize: cacheSizeMegabytes,
			},
		}
	case "weightFunLRU":
		logger.Info("Create Weight Function Cache",
			zap.Float64("cacheSize", cacheSizeMegabytes),
		)

		var (
			selFunctionType cache.FunctionType
		)

		switch weightFunc {
		case "FuncAdditive":
			selFunctionType = cache.FuncAdditive
		case "FuncAdditiveExp":
			selFunctionType = cache.FuncAdditiveExp
		case "FuncMultiplicative":
			selFunctionType = cache.FuncMultiplicative
		case "FuncWeightedRequests":
			selFunctionType = cache.FuncWeightedRequests
		default:
			fmt.Println("ERR: You need to specify a correct weight function.")
			os.Exit(-1)
		}

		cacheInstance = &cache.WeightFunLRU{
			SimpleCache: cache.SimpleCache{
				MaxSize: cacheSizeMegabytes,
			},
			Parameters: cache.WeightFunctionParameters{
				Alpha: weightAlpha,
				Beta:  weightBeta,
				Gamma: weightGamma,
			},
			SelFunctionType: selFunctionType,
		}
		cache.Init(cacheInstance)
	default:
		fmt.Printf("ERR: '%s' is not a valid cache type...\n", cacheType)
		os.Exit(-2)
	}
	return cacheInstance
}
