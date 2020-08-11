package main

import (
	"encoding/json"
	"errors"
	"fmt"
	"math"
	"os"
	"path/filepath"
	"runtime/pprof"
	"strings"
	"time"

	"simulator/v2/cache"

	"go.uber.org/zap"
	"go.uber.org/zap/zapcore"

	"github.com/spf13/cobra"
	"github.com/spf13/viper"
)

func initZapLog(level zapcore.Level) *zap.Logger {
	config := zap.NewDevelopmentConfig()
	config.EncoderConfig.EncodeLevel = zapcore.CapitalColorLevelEncoder
	config.Level = zap.NewAtomicLevelAt(level)
	logger, _ := config.Build()
	return logger
}

var (
	githash    string
	buildstamp string
)

func configureViper(configFilenameWithNoExt string) {
	viper.SetConfigName(configFilenameWithNoExt) // name of config file (without extension)
	viper.SetConfigType("yaml")                  // REQUIRED if the config file does not have the extension in the name
	viper.AddConfigPath(".")                     // optionally look for config in the working directory

	viper.SetDefault("sim.region", "all")
	viper.SetDefault("sim.outputFolder", ".")
	viper.SetDefault("sim.dump", false)
	viper.SetDefault("sim.dumpfilesandstats", true)
	viper.SetDefault("sim.dumpfilename", "")
	viper.SetDefault("sim.loaddump", false)
	viper.SetDefault("sim.loaddumpfilename", "")
	viper.SetDefault("sim.window.size", 7)
	viper.SetDefault("sim.window.start", 0)
	viper.SetDefault("sim.window.stop", 0)
	viper.SetDefault("sim.cache.watermarks", false)
	viper.SetDefault("sim.coldstart", false)
	viper.SetDefault("sim.coldstartnostats", false)
	viper.SetDefault("sim.redirectreq", false)
	viper.SetDefault("sim.bandwidth", 10.0)
	viper.SetDefault("sim.log", false)

	viper.SetDefault("sim.cpuprofile", "")
	viper.SetDefault("sim.memprofile", "")
	viper.SetDefault("sim.outputupdatedelay", 2.4)

	viper.SetDefault("sim.cache.size.value", 100.)
	viper.SetDefault("sim.cache.size.unit", "T")

	viper.SetDefault("sim.weightfunc.name", "FuncAdditiveExp")
	viper.SetDefault("sim.weightfunc.alpha", 1.0)
	viper.SetDefault("sim.weightfunc.beta", 1.0)
	viper.SetDefault("sim.weightfunc.gamma", 1.0)
	viper.SetDefault("sim.loglevel", "INFO")

	viper.SetDefault("sim.ai.rl.epsilon.start", 1.0)
	viper.SetDefault("sim.ai.rl.epsilon.decay", 0.0000042)

	viper.SetDefault("sim.ai.rl.use_k", true)

	viper.SetDefault("sim.ai.featuremap", "")
	viper.SetDefault("sim.ai.rl.addition.featuremap", "")
	viper.SetDefault("sim.ai.rl.eviction.featuremap", "")
	viper.SetDefault("sim.ai.model", "")

	viper.SetDefault("sim.dataset2testpath", "")
}

func simCommand() *cobra.Command {
	// Simulation config variables
	var (
		logLevel string
		// Simulation
		simBandwidth         float64
		simRedirectReq       bool
		simCacheWatermarks   bool
		simColdStart         bool
		simColdStartNoStats  bool
		simDataPath          string
		simDump              bool
		simDumpFileName      string
		simDumpFilesAndStats bool
		simFileType          string
		simLoadDump          bool
		simLoadDumpFileName  string
		simLog               bool
		simOutputFolder      string
		simRegion            string
		simType              string
		simUseK              bool
		simWindowStart       int
		simWindowStop        int
		simWindowSize        int
		// Profiling
		cpuprofile        string
		memprofile        string
		outputUpdateDelay float64
		// Weight function
		weightFunc  string
		weightAlpha float64
		weightBeta  float64
		weightGamma float64
		// ai
		aiFeatureMap           string
		aiModel                string
		aiRLAdditionFeatureMap string
		aiRLEvictionFeatureMap string
		aiRLEpsilonStart       float64
		aiRLEpsilonDecay       float64
		// cache
		cacheType     string
		cacheSize     float64
		cacheSizeUnit string
		// dataset
		dataset2TestPath string
	)

	simCmd := &cobra.Command{
		Use:   "sim config",
		Short: "a simulation environment for Smart Cache in a Data Lake",
		Long: `a simulation environment for Smart Cache in a Data Lake,
		used as comparison measure for the new approaches`,
		Args: func(cmd *cobra.Command, args []string) error {
			if len(args) < 1 {
				return errors.New("requires a configuration file")
			}
			return nil
		},
		Run: func(cmd *cobra.Command, args []string) {
			// Get arguments
			configFile := args[0]

			// Get logger
			logger := zap.L()
			// CHECK DEBUG MODE
			switch logLevel {
			case "INFO", "info":
				logger.Info("ENABLE INFO LOG")
				loggerMgr := initZapLog(zap.InfoLevel)
				zap.ReplaceGlobals(loggerMgr)
				defer func() {
					// TODO: fix error
					// -> https://github.com/uber-go/zap/issues/772
					// -> https://github.com/uber-go/zap/issues/328
					_ = loggerMgr.Sync() // flushes buffer, if any
				}()
			case "DEBUG", "debug":
				logger.Info("ENABLE DEBUG LOG")
				loggerMgr := initZapLog(zap.DebugLevel)
				zap.ReplaceGlobals(loggerMgr)
				defer func() {
					// TODO: fix error
					// -> https://github.com/uber-go/zap/issues/772
					// -> https://github.com/uber-go/zap/issues/328
					_ = loggerMgr.Sync() // flushes buffer, if any
				}()
			}
			// Update logger
			logger = zap.L()

			logger.Info("Get simulation config file", zap.String("config file", configFile))

			configAbsPath, errAbs := filepath.Abs(configFile)
			if errAbs != nil {
				panic(errAbs)
			}
			configDir := filepath.Dir(configAbsPath)
			configFilename := filepath.Base(configAbsPath)
			configFilenameWithNoExt := strings.TrimSuffix(configFilename, filepath.Ext(configFilename))

			logger.Info("Config file ABS path", zap.String("path", configAbsPath))
			logger.Info("Config file Directory", zap.String("path", configDir))
			logger.Info("Config filename", zap.String("file", configFilename))
			logger.Info("Config filename without extension", zap.String("file", configFilenameWithNoExt))

			logger.Info("Change dir moving on config parent folder", zap.String("path", configDir))
			errChdir := os.Chdir(configDir)
			if errChdir != nil {
				panic(errChdir)
			}
			curWd, _ := os.Getwd()
			logger.Info("Current Working Dir", zap.String("path", curWd))

			logger.Info("Load config file")
			configureViper(configFilenameWithNoExt)

			if err := viper.ReadInConfig(); err != nil {
				if _, ok := err.(viper.ConfigFileNotFoundError); ok {
					// Config file not found; ignore error if desired
					panic(err)
				} else {
					// Config file was found but another error was produced
					panic(err)
				}
			}

			cacheSize = viper.GetFloat64("sim.cache.size.value")
			logger.Info("CONF_VAR", zap.Float64("cacheSize", cacheSize))

			cacheSizeUnit = viper.GetString("sim.cache.size.unit")
			logger.Info("CONF_VAR", zap.String("cacheSizeUnit", cacheSizeUnit))

			simBandwidth = viper.GetFloat64("sim.bandwidth")
			logger.Info("CONF_VAR", zap.Float64("simBandwidth", simBandwidth))

			simRedirectReq = viper.GetBool("sim.redirectreq")
			logger.Info("CONF_VAR", zap.Bool("simRedirectReq", simRedirectReq))

			simCacheWatermarks = viper.GetBool("sim.cache.watermarks")
			logger.Info("CONF_VAR", zap.Bool("simCacheWatermarks", simCacheWatermarks))

			simColdStart = viper.GetBool("sim.coldstart")
			logger.Info("CONF_VAR", zap.Bool("simColdStart", simColdStart))

			simColdStartNoStats = viper.GetBool("sim.coldstartnostats")
			logger.Info("CONF_VAR", zap.Bool("simColdStartNoStats", simColdStartNoStats))

			simDataPath = viper.GetString("sim.data")
			simDataPath, errAbs = filepath.Abs(simDataPath)
			if errAbs != nil {
				panic(errAbs)
			}
			logger.Info("CONF_VAR", zap.String("simDataPath", simDataPath))

			simDump = viper.GetBool("sim.dump")
			logger.Info("CONF_VAR", zap.Bool("simDump", simDump))

			simDumpFileName = viper.GetString("sim.dumpfilename")
			logger.Info("CONF_VAR", zap.String("simDumpFileName", simDumpFileName))

			simDumpFilesAndStats = viper.GetBool("sim.dumpfilesandstats")
			logger.Info("CONF_VAR", zap.Bool("simDumpFilesAndStats", simDumpFilesAndStats))

			simFileType = viper.GetString("sim.filetype")
			logger.Info("CONF_VAR", zap.String("simFileType", simFileType))

			simLoadDump = viper.GetBool("sim.loaddump")
			logger.Info("CONF_VAR", zap.Bool("simLoadDump", simLoadDump))

			simLoadDumpFileName = viper.GetString("sim.loaddumpfilename")
			logger.Info("CONF_VAR", zap.String("simLoadDumpFileName", simLoadDumpFileName))

			simLog = viper.GetBool("sim.log")
			logger.Info("CONF_VAR", zap.Bool("simLog", simLog))

			simOutputFolder = viper.GetString("sim.outputFolder")
			simOutputFolder, errAbs = filepath.Abs(simOutputFolder)
			if errAbs != nil {
				panic(errAbs)
			}
			logger.Info("CONF_VAR", zap.String("simOutputFolder", simOutputFolder))

			simRegion = viper.GetString("sim.region")
			logger.Info("CONF_VAR", zap.String("simRegion", simRegion))

			simType = viper.GetString("sim.type")
			logger.Info("CONF_VAR", zap.String("simType", simType))

			simWindowStart = viper.GetInt("sim.window.start")
			logger.Info("CONF_VAR", zap.Int("simWindowStart", simWindowStart))

			simWindowStop = viper.GetInt("sim.window.stop")
			logger.Info("CONF_VAR", zap.Int("simWindowStop", simWindowStop))

			simWindowSize = viper.GetInt("sim.window.size")
			logger.Info("CONF_VAR", zap.Int("simWindowSize", simWindowSize))

			cpuprofile = viper.GetString("sim.cpuprofile")
			logger.Info("CONF_VAR", zap.String("cpuprofile", cpuprofile))

			memprofile = viper.GetString("sim.memprofile")
			logger.Info("CONF_VAR", zap.String("memprofile", memprofile))

			outputUpdateDelay = viper.GetFloat64("sim.outputupdatedelay")
			logger.Info("CONF_VAR", zap.Float64("outputUpdateDelay", outputUpdateDelay))

			weightFunc = viper.GetString("sim.weightfunc.name")
			logger.Info("CONF_VAR", zap.String("weightFunc", weightFunc))

			weightAlpha = viper.GetFloat64("sim.weightfunc.alpha")
			logger.Info("CONF_VAR", zap.Float64("weightAlpha", weightAlpha))

			weightBeta = viper.GetFloat64("sim.weightfunc.beta")
			logger.Info("CONF_VAR", zap.Float64("weightBeta", weightBeta))

			weightGamma = viper.GetFloat64("sim.weightfunc.gamma")
			logger.Info("CONF_VAR", zap.Float64("weightGamma", weightGamma))

			aiFeatureMap = viper.GetString("sim.ai.featuremap")
			if aiFeatureMap != "" {
				aiFeatureMap, errAbs = filepath.Abs(aiFeatureMap)
				if errAbs != nil {
					panic(errAbs)
				}
			}
			logger.Info("CONF_VAR", zap.String("aiFeatureMap", aiFeatureMap))

			dataset2TestPath = viper.GetString("sim.ai.dataset2TestPath")
			logger.Info("CONF_VAR", zap.String("dataset2TestPath", dataset2TestPath))

			aiModel = viper.GetString("sim.ai.model")
			logger.Info("CONF_VAR", zap.String("aiModel", aiModel))

			aiRLAdditionFeatureMap = viper.GetString("sim.ai.rl.addition.featuremap")
			if aiRLAdditionFeatureMap != "" {
				aiRLAdditionFeatureMap, errAbs = filepath.Abs(aiRLAdditionFeatureMap)
				if errAbs != nil {
					panic(errAbs)
				}
			}
			logger.Info("CONF_VAR", zap.String("aiRLAdditionFeatureMap", aiRLAdditionFeatureMap))

			aiRLEvictionFeatureMap = viper.GetString("sim.ai.rl.eviction.featuremap")
			if aiRLEvictionFeatureMap != "" {
				aiRLEvictionFeatureMap, errAbs = filepath.Abs(aiRLEvictionFeatureMap)
				if errAbs != nil {
					panic(errAbs)
				}
			}
			logger.Info("CONF_VAR", zap.String("aiRLEvictionFeatureMap", aiRLEvictionFeatureMap))

			aiRLEpsilonStart = viper.GetFloat64("sim.ai.rl.epsilon.start")
			logger.Info("CONF_VAR", zap.Float64("aiRLEpsilonStart", aiRLEpsilonStart))

			aiRLEpsilonDecay = viper.GetFloat64("sim.ai.rl.epsilon.decay")
			logger.Info("CONF_VAR", zap.Float64("aiRLEpsilonDecay", aiRLEpsilonDecay))

			simUseK = viper.GetBool("sim.ai.rl.use_k")
			logger.Info("CONF_VAR", zap.Bool("simUseK", simUseK))

			cacheType = viper.GetString("sim.cache.type")
			logger.Info("CONF_VAR", zap.String("cacheType", cacheType))

			// Simulation variables
			var (
				numDailyRecords    int64
				numInvalidRecords  int64
				numJumpedRecords   int64
				numFilteredRecords int64
				totNumRecords      int64
				totIterations      uint64
				numIterations      uint64
				windowStepCounter  int
				windowCounter      int
				recordFilter       cache.Filter
				dataTypeFilter     cache.Filter
				succesJobFilter    = cache.SuccessJob{}
				cacheSizeString    string
				redirectedData     float64
				numRedirected      int64
			)

			// Generate simulation file output basename
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
			simOutFile := ""
			dumpFileName := baseName + ".json.gz"
			resultFileName := baseName + "_results.csv"
			resultRunStatsName := baseName + "_run_stats.json"

			if simOutFile == "" {
				simOutFile = resultFileName
			}

			// Create output folder and move working dir
			switch simType {
			case "normal":
				finalOutputFolder := filepath.Join(simOutputFolder, "run_full_normal", baseName)
				err := os.MkdirAll(finalOutputFolder, 0755)
				if err != nil && !os.IsExist(err) {
					panic(err)
				}
				errChdir = os.Chdir(finalOutputFolder)
				if errChdir != nil {
					panic(errChdir)
				}
				curWd, _ := os.Getwd()
				logger.Info("Current Working Dir", zap.String("path", curWd))
			}

			// ------------------------- Create cache --------------------------
			curCacheInstance := genCache(
				cacheType,
				cacheSize,
				cacheSizeUnit,
				simLog,
				simRedirectReq,
				simCacheWatermarks,
				weightFunc,
				weightAlpha,
				weightBeta,
				weightGamma,
			)

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

			if dataset2TestPath != "" {
				cache.Init(curCacheInstance, simLog, simRedirectReq, simCacheWatermarks, dataset2TestPath)
			} else {
				switch cacheType {
				case "aiNN":
					if aiFeatureMap == "" {
						fmt.Println("ERR: No feature map indicated...")
						os.Exit(-1)
					}
					cache.Init(curCacheInstance, simLog, simRedirectReq, simCacheWatermarks, aiFeatureMap, aiModel)
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
						simLog,
						simRedirectReq,
						simCacheWatermarks,
						simUseK,
						aiRLAdditionFeatureMap,
						aiRLEvictionFeatureMap,
						aiRLEpsilonStart,
						aiRLEpsilonDecay,
						selFunctionType,
						weightAlpha,
						weightBeta,
						weightGamma,
					)
				}
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

				renameErr := os.Rename(
					simOutFile,
					fmt.Sprintf("%s_run-%02d.csv",
						strings.Split(simOutFile, ".")[0],
						latestCacheRun,
					),
				)
				if renameErr != nil {
					panic(renameErr)
				}

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
			fileStats, statErr := os.Stat(simDataPath)
			if statErr != nil {
				fmt.Printf("ERR: Cannot open source %s.\n", simDataPath)
				panic(statErr)
			}

			var iterator chan cache.CSVRecord

			switch mode := fileStats.Mode(); {
			case mode.IsRegular():
				iterator = cache.OpenSimFile(simDataPath)
			case mode.IsDir():
				curFolder, _ := os.Open(simDataPath)
				defer func() {
					closeErr := curFolder.Close()
					if closeErr != nil {
						panic(closeErr)
					}
				}()
				iterator = cache.OpenSimFolder(curFolder)
			}

			csvSimOutput := cache.OutputCSV{}
			csvSimOutput.Create(simOutFile, false)
			defer csvSimOutput.Close()

			csvHeaderColumns := []string{"date",
				"num req",
				"num hit",
				"num added",
				"num deleted",
				"num redirected",
				"size",
				"hit rate",
				"hit over miss",
				"weighted hit rate",
				"written data",
				"read data",
				"read on hit data",
				"read on miss data",
				"deleted data",
				"avg free space",
				"std dev free space",
				"CPU efficiency",
				"CPU hit efficiency",
				"CPU miss efficiency",
				"CPU efficiency upper bound",
				"CPU efficiency lower bound",
			}
			if cacheType == "aiRL" {
				csvHeaderColumns = append(csvHeaderColumns, "Addition epsilon")
				csvHeaderColumns = append(csvHeaderColumns, "Eviction epsilon")
				csvHeaderColumns = append(csvHeaderColumns, "Addition qvalue function")
				csvHeaderColumns = append(csvHeaderColumns, "Eviction qvalue function")
				csvHeaderColumns = append(csvHeaderColumns, "Eviction calls")
				csvHeaderColumns = append(csvHeaderColumns, "Eviction forced calls")
				csvHeaderColumns = append(csvHeaderColumns, "Eviction step")
				csvHeaderColumns = append(csvHeaderColumns, "Action store")
				csvHeaderColumns = append(csvHeaderColumns, "Action not store")
				csvHeaderColumns = append(csvHeaderColumns, "Action delete all")
				csvHeaderColumns = append(csvHeaderColumns, "Action delete half")
				csvHeaderColumns = append(csvHeaderColumns, "Action delete quarter")
				csvHeaderColumns = append(csvHeaderColumns, "Action delete one")
				csvHeaderColumns = append(csvHeaderColumns, "Action not delete")
			}
			csvSimOutput.Write(csvHeaderColumns)

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
				startProfileErr := pprof.StartCPUProfile(profileOut)
				if startProfileErr != nil {
					panic(startProfileErr)
				}
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
					if windowCounter >= simWindowStart {
						csvRow := []string{
							latestTime.String(),
							fmt.Sprintf("%d", cache.NumRequests(curCacheInstance)),
							fmt.Sprintf("%d", cache.NumHits(curCacheInstance)),
							fmt.Sprintf("%d", cache.NumAdded(curCacheInstance)),
							fmt.Sprintf("%d", cache.NumDeleted(curCacheInstance)),
							fmt.Sprintf("%d", cache.NumRedirected(curCacheInstance)),
							fmt.Sprintf("%f", cache.Size(curCacheInstance)),
							fmt.Sprintf("%0.2f", cache.HitRate(curCacheInstance)),
							fmt.Sprintf("%0.2f", cache.HitOverMiss(curCacheInstance)),
							fmt.Sprintf("%0.2f", cache.WeightedHitRate(curCacheInstance)),
							fmt.Sprintf("%f", cache.DataWritten(curCacheInstance)),
							fmt.Sprintf("%f", cache.DataRead(curCacheInstance)),
							fmt.Sprintf("%f", cache.DataReadOnHit(curCacheInstance)),
							fmt.Sprintf("%f", cache.DataReadOnMiss(curCacheInstance)),
							fmt.Sprintf("%f", cache.DataDeleted(curCacheInstance)),
							fmt.Sprintf("%f", cache.AvgFreeSpace(curCacheInstance)),
							fmt.Sprintf("%f", cache.StdDevFreeSpace(curCacheInstance)),
							fmt.Sprintf("%f", cache.CPUEff(curCacheInstance)),
							fmt.Sprintf("%f", cache.CPUHitEff(curCacheInstance)),
							fmt.Sprintf("%f", cache.CPUMissEff(curCacheInstance)),
							fmt.Sprintf("%f", cache.CPUEffUpperBound(curCacheInstance)),
							fmt.Sprintf("%f", cache.CPUEffLowerBound(curCacheInstance)),
						}
						if cacheType == "aiRL" {
							csvRow = append(csvRow, strings.Split(cache.ExtraOutput(curCacheInstance, "epsilonStats"), ",")...)
							csvRow = append(csvRow, strings.Split(cache.ExtraOutput(curCacheInstance, "valueFunctions"), ",")...)
							csvRow = append(csvRow, strings.Split(cache.ExtraOutput(curCacheInstance, "evictionStats"), ",")...)
							csvRow = append(csvRow, strings.Split(cache.ExtraOutput(curCacheInstance, "actionStats"), ",")...)
						}
						csvSimOutput.Write(csvRow)
					}
					cache.ClearStats(curCacheInstance)
					// Update time window
					latestTime = curTime
					windowStepCounter++
				}

				if windowStepCounter == simWindowSize {
					windowCounter++
					windowStepCounter = 0
					numDailyRecords = 0
				}
				if windowCounter == simWindowStop {
					break
				}

				totNumRecords++

				if windowCounter >= simWindowStart {
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
							zap.Int("window", windowCounter),
							zap.Int("step", windowStepCounter),
							zap.Int("windowSize", simWindowSize),
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
							zap.Int("window", windowCounter),
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
				profileWriteErr := pprof.WriteHeapProfile(profileOut)
				if profileWriteErr != nil {
					panic(profileWriteErr)
				}
				profileCloseErr := profileOut.Close()
				if profileCloseErr != nil {
					panic(profileCloseErr)
				}
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
			defer func() {
				closeErr := statFile.Close()
				if closeErr != nil {
					panic(closeErr)
				}
			}()
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
			_, statFileWriteErr := statFile.Write(jsonBytes)
			if statFileWriteErr != nil {
				panic(statFileWriteErr)
			}

			if simDump {
				cache.Dump(curCacheInstance, simDumpFileName, simDumpFilesAndStats)
			}

			if cacheType == "aiRL" {
				// Save tables
				logger.Info("Save addition table...")
				cache.ExtraOutput(curCacheInstance, "additionQTable")
				logger.Info("Save eviction table...")
				cache.ExtraOutput(curCacheInstance, "evictionQTable")
			}

			_ := cache.Terminate(curCacheInstance)

			errViperWrite := viper.WriteConfigAs("config.yaml")
			if errViperWrite != nil {
				panic(errViperWrite)
			}

			logger.Info("Simulation DONE!")
			_ = logger.Sync()
			// TODO: fix error
			// -> https://github.com/uber-go/zap/issues/772
			// -> https://github.com/uber-go/zap/issues/328

		},
	}
	simCmd.PersistentFlags().StringVar(
		&logLevel, "logLevel", "INFO",
		"[Debugging] Enable or not a level of logging",
	)
	return simCmd
}

func main() {
	rootCmd := &cobra.Command{}
	rootCmd.AddCommand(simCommand())
	//rootCmd.AddCommand(commandSimulate())
	//rootCmd.AddCommand(commandSimulateAI())
	//rootCmd.AddCommand(testDataset())

	rootCmd.AddCommand(&cobra.Command{
		Use:   "version",
		Short: "Print the version number",
		Long:  "Print the version number of the executable",
		Run: func(cmd *cobra.Command, args []string) {
			fmt.Printf("Build time:\t%s\nGit hash:\t%s\n", buildstamp, githash)
		},
	})

	if err := rootCmd.Execute(); err != nil {
		fmt.Println(err.Error())
		os.Exit(-1)
	}
}

func genCache(cacheType string, cacheSize float64, cacheSizeUnit string, log bool, redirect bool, watermarks bool, weightFunc string, weightAlpha float64, weightBeta float64, weightGamma float64) cache.Cache {
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
		cache.Init(cacheInstance, cache.LRUQueue, log, redirect, watermarks)
	case "lfu":
		logger.Info("Create LFU Cache",
			zap.Float64("cacheSize", cacheSizeMegabytes),
		)
		cacheInstance = &cache.SimpleCache{
			MaxSize: cacheSizeMegabytes,
		}
		cache.Init(cacheInstance, cache.LFUQueue, log, redirect, watermarks)
	case "sizeBig":
		logger.Info("Create Size Big Cache",
			zap.Float64("cacheSize", cacheSizeMegabytes),
		)
		cacheInstance = &cache.SimpleCache{
			MaxSize: cacheSizeMegabytes,
		}
		cache.Init(cacheInstance, cache.SizeBigQueue, log, redirect, watermarks)
	case "sizeSmall":
		logger.Info("Create Size Small Cache",
			zap.Float64("cacheSize", cacheSizeMegabytes),
		)
		cacheInstance = &cache.SimpleCache{
			MaxSize: cacheSizeMegabytes,
		}
		cache.Init(cacheInstance, cache.SizeSmallQueue, log, redirect, watermarks)
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

		cacheInstance = &cache.WeightFun{
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
		cache.Init(cacheInstance, cache.LRUQueue, log, redirect, watermarks)
	default:
		fmt.Printf("ERR: '%s' is not a valid cache type...\n", cacheType)
		os.Exit(-2)
	}
	return cacheInstance
}
