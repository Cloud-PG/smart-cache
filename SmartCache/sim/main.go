package main

import (
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strings"

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

func configureViper(configFilenameWithNoExt string) { //nolint:ignore,funlen
	viper.SetConfigName(configFilenameWithNoExt) // name of config file (without extension)
	viper.SetConfigType("yaml")                  // REQUIRED if the config file does not have the extension in the name
	viper.AddConfigPath(".")                     // optionally look for config in the working directory

	viper.SetDefault("sim.region", "all")
	viper.SetDefault("sim.outputFolder", ".")
	viper.SetDefault("sim.overwrite", false)
	viper.SetDefault("sim.dump", false)
	viper.SetDefault("sim.dumpfilesandstats", true)
	viper.SetDefault("sim.dumpfilename", "")
	viper.SetDefault("sim.loaddump", false)
	viper.SetDefault("sim.loaddumpfilename", "")
	viper.SetDefault("sim.window.size", 7)
	viper.SetDefault("sim.window.start", 0)
	viper.SetDefault("sim.window.stop", 0)
	viper.SetDefault("sim.cache.watermarks", false)
	viper.SetDefault("sim.cache.watermark.high", 95.0)
	viper.SetDefault("sim.cache.watermark.low", 75.0)
	viper.SetDefault("sim.coldstart", false)
	viper.SetDefault("sim.coldstartnostats", false)
	viper.SetDefault("sim.log", false)
	viper.SetDefault("sim.seed", 42)

	viper.SetDefault("sim.cpuprofile", "")
	viper.SetDefault("sim.memprofile", "")
	viper.SetDefault("sim.outputupdatedelay", 2.4)

	viper.SetDefault("sim.cache.size.value", 100.)
	viper.SetDefault("sim.cache.size.unit", "T")
	viper.SetDefault("sim.cache.bandwidth.value", 10.0)
	viper.SetDefault("sim.cache.bandwidth.redirect", false)
	viper.SetDefault("sim.cache.stats.maxNumDayDiff", 6.0)
	viper.SetDefault("sim.cache.stats.deltaDaysStep", 7.0)

	viper.SetDefault("sim.weightfunc.name", "FuncAdditiveExp")
	viper.SetDefault("sim.weightfunc.alpha", 1.0)
	viper.SetDefault("sim.weightfunc.beta", 1.0)
	viper.SetDefault("sim.weightfunc.gamma", 1.0)
	viper.SetDefault("sim.loglevel", "INFO")

	viper.SetDefault("sim.ai.rl.epsilon.start", 1.0)
	viper.SetDefault("sim.ai.rl.epsilon.decay", 0.0000042)

	viper.SetDefault("sim.ai.featuremap", "")
	viper.SetDefault("sim.ai.rl.type", "SCDL2")

	viper.SetDefault("sim.ai.rl.addition.featuremap", "")
	viper.SetDefault("sim.ai.rl.addition.epsilon.start", -1.0)
	viper.SetDefault("sim.ai.rl.addition.epsilon.decay", -1.0)

	viper.SetDefault("sim.ai.rl.eviction.featuremap", "")
	viper.SetDefault("sim.ai.rl.eviction.k", 32)
	viper.SetDefault("sim.ai.rl.eviction.type", "onK")
	viper.SetDefault("sim.ai.rl.eviction.epsilon.start", -1.0)
	viper.SetDefault("sim.ai.rl.eviction.epsilon.decay", -1.0)

	viper.SetDefault("sim.ai.model", "")

	viper.SetDefault("sim.dataset2testpath", "")
}

func simCommand() *cobra.Command { //nolint:ignore,funlen
	// Simulation config variables
	var (
		logLevel string
		// Simulation
		randSeed              int64
		simOverwrite          bool
		simBandwidth          float64
		simRedirectReq        bool
		simCacheWatermarks    bool
		simCacheHighWatermark float64
		simCacheLowWatermark  float64
		simColdStart          bool
		simColdStartNoStats   bool
		simDataPath           string
		simDump               bool
		simDumpFileName       string
		simDumpFilesAndStats  bool
		simFileType           string
		simLoadDump           bool
		simLoadDumpFileName   string
		simLog                bool
		simOutputFolder       string
		simRegion             string
		simType               string
		simEvictionType       string
		simWindowStart        int
		simWindowStop         int
		simWindowSize         int
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
		aiFeatureMap             string
		aiModel                  string
		aiRLType                 string
		aiRLAdditionFeatureMap   string
		aiRLEvictionFeatureMap   string
		aiRLEpsilonStart         float64
		aiRLEpsilonDecay         float64
		aiRLEvictionK            int64
		aiRLAdditionEpsilonStart float64
		aiRLAdditionEpsilonDecay float64
		aiRLEvictionEpsilonStart float64
		aiRLEvictionEpsilonDecay float64
		// cache
		cacheType     string
		cacheSize     float64
		cacheSizeUnit string
		// dataset
		dataset2TestPath string
		// Stats
		maxNumDayDiff float64
		deltaDaysStep float64
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

			randSeed = viper.GetInt64("sim.seed")
			logger.Info("CONF_VAR", zap.Int64("randSeed", randSeed))

			cacheSize = viper.GetFloat64("sim.cache.size.value")
			logger.Info("CONF_VAR", zap.Float64("cacheSize", cacheSize))

			cacheSizeUnit = viper.GetString("sim.cache.size.unit")
			logger.Info("CONF_VAR", zap.String("cacheSizeUnit", cacheSizeUnit))

			simOverwrite = viper.GetBool("sim.overwrite")
			logger.Info("CONF_VAR", zap.Bool("simOverwrite", simOverwrite))

			simBandwidth = viper.GetFloat64("sim.cache.bandwidth.value")
			logger.Info("CONF_VAR", zap.Float64("simBandwidth", simBandwidth))

			simRedirectReq = viper.GetBool("sim.cache.bandwidth.redirect")
			logger.Info("CONF_VAR", zap.Bool("simRedirectReq", simRedirectReq))

			simCacheWatermarks = viper.GetBool("sim.cache.watermarks")
			logger.Info("CONF_VAR", zap.Bool("simCacheWatermarks", simCacheWatermarks))

			simCacheHighWatermark = viper.GetFloat64("sim.cache.watermark.high")
			logger.Info("CONF_VAR", zap.Float64("simCacheHighWatermark", simCacheHighWatermark))

			simCacheLowWatermark = viper.GetFloat64("sim.cache.watermark.low")
			logger.Info("CONF_VAR", zap.Float64("simCacheLowWatermark", simCacheLowWatermark))

			maxNumDayDiff = viper.GetFloat64("sim.cache.stats.maxNumDayDiff")
			logger.Info("CONF_VAR", zap.Float64("maxNumDayDiff", maxNumDayDiff))

			deltaDaysStep = viper.GetFloat64("sim.cache.stats.deltaDaysStep")
			logger.Info("CONF_VAR", zap.Float64("deltaDaysStep", deltaDaysStep))

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

			aiRLType = viper.GetString("sim.ai.rl.type")
			logger.Info("CONF_VAR", zap.String("aiRLType", aiRLType))

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

			aiRLEvictionK = viper.GetInt64("sim.ai.rl.eviction.k")
			logger.Info("CONF_VAR", zap.Int64("aiRLEvictionK", aiRLEvictionK))

			aiRLEpsilonStart = viper.GetFloat64("sim.ai.rl.epsilon.start")
			logger.Info("CONF_VAR", zap.Float64("aiRLEpsilonStart", aiRLEpsilonStart))

			aiRLEpsilonDecay = viper.GetFloat64("sim.ai.rl.epsilon.decay")
			logger.Info("CONF_VAR", zap.Float64("aiRLEpsilonDecay", aiRLEpsilonDecay))

			aiRLAdditionEpsilonStart = viper.GetFloat64("sim.ai.rl.addition.epsilon.start")
			logger.Info("CONF_VAR", zap.Float64("aiRLAdditionEpsilonStart", aiRLEpsilonStart))
			if aiRLAdditionEpsilonStart == -1.0 {
				aiRLAdditionEpsilonStart = aiRLEpsilonStart
			}

			aiRLAdditionEpsilonDecay = viper.GetFloat64("sim.ai.rl.addition.epsilon.decay")
			logger.Info("CONF_VAR", zap.Float64("aiRLAdditionEpsilonDecay", aiRLEpsilonDecay))
			if aiRLAdditionEpsilonDecay == -1.0 {
				aiRLAdditionEpsilonDecay = aiRLEpsilonDecay
			}

			aiRLEvictionEpsilonStart = viper.GetFloat64("sim.ai.rl.eviction.epsilon.start")
			logger.Info("CONF_VAR", zap.Float64("aiRLEvictionEpsilonStart", aiRLEpsilonStart))
			if aiRLEvictionEpsilonStart == -1.0 {
				aiRLEvictionEpsilonStart = aiRLEpsilonStart
			}

			aiRLEvictionEpsilonDecay = viper.GetFloat64("sim.ai.rl.eviction.epsilon.decay")
			logger.Info("CONF_VAR", zap.Float64("aiRLEvictionEpsilonDecay", aiRLEpsilonDecay))
			if aiRLEvictionEpsilonDecay == -1.0 {
				aiRLEvictionEpsilonDecay = aiRLEpsilonDecay
			}

			simEvictionType = viper.GetString("sim.ai.rl.eviction.type")
			logger.Info("CONF_VAR", zap.String("simEvictionType", simEvictionType))

			cacheType = viper.GetString("sim.cache.type")
			logger.Info("CONF_VAR", zap.String("cacheType", cacheType))

			// Generate simulation file output basename
			cacheSizeString := fmt.Sprintf("%0.0f%s", cacheSize, strings.ToUpper(cacheSizeUnit))
			cacheBandwidthString := fmt.Sprintf("%0.0fGbit", simBandwidth)

			var baseName string

			switch cacheType {
			case "weightFunLRU":
				parameters := strings.Join([]string{
					fmt.Sprintf("%0.2f", weightAlpha),
					fmt.Sprintf("%0.2f", weightBeta),
					fmt.Sprintf("%0.2f", weightGamma),
				}, "_")
				baseName = strings.Join([]string{
					cacheType,
					weightFunc,
					parameters,
				}, "_")
			case "aiRL":
				subAIType := aiRLType
				if aiRLType == "SCDL2" {
					subAIType += "-" + simEvictionType
				}
				baseName = strings.Join([]string{
					cacheType,
					subAIType,
					cacheSizeString,
					cacheBandwidthString,
					simRegion,
				}, "_")
			default:
				baseName = strings.Join([]string{
					cacheType,
					cacheSizeString,
					cacheBandwidthString,
					simRegion,
				}, "_")
			}

			// Output files
			simOutFile := ""
			dumpFileName := "cache_dump.json.gz"
			resultFileName := "simulation_results.csv"
			resultRunStatsName := "simulation_run_stats.json"

			if simOutFile == "" {
				simOutFile = resultFileName
			}

			// Create output folder and move working dir
			switch simType { //nolint:ignore,nestif
			case "normal":
				finalOutputFolder := filepath.Join(simOutputFolder, "run_full_normal", baseName)
				errMkdir := os.MkdirAll(finalOutputFolder, 0755)
				if errMkdir != nil && !os.IsExist(errMkdir) {
					panic(errMkdir)
				}
				errChdir = os.Chdir(finalOutputFolder)
				if errChdir != nil {
					panic(errChdir)
				}
				curWd, _ := os.Getwd()
				logger.Info("Current Working Dir", zap.String("path", curWd))
			}

			// Check previous simulation results
			if !simOverwrite { //nolint:ignore,nestif
				fileStat, errStat := os.Stat(simOutFile)
				if errStat != nil {
					if !os.IsNotExist(errStat) {
						panic(errStat)
					}
				} else {
					if fileStat.Size() > 600 {
						// TODO: check if the configuration is the same
						logger.Info("Simulation already DONE! NO OVERWRITE...")
						_ = logger.Sync()
						// TODO: fix error
						// -> https://github.com/uber-go/zap/issues/772
						// -> https://github.com/uber-go/zap/issues/328
						return
					} else {
						logger.Info("Simulation results is empty... OVERWRITE...")
					}
				}
			}

			// ------------------------- Create cache --------------------------
			curCacheInstance := cache.Create(
				cacheType,
				cacheSize,
				cacheSizeUnit,
				weightFunc,
				cache.WeightFunctionParameters{
					Alpha: weightAlpha,
					Beta:  weightBeta,
					Gamma: weightGamma,
				},
			)

			// ------------------------- Init cache ----------------------------
			cache.InitInstance(
				cacheType,
				curCacheInstance,
				cache.InitParameters{
					Log:                simLog,
					RedirectReq:        simRedirectReq,
					Watermarks:         simCacheWatermarks,
					HighWatermark:      simCacheHighWatermark,
					LowWatermark:       simCacheLowWatermark,
					Dataset2TestPath:   dataset2TestPath,
					AIFeatureMap:       aiFeatureMap,
					AIModel:            aiModel,
					FunctionTypeString: weightFunc,
					WfParams: cache.WeightFunctionParameters{
						Alpha: weightAlpha,
						Beta:  weightBeta,
						Gamma: weightGamma,
					},
					EvictionAgentType:        simEvictionType,
					AIRLEvictionK:            aiRLEvictionK,
					AIRLType:                 aiRLType,
					AIRLAdditionFeatureMap:   aiRLAdditionFeatureMap,
					AIRLEvictionFeatureMap:   aiRLEvictionFeatureMap,
					AIRLAdditionEpsilonStart: aiRLAdditionEpsilonStart,
					AIRLAdditionEpsilonDecay: aiRLAdditionEpsilonDecay,
					AIRLEvictionEpsilonStart: aiRLEvictionEpsilonStart,
					AIRLEvictionEpsilonDecay: aiRLEvictionEpsilonDecay,
					MaxNumDayDiff:            maxNumDayDiff,
					DeltaDaysStep:            deltaDaysStep,
					RandSeed:                 randSeed,
				},
			)

			// --------------------- Set cache Bandwidth -----------------------
			cache.SetBandwidth(curCacheInstance, simBandwidth)

			// ----------------------- Set cache Region ------------------------
			var (
				recordFilter   cache.Filter
				dataTypeFilter cache.Filter
			)
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

			// --------------------- Prepare simulation ------------------------
			if simDumpFileName == "" {
				simDumpFileName = dumpFileName
			}
			if simLoadDumpFileName == "" {
				simLoadDumpFileName = dumpFileName
			}

			// -------------------------- Simulate -----------------------------
			cache.Simulate(
				cacheType,
				curCacheInstance,
				cache.SimulationParams{
					CPUprofile:         cpuprofile,
					MEMprofile:         memprofile,
					DataPath:           simDataPath,
					OutFile:            simOutFile,
					BaseName:           baseName,
					ResultRunStatsName: resultRunStatsName,
					DumpFilename:       simDumpFileName,
					LoadDump:           simLoadDump,
					LoadDumpFileName:   simLoadDumpFileName,
					Dump:               simDump,
					DumpFileName:       simDumpFileName,
					DumpFilesAndStats:  simDumpFilesAndStats,
					AIRLEpsilonStart:   aiRLEpsilonStart,
					AIRLEpsilonDecay:   aiRLEpsilonDecay,
					ColdStart:          simColdStart,
					ColdStartNoStats:   simColdStartNoStats,
					WindowSize:         simWindowSize,
					WindowStart:        simWindowStart,
					WindowStop:         simWindowStop,
					OutputUpdateDelay:  outputUpdateDelay,
					RecordFilter:       recordFilter,
					DataTypeFilter:     dataTypeFilter,
				},
			)

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
