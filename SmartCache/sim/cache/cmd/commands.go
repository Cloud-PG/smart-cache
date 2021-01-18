package cmd

import (
	"errors"
	"fmt"
	"io/ioutil"
	"net/http"
	"os"
	"path/filepath"
	"strings"
	"time"

	"simulator/v2/cache"
	"simulator/v2/cache/service"

	"github.com/spf13/cobra"
	"github.com/spf13/viper"

	"github.com/rs/zerolog"
	"github.com/rs/zerolog/log"
)

func initLog(level zerolog.Level) {
	log.Logger = log.Output(zerolog.ConsoleWriter{Out: os.Stderr})

	zerolog.SetGlobalLevel(level)
}

var (
	githash         string
	buildstamp      string
	errorNoConfFile error = errors.New("requires a configuration file")
)

func serve() *cobra.Command { //nolint: funlen
	var (
		logLevel string
		conf     serviceConfig
	)

	serveCmd := &cobra.Command{ // nolint: exhaustivestruct
		Use:   "serve config",
		Short: "start the smart cache service",
		Long:  `the service to enhance caching data management in a Data Lake`,
		Args: func(cmd *cobra.Command, args []string) error {
			if len(args) < 1 {
				return errorNoConfFile
			}

			return nil
		},
		Run: func(cmd *cobra.Command, args []string) {
			// Get arguments
			configFile := args[0]

			// CHECK DEBUG MODE
			switch logLevel {
			case "INFO", "info":
				initLog(zerolog.InfoLevel)
			case "DEBUG", "debug":
				initLog(zerolog.DebugLevel)
			}

			log.Info().Str("config file", configFile).Msg("Get service config file")

			configAbsPath, errAbs := filepath.Abs(configFile)
			if errAbs != nil {
				panic(errAbs)
			}
			configDir := filepath.Dir(configAbsPath)
			configFilename := filepath.Base(configAbsPath)
			configFilenameWithNoExt := strings.TrimSuffix(configFilename, filepath.Ext(configFilename))

			log.Info().Str("path", configAbsPath).Msg("Config file ABS path")
			log.Info().Str("path", configDir).Msg("Config file Directory")
			log.Info().Str("file", configFilename).Msg("Config filename")
			log.Info().Str("file", configFilenameWithNoExt).Msg("Config filename without extension")

			log.Info().Str("path", configDir).Msg("Change dir moving on config parent folder")
			errChdir := os.Chdir(configDir)
			if errChdir != nil {
				panic(errChdir)
			}
			curWd, _ := os.Getwd()
			log.Info().Str("path", curWd).Msg("Current Working Dir")

			log.Info().Msg("Set config defaults")
			configureServiceViperVars(configFilenameWithNoExt)

			log.Info().Msg("Read conf file")
			if err := viper.ReadInConfig(); err != nil {
				if _, ok := err.(viper.ConfigFileNotFoundError); ok {
					// Config file not found; ignore error if desired
					panic(err)
				} else {
					// Config file was found but another error was produced
					panic(err)
				}
			}

			err := viper.Unmarshal(&conf)
			if err != nil {
				panic(fmt.Errorf("unable to decode into struct, %w", err))
			}

			// fmt.Printf("%+v\n", conf)

			switch conf.Service.Protocol {
			case "http", "HTTP", "Http":
				log.Info().Msg("Create HTTP server")

				host := conf.Service.Host
				port := conf.Service.Port

				server := &http.Server{
					Addr:         fmt.Sprintf("%s:%d", host, port),
					ReadTimeout:  5 * time.Minute, // 5 min to allow for delays when 'curl' on OSx prompts for username/password
					WriteTimeout: 10 * time.Second,
					// TLSConfig:    &tls.Config{ServerName: *host},
				}

				http.HandleFunc("/", func(w http.ResponseWriter, r *http.Request) {
					log.Printf("Received %s request for host %s from IP address %s and X-FORWARDED-FOR %s",
						r.Method, r.Host, r.RemoteAddr, r.Header.Get("X-FORWARDED-FOR"))
					body, err := ioutil.ReadAll(r.Body)
					if err != nil {
						body = []byte(fmt.Sprintf("error reading request body: %s", err))
					}
					resp := fmt.Sprintf("Hello, %s from Smart Cache Service!", body)
					_, errWrite := w.Write([]byte(resp))
					if errWrite != nil {
						log.Err(errWrite).Str("resp", resp).Msg("Cannot write a response")
					} else {
						log.Printf("Sent response %s", resp)
					}
				})

				http.HandleFunc("/version", service.Version(buildstamp, githash))

				log.Info().Str("host", host).Uint("port", port).Msg("Starting HTTP server")
				if err := server.ListenAndServe(); err != nil {
					log.Err(err).Msg("Cannot start Smart Cache Service")
				}
				log.Info().Str("host", host).Uint("port", port).Msg("Server HTTP started!")
			default:
				panic(fmt.Errorf("protocol '%s' is not supported", conf.Service.Protocol))
			}
		},
	}
	serveCmd.PersistentFlags().StringVar(
		&logLevel, "logLevel", "INFO",
		"[Debugging] Enable or not a level of logging",
	)

	return serveCmd
}

func sim() *cobra.Command { //nolint:ignore,funlen
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
		aiFeatureMap               string
		aiModel                    string
		aiRLType                   string
		aiRLAdditionFeatureMap     string
		aiRLEvictionFeatureMap     string
		aiRLEpsilonStart           float64
		aiRLEpsilonDecay           float64
		aiRLEpsilonUnleash         bool
		aiRLEvictionK              int64
		aiRLAdditionEpsilonStart   float64
		aiRLAdditionEpsilonDecay   float64
		aiRLAdditionEpsilonUnleash bool
		aiRLEvictionEpsilonStart   float64
		aiRLEvictionEpsilonDecay   float64
		aiRLEvictionEpsilonUnleash bool
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

	simCmd := &cobra.Command{ // nolint: exhaustivestruct
		Use:   "sim config",
		Short: "a simulation environment for Smart Cache in a Data Lake",
		Long: `a simulation environment for Smart Cache in a Data Lake,
		used as comparison measure for the new approaches`,
		Args: func(cmd *cobra.Command, args []string) error {
			if len(args) < 1 {
				return errorNoConfFile
			}

			return nil
		},
		Run: func(cmd *cobra.Command, args []string) {
			// Get arguments
			configFile := args[0]

			// CHECK DEBUG MODE
			switch logLevel {
			case "INFO", "info":
				initLog(zerolog.InfoLevel)
			case "DEBUG", "debug":
				initLog(zerolog.DebugLevel)
			}

			log.Info().Str("config file", configFile).Msg("Get simulation config file")

			configAbsPath, errAbs := filepath.Abs(configFile)
			if errAbs != nil {
				panic(errAbs)
			}
			configDir := filepath.Dir(configAbsPath)
			configFilename := filepath.Base(configAbsPath)
			configFilenameWithNoExt := strings.TrimSuffix(configFilename, filepath.Ext(configFilename))

			log.Info().Str("path", configAbsPath).Msg("Config file ABS path")
			log.Info().Str("path", configDir).Msg("Config file Directory")
			log.Info().Str("file", configFilename).Msg("Config filename")
			log.Info().Str("file", configFilenameWithNoExt).Msg("Config filename without extension")

			log.Info().Str("path", configDir).Msg("Change dir moving on config parent folder")
			errChdir := os.Chdir(configDir)
			if errChdir != nil {
				panic(errChdir)
			}
			curWd, _ := os.Getwd()
			log.Info().Str("path", curWd).Msg("Current Working Dir")

			log.Info().Msg("Set config defaults")
			configureSimViperVars(configFilenameWithNoExt)

			log.Info().Msg("Read conf file")
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
			log.Info().Int64("randSeed", randSeed).Msg("CONF_VAR")

			cacheSize = viper.GetFloat64("sim.cache.size.value")
			log.Info().Float64("cacheSize", cacheSize).Msg("CONF_VAR")

			cacheSizeUnit = viper.GetString("sim.cache.size.unit")
			log.Info().Str("cacheSizeUnit", cacheSizeUnit).Msg("CONF_VAR")

			simOverwrite = viper.GetBool("sim.overwrite")
			log.Info().Bool("simOverwrite", simOverwrite).Msg("CONF_VAR")

			simBandwidth = viper.GetFloat64("sim.cache.bandwidth.value")
			log.Info().Float64("simBandwidth", simBandwidth).Msg("CONF_VAR")

			simRedirectReq = viper.GetBool("sim.cache.bandwidth.redirect")
			log.Info().Bool("simRedirectReq", simRedirectReq).Msg("CONF_VAR")

			simCacheWatermarks = viper.GetBool("sim.cache.watermarks")
			log.Info().Bool("simCacheWatermarks", simCacheWatermarks).Msg("CONF_VAR")

			simCacheHighWatermark = viper.GetFloat64("sim.cache.watermark.high")
			log.Info().Float64("simCacheHighWatermark", simCacheHighWatermark).Msg("CONF_VAR")

			simCacheLowWatermark = viper.GetFloat64("sim.cache.watermark.low")
			log.Info().Float64("simCacheLowWatermark", simCacheLowWatermark).Msg("CONF_VAR")

			maxNumDayDiff = viper.GetFloat64("sim.cache.stats.maxNumDayDiff")
			log.Info().Float64("maxNumDayDiff", maxNumDayDiff).Msg("CONF_VAR")

			deltaDaysStep = viper.GetFloat64("sim.cache.stats.deltaDaysStep")
			log.Info().Float64("deltaDaysStep", deltaDaysStep).Msg("CONF_VAR")

			simColdStart = viper.GetBool("sim.coldstart")
			log.Info().Bool("simColdStart", simColdStart).Msg("CONF_VAR")

			simColdStartNoStats = viper.GetBool("sim.coldstartnostats")
			log.Info().Bool("simColdStartNoStats", simColdStartNoStats).Msg("CONF_VAR")

			simDataPath = viper.GetString("sim.data")
			simDataPath, errAbs = filepath.Abs(simDataPath)
			if errAbs != nil {
				panic(errAbs)
			}
			log.Info().Str("simDataPath", simDataPath).Msg("CONF_VAR")

			simDump = viper.GetBool("sim.dump")
			log.Info().Bool("simDump", simDump).Msg("CONF_VAR")

			simDumpFileName = viper.GetString("sim.dumpfilename")
			log.Info().Str("simDumpFileName", simDumpFileName).Msg("CONF_VAR")

			simDumpFilesAndStats = viper.GetBool("sim.dumpfilesandstats")
			log.Info().Bool("simDumpFilesAndStats", simDumpFilesAndStats).Msg("CONF_VAR")

			simFileType = viper.GetString("sim.filetype")
			log.Info().Str("simFileType", simFileType).Msg("CONF_VAR")

			simLoadDump = viper.GetBool("sim.loaddump")
			log.Info().Bool("simLoadDump", simLoadDump).Msg("CONF_VAR")

			simLoadDumpFileName = viper.GetString("sim.loaddumpfilename")
			log.Info().Str("simLoadDumpFileName", simLoadDumpFileName).Msg("CONF_VAR")

			simLog = viper.GetBool("sim.log")
			log.Info().Bool("simLog", simLog).Msg("CONF_VAR")

			simOutputFolder = viper.GetString("sim.outputFolder")
			simOutputFolder, errAbs = filepath.Abs(simOutputFolder)
			if errAbs != nil {
				panic(errAbs)
			}
			log.Info().Str("simOutputFolder", simOutputFolder).Msg("CONF_VAR")

			simRegion = viper.GetString("sim.region")
			log.Info().Str("simRegion", simRegion).Msg("CONF_VAR")

			simType = viper.GetString("sim.type")
			log.Info().Str("simType", simType).Msg("CONF_VAR")

			simWindowStart = viper.GetInt("sim.window.start")
			log.Info().Int("simWindowStart", simWindowStart).Msg("CONF_VAR")

			simWindowStop = viper.GetInt("sim.window.stop")
			log.Info().Int("simWindowStop", simWindowStop).Msg("CONF_VAR")

			simWindowSize = viper.GetInt("sim.window.size")
			log.Info().Int("simWindowSize", simWindowSize).Msg("CONF_VAR")

			cpuprofile = viper.GetString("sim.cpuprofile")
			log.Info().Str("cpuprofile", cpuprofile).Msg("CONF_VAR")

			memprofile = viper.GetString("sim.memprofile")
			log.Info().Str("memprofile", memprofile).Msg("CONF_VAR")

			outputUpdateDelay = viper.GetFloat64("sim.outputupdatedelay")
			log.Info().Float64("outputUpdateDelay", outputUpdateDelay).Msg("CONF_VAR")

			weightFunc = viper.GetString("sim.weightfunc.name")
			log.Info().Str("weightFunc", weightFunc).Msg("CONF_VAR")

			weightAlpha = viper.GetFloat64("sim.weightfunc.alpha")
			log.Info().Float64("weightAlpha", weightAlpha).Msg("CONF_VAR")

			weightBeta = viper.GetFloat64("sim.weightfunc.beta")
			log.Info().Float64("weightBeta", weightBeta).Msg("CONF_VAR")

			weightGamma = viper.GetFloat64("sim.weightfunc.gamma")
			log.Info().Float64("weightGamma", weightGamma).Msg("CONF_VAR")

			aiFeatureMap = viper.GetString("sim.ai.featuremap")
			if aiFeatureMap != "" {
				aiFeatureMap, errAbs = filepath.Abs(aiFeatureMap)
				if errAbs != nil {
					panic(errAbs)
				}
			}
			log.Info().Str("aiFeatureMap", aiFeatureMap).Msg("CONF_VAR")

			dataset2TestPath = viper.GetString("sim.ai.dataset2TestPath")
			log.Info().Str("dataset2TestPath", dataset2TestPath).Msg("CONF_VAR")

			aiModel = viper.GetString("sim.ai.model")
			log.Info().Str("aiModel", aiModel).Msg("CONF_VAR")

			aiRLType = viper.GetString("sim.ai.rl.type")
			log.Info().Str("aiRLType", aiRLType).Msg("CONF_VAR")

			aiRLAdditionFeatureMap = viper.GetString("sim.ai.rl.addition.featuremap")
			if aiRLAdditionFeatureMap != "" {
				aiRLAdditionFeatureMap, errAbs = filepath.Abs(aiRLAdditionFeatureMap)
				if errAbs != nil {
					panic(errAbs)
				}
			}
			log.Info().Str("aiRLAdditionFeatureMap", aiRLAdditionFeatureMap).Msg("CONF_VAR")

			aiRLEvictionFeatureMap = viper.GetString("sim.ai.rl.eviction.featuremap")
			if aiRLEvictionFeatureMap != "" {
				aiRLEvictionFeatureMap, errAbs = filepath.Abs(aiRLEvictionFeatureMap)
				if errAbs != nil {
					panic(errAbs)
				}
			}
			log.Info().Str("aiRLEvictionFeatureMap", aiRLEvictionFeatureMap).Msg("CONF_VAR")

			aiRLEvictionK = viper.GetInt64("sim.ai.rl.eviction.k")
			log.Info().Int64("aiRLEvictionK", aiRLEvictionK).Msg("CONF_VAR")

			aiRLEpsilonStart = viper.GetFloat64("sim.ai.rl.epsilon.start")
			log.Info().Float64("aiRLEpsilonStart", aiRLEpsilonStart).Msg("CONF_VAR")

			aiRLEpsilonDecay = viper.GetFloat64("sim.ai.rl.epsilon.decay")
			log.Info().Float64("aiRLEpsilonDecay", aiRLEpsilonDecay).Msg("CONF_VAR")

			aiRLEpsilonUnleash = viper.GetBool("sim.ai.rl.epsilon.unleash")
			log.Info().Bool("aiRLEpsilonUnleash", aiRLEpsilonUnleash).Msg("CONF_VAR")

			aiRLAdditionEpsilonStart = viper.GetFloat64("sim.ai.rl.addition.epsilon.start")
			log.Info().Float64("aiRLAdditionEpsilonStart", aiRLAdditionEpsilonStart).Msg("CONF_VAR")
			if aiRLAdditionEpsilonStart == -1.0 {
				aiRLAdditionEpsilonStart = aiRLEpsilonStart
				log.Info().Float64("aiRLAdditionEpsilonStartOverwrite", aiRLAdditionEpsilonStart).Msg("CONF_VAR")
			}

			aiRLAdditionEpsilonDecay = viper.GetFloat64("sim.ai.rl.addition.epsilon.decay")
			log.Info().Float64("aiRLAdditionEpsilonDecay", aiRLAdditionEpsilonDecay).Msg("CONF_VAR")
			if aiRLAdditionEpsilonDecay == -1.0 {
				aiRLAdditionEpsilonDecay = aiRLEpsilonDecay
				log.Info().Float64("aiRLAdditionEpsilonDecayOverwrite", aiRLAdditionEpsilonDecay).Msg("CONF_VAR")
			}

			aiRLAdditionEpsilonUnleash = viper.GetBool("sim.ai.rl.addition.epsilon.unleash")
			log.Info().Bool("aiRLAdditionEpsilonUnleash", aiRLAdditionEpsilonUnleash).Msg("CONF_VAR")
			if aiRLEpsilonUnleash {
				aiRLAdditionEpsilonUnleash = aiRLEpsilonUnleash
				log.Info().Bool("aiRLAdditionEpsilonUnleashOverwrite", aiRLAdditionEpsilonUnleash).Msg("CONF_VAR")
			}

			aiRLEvictionEpsilonStart = viper.GetFloat64("sim.ai.rl.eviction.epsilon.start")
			log.Info().Float64("aiRLEvictionEpsilonStart", aiRLEvictionEpsilonStart).Msg("CONF_VAR")
			if aiRLEvictionEpsilonStart == -1.0 {
				aiRLEvictionEpsilonStart = aiRLEpsilonStart
				log.Info().Float64("aiRLEvictionEpsilonStartOverwrite", aiRLEvictionEpsilonStart).Msg("CONF_VAR")
			}

			aiRLEvictionEpsilonDecay = viper.GetFloat64("sim.ai.rl.eviction.epsilon.decay")
			log.Info().Float64("aiRLEvictionEpsilonDecay", aiRLEvictionEpsilonDecay).Msg("CONF_VAR")
			if aiRLEvictionEpsilonDecay == -1.0 {
				aiRLEvictionEpsilonDecay = aiRLEpsilonDecay
				log.Info().Float64("aiRLEvictionEpsilonDecayOverwrite", aiRLEvictionEpsilonDecay).Msg("CONF_VAR")
			}

			aiRLEvictionEpsilonUnleash = viper.GetBool("sim.ai.rl.eviction.epsilon.unleash")
			log.Info().Bool("aiRLEvictionEpsilonUnleash", aiRLEvictionEpsilonUnleash).Msg("CONF_VAR")
			if aiRLEpsilonUnleash {
				aiRLEvictionEpsilonUnleash = aiRLEpsilonUnleash
				log.Info().Bool("aiRLEvictionEpsilonUnleashOverwrite", aiRLEvictionEpsilonUnleash).Msg("CONF_VAR")
			}

			simEvictionType = viper.GetString("sim.ai.rl.eviction.type")
			log.Info().Str("simEvictionType", simEvictionType).Msg("CONF_VAR")

			cacheType = viper.GetString("sim.cache.type")
			log.Info().Str("cacheType", cacheType).Msg("CONF_VAR")

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
				log.Info().Str("path", curWd).Msg("Current Working Dir")
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
						log.Info().Msg("Simulation already DONE! NO OVERWRITE...")
						return
					} else {
						log.Info().Msg("Simulation results is empty... OVERWRITE...")
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
					EvictionAgentType:          simEvictionType,
					AIRLEvictionK:              aiRLEvictionK,
					AIRLType:                   aiRLType,
					AIRLAdditionFeatureMap:     aiRLAdditionFeatureMap,
					AIRLEvictionFeatureMap:     aiRLEvictionFeatureMap,
					AIRLAdditionEpsilonStart:   aiRLAdditionEpsilonStart,
					AIRLAdditionEpsilonDecay:   aiRLAdditionEpsilonDecay,
					AIRLAdditionEpsilonUnleash: aiRLAdditionEpsilonUnleash,
					AIRLEvictionEpsilonStart:   aiRLEvictionEpsilonStart,
					AIRLEvictionEpsilonDecay:   aiRLEvictionEpsilonDecay,
					AIRLEvictionEpsilonUnleash: aiRLEvictionEpsilonUnleash,
					MaxNumDayDiff:              maxNumDayDiff,
					DeltaDaysStep:              deltaDaysStep,
					RandSeed:                   randSeed,
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

func Execute() error {
	rootCmd := &cobra.Command{}
	rootCmd.AddCommand(sim())
	rootCmd.AddCommand(serve())

	//rootCmd.AddCommand(commandSimulate())
	//rootCmd.AddCommand(commandSimulateAI())
	//rootCmd.AddCommand(testDataset())

	rootCmd.AddCommand(&cobra.Command{ // nolint: exhaustivestruct
		Use:   "version",
		Short: "Print the version number",
		Long:  "Print the version number of the executable",
		Run: func(cmd *cobra.Command, args []string) {
			fmt.Printf("Build time:\t%s\nGit hash:\t%s\n", buildstamp, githash)
		},
	})

	return rootCmd.Execute()
}
