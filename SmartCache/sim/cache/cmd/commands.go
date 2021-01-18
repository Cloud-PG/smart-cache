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
	"simulator/v2/cache/functions"
	"simulator/v2/cache/queue"
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
			conf.configure(configFilenameWithNoExt)

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

			conf.check()
			fmt.Printf("%+v\n", conf)

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
		conf     simConfig
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
			conf.configure(configFilenameWithNoExt)

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

			conf.check()
			// fmt.Printf("%+v\n", conf)

			// Generate simulation file output basename
			cacheSizeString := fmt.Sprintf("%d%s",
				conf.Sim.Cache.Size.Value,
				strings.ToUpper(conf.Sim.Cache.Size.Unit),
			)
			cacheBandwidthString := fmt.Sprintf("%dGbit", conf.Sim.Cache.Bandwidth.Value)

			var baseName string

			switch conf.Sim.Cache.Type {
			case "weightFunLRU":
				parameters := strings.Join([]string{
					fmt.Sprintf("%0.2f", conf.Sim.WeightFunction.Alpha),
					fmt.Sprintf("%0.2f", conf.Sim.WeightFunction.Beta),
					fmt.Sprintf("%0.2f", conf.Sim.WeightFunction.Gamma),
				}, "_")
				baseName = strings.Join([]string{
					conf.Sim.Cache.Type,
					conf.Sim.WeightFunction.Name,
					parameters,
				}, "_")
			case "aiRL":
				subAIType := conf.Sim.AI.RL.Type
				if conf.Sim.AI.RL.Type == "SCDL2" {
					subAIType += "-" + conf.Sim.AI.RL.Eviction.Type
				}
				baseName = strings.Join([]string{
					conf.Sim.Cache.Type,
					subAIType,
					cacheSizeString,
					cacheBandwidthString,
					conf.Sim.Region,
				}, "_")
			default:
				baseName = strings.Join([]string{
					conf.Sim.Cache.Type,
					cacheSizeString,
					cacheBandwidthString,
					conf.Sim.Region,
				}, "_")
			}

			// Output files
			dumpFileName := "cache_dump.json.gz"
			resultFileName := "simulation_results.csv"
			resultRunStatsName := "simulation_run_stats.json"

			// Create output folder and move working dir
			switch conf.Sim.Type { //nolint:ignore,nestif
			case "normal":
				finalOutputFolder := filepath.Join(conf.Sim.OutputFolder, "run_full_normal", baseName)
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
			if !conf.Sim.Overwrite { //nolint:ignore,nestif
				fileStat, errStat := os.Stat(resultFileName)
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

			wfunctionParams := cache.WeightFunctionParameters{
				Alpha: conf.Sim.WeightFunction.Alpha,
				Beta:  conf.Sim.WeightFunction.Beta,
				Gamma: conf.Sim.WeightFunction.Gamma,
			}

			// ------------------------- Create cache --------------------------
			curCacheInstance := cache.Create(
				conf.Sim.Cache.Type,
				float64(conf.Sim.Cache.Size.Value),
				conf.Sim.Cache.Size.Unit,
				conf.Sim.WeightFunction.Name,
				wfunctionParams,
			)

			// ------------------------- Init cache ----------------------------
			cache.InitInstance(
				conf.Sim.Cache.Type,
				curCacheInstance,
				cache.InitParameters{
					Log:                        conf.Sim.Log,
					RedirectReq:                conf.Sim.Cache.Bandwidth.Redirect,
					Watermarks:                 conf.Sim.Cache.Watermarks,
					HighWatermark:              conf.Sim.Cache.Watermark.High,
					LowWatermark:               conf.Sim.Cache.Watermark.Low,
					Dataset2TestPath:           conf.Sim.AI.Dataset2TestPath,
					AIFeatureMap:               conf.Sim.AI.Featuremap,
					AIModel:                    conf.Sim.AI.Model,
					FunctionTypeString:         conf.Sim.WeightFunction.Name,
					QueueType:                  queue.Unassigned,
					WfType:                     functions.Unassigned,
					CalcWeight:                 false,
					WfParams:                   wfunctionParams,
					EvictionAgentType:          conf.Sim.AI.RL.Eviction.Type,
					AIRLEvictionK:              int64(conf.Sim.AI.RL.Eviction.K),
					AIRLType:                   conf.Sim.AI.RL.Type,
					AIRLAdditionFeatureMap:     conf.Sim.AI.RL.Addition.Featuremap,
					AIRLEvictionFeatureMap:     conf.Sim.AI.RL.Eviction.Featuremap,
					AIRLAdditionEpsilonStart:   conf.Sim.AI.RL.Addition.Epsilon.Start,
					AIRLAdditionEpsilonDecay:   conf.Sim.AI.RL.Addition.Epsilon.Decay,
					AIRLAdditionEpsilonUnleash: conf.Sim.AI.RL.Addition.Epsilon.Unleash,
					AIRLEvictionEpsilonStart:   conf.Sim.AI.RL.Eviction.Epsilon.Start,
					AIRLEvictionEpsilonDecay:   conf.Sim.AI.RL.Eviction.Epsilon.Decay,
					AIRLEvictionEpsilonUnleash: conf.Sim.AI.RL.Eviction.Epsilon.Unleash,
					MaxNumDayDiff:              conf.Sim.Cache.Stats.MaxNumDayDiff,
					DeltaDaysStep:              conf.Sim.Cache.Stats.DeltaDaysStep,
					RandSeed:                   int64(conf.Sim.Seed),
				},
			)

			// --------------------- Set cache Bandwidth -----------------------
			cache.SetBandwidth(curCacheInstance, float64(conf.Sim.Cache.Bandwidth.Value))

			// ----------------------- Set cache Region ------------------------
			var (
				recordFilter   cache.Filter
				dataTypeFilter cache.Filter
			)

			switch conf.Sim.Region {
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
			if conf.Sim.Dumpfilename == "" {
				conf.Sim.Dumpfilename = dumpFileName
			}
			if conf.Sim.Loaddumpfilename == "" {
				conf.Sim.Loaddumpfilename = dumpFileName
			}

			// -------------------------- Simulate -----------------------------
			cache.Simulate(
				conf.Sim.Cache.Type,
				curCacheInstance,
				cache.SimulationParams{
					CPUprofile:         conf.Sim.CPUProfile,
					MEMprofile:         conf.Sim.MEMProfile,
					DataPath:           conf.Sim.Data,
					OutFile:            resultFileName,
					BaseName:           baseName,
					ResultRunStatsName: resultRunStatsName,
					LoadDump:           conf.Sim.Loaddump,
					LoadDumpFileName:   conf.Sim.Loaddumpfilename,
					Dump:               conf.Sim.Dump,
					DumpFileName:       conf.Sim.Dumpfilename,
					DumpFilesAndStats:  conf.Sim.Dumpfilesandstats,
					AIRLEpsilonStart:   conf.Sim.AI.RL.Epsilon.Start,
					AIRLEpsilonDecay:   conf.Sim.AI.RL.Epsilon.Decay,
					ColdStart:          conf.Sim.Coldstart,
					ColdStartNoStats:   conf.Sim.Coldstartnostats,
					WindowSize:         int(conf.Sim.Window.Size),
					WindowStart:        int(conf.Sim.Window.Start),
					WindowStop:         int(conf.Sim.Window.Stop),
					OutputUpdateDelay:  conf.Sim.Outputupdatedelay,
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
