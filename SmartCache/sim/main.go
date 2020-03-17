package main

import (
	"encoding/json"
	"fmt"
	"log"
	"math"
	"net"
	"os"
	"runtime/pprof"
	"strings"
	"time"

	"simulator/v2/cache"
	pb "simulator/v2/cache/simService"

	"github.com/fatih/color"
	"go.uber.org/zap"
	"go.uber.org/zap/zapcore"

	"google.golang.org/grpc"

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
	aiHost                 string
	aiModel                string
	aiPort                 string
	aiRLAdditionFeatureMap string
	aiRLEvictionFeatureMap string
	aiRLExtTable           bool
	aiRLEpsilonStart       float64
	aiRLEpsilonDecay       float64
	buildstamp             string
	cacheSize              float64
	cpuprofile             string
	dataset2TestPath       string
	githash                string
	logger                 = log.New(os.Stderr, color.MagentaString("[SIM] "), log.Lshortfile|log.LstdFlags)
	logLevel               string
	memprofile             string
	outputUpdateDelay      float64
	serviceHost            string
	servicePort            int32
	simColdStart           bool
	simColdStartNoStats    bool
	simDump                bool
	simDumpFileName        string
	simFileType            string
	simLoadDump            bool
	simLoadDumpFileName    string
	simOutFile             string
	simRegion              string
	simStartFromWindow     uint32
	simStopWindow          uint32
	simWindowSize          uint32
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
	rootCmd.AddCommand(commandServe())
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
		&cacheSize, "size", 10485760., // 10TB
		"[Simulation] cache size",
	)
	rootCmd.PersistentFlags().StringVar(
		&serviceHost, "host", "localhost",
		"[Simulation] Ip to listen to",
	)
	rootCmd.PersistentFlags().Int32Var(
		&servicePort, "port", 5432,
		"[Simulation] cache sim service port",
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
	}
}

func commandServe() *cobra.Command {
	cmd := &cobra.Command{
		Run: func(cmd *cobra.Command, args []string) {
			// Get first element and reuse same memory space to allocate args
			cacheType := args[0]
			copy(args, args[1:])
			args = args[:len(args)-1]

			// Create cache
			curCacheInstance := genCache(cacheType)

			grpcServer := grpc.NewServer()
			fmt.Printf("[Register '%s' Cache]\n", cacheType)
			pb.RegisterSimServiceServer(grpcServer, curCacheInstance)

			fmt.Printf("[Try to listen to %s:%d]\n", serviceHost, servicePort)
			lis, err := net.Listen("tcp", fmt.Sprintf("%s:%d", serviceHost, servicePort))
			if err != nil {
				log.Fatalf("ERR: failed to listen on %s:%d -> %v", serviceHost, servicePort, err)
			}
			fmt.Printf("[Start server on %s:%d]\n", serviceHost, servicePort)

			if err := grpcServer.Serve(lis); err != nil {
				log.Fatalf("ERR: grpc serve error '%s'", err)
			}
		},
		Use:   `serve cacheType`,
		Short: "Simulator service",
		Long:  "Run a cache simulator service",
		Args:  cobra.MaximumNArgs(1),
	}
	return cmd
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

			baseName := strings.Join([]string{
				cacheType,
				fmt.Sprintf("%0.0fT", cacheSize/(1024.*1024.)),
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

			// Create cache
			var grpcConn interface{} = nil
			curCacheInstance := genCache(cacheType)

			switch typeCmd {
			case aiSimCmd:
				switch cacheType {
				case "aiNN":
					if aiFeatureMap == "" {
						fmt.Println("ERR: No feature map indicated...")
						os.Exit(-1)
					}
					grpcConn = curCacheInstance.Init(aiHost, aiPort, aiFeatureMap, aiModel)
					if grpcConn != nil {
						defer grpcConn.(*grpc.ClientConn).Close()
					}
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

					curCacheInstance.Init(
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
				curCacheInstance.Init(dataset2TestPath)
			}

			if simDumpFileName == "" {
				simDumpFileName = dumpFileName
			}
			if simLoadDumpFileName == "" {
				simLoadDumpFileName = dumpFileName
			}

			if simLoadDump {
				logger.Info("Loading cache dump", zap.String("filename", simLoadDumpFileName))

				loadedDump := curCacheInstance.Load(simLoadDumpFileName)

				if cacheType == "aiRL" {
					curCacheInstance.Loads(loadedDump, aiRLEpsilonStart, aiRLEpsilonDecay)
				} else {
					curCacheInstance.Loads(loadedDump)
				}

				logger.Info("Cache dump loaded!")
				if simColdStart {
					if simColdStartNoStats {
						curCacheInstance.Clear()
						logger.Info("Cache Files deleted... COLD START with NO STATISTICS")
					} else {
						curCacheInstance.ClearFiles()
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

			if simOutFile == "" {
				simOutFile = resultFileName
			}

			csvSimOutput := cache.OutputCSV{}
			csvSimOutput.Create(simOutFile)
			defer csvSimOutput.Close()

			csvSimOutput.Write([]string{"date",
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
			})

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

			if simDump {
				defer curCacheInstance.Dump(simDumpFileName)
			}

			var (
				numRecords        int64
				totNumRecords     int64
				totIterations     uint32
				numIterations     uint32
				windowStepCounter uint32
				windowCounter     uint32
				recordFilter      *cache.Filter
			)
			// selectedRegion := fmt.Sprintf("_%s_", strings.ToLower(simRegion))
			switch simRegion {
			case "us":
				recordFilter = &cache.UsFilter{}
			}

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

				if recordFilter != nil ; checkRecord := recordFilter.Check(record); checkRecord == false {
					continue
				}

				totNumRecords++

				// if strings.Compare(simRegion, "all") != 0 {
				// 	if strings.Index(strings.ToLower(record.SiteName), selectedRegion) == -1 {
				// 		// TODO: fix jump output
				// 		// fmt.Printf("[Jump region %s]\r",
				// 		// 	record.SiteName,
				// 		// )
				// 		continue
				// 	}
				// }
				// if strings.Compare(simFileType, "all") != 0 {
				// 	if strings.Index(strings.ToLower(record.FileType), strings.ToLower(simFileType)) == -1 {
				// 		// TODO: fix jump output
				// 		// fmt.Printf("[Jump file type %s]\r",
				// 		// 	record.FileType,
				// 		// )
				// 		continue
				// 	}
				// }

				numRecords++

				if latestTime.IsZero() {
					latestTime = time.Unix(record.Day, 0.)
				} else {
					curTime := time.Unix(record.Day, 0.)
					if curTime.Sub(latestTime).Hours() >= 24. {
						if windowCounter >= simStartFromWindow {
							csvSimOutput.Write([]string{
								fmt.Sprintf("%s", latestTime),
								fmt.Sprintf("%f", curCacheInstance.Size()),
								fmt.Sprintf("%0.2f", curCacheInstance.HitRate()),
								fmt.Sprintf("%0.2f", curCacheInstance.HitOverMiss()),
								fmt.Sprintf("%0.2f", curCacheInstance.WeightedHitRate()),
								fmt.Sprintf("%f", curCacheInstance.DataWritten()),
								fmt.Sprintf("%f", curCacheInstance.DataRead()),
								fmt.Sprintf("%f", curCacheInstance.DataReadOnHit()),
								fmt.Sprintf("%f", curCacheInstance.DataReadOnMiss()),
								fmt.Sprintf("%f", curCacheInstance.DataDeleted()),
								fmt.Sprintf("%f", curCacheInstance.CPUEff()),
								fmt.Sprintf("%f", curCacheInstance.CPUHitEff()),
								fmt.Sprintf("%f", curCacheInstance.CPUMissEff()),
								fmt.Sprintf("%f", curCacheInstance.CPUEffUpperBound()),
								fmt.Sprintf("%f", curCacheInstance.CPUEffLowerBound()),
							})
							curCacheInstance.ClearHitMissStats()
						}
						latestTime = time.Unix(record.Day, 0.)
						windowStepCounter++
					}
				}

				if windowCounter == simStopWindow {
					break
				} else if windowCounter >= simStartFromWindow {
					// TODO: make size measure a parameter: [K, M, G, P]
					sizeInMbytes := record.Size / (1024 * 1024)

					cpuEff := (record.CPUTime / (record.CPUTime + record.IOTime)) * 100.
					if cpuEff < 0. {
						cpuEff = 0.
					} else if math.IsInf(cpuEff, 0) {
						cpuEff = 0.
					} else if math.IsNaN(cpuEff) {
						cpuEff = 0.
					} else if cpuEff > 100. {
						cpuEff = 0.
					}

					cache.GetFile(
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

					numIterations++

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
							zap.Int64("numRecords", numRecords),
							zap.Float64("hitRate", curCacheInstance.HitRate()),
							zap.Float64("capacity", curCacheInstance.Capacity()),
							zap.String("extra", curCacheInstance.ExtraStats()),
							zap.Float64("it/s", float64(numIterations)/time.Now().Sub(start).Seconds()),
						)
						totIterations += numIterations
						numIterations = 0
						start = time.Now()
					}

					if windowStepCounter == simWindowSize {
						windowCounter++
						windowStepCounter = 0
					}
				} else if windowStepCounter == simWindowSize {
					logger.Info("Jump records",
						zap.Int64("numRecords", numRecords),
						zap.Uint32("window", windowCounter),
					)
					windowCounter++
					windowStepCounter = 0
					numRecords = 0
				} else {
					if time.Now().Sub(start).Seconds() >= outputUpdateDelay {
						logger.Info("Jump records",
							zap.Int64("numRecords", numRecords),
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
			logger.Info("Simulation END!",
				zap.String("elapsedTime", fmt.Sprintf("%02d:%02d:%02d", elTH, elTM, elTS)),
				zap.Float64("avg it/s", avgSpeed),
				zap.Int64("totRecords", numRecords),
			)
			// Save run statistics
			statFile, errCreateStat := os.Create(resultRunStatsName)
			defer statFile.Close()
			if errCreateStat != nil {
				panic(errCreateStat)
			}
			jsonBytes, errMarshal := json.Marshal(cache.SimulationStats{
				TimeElapsed:   fmt.Sprintf("%02d:%02d:%02d", elTH, elTM, elTS),
				Extra:         curCacheInstance.ExtraStats(),
				TotNumRecords: totNumRecords,
				AvgSpeed:      fmt.Sprintf("Num.Records/s = %0.2f", avgSpeed),
			})
			if cacheType == "aiRL" {
				// Save tables
				writeQTable(resultAdditionQTableName, curCacheInstance.ExtraOutput("additionQtable"))
				writeQTable(resultEvictionQTableName, curCacheInstance.ExtraOutput("evictionQtable"))
			}

			if errMarshal != nil {
				panic(errMarshal)
			}
			statFile.Write(jsonBytes)
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
			&aiHost, "aiHost", "localhost",
			"indicate the filter for record region",
		)
		cmd.PersistentFlags().StringVar(
			&aiPort, "aiPort", "4242",
			"indicate the filter for record region",
		)
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
	switch cacheType {
	case "lru":
		logger.Info("Create LRU Cache",
			zap.Float64("cacheSize", cacheSize),
		)
		cacheInstance = &cache.SimpleCache{
			MaxSize: cacheSize,
		}
		cacheInstance.Init()
	case "lfu":
		logger.Info("Create LFU Cache",
			zap.Float64("cacheSize", cacheSize),
		)
		cacheInstance = &cache.SimpleCache{
			MaxSize: cacheSize,
		}
		cacheInstance.Init(cache.LFUQueue)
	case "sizeBig":
		logger.Info("Create Size Big Cache",
			zap.Float64("cacheSize", cacheSize),
		)
		cacheInstance = &cache.SimpleCache{
			MaxSize: cacheSize,
		}
		cacheInstance.Init(cache.SizeBigQueue)
	case "sizeSmall":
		logger.Info("Create Size Small Cache",
			zap.Float64("cacheSize", cacheSize),
		)
		cacheInstance = &cache.SimpleCache{
			MaxSize: cacheSize,
		}
		cacheInstance.Init(cache.SizeSmallQueue)
	case "lruDatasetVerifier":
		logger.Info("Create lruDatasetVerifier Cache",
			zap.Float64("cacheSize", cacheSize),
		)
		cacheInstance = &cache.LRUDatasetVerifier{
			SimpleCache: cache.SimpleCache{
				MaxSize: cacheSize,
			},
		}
	case "aiNN":
		logger.Info("Create aiNN Cache",
			zap.Float64("cacheSize", cacheSize),
		)
		cacheInstance = &cache.AINN{
			SimpleCache: cache.SimpleCache{
				MaxSize: cacheSize,
			},
		}
	case "aiRL":
		logger.Info("Create aiRL Cache",
			zap.Float64("cacheSize", cacheSize),
		)
		cacheInstance = &cache.AIRL{
			SimpleCache: cache.SimpleCache{
				MaxSize: cacheSize,
			},
		}
	case "weightFunLRU":
		logger.Info("Create Weight Function Cache",
			zap.Float64("cacheSize", cacheSize),
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
				MaxSize: cacheSize,
			},
			Parameters: cache.WeightFunctionParameters{
				Alpha: weightAlpha,
				Beta:  weightBeta,
				Gamma: weightGamma,
			},
			SelFunctionType: selFunctionType,
		}
		cacheInstance.Init()
	default:
		fmt.Printf("ERR: '%s' is not a valid cache type...\n", cacheType)
		os.Exit(-2)
	}
	return cacheInstance
}
