package main

import (
	"encoding/csv"
	"fmt"
	"log"
	"net"
	"os"
	"runtime/pprof"
	"strings"
	"time"

	"simulator/v2/cache"
	pb "simulator/v2/cache/simService"

	"google.golang.org/grpc"

	"github.com/spf13/cobra"
)

var (
	aiFeatureMap        string
	aiHost              string
	aiModel             string
	aiPort              string
	buildstamp          string
	cacheSize           float32
	cpuprofile          string
	dataset2TestPath    string
	githash             string
	memprofile          string
	outputUpdateDelay   float64
	serviceHost         string
	servicePort         int32
	simColdStart        bool
	simDump             bool
	simDumpFileName     string
	simLoadDump         bool
	simLoadDumpFileName string
	simOutFile          string
	simRegion           string
	simStartFromWindow  uint32
	simStopWindow       uint32
	simWindowSize       uint32
	statUpdatePolicy    string
	weightedFunc        string
	weightExp           float32
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
	rootCmd.PersistentFlags().Float32Var(
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
		&outputUpdateDelay, "outputUpdateDelay", 5.,
		"[Simulation] time delay for cmd output",
	)
	rootCmd.PersistentFlags().StringVar(
		&weightedFunc, "weightFunction", "FuncWeightedRequests",
		"[WeightedLRU] function to use with weighted cache",
	)
	rootCmd.PersistentFlags().Float32Var(
		&weightExp, "weightExp", 2.0,
		"[Simulation] Exponential to use with weighted cache function",
	)
	rootCmd.PersistentFlags().StringVar(
		&statUpdatePolicy, "statUpdatePolicy", "request",
		"[WeightedLRU] when to update the file stats: ['miss', 'request']. Default: request",
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
		&simColdStart, "simColdStart", true,
		"indicates if the cache have to be empty after a dump load",
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
			if len(args) != 2 {
				fmt.Println("ERR: You need to specify the cache type and a file or a folder")
				os.Exit(-1)
			}
			cacheType := args[0]
			pathString := args[1]
			copy(args, args[2:])
			args = args[:len(args)-1]

			baseName := strings.Join([]string{
				cacheType,
				fmt.Sprintf("%0.0fT", cacheSize/(1024.*1024.)),
				simRegion,
			},
				"_",
			)

			dumpFileName := baseName + ".json.gz"
			resultFileName := baseName + "_results.csv"

			// Create cache
			var grpcConn interface{} = nil
			curCacheInstance := genCache(cacheType)

			switch typeCmd {
			case aiSimCmd:
				if aiFeatureMap == "" {
					fmt.Println("ERR: No feature map indicated...")
					os.Exit(-1)
				}
				switch cacheType {
				case "aiNN":
					grpcConn = curCacheInstance.Init(aiHost, aiPort, aiFeatureMap, aiModel)
					if grpcConn != nil {
						defer grpcConn.(*grpc.ClientConn).Close()
					}
				case "aiRL":
					curCacheInstance.Init(aiFeatureMap)
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
				fmt.Println("[Loading cache dump...]")
				curCacheInstance.Load(simLoadDumpFileName)
				fmt.Println("[Cache dump loaded!]")
				if simColdStart {
					curCacheInstance.ClearFiles()
					fmt.Println("[Cache Files deleted][COLD START]")
				} else {
					fmt.Println("[Cache Files stored][HOT START]")
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

			outputFile, _ := os.Create(simOutFile)
			defer outputFile.Close()
			csvOutput := csv.NewWriter(outputFile)
			defer csvOutput.Flush()

			csvOutput.Write([]string{"date",
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
			})
			csvOutput.Flush()

			if simDump {
				defer curCacheInstance.Dump(simDumpFileName)
			}

			var (
				numRecords        int
				totIterations     uint32
				numIterations     uint32
				windowStepCounter uint32
				windowCounter     uint32
			)
			selectedRegion := fmt.Sprintf("_%s_", strings.ToLower(simRegion))

			simBeginTime := time.Now()
			start := time.Now()
			var latestTime time.Time

			if cpuprofile != "" {
				profileOut, err := os.Create(cpuprofile)
				if err != nil {
					fmt.Printf("ERR: Can not create CPU profile file %s.\n", err)
					os.Exit(-1)
				}
				pprof.StartCPUProfile(profileOut)
				defer pprof.StopCPUProfile()
			}

			fmt.Println("[Simulation START]")

			for record := range iterator {
				if strings.Compare(simRegion, "all") != 0 {
					if strings.Index(strings.ToLower(record.SiteName), selectedRegion) == -1 {
						continue
					}
				}

				numRecords++

				if latestTime.IsZero() {
					latestTime = time.Unix(record.Day, 0.)
				} else {
					curTime := time.Unix(record.Day, 0.)
					if curTime.Sub(latestTime).Hours() >= 24. {
						if windowCounter >= simStartFromWindow {
							csvOutput.Write([]string{
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
							})
							csvOutput.Flush()
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

					switch typeCmd {
					case aiSimCmd:
						cache.GetFile(
							curCacheInstance,
							record.Filename,
							sizeInMbytes,
							record.CPUTime+record.IOTime, // WTime
							record.CPUTime,
							record.Day,
							record.SiteName,
							record.UserID,
						)
					default:
						cache.GetFile(
							curCacheInstance,
							record.Filename,
							sizeInMbytes,
							record.CPUTime+record.IOTime, // WTime
							record.CPUTime,
							record.Day,
						)
					}

					numIterations++

					if time.Now().Sub(start).Seconds() >= outputUpdateDelay {
						elapsedTime := time.Now().Sub(simBeginTime)
						outString := strings.Join(
							[]string{
								fmt.Sprintf("[%s_%0.0f_%s]", cacheType, cacheSize, simRegion),
								fmt.Sprintf("[Elapsed Time: %02d:%02d:%02d]",
									int(elapsedTime.Hours()),
									int(elapsedTime.Minutes())%60,
									int(elapsedTime.Seconds())%60,
								),
								fmt.Sprintf("[Window %d]", windowCounter),
								fmt.Sprintf("[Step %d/%d]", windowStepCounter+1, simWindowSize),
								fmt.Sprintf("[Num.Records %d]", numRecords),
								fmt.Sprintf("[HitRate %.2f%%]", curCacheInstance.HitRate()),
								fmt.Sprintf("[Capacity %.2f%%]", curCacheInstance.Capacity()),
								fmt.Sprintf("[Extra-> %s]", curCacheInstance.ExtraStats()),
								fmt.Sprintf("[%0.0f it/s]", float64(numIterations)/time.Now().Sub(start).Seconds()),
								"\r",
							},
							"",
						)
						fmt.Print(outString)
						totIterations += numIterations
						numIterations = 0
						start = time.Now()
					}

					if windowStepCounter == simWindowSize {
						windowCounter++
						windowStepCounter = 0
					}
				} else if windowStepCounter == simWindowSize {
					fmt.Printf("[Jump %d records of window %d]\n",
						numRecords,
						windowCounter,
					)
					windowCounter++
					windowStepCounter = 0
					numRecords = 0
				} else {
					if time.Now().Sub(start).Seconds() >= outputUpdateDelay {
						fmt.Printf("[Jump %d records of window %d]\r",
							numRecords,
							windowCounter,
						)
						start = time.Now()
					}
				}

				if memprofile != "" {
					profileOut, err := os.Create(memprofile)
					if err != nil {
						fmt.Printf("ERR: Can not create Memory profile file %s.\n", err)
						os.Exit(-1)
					}
					pprof.WriteHeapProfile(profileOut)
					profileOut.Close()
					return
				}

			}
			elapsedTime := time.Now().Sub(simBeginTime)
			fmt.Printf("\n[Simulation END][elapsed Time: %02d:%02d:%02d][Num. Records: %d][Mean Records/s: %0.0f]\n",
				int(elapsedTime.Hours()),
				int(elapsedTime.Minutes())%60,
				int(elapsedTime.Seconds())%60,
				numRecords,
				float64(totIterations)/elapsedTime.Seconds(),
			)
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
	var cacheInstance cache.Cache
	switch cacheType {
	case "lru":
		fmt.Printf("[Create LRU Cache][Size: %f]\n", cacheSize)
		cacheInstance = &cache.LRUCache{
			MaxSize: cacheSize,
		}
		cacheInstance.Init()
	case "lruDatasetVerifier":
		fmt.Printf("[Create lruDatasetVerifier Cache][Size: %f]\n", cacheSize)
		cacheInstance = &cache.LRUDatasetVerifier{
			LRUCache: cache.LRUCache{
				MaxSize: cacheSize,
			},
		}
	case "aiNN":
		fmt.Printf("[Create aiNN Cache][Size: %f]\n", cacheSize)
		cacheInstance = &cache.AINN{
			LRUCache: cache.LRUCache{
				MaxSize: cacheSize,
			},
		}
	case "aiRL":
		fmt.Printf("[Create aiRL Cache][Size: %f]\n", cacheSize)
		cacheInstance = &cache.AIRL{
			LRUCache: cache.LRUCache{
				MaxSize: cacheSize,
			},
		}
	case "weightedLRU":
		fmt.Printf("[Create Weighted Cache][Size: %f]\n", cacheSize)

		var (
			selFunctionType         cache.FunctionType
			selUpdateStatPolicyType cache.UpdateStatsPolicyType
		)

		switch weightedFunc {
		case "FuncFileWeight":
			selFunctionType = cache.FuncFileWeight
		case "FuncFileWeightAndTime":
			selFunctionType = cache.FuncFileWeightAndTime
		case "FuncFileWeightOnlyTime":
			selFunctionType = cache.FuncFileWeightOnlyTime
		case "FuncWeightedRequests":
			selFunctionType = cache.FuncWeightedRequests
		default:
			fmt.Println("ERR: You need to specify a weight function.")
			os.Exit(-1)
		}

		switch statUpdatePolicy {
		case "miss":
			selUpdateStatPolicyType = cache.UpdateStatsOnMiss
		case "request":
			selUpdateStatPolicyType = cache.UpdateStatsOnRequest
		default:
			fmt.Println("ERR: You need to specify a weight function.")
			os.Exit(-1)
		}

		cacheInstance = &cache.WeightedLRU{
			LRUCache: cache.LRUCache{
				MaxSize: cacheSize,
			},
			Exp:                     weightExp,
			SelFunctionType:         selFunctionType,
			SelUpdateStatPolicyType: selUpdateStatPolicyType,
		}
		cacheInstance.Init()
	default:
		fmt.Printf("ERR: '%s' is not a valid cache type...\n", cacheType)
		os.Exit(-2)
	}
	return cacheInstance
}
