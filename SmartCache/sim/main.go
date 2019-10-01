package main

import (
	"compress/gzip"
	"encoding/csv"
	"fmt"
	"log"
	"net"
	"os"
	"strings"
	"time"

	"./cache"
	pb "./cache/simService"
	"google.golang.org/grpc"

	"github.com/spf13/cobra"
)

var (
	buildstamp          string
	githash             string
	cacheSize           float32
	serviceHost         string
	servicePort         int32
	weightExp           float32
	weightedFunc        string
	statUpdatePolicy    string
	limitStatsPolicy    string
	simRegion           string
	simOutFile          string
	simGenDataset       bool
	simGenDatasetName   string
	simDump             bool
	simDumpFileName     string
	simLoadDump         bool
	simLoadDumpFileName string
	simWindowSize       uint32
	simStartFromWindow  uint32
	simStopWindow       uint32
	simColdStart        bool
)

func main() {
	rootCmd := &cobra.Command{}
	rootCmd.AddCommand(commandServe())
	rootCmd.AddCommand(commandSimulate())

	rootCmd.AddCommand(&cobra.Command{
		Use:   "version",
		Short: "Print the version number",
		Long:  "Print the version number of the executable",
		Run: func(cmd *cobra.Command, args []string) {
			fmt.Printf("Build time:\t%s\nGit hash:\t%s\n", buildstamp, githash)
		},
	})
	rootCmd.PersistentFlags().Float32Var(
		&cacheSize, "size", 10485760., // 10TB
		"cache size",
	)
	rootCmd.PersistentFlags().StringVar(
		&serviceHost, "host", "localhost",
		"Ip to listen to",
	)
	rootCmd.PersistentFlags().Int32Var(
		&servicePort, "port", 5432,
		"cache sim service port",
	)
	rootCmd.PersistentFlags().StringVar(
		&weightedFunc, "weightFunction", "FuncWeightedRequests",
		"[WeightedLRU]function to use with weighted cache",
	)
	rootCmd.PersistentFlags().Float32Var(
		&weightExp, "weightExp", 2.0,
		"Exponential to use with weighted cache function",
	)
	rootCmd.PersistentFlags().StringVar(
		&statUpdatePolicy, "statUpdatePolicy", "request",
		"[WeightedLRU] when to update the file stats: ['miss', 'request']. Default: request",
	)
	rootCmd.PersistentFlags().StringVar(
		&limitStatsPolicy, "limitStatsPolicy", "Q1IsDoubleQ2LimitStats",
		"[WeightedLRU] how to maintain the file stats ['noLimit', 'Q1IsDoubleQ2LimitStats']. Default: single",
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

func commandSimulate() *cobra.Command {
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
				fmt.Sprintf("%0.0f", cacheSize),
				simRegion,
			},
				"_",
			)
			dumpFileName := baseName + ".json.gz"
			resultFileName := baseName + "_results.csv"

			// Create cache
			curCacheInstance := genCache(cacheType)

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
				fmt.Printf("ERR: Can not have stat for %s.\n", pathString)
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
				"writtend data",
				"read data",
				"read on hit data",
			})
			csvOutput.Flush()

			var (
				datasetOutFile  *os.File
				datasetGzipFile *gzip.Writer
			)

			if simGenDataset {
				datasetOutFile, _ = os.Create(simGenDatasetName)
				datasetGzipFile = gzip.NewWriter(datasetOutFile)
				defer datasetOutFile.Close()
				defer datasetGzipFile.Close()

				switch cacheType {
				case "weightedLRU":
					datasetHeader := []string{"size",
						"totRequests",
						"nHits",
						"nMiss",
						"meanTime",
						"class",
					}
					datasetGzipFile.Write([]byte(strings.Join(datasetHeader, ",") + "\n"))
				}
			}

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
					sizeInMbytes := record.Size / (1024 * 1024)
					curCacheInstance.Get(record.Filename, sizeInMbytes)

					numIterations++

					if simGenDataset {
						switch cacheType {
						case "weightedLRU":
							_, latestAddDecision := curCacheInstance.GetLatestDecision()
							fileStats, err := curCacheInstance.GetFileStats(record.Filename)
							if err == nil {
								var curClass string
								if latestAddDecision {
									curClass = "1" // STORE
								} else {
									curClass = "0" // DISCARD
								}
								curRow := []string{
									fmt.Sprintf("%f", fileStats.Size),
									fmt.Sprintf("%d", fileStats.TotRequests),
									fmt.Sprintf("%d", fileStats.NHits),
									fmt.Sprintf("%d", fileStats.NMiss),
									fmt.Sprintf("%f", fileStats.MeanTime),
									curClass,
								}
								datasetGzipFile.Write([]byte(strings.Join(curRow, ",") + "\n"))
							}
						}
					}

					if time.Now().Sub(start).Seconds() >= 1. {
						timeElapsed := time.Now().Sub(simBeginTime)
						outString := strings.Join(
							[]string{
								fmt.Sprintf("[%s_%0.0f_%s]", cacheType, cacheSize, simRegion),
								fmt.Sprintf("[Time Elapsed: %02d:%02d:%02d]",
									int(timeElapsed.Hours())%24,
									int(timeElapsed.Minutes())%60,
									int(timeElapsed.Seconds())%60,
								),
								fmt.Sprintf("[Window %d]", windowCounter),
								fmt.Sprintf("[Step %d/%d]", windowStepCounter+1, simWindowSize),
								fmt.Sprintf("[Num.Records %d]", numRecords),
								fmt.Sprintf("[HitRate %.2f]", curCacheInstance.HitRate()),
								fmt.Sprintf("[Capacity %.2f]", curCacheInstance.Capacity()),
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
					if time.Now().Sub(start).Seconds() >= 1. {
						fmt.Printf("[Jump %d records]\r", numRecords)
						start = time.Now()
					}
				}

			}
			timeElapsed := time.Now().Sub(simBeginTime)
			fmt.Printf("\n[Simulation END][Time elapsed: %02d:%02d:%02d][Num. Records: %d][Mean Records/s: %0.0f]\n",
				int(timeElapsed.Hours())%24,
				int(timeElapsed.Minutes())%60,
				int(timeElapsed.Seconds())%60,
				numRecords,
				float64(totIterations)/timeElapsed.Seconds(),
			)
		},
		Use:   `simulate cacheType fileOrFolderPath`,
		Short: "Simulate a session",
		Long:  "Simulate a session from data input",
		Args:  cobra.MaximumNArgs(2),
	}
	cmd.PersistentFlags().StringVar(
		&simRegion, "simRegion", "all",
		"indicate the filter for record region",
	)
	cmd.PersistentFlags().StringVar(
		&simOutFile, "simOutFile", "",
		"the output file name",
	)
	cmd.PersistentFlags().BoolVar(
		&simGenDataset, "simGenDataset", false,
		"indicates if a dataset have to be generated",
	)
	cmd.PersistentFlags().StringVar(
		&simGenDatasetName, "simGenDatasetName", "dataset.csv.gz",
		"the output dataset file name",
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

	return cmd
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
	case "weighted":
		fmt.Printf("[Create Weighted Cache][Size: %f]\n", cacheSize)

		var functionType cache.FunctionType

		switch weightedFunc {
		case "FuncFileWeight":
			functionType = cache.FuncFileWeight
		case "FuncFileWeightAndTime":
			functionType = cache.FuncFileWeightAndTime
		case "FuncFileWeightOnlyTime":
			functionType = cache.FuncFileWeightOnlyTime
		case "FuncWeightedRequests":
			functionType = cache.FuncWeightedRequests
		default:
			fmt.Println("ERR: You need to specify a weight function.")
			os.Exit(-1)
		}
		cacheInstance = &cache.WeightedCache{
			MaxSize:         cacheSize,
			Exp:             weightExp,
			SelFunctionType: functionType,
		}
		cacheInstance.Init()
	case "weightedLRU":
		fmt.Printf("[Create Weighted Cache][Size: %f]\n", cacheSize)

		var (
			selFunctionType         cache.FunctionType
			selUpdateStatPolicyType cache.UpdateStatsPolicyType
			selLimitStatsPolicyType cache.LimitStatsPolicyType
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

		switch limitStatsPolicy {
		case "noLimit":
			selLimitStatsPolicyType = cache.NoLimitStats
		case "Q1IsDoubleQ2LimitStats":
			selLimitStatsPolicyType = cache.Q1IsDoubleQ2LimitStats
		default:
			fmt.Println("ERR: You need to specify a weight function.")
			os.Exit(-1)
		}

		cacheInstance = &cache.WeightedLRU{
			MaxSize:                 cacheSize,
			Exp:                     weightExp,
			SelFunctionType:         selFunctionType,
			SelUpdateStatPolicyType: selUpdateStatPolicyType,
			SelLimitStatsPolicyType: selLimitStatsPolicyType,
		}
		cacheInstance.Init()
	default:
		fmt.Println("ERR: You need to specify a cache type.")
		os.Exit(-2)
	}
	return cacheInstance
}
