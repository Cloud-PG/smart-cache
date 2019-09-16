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

var cacheSize float32
var serviceHost string
var servicePort int32
var weightExp float32
var weightedFunc string
var statUpdatePolicy string
var weightUpdatePolicy string
var limitStatsPolicy string
var simRegion string
var simOutFile string
var simGenDataset bool
var simGenDatasetName string

func main() {
	rootCmd := &cobra.Command{}
	rootCmd.AddCommand(commandServe())
	rootCmd.AddCommand(commandSimulate())

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
		&weightUpdatePolicy, "weightUpdatePolicy", "single",
		"[WeightedLRU] how to update the file weight: ['single', 'all']. Default: single",
	)
	rootCmd.PersistentFlags().StringVar(
		&limitStatsPolicy, "limitStatsPolicy", "Q1IsDoubleQ2LimitStats",
		"[WeightedLRU] how to maintain the file stats ['noLimit', 'Q1IsDoubleQ2LimitStats']. Default: single",
	)

	if err := rootCmd.Execute(); err != nil {
		println(err.Error())
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

			fmt.Printf("[Try to liste to %s:%d]\n", serviceHost, servicePort)
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

			// Create cache
			curCacheInstance := genCache(cacheType)

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

			simBeginTime := time.Now()
			start := time.Now()
			var latestTime time.Time
			var numIterations uint32

			outputFile, _ := os.Create(simOutFile)
			defer outputFile.Close()
			csvOutput := csv.NewWriter(outputFile)
			defer csvOutput.Flush()

			csvOutput.Write([]string{"date", "hit rate", "hit over miss", "weighted hit rate", "written data", "read on hit", "size"})
			csvOutput.Flush()

			var datasetOutFile *os.File
			var datasetGzipFile *gzip.Writer

			if simGenDataset {
				datasetOutFile, _ = os.Create(simGenDatasetName)
				datasetGzipFile = gzip.NewWriter(datasetOutFile)
				defer datasetOutFile.Close()
				defer datasetGzipFile.Close()

				switch cacheType {
				case "weightedLRU":
					datasetHeader := []string{"size", "totRequests", "nHits", "nMiss", "meanTime", "class"}
					datasetGzipFile.Write([]byte(strings.Join(datasetHeader, ",") + "\n"))
				}
			}

			for record := range iterator {
				if strings.Compare(simRegion, "all") != 0 {
					if strings.Index(strings.ToLower(record.SiteName), fmt.Sprintf("_%s_", strings.ToLower(simRegion))) == -1 {
						continue
					}
				}
				if latestTime.IsZero() {
					latestTime = time.Unix(record.Day, 0.)
				} else {
					curTime := time.Unix(record.Day, 0.)
					if curTime.Sub(latestTime).Hours() >= 24. {
						csvOutput.Write([]string{
							fmt.Sprintf("%s", latestTime),
							fmt.Sprintf("%0.2f", curCacheInstance.HitRate()),
							fmt.Sprintf("%0.2f", curCacheInstance.HitOverMiss()),
							fmt.Sprintf("%0.2f", curCacheInstance.WeightedHitRate()),
							fmt.Sprintf("%f", curCacheInstance.WrittenData()),
							fmt.Sprintf("%f", curCacheInstance.ReadOnHit()),
							fmt.Sprintf("%f", curCacheInstance.Size()),
						})
						csvOutput.Flush()
						curCacheInstance.ClearHitMissStats()
						latestTime = time.Unix(record.Day, 0.)
					}
				}

				sizeInMbytes := record.Size / (1024 * 1024)
				curCacheInstance.Get(record.Filename, sizeInMbytes)
				if time.Now().Sub(start).Seconds() >= 1. {
					fmt.Printf("[Hit Rate %.2f][Capacity %.2f][%d it/s]\r",
						curCacheInstance.HitRate(),
						curCacheInstance.Capacity(),
						numIterations,
					)
					numIterations = 0
					start = time.Now()
				}
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
			}
			fmt.Printf("[Simulation ended][Time elapsed: %f minutes]\n", time.Now().Sub(simBeginTime).Minutes())
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
		&simOutFile, "simOutFile", "result.csv",
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

		var selFunctionType cache.FunctionType
		var selUpdateStatPolicyType cache.UpdateStatsPolicyType
		var selUpdateWeightPolicyType cache.UpdateWeightPolicyType
		var selLimitStatsPolicyType cache.LimitStatsPolicyType

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

		switch weightUpdatePolicy {
		case "single":
			selUpdateWeightPolicyType = cache.UpdateSingleWeight
		case "all":
			selUpdateWeightPolicyType = cache.UpdateAllWeights
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
			MaxSize:                   cacheSize,
			Exp:                       weightExp,
			SelFunctionType:           selFunctionType,
			SelUpdateStatPolicyType:   selUpdateStatPolicyType,
			SelUpdateWeightPolicyType: selUpdateWeightPolicyType,
			SelLimitStatsPolicyType:   selLimitStatsPolicyType,
		}
		cacheInstance.Init()
	default:
		fmt.Println("ERR: You need to specify a cache type.")
		os.Exit(-2)
	}
	return cacheInstance
}
