package main

import (
	"fmt"
	"log"
	"net"
	"os"
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

func main() {
	rootCmd := &cobra.Command{}
	rootCmd.AddCommand(commandServe())
	rootCmd.AddCommand(commandSimulate())

	rootCmd.PersistentFlags().Float32Var(
		&cacheSize, "size", 0.0,
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
				println("folder")
			}

			// var passedTime time.Time
			start := time.Now()
			var numIterations uint32

			for record := range iterator {
				curCacheInstance.Get(record.Filename, record.Size)
				if time.Now().Sub(start).Seconds() >= 1. {
					fmt.Printf("[%d it/s]\r", numIterations)
					numIterations = 0
					start = time.Now()
				}
				numIterations++
			}
		},
		Use:   `simulate cacheType fileOrFolderPath`,
		Short: "Simulate a session",
		Long:  "Simulate a session from data input",
		Args:  cobra.MaximumNArgs(2),
	}
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
