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

var cacheInstance cache.Cache
var cacheSize float32
var serviceHost string
var servicePort int32
var weightExp float32
var weightedFunc string
var weightUpdatePolicy string

func main() {
	rootCmd := &cobra.Command{}
	rootCmd.AddCommand(commandServe())
	rootCmd.AddCommand(commandSimulate())

	rootCmd.PersistentFlags().Float32Var(&cacheSize, "size", 0.0, "cache size")
	rootCmd.PersistentFlags().StringVar(&serviceHost, "host", "localhost", "Ip to listen to")
	rootCmd.PersistentFlags().Int32Var(&servicePort, "port", 5432, "cache sim service port")
	rootCmd.PersistentFlags().StringVar(&weightedFunc, "weightFunction", "FuncWeightedRequests", "function to use with weighted cache")
	rootCmd.PersistentFlags().Float32Var(&weightExp, "weightExp", 2.0, "Exponential to use with weighted cache function")
	rootCmd.PersistentFlags().StringVar(&weightUpdatePolicy, "weightUpdatePolicy", "miss", "when to update the file stats: ['miss', 'request']. Default: request")

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

			grpcServer := grpc.NewServer()

			switch cacheType {
			case "lru":
				fmt.Printf("[Create LRU Cache][Size: %f]\n", cacheSize)
				cacheInstance = &cache.LRUCache{
					MaxSize: cacheSize,
				}
				cacheInstance.Init()
				fmt.Printf("[Register LRU Cache]\n")
				pb.RegisterSimServiceServer(grpcServer, cacheInstance)
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
				fmt.Printf("[Register Weighted Cache]\n")
				pb.RegisterSimServiceServer(grpcServer, cacheInstance)
			case "weightedLRU":
				fmt.Printf("[Create Weighted Cache][Size: %f]\n", cacheSize)
				var functionType cache.FunctionType
				var updatePolicyType cache.UpdateStatsPolicyType
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
				switch weightUpdatePolicy {
				case "miss":
					updatePolicyType = cache.UpdateStatsOnMiss
				case "request":
					updatePolicyType = cache.UpdateStatsOnRequest
				default:
					fmt.Println("ERR: You need to specify a weight function.")
					os.Exit(-1)
				}
				cacheInstance = &cache.WeightedLRU{
					MaxSize:             cacheSize,
					Exp:                 weightExp,
					SelFunctionType:     functionType,
					SelUpdatePolicyType: updatePolicyType,
				}
				cacheInstance.Init()
				fmt.Printf("[Register Weighted LRU Cache]\n")
				pb.RegisterSimServiceServer(grpcServer, cacheInstance)
			default:
				fmt.Println("ERR: You need to specify a cache type.")
				os.Exit(-2)
			}

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
				var functionType cache.FunctionType
				var updatePolicyType cache.UpdateStatsPolicyType
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
				switch weightUpdatePolicy {
				case "miss":
					updatePolicyType = cache.UpdateStatsOnMiss
				case "request":
					updatePolicyType = cache.UpdateStatsOnRequest
				default:
					fmt.Println("ERR: You need to specify a weight function.")
					os.Exit(-1)
				}
				cacheInstance = &cache.WeightedLRU{
					MaxSize:             cacheSize,
					Exp:                 weightExp,
					SelFunctionType:     functionType,
					SelUpdatePolicyType: updatePolicyType,
				}
				cacheInstance.Init()
			default:
				fmt.Println("ERR: You need to specify a cache type.")
				os.Exit(-2)
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
				println("folder")
			}

			// var passedTime time.Time
			start := time.Now()
			var numIterations uint32

			for record := range iterator {
				cacheInstance.Get(record.Filename, record.Size)
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
