package main

import (
	"fmt"
	"log"
	"net"
	"os"

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

func main() {
	rootCmd := &cobra.Command{}
	rootCmd.AddCommand(commandRun())

	rootCmd.PersistentFlags().StringVar(&serviceHost, "host", "localhost", "Ip to listen to")
	rootCmd.PersistentFlags().Int32Var(&servicePort, "port", 5432, "cache sim service port")
	rootCmd.PersistentFlags().StringVar(&weightedFunc, "weightFunction", "FuncFileGroupWeight", "function to use with weighted cache")
	rootCmd.PersistentFlags().Float32Var(&weightExp, "weightExp", 2.0, "Exponential to use with weighted cache function")

	if err := rootCmd.Execute(); err != nil {
		println(err.Error())
	}
}

func commandRun() *cobra.Command {
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
				cacheInstance = &cache.WeightedCache{
					MaxSize: cacheSize,
				}
				switch weightedFunc {
				case "FuncFileWeight":
					cacheInstance.Init(cache.FuncFileWeight, weightExp)
				case "FuncFileWeightAndTime":
					cacheInstance.Init(cache.FuncFileWeightAndTime, weightExp)
				case "FuncFileWeightOnlyTime":
					cacheInstance.Init(cache.FuncFileWeightOnlyTime, weightExp)
				case "FuncWeightedRequests":
					cacheInstance.Init(cache.FuncWeightedRequests, weightExp)
				default:
					fmt.Println("ERR: You need to specify a weight function.")
					os.Exit(-1)
				}
				fmt.Printf("[Register Weighted Cache]\n")
				pb.RegisterSimServiceServer(grpcServer, cacheInstance)
			case "weightedLRU":
				fmt.Printf("[Create Weighted Cache][Size: %f]\n", cacheSize)
				cacheInstance = &cache.WeightedLRU{
					MaxSize: cacheSize,
				}
				switch weightedFunc {
				case "FuncFileWeight":
					cacheInstance.Init(cache.FuncFileWeight, weightExp)
				case "FuncFileWeightAndTime":
					cacheInstance.Init(cache.FuncFileWeightAndTime, weightExp)
				case "FuncFileWeightOnlyTime":
					cacheInstance.Init(cache.FuncFileWeightOnlyTime, weightExp)
				case "FuncWeightedRequests":
					cacheInstance.Init(cache.FuncWeightedRequests, weightExp)
				default:
					fmt.Println("ERR: You need to specify a weight function.")
					os.Exit(-1)
				}
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
		Use:   `run cacheType`,
		Short: "Command run",
		Long:  "Run a cache simulator",
		Args:  cobra.MaximumNArgs(1),
	}
	return cmd
}