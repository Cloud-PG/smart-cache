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
var servicePort int32

func main() {
	rootCmd := &cobra.Command{}
	rootCmd.AddCommand(commandRun())

	rootCmd.PersistentFlags().Float32Var(&cacheSize, "size", 0.0, "cache size")
	rootCmd.PersistentFlags().Int32Var(&servicePort, "port", 5432, "cache sim service port")

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
				cacheInstance = &cache.LRU{
					MaxSize: cacheSize,
				}
				cacheInstance.Init()
				fmt.Printf("[Register LRU Cache]\n")
				pb.RegisterSimServiceServer(grpcServer, cacheInstance)
			case "weighted":
				fmt.Printf("[Create Weighted Cache][Size: %f]\n", cacheSize)
				cacheInstance = &cache.Weighted{
					MaxSize: cacheSize,
				}
				cacheInstance.Init(cache.FuncFileGroupWeight)
				fmt.Printf("[Register Weighted Cache]\n")
				pb.RegisterSimServiceServer(grpcServer, cacheInstance)
			default:
				fmt.Println("ERR: You need to specify a cache type.")
				os.Exit(-1)
			}

			fmt.Printf("[Try to liste to port %d]\n", servicePort)
			lis, err := net.Listen("tcp", fmt.Sprintf("localhost:%d", servicePort))
			if err != nil {
				log.Fatalf("ERR: failed to listen on localhost:%d -> %v", servicePort, err)
			}
			fmt.Printf("[Start server on port %d]\n", servicePort)

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
