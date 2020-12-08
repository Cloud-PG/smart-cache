package main

import (
	"fmt"
	"log"
	"net"
	"os"

	"./service"
	pb "./service/pluginProto"
	"google.golang.org/grpc"

	"github.com/spf13/cobra"
)

// FIXME: complete build variables
var (
	serviceInstance service.XCachePlugin
	cacheSize       float32
	serviceHost     string
	servicePort     int32
	weightExp       float32
	weightedFunc    string
)

// FIXME: make plugin same version of simulator

func main() {
	rootCmd := &cobra.Command{}
	rootCmd.AddCommand(commandRun())

	rootCmd.PersistentFlags().StringVar(&serviceHost, "host", "localhost", "Ip to listen to")
	rootCmd.PersistentFlags().Int32Var(&servicePort, "port", 5432, "cache sim service port")
	rootCmd.PersistentFlags().StringVar(&weightedFunc, "weightFunction", "FuncWeightedRequests", "function to use with weight function cache")
	rootCmd.PersistentFlags().Float32Var(&weightExp, "weightExp", 2.0, "Exponential to use with weight function cache function")

	if err := rootCmd.Execute(); err != nil {
		println(err.Error())
	}
}

func commandRun() *cobra.Command {
	cmd := &cobra.Command{
		Run: func(cmd *cobra.Command, args []string) {
			if len(args) != 1 {
				fmt.Println("ERR: You need to specify only 1 type of service...")
				os.Exit(-1)
			}
			// Get first element and reuse same memory space to allocate args
			serviceType := args[0]
			copy(args, args[1:])
			args = args[:len(args)-1]

			grpcServer := grpc.NewServer()
			var function service.FunctionType

			switch weightedFunc {
			case "FuncFileWeight":
				function = service.FuncFileWeight
			case "FuncFileWeightAndTime":
				function = service.FuncFileWeightAndTime
			case "FuncFileWeightOnlyTime":
				function = service.FuncFileWeightOnlyTime
			case "FuncWeightedRequests":
				function = service.FuncWeightedRequests
			default:
				fmt.Printf("ERR: You need to specify a valid weight function. '%s' is not valid...\n", weightedFunc)
				os.Exit(-2)
			}

			switch serviceType {
			case "weightedLRU":
				fmt.Printf("[Create weight function cache]")
				serviceInstance = service.PluginServiceServer{
					Exp:             weightExp,
					SelFunctionType: function,
				}
				serviceInstance.Init()
				fmt.Printf("[Register Weighted LRU Cache]\n")
				pb.RegisterPluginProtoServer(grpcServer, serviceInstance)
			default:
				fmt.Println("ERR: You need to specify a cache type.")
				os.Exit(-3)
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
		Short: "Command serve",
		Long:  "Run the xcache plugin service",
		Args:  cobra.MaximumNArgs(1),
	}
	return cmd
}
