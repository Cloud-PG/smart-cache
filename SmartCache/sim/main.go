package main

import (
	"fmt"

	"./cache"

	"github.com/spf13/cobra"
)

var CACHE_SIZE float32
var CACHE cache.Cache

func main() {
	rootCmd := &cobra.Command{}
	rootCmd.AddCommand(commandRun())

	rootCmd.PersistentFlags().Float32Var(&CACHE_SIZE, "size", 0.0, "cache size")

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

			switch cacheType {
			case "lru":
				fmt.Println("RUN LRU with size =", CACHE_SIZE)
				CACHE = &cache.LRU{
					MaxSize: CACHE_SIZE,
				}
				fmt.Println(CACHE.HitRate(), CACHE.Size(), CACHE.Capacity())
			case "weight":
				fmt.Println("ERR: To be implemented...")
			default:
				fmt.Println("ERR: You need to specify a cache type.")
			}
		},
		Use:   `run cacheType`,
		Short: "Command run",
		Long:  "Run a cache simulator",
		Args:  cobra.MaximumNArgs(1),
	}
	return cmd
}
