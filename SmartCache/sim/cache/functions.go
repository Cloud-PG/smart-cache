package cache

import (
	"math"
)

// FunctionType is used to select the weight function
type FunctionType int

const (
	// FuncAdditive indicates the simple function for weighted cache with parameters
	FuncAdditive FunctionType = iota - 4
	// FuncMultiplicative  indicates the simple function for weighted cache with parameter as exponentials
	FuncMultiplicative
	// FuncWeightedRequests has a small memory for request time
	FuncWeightedRequests
)

func fileWeightedAdditiveFunction(totRequests uint32, size float32, meanTicks float32, alpha float32, beta float32, gamma float32) float32 {
	return alpha*float32(totRequests) + beta*size + gamma*meanTicks
}

func fileWeightedMultiplicativeFunction(totRequests uint32, size float32, meanTicks float32, alpha float32, beta float32, gamma float32) float32 {
	return float32(math.Pow(float64(totRequests), float64(alpha)) * math.Pow(float64(size), float64(beta)) * math.Pow(float64(meanTicks), float64(gamma)))
}

func fileWeightedRequest(totRequests uint32, size float32, meanTicks float32) float32 {
	return meanTicks + (size / float32(math.Exp(float64(totRequests))))
}
