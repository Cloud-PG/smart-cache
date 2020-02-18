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

func fileWeightedAdditiveFunction(totRequests int64, size float64, meanTicks float64, alpha float64, beta float64, gamma float64) float64 {
	return alpha*float64(totRequests) + beta*size + gamma*meanTicks
}

func fileWeightedMultiplicativeFunction(totRequests int64, size float64, meanTicks float64, alpha float64, beta float64, gamma float64) float64 {
	return float64(math.Pow(float64(totRequests), alpha) * math.Pow(float64(size), beta) * math.Pow(float64(meanTicks), gamma))
}

func fileWeightedRequest(totRequests int64, size float64, meanTicks float64) float64 {
	return meanTicks + (size / math.Exp(float64(totRequests)))
}
