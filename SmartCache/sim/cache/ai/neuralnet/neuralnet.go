package neuralnet

import (
	"compress/gzip"
	"encoding/json"
	"fmt"
	"log"
	"math"
	"os"
	"path/filepath"

	"gonum.org/v1/gonum/mat"
)

// LayerWeights is composed by layer weights and shape
type LayerWeights struct {
	Shape  []int       `json:"shape"`
	Values [][]float64 `json:"values"`
	Tensor *mat.Dense
}

// LayerBias is composed by layer weights and shape
type LayerBias struct {
	Shape  []int     `json:"shape"`
	Values []float64 `json:"values"`
	Tensor *mat.Dense
}

// ModelLayer is composed by layer weights and shape
type ModelLayer struct {
	Name               string       `json:"name"`
	Weights            LayerWeights `json:"weights"`
	Bias               LayerBias    `json:"bias"`
	ActivationFunction string       `json:"activation_function"`
}

// AIModel is the AI model dump
type AIModel struct {
	Name   string       `json:"name"`
	Layers []ModelLayer `json:"layers"`
}

// sigmoid implements the sigmoid function
// for use in activation functions.
func sigmoid(x float64) float64 {
	return 1.0 / (1.0 + math.Exp(-x))
}

// hardSigmoid implements the hard sigmoid function
// for use in activation functions.
func hardSigmoid(x float64) float64 {
	return math.Max(0, math.Min(1, x*0.2+0.5))
}

// softMax implements the soft max function
// for use in activation functions.
func softMax(matrix mat.Matrix) *mat.Dense {
	// Implement the softMax for 2D matrices. The formula is:
	//  S(y_i) = e^(y_i) / sum(e^(y_i))

	var expVector mat.Dense

	// mat.Dens.Exp works only for 2D matrices so this applyVectorExp function
	// is used to manage a vector
	applyVectorExp := func(_ int, _ int, v float64) float64 { return math.Exp(v) }
	expVector.Apply(applyVectorExp, matrix)

	rows, columns := expVector.Dims()
	for row := 0; row < rows; row++ {
		var sum float64
		for column := 0; column < columns; column++ {
			sum += expVector.At(row, column)
		}
		for column := 0; column < columns; column++ {
			expVector.Set(
				row,
				column,
				expVector.At(row, column)/sum,
			)
		}
	}

	return &expVector
}

// LoadModel loads an AI model from a gzip file
func LoadModel(modelFilePath string) *AIModel {
	var curModel AIModel
	fileExtension := filepath.Ext(modelFilePath)

	modelFile, errOpenFile := os.Open(modelFilePath)
	if errOpenFile != nil {
		log.Fatalf("[Model Error]: Cannot open file '%s'\n", errOpenFile)
	}

	if fileExtension == ".gzip" || fileExtension == ".gz" {
		modelFileGz, errOpenZipFile := gzip.NewReader(modelFile)
		if errOpenZipFile != nil {
			log.Fatalf("[Model Error]: Cannot open zip stream from file '%s'\nError: %s\n", modelFilePath, errOpenZipFile)
		}

		errJSONUnmarshal := json.NewDecoder(modelFileGz).Decode(&curModel)
		if errJSONUnmarshal != nil {
			log.Fatalf("[Model Error]: Cannot unmarshal json from file '%s'\nError: %s\n", modelFilePath, errJSONUnmarshal)
		}
	} else if fileExtension == ".json" {
		errJSONUnmarshal := json.NewDecoder(modelFile).Decode(&curModel)
		if errJSONUnmarshal != nil {
			log.Fatalf("[Model Error]: Cannot unmarshal json from file '%s'\nError: %s\n", modelFilePath, errJSONUnmarshal)
		}
	} else {
		log.Fatalf("Cannot unmarshal file '%s' with extension '%s'", modelFilePath, fileExtension)
	}

	for idx, layer := range curModel.Layers {
		// Create weight tensor
		curWeights := mat.NewDense(
			layer.Weights.Shape[0],
			layer.Weights.Shape[1],
			nil,
		)
		for row := 0; row < layer.Weights.Shape[0]; row++ {
			curWeights.SetRow(row, layer.Weights.Values[row])
		}
		curModel.Layers[idx].Weights.Tensor = curWeights
		// Create bias tensor
		curBias := mat.NewDense(
			layer.Bias.Shape[0],
			1,
			nil,
		)
		for row := 0; row < layer.Bias.Shape[0]; row++ {
			curBias.Set(row, 0, layer.Bias.Values[row])
		}
		curModel.Layers[idx].Bias.Tensor = curBias
	}

	return &curModel
}

// PrintTensor make an output for mat.Matrix
func PrintTensor(tensor mat.Matrix) {
	d0, d1 := tensor.Dims()
	fmt.Printf("(%d,%d)\n", d0, d1)
	fmt.Print(" ")
	for row := 0; row < d0; row++ {
		fmt.Print("[")
		for column := 0; column < d1; column++ {
			fmt.Printf("%0.2f", tensor.At(row, column))
			if column != d1-1 {
				fmt.Print(" ")
			} else {
				fmt.Print("]")
			}
		}
		fmt.Println()
	}
	fmt.Print("]")
	fmt.Println()
}

// Predict implements the feed dorward prediction
func (model AIModel) Predict(input *mat.Dense) *mat.Dense {
	var output mat.Dense

	for lvl, layer := range model.Layers {
		var mulRes, sumRes, activationRes mat.Dense
		// println(lvl, layer.Name)
		if lvl == 0 {
			// PrintTensor(input.T())
			// PrintTensor(layer.Weights.Tensor)
			mulRes.Mul(input.T(), layer.Weights.Tensor)
			// PrintTensor(&mulRes)
		} else {
			// PrintTensor(output.T())
			// PrintTensor(layer.Weights.Tensor)
			mulRes.Mul(output.T(), layer.Weights.Tensor)
			// PrintTensor(&mulRes)
		}
		// PrintTensor(layer.Bias.Tensor)
		sumRes.Add(mulRes.T(), layer.Bias.Tensor)
		// PrintTensor(&sumRes)

		var activationFunction func(int, int, float64) float64
		switch layer.ActivationFunction {
		case "sigmoid":
			activationFunction = func(_, _ int, x float64) float64 { return sigmoid(x) }
			activationRes.Apply(activationFunction, &sumRes)
		case "hard_sigmoid":
			activationFunction = func(_, _ int, x float64) float64 { return hardSigmoid(x) }
			activationRes.Apply(activationFunction, &sumRes)
		case "softmax":
			// fmt.Println("SumRes")
			// PrintTensor(&sumRes)
			softMaxRes := softMax(sumRes.T())
			// fmt.Println("SoftMax")
			// PrintTensor(softMaxRes)
			activationRes.CloneFrom(softMaxRes)
		}

		output.CloneFrom(&activationRes)
		// PrintTensor(&output)
	}

	return &output
}

// GetPredictionArgMax returns the index of the maximum value in the Dense vector
func GetPredictionArgMax(input *mat.Dense) int {
	_, d1 := input.Dims()
	maxIdx := 0
	var maxVal float64 = input.At(0, 0)
	for idx := 1; idx < d1; idx++ {
		curVal := input.At(0, idx)
		if curVal > maxVal {
			maxVal = curVal
			maxIdx = idx
		}
	}
	return maxIdx
}
