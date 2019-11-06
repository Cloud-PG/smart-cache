package cache

import (
	"compress/gzip"
	"encoding/json"
	"log"
	"math"
	"os"

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
func softMax(matrix *mat.Dense) *mat.Dense {
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
	var curModel *AIModel

	modelFile, errOpenFile := os.Open(modelFilePath)
	if errOpenFile != nil {
		log.Fatalf("[Model Error]: Cannot open file '%s'\n", errOpenFile)
	}

	modelFileGz, errOpenZipFile := gzip.NewReader(modelFile)
	if errOpenZipFile != nil {
		log.Fatalf("[Model Error]: Cannot open zip stream from file '%s'\nError: %s\n", modelFilePath, errOpenZipFile)
	}

	errJSONUnmarshal := json.NewDecoder(modelFileGz).Decode(curModel)
	if errJSONUnmarshal != nil {
		log.Fatalf("[Model Error]: Cannot unmarshal json from file '%s'\nError: %s\n", modelFilePath, errJSONUnmarshal)
	}

	for _, layer := range curModel.Layers {
		// Create weight tensor
		curWeights := mat.NewDense(
			layer.Weights.Shape[0],
			layer.Weights.Shape[1],
			nil,
		)
		for row := 0; row < layer.Weights.Shape[0]; row++ {
			curWeights.SetRow(row, layer.Weights.Values[row])
		}
		layer.Weights.Tensor = curWeights
		// Create bias tensor
		curBias := mat.NewDense(
			layer.Weights.Shape[0],
			1,
			nil,
		)
		for row := 0; row < layer.Bias.Shape[0]; row++ {
			curBias.Set(row, 0, layer.Bias.Values[row])
		}
		layer.Bias.Tensor = curBias
	}

	return curModel
}

// Predict implements the feed dorward prediction
func (model AIModel) Predict(input *mat.Dense) mat.Dense {
	var output mat.Dense

	for lvl, layer := range model.Layers {
		var mulRes, sumRes, activationRes mat.Dense
		if lvl == 0 {
			mulRes.Mul(&input, layer.Weights.Tensor)
		} else {
			mulRes.Mul(&output, layer.Weights.Tensor)
		}
		sumRes.Add(&mulRes, layer.Bias.Tensor)

		var activationFunction func(int, int, float64) float64
		switch layer.ActivationFunction {
		case "sigmoid":
			activationFunction = func(_, _ int, x float64) float64 { return sigmoid(x) }
			activationRes.Apply(activationFunction, &sumRes)
		case "hard_sigmoid":
			activationFunction = func(_, _ int, x float64) float64 { return hardSigmoid(x) }
			activationRes.Apply(activationFunction, &sumRes)
		case "softmax":
			activationRes.Copy(softMax(&sumRes))
		}

		output = activationRes
	}

	return output
}
