package cmd

import (
	"path/filepath"

	"github.com/rs/zerolog/log"
	"github.com/spf13/viper"
)

type epsilon struct {
	Start   float64
	Decay   float64
	Unleash string
}

type ai struct {
	Featuremap       string
	Dataset2TestPath string `mapstructure:"dataset2TestPath"`
	Model            string

	RL struct {
		Type     string
		Epsilon  epsilon
		Addition struct {
			Featuremap string
			Epsilon    epsilon
		}
		Eviction struct {
			Featuremap string
			Type       string
			K          uint
			Epsilon    epsilon
		}
	} `mapstructure:"rl"`
}

type weightFunction struct {
	Name  string
	Alpha float64
	Beta  float64
	Gamma float64
}

type cacheBaseConf struct {
	Type       string
	Watermarks bool
	Watermark  struct {
		High float64
		Low  float64
	}
	Size struct {
		Value uint
		Unit  string
	}
	Bandwidth struct {
		Value    uint
		Redirect bool
	}
	Stats struct {
		MaxNumDayDiff float64
		DeltaDaysStep float64
	}
}

type SimConfig struct {
	Sim struct {
		Data              string
		Type              string
		Region            string
		OutputFolder      string `mapstructure:"outputFolder"` //nolint:govet
		Dumpfilename      string
		Loaddumpfilename  string
		Seed              int
		Overwrite         bool
		Dump              bool
		Dumpfilesandstats bool
		Loaddump          bool
		Coldstart         bool
		Coldstartnostats  bool
		Log               bool
		Window            struct {
			Size  uint
			Start uint
			Stop  uint
		}

		CPUProfile        string `mapstructure:"cpuprofile"` //nolint:govet
		MEMProfile        string `mapstructure:"memprofile"` //nolint:govet
		Outputupdatedelay float64

		Cache          cacheBaseConf
		AI             ai             `mapstructure:"ai"`
		WeightFunction weightFunction `mapstructure:"weightfunc"`
	}
}

func (conf SimConfig) configure(configFilenameWithNoExt string) { //nolint:ignore,funlen
	viper.SetConfigName(configFilenameWithNoExt) // name of config file (without extension)
	viper.SetConfigType("yaml")                  // REQUIRED if the config file does not have the extension in the name
	viper.AddConfigPath(".")                     // optionally look for config in the working directory

	viper.SetDefault("sim.type", "normal")
	viper.SetDefault("sim.region", "all")
	viper.SetDefault("sim.outputFolder", ".")
	viper.SetDefault("sim.overwrite", false)
	viper.SetDefault("sim.dump", false)
	viper.SetDefault("sim.dumpfilesandstats", true)
	viper.SetDefault("sim.dumpfilename", "")
	viper.SetDefault("sim.loaddump", false)
	viper.SetDefault("sim.loaddumpfilename", "")
	viper.SetDefault("sim.window.size", 7)
	viper.SetDefault("sim.window.start", 0)
	viper.SetDefault("sim.window.stop", 0)
	viper.SetDefault("sim.coldstart", false)
	viper.SetDefault("sim.coldstartnostats", false)
	viper.SetDefault("sim.log", false)
	viper.SetDefault("sim.seed", 42)

	viper.SetDefault("sim.cpuprofile", "")
	viper.SetDefault("sim.memprofile", "")
	viper.SetDefault("sim.outputupdatedelay", 2.4)

	viper.SetDefault("sim.cache.type", "")
	viper.SetDefault("sim.cache.watermarks", false)
	viper.SetDefault("sim.cache.watermark.high", 95.0)
	viper.SetDefault("sim.cache.watermark.low", 75.0)
	viper.SetDefault("sim.cache.size.value", 100)
	viper.SetDefault("sim.cache.size.unit", "T")
	viper.SetDefault("sim.cache.bandwidth.value", 10.0)
	viper.SetDefault("sim.cache.bandwidth.redirect", false)
	viper.SetDefault("sim.cache.stats.maxNumDayDiff", 6.0)
	viper.SetDefault("sim.cache.stats.deltaDaysStep", 7.0)

	viper.SetDefault("sim.weightfunc.name", "FuncAdditiveExp")
	viper.SetDefault("sim.weightfunc.alpha", 1.0)
	viper.SetDefault("sim.weightfunc.beta", 1.0)
	viper.SetDefault("sim.weightfunc.gamma", 1.0)

	viper.SetDefault("sim.ai.rl.epsilon.start", 1.0)
	viper.SetDefault("sim.ai.rl.epsilon.decay", 0.0000042)
	viper.SetDefault("sim.ai.rl.epsilon.unleash", "yes")

	viper.SetDefault("sim.ai.featuremap", "")
	viper.SetDefault("sim.ai.rl.type", "SCDL2")

	viper.SetDefault("sim.ai.rl.addition.featuremap", "")
	viper.SetDefault("sim.ai.rl.addition.epsilon.start", -1.0)
	viper.SetDefault("sim.ai.rl.addition.epsilon.decay", -1.0)
	viper.SetDefault("sim.ai.rl.addition.epsilon.unleash", "default")

	viper.SetDefault("sim.ai.rl.eviction.featuremap", "")
	viper.SetDefault("sim.ai.rl.eviction.k", 32)
	viper.SetDefault("sim.ai.rl.eviction.type", "onK")
	viper.SetDefault("sim.ai.rl.eviction.epsilon.start", -1.0)
	viper.SetDefault("sim.ai.rl.eviction.epsilon.decay", -1.0)
	viper.SetDefault("sim.ai.rl.eviction.epsilon.unleash", "default")

	viper.SetDefault("sim.ai.model", "")

	viper.SetDefault("sim.dataset2testpath", "")
}

func (conf *SimConfig) check() { //nolint:funlen
	log.Info().Int("conf.Sim.Seed", conf.Sim.Seed).Msg("CONF_VAR")
	log.Info().Uint("conf.Sim.Cache.Size.Value", conf.Sim.Cache.Size.Value).Msg("CONF_VAR")
	log.Info().Str("conf.Sim.Cache.Size.Unit", conf.Sim.Cache.Size.Unit).Msg("CONF_VAR")
	log.Info().Bool("conf.Sim.Overwrite", conf.Sim.Overwrite).Msg("CONF_VAR")
	log.Info().Uint("conf.Sim.Cache.Bandwidth.Value", conf.Sim.Cache.Bandwidth.Value).Msg("CONF_VAR")
	log.Info().Bool("conf.Sim.Cache.Bandwidth.Redirect", conf.Sim.Cache.Bandwidth.Redirect).Msg("CONF_VAR")
	log.Info().Bool("conf.Sim.Cache.Watermarks", conf.Sim.Cache.Watermarks).Msg("CONF_VAR")
	log.Info().Float64("conf.Sim.Cache.Watermark.High", conf.Sim.Cache.Watermark.High).Msg("CONF_VAR")
	log.Info().Float64("conf.Sim.Cache.Watermark.Low", conf.Sim.Cache.Watermark.Low).Msg("CONF_VAR")
	log.Info().Float64("conf.Sim.Cache.Stats.MaxNumDayDiff", conf.Sim.Cache.Stats.MaxNumDayDiff).Msg("CONF_VAR")
	log.Info().Float64("conf.Sim.Cache.Stats.DeltaDaysStep", conf.Sim.Cache.Stats.DeltaDaysStep).Msg("CONF_VAR")
	log.Info().Bool("conf.Sim.Coldstart", conf.Sim.Coldstart).Msg("CONF_VAR")
	log.Info().Bool("conf.Sim.Coldstartnostats", conf.Sim.Coldstartnostats).Msg("CONF_VAR")

	simDataPathAbs, errAbs := filepath.Abs(conf.Sim.Data)
	if errAbs != nil {
		panic(errAbs)
	}
	conf.Sim.Data = simDataPathAbs
	log.Info().Str("conf.Sim.Data", conf.Sim.Data).Msg("CONF_VAR")

	log.Info().Bool("conf.Sim.Dump", conf.Sim.Dump).Msg("CONF_VAR")
	log.Info().Str("conf.Sim.Dumpfilename", conf.Sim.Dumpfilename).Msg("CONF_VAR")
	log.Info().Bool("conf.Sim.Dumpfilesandstats", conf.Sim.Dumpfilesandstats).Msg("CONF_VAR")
	log.Info().Bool("conf.Sim.Loaddump", conf.Sim.Loaddump).Msg("CONF_VAR")
	log.Info().Str("conf.Sim.Loaddumpfilename", conf.Sim.Loaddumpfilename).Msg("CONF_VAR")
	log.Info().Bool("conf.Sim.Log", conf.Sim.Log).Msg("CONF_VAR")

	simOutputFolderAbs, errAbs := filepath.Abs(conf.Sim.OutputFolder)
	if errAbs != nil {
		panic(errAbs)
	}
	conf.Sim.OutputFolder = simOutputFolderAbs
	log.Info().Str("conf.Sim.OutputFolder", conf.Sim.OutputFolder).Msg("CONF_VAR")

	log.Info().Str("conf.Sim.Region", conf.Sim.Region).Msg("CONF_VAR")
	log.Info().Str("conf.Sim.Type", conf.Sim.Type).Msg("CONF_VAR")
	log.Info().Uint("conf.Sim.Window.Start", conf.Sim.Window.Start).Msg("CONF_VAR")
	log.Info().Uint("conf.Sim.Window.Stop", conf.Sim.Window.Stop).Msg("CONF_VAR")
	log.Info().Uint("conf.Sim.Window.Size", conf.Sim.Window.Size).Msg("CONF_VAR")
	log.Info().Str("conf.Sim.CPUProfile", conf.Sim.CPUProfile).Msg("CONF_VAR")
	log.Info().Str("conf.Sim.MEMProfile", conf.Sim.MEMProfile).Msg("CONF_VAR")
	log.Info().Float64("conf.Sim.Outputupdatedelay", conf.Sim.Outputupdatedelay).Msg("CONF_VAR")

	log.Info().Str("conf.Sim.WeightFunction.Name", conf.Sim.WeightFunction.Name).Msg("CONF_VAR")
	log.Info().Float64("conf.Sim.WeightFunction.Alpha", conf.Sim.WeightFunction.Alpha).Msg("CONF_VAR")
	log.Info().Float64("conf.Sim.WeightFunction.Beta", conf.Sim.WeightFunction.Beta).Msg("CONF_VAR")
	log.Info().Float64("conf.Sim.WeightFunction.Gamma", conf.Sim.WeightFunction.Gamma).Msg("CONF_VAR")

	if conf.Sim.AI.Featuremap != "" {
		aiFeatureMapAbs, errAbs := filepath.Abs(conf.Sim.AI.Featuremap)
		if errAbs != nil {
			panic(errAbs)
		}
		conf.Sim.AI.Featuremap = aiFeatureMapAbs
	}
	log.Info().Str("conf.Sim.AI.Featuremap", conf.Sim.AI.Featuremap).Msg("CONF_VAR")

	log.Info().Str("conf.Sim.AI.Dataset2TestPath", conf.Sim.AI.Dataset2TestPath).Msg("CONF_VAR")
	log.Info().Str("conf.Sim.AI.Model", conf.Sim.AI.Model).Msg("CONF_VAR")

	log.Info().Str("conf.Sim.AI.RL.Type", conf.Sim.AI.RL.Type).Msg("CONF_VAR")

	if conf.Sim.AI.RL.Addition.Featuremap != "" {
		aiRLAdditionFeatureMapAbs, errAbs := filepath.Abs(conf.Sim.AI.RL.Addition.Featuremap)
		if errAbs != nil {
			panic(errAbs)
		}
		conf.Sim.AI.RL.Addition.Featuremap = aiRLAdditionFeatureMapAbs
	}
	log.Info().Str("conf.Sim.AI.RL.Addition.Featuremap", conf.Sim.AI.RL.Addition.Featuremap).Msg("CONF_VAR")

	if conf.Sim.AI.RL.Eviction.Featuremap != "" {
		aiRLEvictionFeatureMapAbs, errAbs := filepath.Abs(conf.Sim.AI.RL.Eviction.Featuremap)
		if errAbs != nil {
			panic(errAbs)
		}
		conf.Sim.AI.RL.Eviction.Featuremap = aiRLEvictionFeatureMapAbs
	}
	log.Info().Str("conf.Sim.AI.RL.Eviction.Featuremap", conf.Sim.AI.RL.Eviction.Featuremap).Msg("CONF_VAR")
	log.Info().Uint("conf.Sim.AI.RL.Eviction.K", conf.Sim.AI.RL.Eviction.K).Msg("CONF_VAR")

	log.Info().Float64("conf.Sim.AI.RL.Epsilon.Start", conf.Sim.AI.RL.Epsilon.Start).Msg("CONF_VAR")
	log.Info().Float64("conf.Sim.AI.RL.Epsilon.Decay", conf.Sim.AI.RL.Epsilon.Decay).Msg("CONF_VAR")
	log.Info().Str("conf.Sim.AI.RL.Epsilon.Unleash", conf.Sim.AI.RL.Epsilon.Unleash).Msg("CONF_VAR")

	if conf.Sim.AI.RL.Addition.Epsilon.Start == -1.0 {
		conf.Sim.AI.RL.Addition.Epsilon.Start = conf.Sim.AI.RL.Epsilon.Start
	}
	log.Info().Float64("conf.Sim.AI.RL.Addition.Epsilon.Start", conf.Sim.AI.RL.Addition.Epsilon.Start).Msg("CONF_VAR")

	if conf.Sim.AI.RL.Addition.Epsilon.Decay == -1.0 {
		conf.Sim.AI.RL.Addition.Epsilon.Decay = conf.Sim.AI.RL.Epsilon.Decay
	}
	log.Info().Float64("conf.Sim.AI.RL.Addition.Epsilon.Decay", conf.Sim.AI.RL.Addition.Epsilon.Decay).Msg("CONF_VAR")

	if conf.Sim.AI.RL.Epsilon.Unleash == "default" {
		conf.Sim.AI.RL.Addition.Epsilon.Unleash = string(conf.Sim.AI.RL.Epsilon.Unleash)
	}
	log.Info().Str("conf.Sim.AI.RL.Addition.Epsilon.Unleash", conf.Sim.AI.RL.Addition.Epsilon.Unleash).Msg("CONF_VAR")

	if conf.Sim.AI.RL.Eviction.Epsilon.Start == -1.0 {
		conf.Sim.AI.RL.Eviction.Epsilon.Start = conf.Sim.AI.RL.Epsilon.Start
	}
	log.Info().Float64("conf.Sim.AI.RL.Eviction.Epsilon.Start", conf.Sim.AI.RL.Eviction.Epsilon.Start).Msg("CONF_VAR")

	if conf.Sim.AI.RL.Eviction.Epsilon.Decay == -1.0 {
		conf.Sim.AI.RL.Eviction.Epsilon.Decay = conf.Sim.AI.RL.Epsilon.Decay
	}
	log.Info().Float64("conf.Sim.AI.RL.Eviction.Epsilon.Decay", conf.Sim.AI.RL.Eviction.Epsilon.Decay).Msg("CONF_VAR")

	if conf.Sim.AI.RL.Eviction.Epsilon.Unleash == "default" {
		conf.Sim.AI.RL.Eviction.Epsilon.Unleash = conf.Sim.AI.RL.Epsilon.Unleash
	}
	log.Info().Str("conf.Sim.AI.RL.Eviction.Epsilon.Unleash", conf.Sim.AI.RL.Eviction.Epsilon.Unleash).Msg("CONF_VAR")

	log.Info().Str("conf.Sim.AI.RL.Eviction.Type", conf.Sim.AI.RL.Eviction.Type).Msg("CONF_VAR")

	log.Info().Str("conf.Sim.Cache.Type", conf.Sim.Cache.Type).Msg("CONF_VAR")
}

type ServiceConfig struct {
	Service struct {
		Protocol     string
		Host         string
		Port         uint
		Type         string
		Seed         int
		OutputFolder string `mapstructure:"outputFolder"` //nolint:govet
		CPUProfile   string `mapstructure:"cpuprofile"`   //nolint:govet
		MEMProfile   string `mapstructure:"memprofile"`   //nolint:govet

		Cache          cacheBaseConf
		AI             ai             `mapstructure:"ai"`
		WeightFunction weightFunction `mapstructure:"weightfunc"`
	}
}

func (conf ServiceConfig) configure(configFilenameWithNoExt string) { //nolint:ignore,funlen
	viper.SetConfigName(configFilenameWithNoExt) // name of config file (without extension)
	viper.SetConfigType("yaml")                  // REQUIRED if the config file does not have the extension in the name
	viper.AddConfigPath(".")                     // optionally look for config in the working directory

	viper.SetDefault("service.protocol", "http")
	viper.SetDefault("service.host", "localhost")
	viper.SetDefault("service.port", 46692)
	viper.SetDefault("service.type", "normal")
	viper.SetDefault("service.outputFolder", "./")
	viper.SetDefault("service.cpuprofile", "")
	viper.SetDefault("service.memprofile", "")
	viper.SetDefault("service.seed", 42)

	viper.SetDefault("service.cache.watermarks", false)
	viper.SetDefault("service.cache.watermark.high", 95.0)
	viper.SetDefault("service.cache.watermark.low", 75.0)
	viper.SetDefault("service.cache.size.value", 100)
	viper.SetDefault("service.cache.size.unit", "T")
	viper.SetDefault("service.cache.bandwidth.value", 10.0)
	viper.SetDefault("service.cache.bandwidth.redirect", false)
	viper.SetDefault("service.cache.stats.maxNumDayDiff", 6.0)
	viper.SetDefault("service.cache.stats.deltaDaysStep", 7.0)

	viper.SetDefault("service.weightfunc.name", "FuncAdditiveExp")
	viper.SetDefault("service.weightfunc.alpha", 1.0)
	viper.SetDefault("service.weightfunc.beta", 1.0)
	viper.SetDefault("service.weightfunc.gamma", 1.0)

	viper.SetDefault("service.ai.rl.epsilon.start", 1.0)
	viper.SetDefault("service.ai.rl.epsilon.decay", 0.0000042)
	viper.SetDefault("service.ai.rl.epsilon.unleash", true)

	viper.SetDefault("service.ai.featuremap", "")
	viper.SetDefault("service.ai.rl.type", "SCDL2")

	viper.SetDefault("service.ai.rl.addition.featuremap", "")
	viper.SetDefault("service.ai.rl.addition.epsilon.start", -1.0)
	viper.SetDefault("service.ai.rl.addition.epsilon.decay", -1.0)
	viper.SetDefault("service.ai.rl.addition.epsilon.unleash", false)

	viper.SetDefault("service.ai.rl.eviction.featuremap", "")
	viper.SetDefault("service.ai.rl.eviction.k", 32)
	viper.SetDefault("service.ai.rl.eviction.type", "onK")
	viper.SetDefault("service.ai.rl.eviction.epsilon.start", -1.0)
	viper.SetDefault("service.ai.rl.eviction.epsilon.decay", -1.0)
	viper.SetDefault("service.ai.rl.eviction.epsilon.unleash", false)
}

func (conf *ServiceConfig) check() {
	log.Info().Uint("conf.Service.Cache.Size.Value", conf.Service.Cache.Size.Value).Msg("CONF_VAR")
	log.Info().Str("conf.Service.Cache.Size.Unit", conf.Service.Cache.Size.Unit).Msg("CONF_VAR")
	log.Info().Uint("conf.Service.Cache.Bandwidth.Value", conf.Service.Cache.Bandwidth.Value).Msg("CONF_VAR")
	log.Info().Bool("conf.Service.Cache.Bandwidth.Redirect", conf.Service.Cache.Bandwidth.Redirect).Msg("CONF_VAR")
	log.Info().Bool("conf.Service.Cache.Watermarks", conf.Service.Cache.Watermarks).Msg("CONF_VAR")
	log.Info().Float64("conf.Service.Cache.Watermark.High", conf.Service.Cache.Watermark.High).Msg("CONF_VAR")
	log.Info().Float64("conf.Service.Cache.Watermark.Low", conf.Service.Cache.Watermark.Low).Msg("CONF_VAR")
	log.Info().Float64("conf.Service.Cache.Stats.MaxNumDayDiff", conf.Service.Cache.Stats.MaxNumDayDiff).Msg("CONF_VAR")
	log.Info().Float64("conf.Service.Cache.Stats.DeltaDaysStep", conf.Service.Cache.Stats.DeltaDaysStep).Msg("CONF_VAR")

	simOutputFolderAbs, errAbs := filepath.Abs(conf.Service.OutputFolder)
	if errAbs != nil {
		panic(errAbs)
	}
	conf.Service.OutputFolder = simOutputFolderAbs
	log.Info().Str("conf.Service.OutputFolder", conf.Service.OutputFolder).Msg("CONF_VAR")

	log.Info().Str("conf.Service.CPUProfile", conf.Service.CPUProfile).Msg("CONF_VAR")
	log.Info().Str("conf.Service.MEMProfile", conf.Service.MEMProfile).Msg("CONF_VAR")

	log.Info().Str("conf.Service.WeightFunction.Name", conf.Service.WeightFunction.Name).Msg("CONF_VAR")
	log.Info().Float64("conf.Service.WeightFunction.Alpha", conf.Service.WeightFunction.Alpha).Msg("CONF_VAR")
	log.Info().Float64("conf.Service.WeightFunction.Beta", conf.Service.WeightFunction.Beta).Msg("CONF_VAR")
	log.Info().Float64("conf.Service.WeightFunction.Gamma", conf.Service.WeightFunction.Gamma).Msg("CONF_VAR")

	if conf.Service.AI.Featuremap != "" {
		aiFeatureMapAbs, errAbs := filepath.Abs(conf.Service.AI.Featuremap)
		if errAbs != nil {
			panic(errAbs)
		}
		conf.Service.AI.Featuremap = aiFeatureMapAbs
	}
	log.Info().Str("conf.Service.AI.Featuremap", conf.Service.AI.Featuremap).Msg("CONF_VAR")

	log.Info().Str("conf.Service.AI.Dataset2TestPath", conf.Service.AI.Dataset2TestPath).Msg("CONF_VAR")
	log.Info().Str("conf.Service.AI.Model", conf.Service.AI.Model).Msg("CONF_VAR")

	log.Info().Str("conf.Service.AI.RL.Type", conf.Service.AI.RL.Type).Msg("CONF_VAR")

	if conf.Service.AI.RL.Addition.Featuremap != "" {
		aiRLAdditionFeatureMapAbs, errAbs := filepath.Abs(conf.Service.AI.RL.Addition.Featuremap)
		if errAbs != nil {
			panic(errAbs)
		}
		conf.Service.AI.RL.Addition.Featuremap = aiRLAdditionFeatureMapAbs
	}
	log.Info().Str("conf.Service.AI.RL.Addition.Featuremap", conf.Service.AI.RL.Addition.Featuremap).Msg("CONF_VAR")

	if conf.Service.AI.RL.Eviction.Featuremap != "" {
		aiRLEvictionFeatureMapAbs, errAbs := filepath.Abs(conf.Service.AI.RL.Eviction.Featuremap)
		if errAbs != nil {
			panic(errAbs)
		}
		conf.Service.AI.RL.Eviction.Featuremap = aiRLEvictionFeatureMapAbs
	}
	log.Info().Str("conf.Service.AI.RL.Eviction.Featuremap", conf.Service.AI.RL.Eviction.Featuremap).Msg("CONF_VAR")
	log.Info().Uint("conf.Service.AI.RL.Eviction.K", conf.Service.AI.RL.Eviction.K).Msg("CONF_VAR")

	log.Info().Float64("conf.Service.AI.RL.Epsilon.Start", conf.Service.AI.RL.Epsilon.Start).Msg("CONF_VAR")
	log.Info().Float64("conf.Service.AI.RL.Epsilon.Decay", conf.Service.AI.RL.Epsilon.Decay).Msg("CONF_VAR")
	log.Info().Str("conf.Service.AI.RL.Epsilon.Unleash", conf.Service.AI.RL.Epsilon.Unleash).Msg("CONF_VAR")

	if conf.Service.AI.RL.Addition.Epsilon.Start == -1.0 {
		conf.Service.AI.RL.Addition.Epsilon.Start = conf.Service.AI.RL.Epsilon.Start
	}
	log.Info().Float64("conf.Service.AI.RL.Addition.Epsilon.Start", conf.Service.AI.RL.Addition.Epsilon.Start).Msg("CONF_VAR")

	if conf.Service.AI.RL.Addition.Epsilon.Decay == -1.0 {
		conf.Service.AI.RL.Addition.Epsilon.Decay = conf.Service.AI.RL.Epsilon.Decay
	}
	log.Info().Float64("conf.Service.AI.RL.Addition.Epsilon.Decay", conf.Service.AI.RL.Addition.Epsilon.Decay).Msg("CONF_VAR")

	if conf.Service.AI.RL.Addition.Epsilon.Unleash == "default" {
		conf.Service.AI.RL.Addition.Epsilon.Unleash = conf.Service.AI.RL.Epsilon.Unleash
	}
	log.Info().Str("conf.Service.AI.RL.Addition.Epsilon.Unleash", conf.Service.AI.RL.Addition.Epsilon.Unleash).Msg("CONF_VAR")

	if conf.Service.AI.RL.Eviction.Epsilon.Start == -1.0 {
		conf.Service.AI.RL.Eviction.Epsilon.Start = conf.Service.AI.RL.Epsilon.Start
	}
	log.Info().Float64("conf.Service.AI.RL.Eviction.Epsilon.Start", conf.Service.AI.RL.Eviction.Epsilon.Start).Msg("CONF_VAR")

	if conf.Service.AI.RL.Eviction.Epsilon.Decay == -1.0 {
		conf.Service.AI.RL.Eviction.Epsilon.Decay = conf.Service.AI.RL.Epsilon.Decay
	}
	log.Info().Float64("conf.Service.AI.RL.Eviction.Epsilon.Decay", conf.Service.AI.RL.Eviction.Epsilon.Decay).Msg("CONF_VAR")

	if conf.Service.AI.RL.Eviction.Epsilon.Unleash == "default" {
		conf.Service.AI.RL.Eviction.Epsilon.Unleash = conf.Service.AI.RL.Epsilon.Unleash
	}
	log.Info().Str("conf.Service.AI.RL.Eviction.Epsilon.Unleash", conf.Service.AI.RL.Eviction.Epsilon.Unleash).Msg("CONF_VAR")

	log.Info().Str("conf.Service.AI.RL.Eviction.Type", conf.Service.AI.RL.Eviction.Type).Msg("CONF_VAR")

	log.Info().Str("conf.Service.Cache.Type", conf.Service.Cache.Type).Msg("CONF_VAR")
}
