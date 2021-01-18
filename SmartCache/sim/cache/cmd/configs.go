package cmd

import "github.com/spf13/viper"

func configureSimViperVars(configFilenameWithNoExt string) { //nolint:ignore,funlen
	viper.SetConfigName(configFilenameWithNoExt) // name of config file (without extension)
	viper.SetConfigType("yaml")                  // REQUIRED if the config file does not have the extension in the name
	viper.AddConfigPath(".")                     // optionally look for config in the working directory

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
	viper.SetDefault("sim.cache.watermarks", false)
	viper.SetDefault("sim.cache.watermark.high", 95.0)
	viper.SetDefault("sim.cache.watermark.low", 75.0)
	viper.SetDefault("sim.coldstart", false)
	viper.SetDefault("sim.coldstartnostats", false)
	viper.SetDefault("sim.log", false)
	viper.SetDefault("sim.seed", 42)

	viper.SetDefault("sim.cpuprofile", "")
	viper.SetDefault("sim.memprofile", "")
	viper.SetDefault("sim.outputupdatedelay", 2.4)

	viper.SetDefault("sim.cache.size.value", 100.)
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
	viper.SetDefault("sim.ai.rl.epsilon.unleash", true)

	viper.SetDefault("sim.ai.featuremap", "")
	viper.SetDefault("sim.ai.rl.type", "SCDL2")

	viper.SetDefault("sim.ai.rl.addition.featuremap", "")
	viper.SetDefault("sim.ai.rl.addition.epsilon.start", -1.0)
	viper.SetDefault("sim.ai.rl.addition.epsilon.decay", -1.0)
	viper.SetDefault("sim.ai.rl.addition.epsilon.unleash", false)

	viper.SetDefault("sim.ai.rl.eviction.featuremap", "")
	viper.SetDefault("sim.ai.rl.eviction.k", 32)
	viper.SetDefault("sim.ai.rl.eviction.type", "onK")
	viper.SetDefault("sim.ai.rl.eviction.epsilon.start", -1.0)
	viper.SetDefault("sim.ai.rl.eviction.epsilon.decay", -1.0)
	viper.SetDefault("sim.ai.rl.eviction.epsilon.unleash", false)

	viper.SetDefault("sim.ai.model", "")

	viper.SetDefault("sim.dataset2testpath", "")
}

type serviceConfig struct {
	Service struct {
		Protocol string
	}
}

func configureServiceViperVars(configFilenameWithNoExt string) { //nolint:ignore,funlen
	viper.SetConfigName(configFilenameWithNoExt) // name of config file (without extension)
	viper.SetConfigType("yaml")                  // REQUIRED if the config file does not have the extension in the name
	viper.AddConfigPath(".")                     // optionally look for config in the working directory

	viper.SetDefault("service.protocol", "http")
}
