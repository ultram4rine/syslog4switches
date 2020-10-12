package conf

import (
	log "github.com/sirupsen/logrus"
	"github.com/spf13/viper"
)

// Config is the configuration.
var Config struct {
	DBHost string `mapstructure:"db_host"`
	DBName string `mapstructure:"db_name"`
	DBUser string `mapstructure:"db_user"`
	DBPass string `mapstructure:"db_pass"`
}

// Load parses the config from file or from ENV variables s into a Config.
func Load(confName string) error {
	viper.SetConfigName(confName)
	viper.AddConfigPath(".")
	if err := viper.ReadInConfig(); err != nil {
		log.Warnf("Error decoding config file from %s: %s", confName, err)
	}

	viper.SetEnvPrefix("s4s")
	if err := viper.BindEnv("db_host"); err != nil {
		return err
	}
	if err := viper.BindEnv("db_name"); err != nil {
		return err
	}
	if err := viper.BindEnv("db_user"); err != nil {
		return err
	}
	if err := viper.BindEnv("db_pass"); err != nil {
		return err
	}

	if err := viper.Unmarshal(&Config); err != nil {
		return err
	}

	return nil
}
