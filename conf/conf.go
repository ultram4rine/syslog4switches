package conf

import (
	log "github.com/sirupsen/logrus"
	"github.com/spf13/viper"
)

// Config is the configuration.
var Config struct {
	DBHost          string `mapstructure:"db_host"`
	DBName          string `mapstructure:"db_name"`
	DBUser          string `mapstructure:"db_user"`
	DBPass          string `mapstructure:"db_pass"`
	NetDataServer   string `mapstructure:"netdata_server"`
	NginxBatchSize  int64  `mapstructure:"nginx_batch_size"`
	MailBatchSize   int64  `mapstructure:"mail_batch_size"`
	SwitchBatchSize int64  `mapstructure:"switch_batch_size"`
}

// Load parses the config from file or from ENV variables s into a Config.
func Load(confName string) error {
	viper.SetConfigName(confName)
	viper.AddConfigPath(".")
	if err := viper.ReadInConfig(); err != nil {
		log.Warnf("Error decoding config file from %s: %s", confName, err)
	}

	viper.SetEnvPrefix("slog")
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
	if err := viper.BindEnv("netdata_server"); err != nil {
		return err
	}
	if err := viper.BindEnv("nginx_batch_size"); err != nil {
		return err
	}
	if err := viper.BindEnv("mail_batch_size"); err != nil {
		return err
	}
	if err := viper.BindEnv("switch_batch_size"); err != nil {
		return err
	}

	if err := viper.Unmarshal(&Config); err != nil {
		return err
	}

	return nil
}
