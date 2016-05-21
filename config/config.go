package config

import (
	"fmt"
	"strconv"

	"github.com/spf13/viper"
)

func GetServersFromConfig() (servers map[int]string, err error) {
	viper.SetConfigName("config")
	viper.AddConfigPath(".")
	viper.AddConfigPath("./config")
	viper.AddConfigPath("../config")

	if err := viper.ReadInConfig(); err != nil {
		fmt.Println("No configuration file found!")
		return servers, err
	}

	servers = make(map[int]string)

	allServers := viper.GetStringMap("Servers")

	for i, value := range allServers {
		servId, _ := strconv.Atoi(i)

		servers[servId] = value.(string)
	}

	return servers, err
}
