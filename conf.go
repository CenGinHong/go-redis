package main

import (
	"io"
	"os"
	"strconv"

	"gopkg.in/yaml.v2"
)

type Config struct {
	Port int `yaml:"port"`
}

func LoadConfig(path string) (config *Config, err error) {
	config = &Config{}
	if path == "" {
		p, err := strconv.Atoi(os.Getenv("PORT"))
		if err != nil {
			return nil, err
		}
		config.Port = p
		return config, nil
	}
	file, err := os.Open(path)
	if err != nil {
		return
	}
	defer func(file *os.File) {
		_ = file.Close()
	}(file)
	jsonStr, err := io.ReadAll(file)
	if err != nil {
		return
	}

	if err = yaml.Unmarshal(jsonStr, config); err != nil {
		return nil, err
	}
	return
}
