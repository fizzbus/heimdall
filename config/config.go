package config

import (
	"os"

	"gopkg.in/yaml.v3"
)

type Config struct {
	ListenAddr      string `yaml:"listen_addr"`
	MonitorAddr     string `yaml:"monitor_addr"`
	DataDir         string `yaml:"data_dir"`
	SegmentMaxBytes int64  `yaml:"segment_max_bytes"`
	RetentionHours  int    `yaml:"retention_hours"`
	MaxConnections  int    `yaml:"max_connections"`
}

func Load(path string) (*Config, error) {
	data, err := os.ReadFile(path)
	if err != nil {
		return nil, err
	}
	var cfg Config
	if err := yaml.Unmarshal(data, &cfg); err != nil {
		return nil, err
	}
	return &cfg, nil
}
