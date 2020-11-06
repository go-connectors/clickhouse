package clickhouse

import (
	"errors"
	"fmt"
)

// ErrConfigValidation is general config validation error message.
var ErrConfigValidation = errors.New("clickhouse config validation error")

// Config contains config for ClickHouse database.
type Config struct {
	Addr     string `json:"addr" yaml:"addr"`
	Database string `json:"database" yaml:"database"`
	Debug    bool   `json:"debug" yaml:"debug"`
	ZoneInfo string `json:"zone_info" yaml:"zoneinfo"`
}

// Validate checks required fields and validates for allowed values.
func (cfg Config) Validate() error {
	if cfg.Addr == "" {
		return fmt.Errorf("%w: addr is empty", ErrConfigValidation)
	}

	return nil
}

// GetDSN returns Data Source Name connection string to ClickHouse database.
func (cfg *Config) GetDSN() string {
	debug := "False"
	if cfg.Debug {
		debug = "True"
	}

	database := ""
	if cfg.Database != "" {
		database = "&database=" + cfg.Database
	}

	return fmt.Sprintf("tcp://%s?charset=utf8&parseTime=True&debug=%s%s", cfg.Addr, debug, database)
}
