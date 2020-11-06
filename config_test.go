package clickhouse

import (
	"fmt"
	"testing"
)

var config = Config{
	Addr:     "127.0.0.1:9000",
	Database: "test",
	Debug:    false,
	ZoneInfo: "",
}

func TestConfig_Validate(t *testing.T) {
	tests := []struct {
		name    string
		config  Config
		wantErr bool
	}{
		{"positive validation", config, false},
		{"empty addr", Config{}, true},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if err := tt.config.Validate(); (err != nil) != tt.wantErr {
				t.Errorf("validation error expected: %v, got %v", tt.wantErr, err)
			}
		})
	}
}

func TestConfig_GetDSN(t *testing.T) {
	expected := fmt.Sprintf("tcp://%s?charset=utf8&parseTime=True&debug=%s&database=%s", config.Addr, "False", config.Database)

	if got := config.GetDSN(); got != expected {
		t.Errorf("dns expected: %v, got %v", expected, got)
	}
}
