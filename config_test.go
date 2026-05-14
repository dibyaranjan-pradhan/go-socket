package gosocket

import (
	"testing"
)

func TestConfigHubEmitBufferSizeDefault(t *testing.T) {
	cfg := Config{}
	normalized := cfg.normalized()

	if normalized.HubEmitBufferSize != DefaultHubEmitBufferSize {
		t.Errorf("expected default HubEmitBufferSize %d, got %d",
			DefaultHubEmitBufferSize, normalized.HubEmitBufferSize)
	}
}

func TestConfigHubEmitBufferSizeCustom(t *testing.T) {
	cfg := Config{HubEmitBufferSize: 8192}
	normalized := cfg.normalized()

	if normalized.HubEmitBufferSize != 8192 {
		t.Errorf("expected custom HubEmitBufferSize 8192, got %d",
			normalized.HubEmitBufferSize)
	}
}

func TestConfigHubEmitBufferSizeNegativeUsesDefault(t *testing.T) {
	cfg := Config{HubEmitBufferSize: -1}
	normalized := cfg.normalized()

	if normalized.HubEmitBufferSize != DefaultHubEmitBufferSize {
		t.Errorf("expected negative size to use default, got %d",
			normalized.HubEmitBufferSize)
	}
}
