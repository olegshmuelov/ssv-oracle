package contract

import (
	"testing"
)

func TestOracleABI_Loaded(t *testing.T) {
	if oracleABI == "" {
		t.Fatal("Oracle ABI not loaded")
	}

	t.Logf("Oracle ABI loaded successfully (%d bytes)", len(oracleABI))
}

func TestOracleConfig(t *testing.T) {
	config := &OracleConfig{
		StartEpoch:    100,
		EpochInterval: 32,
	}

	if config.StartEpoch != 100 {
		t.Errorf("Expected StartEpoch=100, got %d", config.StartEpoch)
	}

	if config.EpochInterval != 32 {
		t.Errorf("Expected EpochInterval=32, got %d", config.EpochInterval)
	}

	t.Logf("OracleConfig: startEpoch=%d, epochInterval=%d", config.StartEpoch, config.EpochInterval)
}

// Note: Full integration tests will be added when contract is deployed to testnet
func TestClient_PlaceholderForFutureTests(t *testing.T) {
	t.Skip("Skipping until contract is deployed to testnet")

	// Future tests:
	// - TestClient_GetOracleTimingConfig
	// - TestClient_CommitRoot
	// - TestClient_WaitForReceipt
}
