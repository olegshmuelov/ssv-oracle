package ethsync

import (
	"encoding/hex"
	"testing"
	"time"

	"github.com/ethereum/go-ethereum/common"
)

func TestComputeClusterID(t *testing.T) {
	// Test with example data
	owner := common.HexToAddress("0x1234567890123456789012345678901234567890")
	operatorIDs := []uint64{1, 2, 3, 4}

	clusterID := ComputeClusterID(owner, operatorIDs)

	// Verify it produces a 32-byte hash
	if len(clusterID) != 32 {
		t.Errorf("Expected 32 bytes, got %d", len(clusterID))
	}

	// Verify it's deterministic (same inputs = same output)
	clusterID2 := ComputeClusterID(owner, operatorIDs)
	if clusterID != clusterID2 {
		t.Error("ComputeClusterID is not deterministic")
	}

	// Verify different operators produce different IDs
	differentOps := []uint64{1, 2, 3, 5}
	differentID := ComputeClusterID(owner, differentOps)
	if clusterID == differentID {
		t.Error("Different operator IDs should produce different cluster IDs")
	}

	// Verify different owner produces different ID
	differentOwner := common.HexToAddress("0x9876543210987654321098765432109876543210")
	differentOwnerID := ComputeClusterID(differentOwner, operatorIDs)
	if clusterID == differentOwnerID {
		t.Error("Different owners should produce different cluster IDs")
	}

	t.Logf("Cluster ID: 0x%s", hex.EncodeToString(clusterID[:]))
}

func TestComputeClusterID_SortingInvariant(t *testing.T) {
	owner := common.HexToAddress("0x1234567890123456789012345678901234567890")

	// Operator IDs in different order should produce the SAME cluster ID (sorted internally)
	ops1 := []uint64{1, 2, 3, 4}
	ops2 := []uint64{4, 3, 2, 1}
	ops3 := []uint64{2, 4, 1, 3}

	id1 := ComputeClusterID(owner, ops1)
	id2 := ComputeClusterID(owner, ops2)
	id3 := ComputeClusterID(owner, ops3)

	if id1 != id2 {
		t.Error("Same operators in different order should produce same cluster ID")
	}
	if id1 != id3 {
		t.Error("Same operators in different order should produce same cluster ID")
	}

	t.Logf("Cluster ID (sorted): 0x%s", hex.EncodeToString(id1[:]))
}

// TestSpec tests the Spec struct methods for slot/epoch calculations
func TestSpec(t *testing.T) {
	// Use Mainnet genesis time: Dec 1, 2020 12:00:23 UTC
	genesisTime := time.Date(2020, 12, 1, 12, 0, 23, 0, time.UTC)
	spec := NewSpec(genesisTime)

	t.Run("SlotAt", func(t *testing.T) {
		// Slot 0 at genesis
		slot := spec.SlotAt(genesisTime)
		if slot != 0 {
			t.Errorf("Expected slot 0 at genesis, got %d", slot)
		}

		// Slot 1 at genesis + 12 seconds
		slot = spec.SlotAt(genesisTime.Add(12 * time.Second))
		if slot != 1 {
			t.Errorf("Expected slot 1, got %d", slot)
		}

		// Slot 32 at genesis + 384 seconds (1 epoch)
		slot = spec.SlotAt(genesisTime.Add(384 * time.Second))
		if slot != 32 {
			t.Errorf("Expected slot 32, got %d", slot)
		}

		// Before genesis should return 0
		slot = spec.SlotAt(genesisTime.Add(-1 * time.Hour))
		if slot != 0 {
			t.Errorf("Expected slot 0 before genesis, got %d", slot)
		}
	})

	t.Run("TimeAt", func(t *testing.T) {
		// Time at slot 0 should be genesis
		slotTime := spec.TimeAt(0)
		if !slotTime.Equal(genesisTime) {
			t.Errorf("Expected genesis time at slot 0, got %v", slotTime)
		}

		// Time at slot 100
		slotTime = spec.TimeAt(100)
		expected := genesisTime.Add(100 * 12 * time.Second)
		if !slotTime.Equal(expected) {
			t.Errorf("Expected %v at slot 100, got %v", expected, slotTime)
		}
	})

	t.Run("EpochAt", func(t *testing.T) {
		tests := []struct {
			slot     uint64
			expected uint64
		}{
			{0, 0},
			{31, 0},
			{32, 1},
			{63, 1},
			{64, 2},
			{320, 10},
		}

		for _, tt := range tests {
			epoch := spec.EpochAt(tt.slot)
			if epoch != tt.expected {
				t.Errorf("EpochAt(%d) = %d, expected %d", tt.slot, epoch, tt.expected)
			}
		}
	})

	t.Run("FirstSlot", func(t *testing.T) {
		tests := []struct {
			epoch    uint64
			expected uint64
		}{
			{0, 0},
			{1, 32},
			{2, 64},
			{10, 320},
		}

		for _, tt := range tests {
			slot := spec.FirstSlot(tt.epoch)
			if slot != tt.expected {
				t.Errorf("FirstSlot(%d) = %d, expected %d", tt.epoch, slot, tt.expected)
			}
		}
	})

	t.Run("LastSlot", func(t *testing.T) {
		tests := []struct {
			epoch    uint64
			expected uint64
		}{
			{0, 31},
			{1, 63},
			{2, 95},
			{10, 351},
		}

		for _, tt := range tests {
			slot := spec.LastSlot(tt.epoch)
			if slot != tt.expected {
				t.Errorf("LastSlot(%d) = %d, expected %d", tt.epoch, slot, tt.expected)
			}
		}
	})

	t.Run("RealWorldScenario", func(t *testing.T) {
		// Simulate a real block time and verify calculations
		// Block at slot 1000 (epoch 31)
		blockTime := genesisTime.Add(1000 * 12 * time.Second)

		slot := spec.SlotAt(blockTime)
		if slot != 1000 {
			t.Errorf("Expected slot 1000, got %d", slot)
		}

		epoch := spec.EpochAt(slot)
		if epoch != 31 {
			t.Errorf("Expected epoch 31, got %d", epoch)
		}

		// Verify first and last slot of epoch 31
		firstSlot := spec.FirstSlot(31)
		if firstSlot != 992 {
			t.Errorf("Expected first slot 992, got %d", firstSlot)
		}

		lastSlot := spec.LastSlot(31)
		if lastSlot != 1023 {
			t.Errorf("Expected last slot 1023, got %d", lastSlot)
		}
	})
}
