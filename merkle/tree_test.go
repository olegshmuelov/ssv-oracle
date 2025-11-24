package merkle

import (
	"encoding/hex"
	"testing"
)

func TestBuildMerkleTree_Empty(t *testing.T) {
	clusters := map[[32]byte]uint64{}

	root := BuildMerkleTree(clusters)

	// Empty tree should return keccak256([])
	// This is the standard approach (not using empty leaf)
	expected := "c5d2460186f7233c927e7db2dcc703c0e500b653ca82273b7bfad8045d85a470"
	expectedBytes, _ := hex.DecodeString(expected)
	var expectedHash [32]byte
	copy(expectedHash[:], expectedBytes)

	if root != expectedHash {
		t.Errorf("Empty tree root mismatch")
		t.Logf("Expected: 0x%x", expectedHash)
		t.Logf("Got: 0x%x", root)
	}

	t.Logf("Empty tree root: 0x%x", root)
}

func TestBuildMerkleTree_SingleCluster(t *testing.T) {
	clusterID := [32]byte{0x12, 0x34}
	clusters := map[[32]byte]uint64{
		clusterID: 32000000000,
	}

	root := BuildMerkleTree(clusters)

	// Single cluster should be: keccak256(leaf || emptyLeaf)
	leaf := EncodeMerkleLeaf(clusterID, 32000000000)
	emptyLeaf := EncodeEmptyLeaf()

	combined := make([]byte, 64)
	copy(combined[0:32], leaf[:])
	copy(combined[32:64], emptyLeaf[:])

	// This is NOT the final root - just for reference
	// expectedRoot := crypto.Keccak256Hash(combined)

	t.Logf("Single cluster root: 0x%x", root)
	t.Logf("Leaf: 0x%x", leaf)
	t.Logf("Empty leaf: 0x%x", emptyLeaf)

	// TODO: Verify against Solidity test
}

func TestBuildMerkleTree_TwoClusters(t *testing.T) {
	cluster1 := [32]byte{0x11, 0x11}
	cluster2 := [32]byte{0x22, 0x22}

	clusters := map[[32]byte]uint64{
		cluster1: 32000000000,
		cluster2: 31000000000,
	}

	root := BuildMerkleTree(clusters)

	t.Logf("Two clusters root: 0x%x", root)

	// Verify determinism
	root2 := BuildMerkleTree(clusters)
	if root != root2 {
		t.Error("BuildMerkleTree is not deterministic")
	}

	// TODO: Verify against Solidity test
}

func TestBuildMerkleTree_ThreeClusters(t *testing.T) {
	// Three clusters should trigger empty leaf rule
	cluster1 := [32]byte{0x11}
	cluster2 := [32]byte{0x22}
	cluster3 := [32]byte{0x33}

	clusters := map[[32]byte]uint64{
		cluster1: 32000000000,
		cluster2: 31000000000,
		cluster3: 32000000000,
	}

	root := BuildMerkleTree(clusters)

	t.Logf("Three clusters root: 0x%x", root)

	// Verify determinism
	root2 := BuildMerkleTree(clusters)
	if root != root2 {
		t.Error("BuildMerkleTree is not deterministic")
	}

	// TODO: Verify against Solidity test
}

func TestBuildMerkleTree_Sorting(t *testing.T) {
	// Create clusters with IDs in different order
	cluster1 := [32]byte{0xAA} // Higher
	cluster2 := [32]byte{0x11} // Lower

	clusters := map[[32]byte]uint64{
		cluster1: 32000000000,
		cluster2: 32000000000,
	}

	root := BuildMerkleTree(clusters)

	// Should be same regardless of iteration order
	// (Go maps have random iteration order)
	root2 := BuildMerkleTree(clusters)

	if root != root2 {
		t.Error("BuildMerkleTree sorting is not working correctly")
	}

	t.Logf("Root with sorting: 0x%x", root)
}

func TestBuildMerkleTree_TestDataFixtures(t *testing.T) {
	// Use our test data cluster IDs (computed with keccak256)
	cluster1Hex := "b183c42279b4dc3eb381352db3458ae66a3439765e4f880a027f62ac2c4edba9"
	cluster2Hex := "44ab2ba437cef9cb17bb6bc6af7d87715b9a3e245fbb153e66b09bb79697d316"
	cluster3Hex := "a1ea9fe3f3ba4ba4d19a681b639f7033740e55371ee0f00c2e32c15b4fe3c468"

	cluster1Bytes, _ := hex.DecodeString(cluster1Hex)
	cluster2Bytes, _ := hex.DecodeString(cluster2Hex)
	cluster3Bytes, _ := hex.DecodeString(cluster3Hex)

	var cluster1, cluster2, cluster3 [32]byte
	copy(cluster1[:], cluster1Bytes)
	copy(cluster2[:], cluster2Bytes)
	copy(cluster3[:], cluster3Bytes)

	clusters := map[[32]byte]uint64{
		cluster1: 64000000000, // 64 ETH
		cluster2: 31000000000, // 31 ETH
		cluster3: 32000000000, // 32 ETH
	}

	root := BuildMerkleTree(clusters)

	t.Log("Merkle tree from test data:")
	t.Logf("  Cluster 1: 0x%s (64 ETH)", cluster1Hex)
	t.Logf("  Cluster 2: 0x%s (31 ETH)", cluster2Hex)
	t.Logf("  Cluster 3: 0x%s (32 ETH)", cluster3Hex)
	t.Logf("  Merkle Root: 0x%x", root)

	// TODO: Verify against Solidity test with same data
}

func TestBuildMerkleTree_DifferentOrderSameRoot(t *testing.T) {
	cluster1 := [32]byte{0x11}
	cluster2 := [32]byte{0x22}
	cluster3 := [32]byte{0x33}

	// Create same clusters in different orders
	clusters1 := map[[32]byte]uint64{
		cluster1: 32000000000,
		cluster2: 31000000000,
		cluster3: 32000000000,
	}

	clusters2 := map[[32]byte]uint64{
		cluster3: 32000000000,
		cluster1: 32000000000,
		cluster2: 31000000000,
	}

	root1 := BuildMerkleTree(clusters1)
	root2 := BuildMerkleTree(clusters2)

	if root1 != root2 {
		t.Error("Different cluster insertion order produced different roots")
		t.Logf("Root 1: 0x%x", root1)
		t.Logf("Root 2: 0x%x", root2)
	}
}

func TestBuildMerkleTree_PowerOfTwo(t *testing.T) {
	// Test with 2, 4, 8 clusters (powers of 2 - no empty leaf needed except for initial pairing)
	testCases := []int{2, 4, 8}

	for _, numClusters := range testCases {
		t.Run(string(rune(numClusters))+" clusters", func(t *testing.T) {
			clusters := make(map[[32]byte]uint64)

			for i := 0; i < numClusters; i++ {
				var clusterID [32]byte
				clusterID[0] = byte(i)
				clusters[clusterID] = 32000000000
			}

			root := BuildMerkleTree(clusters)

			t.Logf("%d clusters root: 0x%x", numClusters, root)

			// Verify determinism
			root2 := BuildMerkleTree(clusters)
			if root != root2 {
				t.Error("Not deterministic")
			}
		})
	}
}

func TestBuildMerkleTree_NonPowerOfTwo(t *testing.T) {
	// Test with 3, 5, 7 clusters (requires empty leaf padding)
	testCases := []int{3, 5, 7}

	for _, numClusters := range testCases {
		t.Run(string(rune(numClusters))+" clusters", func(t *testing.T) {
			clusters := make(map[[32]byte]uint64)

			for i := 0; i < numClusters; i++ {
				var clusterID [32]byte
				clusterID[0] = byte(i)
				clusters[clusterID] = 32000000000
			}

			root := BuildMerkleTree(clusters)

			t.Logf("%d clusters root: 0x%x", numClusters, root)

			// Verify determinism
			root2 := BuildMerkleTree(clusters)
			if root != root2 {
				t.Error("Not deterministic")
			}
		})
	}
}
