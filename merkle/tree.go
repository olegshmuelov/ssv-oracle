package merkle

import (
	"bytes"
	"sort"

	"github.com/ethereum/go-ethereum/crypto"
)

// leafData holds cluster ID and its encoded leaf hash for sorting
type leafData struct {
	clusterID [32]byte
	leafHash  [32]byte
}

// BuildMerkleTree constructs a Merkle tree from cluster balances.
// Returns the Merkle root.
//
// Algorithm (following Bitcoin/OpenZeppelin standard):
// 1. Encode each (clusterID, balance) as a leaf
// 2. Sort leaves by clusterID ascending
// 3. Build binary tree bottom-up with duplicate strategy
// 4. Use OpenZeppelin sibling sorting (sort siblings before hashing)
func BuildMerkleTree(clusters map[[32]byte]uint64) [32]byte {
	// Special case: empty tree - return hash of empty data
	if len(clusters) == 0 {
		return crypto.Keccak256Hash([]byte{})
	}

	// 1. Encode leaves and collect them with cluster IDs for sorting
	leaves := make([]leafData, 0, len(clusters))
	for clusterID, balance := range clusters {
		leafHash := EncodeMerkleLeaf(clusterID, balance)
		leaves = append(leaves, leafData{
			clusterID: clusterID,
			leafHash:  leafHash,
		})
	}

	// 2. Sort leaves by clusterID ascending
	sort.Slice(leaves, func(i, j int) bool {
		return bytes.Compare(leaves[i].clusterID[:], leaves[j].clusterID[:]) < 0
	})

	// 3. Extract sorted leaf hashes
	hashes := make([][32]byte, len(leaves))
	for i, leaf := range leaves {
		hashes[i] = leaf.leafHash
	}

	// 4. Build binary tree with duplicate strategy (Bitcoin/OpenZeppelin standard)
	for len(hashes) > 1 {
		nextLevel := make([][32]byte, 0, (len(hashes)+1)/2)

		for i := 0; i < len(hashes); i += 2 {
			left := hashes[i]
			var right [32]byte

			// Duplicate last node if odd count (standard approach)
			if i+1 < len(hashes) {
				right = hashes[i+1]
			} else {
				right = left // Duplicate last node
			}

			// OpenZeppelin sibling sorting: sort siblings before hashing
			// This ensures h(a,b) = h(b,a) for verification
			if bytes.Compare(left[:], right[:]) > 0 {
				left, right = right, left
			}

			// Concatenate and hash: keccak256(left || right)
			combined := make([]byte, 64)
			copy(combined[0:32], left[:])
			copy(combined[32:64], right[:])

			parent := crypto.Keccak256Hash(combined)
			nextLevel = append(nextLevel, parent)
		}

		hashes = nextLevel
	}

	return hashes[0]
}
