package ethsync

import (
	"math/big"
	"sort"
	"time"

	"github.com/ethereum/go-ethereum/common"
	"github.com/ethereum/go-ethereum/crypto"
	"golang.org/x/crypto/sha3"
)

// SSV Contract Events
// Based on ssvlabs/ssv-network ISSVClusters interface

const (
	EventValidatorAdded     = "ValidatorAdded"
	EventValidatorRemoved   = "ValidatorRemoved"
	EventClusterLiquidated  = "ClusterLiquidated"
	EventClusterReactivated = "ClusterReactivated"
	EventClusterWithdrawn   = "ClusterWithdrawn"
	EventClusterDeposited   = "ClusterDeposited"
)

// Event signatures (keccak256 of event signature)
// IMPORTANT: Use Solidity format for tuples: (type1,type2,...) NOT tuple(type1,type2,...)
var (
	EventSigValidatorAdded     = crypto.Keccak256Hash([]byte("ValidatorAdded(address,uint64[],bytes,bytes,(uint32,uint64,uint64,bool,uint256))"))
	EventSigValidatorRemoved   = crypto.Keccak256Hash([]byte("ValidatorRemoved(address,uint64[],bytes,(uint32,uint64,uint64,bool,uint256))"))
	EventSigClusterLiquidated  = crypto.Keccak256Hash([]byte("ClusterLiquidated(address,uint64[],(uint32,uint64,uint64,bool,uint256))"))
	EventSigClusterReactivated = crypto.Keccak256Hash([]byte("ClusterReactivated(address,uint64[],(uint32,uint64,uint64,bool,uint256))"))
	EventSigClusterWithdrawn   = crypto.Keccak256Hash([]byte("ClusterWithdrawn(address,uint64[],uint256,(uint32,uint64,uint64,bool,uint256))"))
	EventSigClusterDeposited   = crypto.Keccak256Hash([]byte("ClusterDeposited(address,uint64[],uint256,(uint32,uint64,uint64,bool,uint256))"))
)

// Cluster represents an SSV cluster state.
// Matches the Cluster struct from ISSVNetworkCore.sol
type Cluster struct {
	ValidatorCount   uint32   // The number of validators in the cluster
	NetworkFeeIndex  uint64   // The index of network fees related to this cluster
	Index            uint64   // The last index calculated for the cluster
	Active           bool     // Flag indicating whether the cluster is active
	Balance          *big.Int // The balance of the cluster (uint256)
}

// SSV Event Data Structures
// These represent parsed event data from JSONB raw_event

// ValidatorAddedEvent represents the ValidatorAdded event
type ValidatorAddedEvent struct {
	Owner       common.Address
	OperatorIDs []uint64
	PublicKey   []byte
	Shares      []byte
	Cluster     Cluster
}

// ValidatorRemovedEvent represents the ValidatorRemoved event
type ValidatorRemovedEvent struct {
	Owner       common.Address
	OperatorIDs []uint64
	PublicKey   []byte
	Cluster     Cluster
}

// ClusterLiquidatedEvent represents the ClusterLiquidated event
type ClusterLiquidatedEvent struct {
	Owner       common.Address
	OperatorIDs []uint64
	Cluster     Cluster
}

// ClusterReactivatedEvent represents the ClusterReactivated event
type ClusterReactivatedEvent struct {
	Owner       common.Address
	OperatorIDs []uint64
	Cluster     Cluster
}

// ClusterWithdrawnEvent represents the ClusterWithdrawn event
type ClusterWithdrawnEvent struct {
	Owner       common.Address
	OperatorIDs []uint64
	Value       *big.Int
	Cluster     Cluster
}

// ClusterDepositedEvent represents the ClusterDeposited event
type ClusterDepositedEvent struct {
	Owner       common.Address
	OperatorIDs []uint64
	Value       *big.Int
	Cluster     Cluster
}

// ComputeClusterID computes the cluster ID from owner address and operator IDs.
// Matches SSV contract's cluster ID computation:
// 1. Sort operator IDs in ascending order
// 2. keccak256(abi.encodePacked(owner, uint256(operatorIds[0]), uint256(operatorIds[1]), ...))
func ComputeClusterID(owner common.Address, operatorIDs []uint64) [32]byte {
	// Sort operator IDs in ascending order (make a copy to avoid mutating input)
	sortedIDs := make([]uint64, len(operatorIDs))
	copy(sortedIDs, operatorIDs)
	sort.Slice(sortedIDs, func(i, j int) bool {
		return sortedIDs[i] < sortedIDs[j]
	})

	// Calculate total size: 20 bytes (address) + 32 bytes per operator ID
	size := 20 + (32 * len(sortedIDs))
	data := make([]byte, 0, size)

	// Append owner address (20 bytes)
	data = append(data, owner.Bytes()...)

	// Append each operator ID as uint256 (32 bytes each)
	for _, id := range sortedIDs {
		// Convert uint64 to uint256 (big-endian)
		idBytes := make([]byte, 32)
		big.NewInt(int64(id)).FillBytes(idBytes)
		data = append(data, idBytes...)
	}

	// Compute keccak256 hash
	hash := sha3.NewLegacyKeccak256()
	hash.Write(data)
	var result [32]byte
	hash.Sum(result[:0])

	return result
}

// Spec holds beacon chain specification parameters.
type Spec struct {
	GenesisTime   time.Time
	SlotsPerEpoch uint64
	SlotDuration  time.Duration
}

// NewSpec creates a new Spec with standard Ethereum values.
func NewSpec(genesisTime time.Time) *Spec {
	return &Spec{
		GenesisTime:   genesisTime,
		SlotsPerEpoch: 32,
		SlotDuration:  12 * time.Second,
	}
}

// SlotAt returns the slot number at the given time.
func (s *Spec) SlotAt(t time.Time) uint64 {
	if t.Before(s.GenesisTime) {
		return 0
	}
	return uint64(t.Sub(s.GenesisTime) / s.SlotDuration)
}

// TimeAt returns the time at the given slot.
func (s *Spec) TimeAt(slot uint64) time.Time {
	return s.GenesisTime.Add(time.Duration(slot) * s.SlotDuration)
}

// EpochAt returns the epoch number at the given slot.
func (s *Spec) EpochAt(slot uint64) uint64 {
	return slot / s.SlotsPerEpoch
}

// FirstSlot returns the first slot of the given epoch.
func (s *Spec) FirstSlot(epoch uint64) uint64 {
	return epoch * s.SlotsPerEpoch
}

// LastSlot returns the last slot of the given epoch.
func (s *Spec) LastSlot(epoch uint64) uint64 {
	return s.FirstSlot(epoch+1) - 1
}
