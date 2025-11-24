package oracle

import (
	"context"
	"fmt"
	"log"
	"time"

	"ssv-oracle/contract"
	"ssv-oracle/merkle"
	"ssv-oracle/pkg/ethsync"
)

const (
	// SecondsPerSlot is the duration of a beacon chain slot
	SecondsPerSlot = 12
	// SlotsPerEpoch is the number of slots in an epoch
	SlotsPerEpoch = 32
)

// Oracle coordinates the cluster balance tracking and Merkle root commitments.
type Oracle struct {
	storage        ethsync.Storage
	contractClient *contract.Client
}

// Config holds the oracle configuration.
type Config struct {
	Storage        ethsync.Storage
	ContractClient *contract.Client
}

// New creates a new Oracle instance.
func New(cfg *Config) *Oracle {
	return &Oracle{
		storage:        cfg.Storage,
		contractClient: cfg.ContractClient,
	}
}

// Run starts the oracle loop.
// The oracle checks at the first slot of each epoch, which is when finalization occurs.
func (o *Oracle) Run(ctx context.Context, syncer *ethsync.EventSyncer, beaconClient *ethsync.BeaconClient) error {
	log.Println("Oracle starting...")

	// Get genesis time for epoch-aligned timing
	genesisTime, err := beaconClient.GetGenesisTime(ctx)
	if err != nil {
		return fmt.Errorf("failed to get genesis time: %w", err)
	}
	log.Printf("Genesis time: %s", genesisTime.Format(time.RFC3339))

	// Get initial config for logging
	config, err := o.contractClient.GetTimingConfig(ctx)
	if err != nil {
		return fmt.Errorf("failed to get oracle config: %w", err)
	}

	if config.EpochInterval == 0 {
		return fmt.Errorf("invalid epoch interval: 0")
	}

	log.Printf("Oracle config: startEpoch=%d, epochInterval=%d epochs",
		config.StartEpoch, config.EpochInterval)

	// Initial check (in case we can commit right now)
	if err := o.cycle(ctx, syncer, beaconClient); err != nil {
		log.Printf("Initial cycle error: %v", err)
	}

	// Main loop: wait for first slot of each epoch, then check
	for {
		// Calculate next epoch boundary
		now := time.Now()
		currentSlot := uint64(now.Sub(genesisTime) / (SecondsPerSlot * time.Second))
		currentEpoch := currentSlot / SlotsPerEpoch
		nextEpoch := currentEpoch + 1

		nextEpochSlot := nextEpoch * SlotsPerEpoch
		nextEpochTime := genesisTime.Add(time.Duration(nextEpochSlot*SecondsPerSlot) * time.Second)
		waitDuration := time.Until(nextEpochTime)

		log.Printf("Waiting for epoch %d (in %v)", nextEpoch, waitDuration.Round(time.Second))

		select {
		case <-ctx.Done():
			log.Println("Oracle stopping...")
			return ctx.Err()
		case <-time.After(waitDuration):
		}

		// Now at first slot of epoch - check finalization and commit if ready
		if err := o.cycle(ctx, syncer, beaconClient); err != nil {
			log.Printf("Cycle error: %v", err)
		}
	}
}

// cycle executes one oracle cycle:
// 1. Sync events to finalized
// 2. Check if we need to commit (based on round)
// 3. Fetch effective balances
// 4. Build merkle root
// 5. Commit
func (o *Oracle) cycle(ctx context.Context, syncer *ethsync.EventSyncer, beaconClient *ethsync.BeaconClient) error {
	// 1. Sync events to finalized
	if err := syncer.SyncIncremental(ctx); err != nil {
		return fmt.Errorf("failed to sync events: %w", err)
	}

	// 2. Get oracle config and state
	config, err := o.contractClient.GetTimingConfig(ctx)
	if err != nil {
		return fmt.Errorf("failed to get oracle config: %w", err)
	}

	latestCommittedRound, err := o.contractClient.GetLatestCommittedRound(ctx)
	if err != nil {
		return fmt.Errorf("failed to get latest committed round: %w", err)
	}

	finalizedEpoch, err := beaconClient.GetFinalizedEpoch(ctx)
	if err != nil {
		return fmt.Errorf("failed to get finalized epoch: %w", err)
	}

	// 3. Calculate current round
	if finalizedEpoch < config.StartEpoch {
		log.Printf("Waiting for start epoch (finalized=%d, start=%d)", finalizedEpoch, config.StartEpoch)
		return nil
	}

	currentRound := (finalizedEpoch - config.StartEpoch) / config.EpochInterval
	targetEpoch := config.StartEpoch + (currentRound * config.EpochInterval)

	// 4. Check if already committed
	if currentRound <= latestCommittedRound {
		log.Printf("Round %d already committed, waiting for next", currentRound)
		return nil
	}

	// 5. Get the last execution block of the target epoch
	targetBlock, err := beaconClient.GetLastBlockOfEpoch(ctx, targetEpoch)
	if err != nil {
		return fmt.Errorf("failed to get last block of epoch %d: %w", targetEpoch, err)
	}

	log.Printf("--- Cycle: round %d, epoch %d, block %d ---", currentRound, targetEpoch, targetBlock)

	// 6. Sync events to target block (ensure consistency)
	if err := syncer.SyncToBlock(ctx, targetBlock); err != nil {
		return fmt.Errorf("failed to sync to block %d: %w", targetBlock, err)
	}

	// 7. Fetch and store effective balances
	if err := o.fetchAndStoreBalances(ctx, beaconClient, targetEpoch); err != nil {
		return fmt.Errorf("failed to fetch balances: %w", err)
	}

	// 8. Get cluster balances and build merkle root
	clusterBalances, err := o.storage.GetClusterBalances(ctx, targetEpoch)
	if err != nil {
		return fmt.Errorf("failed to get cluster balances: %w", err)
	}

	clusterMap := make(map[[32]byte]uint64)
	for _, bal := range clusterBalances {
		var clusterID [32]byte
		copy(clusterID[:], bal.ClusterID)
		clusterMap[clusterID] = bal.TotalEffectiveBalance
	}

	merkleRoot := merkle.BuildMerkleTree(clusterMap)
	log.Printf("Merkle root: 0x%x (%d clusters)", merkleRoot[:], len(clusterBalances))

	// 9. Commit to contract
	txHash, err := o.contractClient.CommitRoot(ctx, currentRound, merkleRoot, targetBlock, targetEpoch)
	if err != nil {
		return fmt.Errorf("failed to commit: %w", err)
	}

	// 10. Wait for confirmation
	receipt, err := o.contractClient.WaitForReceipt(ctx, txHash)
	if err != nil {
		return fmt.Errorf("failed waiting for receipt: %w", err)
	}

	if receipt.Status == 1 {
		log.Printf("✓ Committed (tx: %s)", txHash)
	} else {
		return fmt.Errorf("transaction reverted")
	}

	return nil
}

// fetchAndStoreBalances fetches validator balances from beacon and stores them in the database.
// Only stores balances that have changed from the previous epoch (optimization).
func (o *Oracle) fetchAndStoreBalances(ctx context.Context, beaconClient *ethsync.BeaconClient, targetEpoch uint64) error {
	// Get active validators with their cluster IDs (epoch-based query)
	validators, err := o.storage.GetActiveValidatorsWithClusters(ctx, targetEpoch)
	if err != nil {
		return fmt.Errorf("failed to get active validators: %w", err)
	}

	if len(validators) == 0 {
		log.Printf("Balances: no active validators")
		return nil
	}

	// Build a list of unique pubkeys for beacon query
	pubkeySet := make(map[string]struct{})
	var pubkeys [][]byte
	for _, v := range validators {
		pubkeyHex := fmt.Sprintf("0x%x", v.ValidatorPubkey)
		if _, exists := pubkeySet[pubkeyHex]; !exists {
			pubkeySet[pubkeyHex] = struct{}{}
			pubkeys = append(pubkeys, v.ValidatorPubkey)
		}
	}

	// Fetch balances from beacon
	balanceMap, err := beaconClient.GetValidatorBalances(ctx, targetEpoch, pubkeys)
	if err != nil {
		return fmt.Errorf("failed to fetch validator balances: %w", err)
	}

	// Get previous balances to detect changes
	prevBalances, err := o.storage.GetLatestValidatorBalances(ctx, validators, targetEpoch)
	if err != nil {
		return fmt.Errorf("failed to get previous balances: %w", err)
	}

	// Store balances when changed or first seen on beacon
	// - Validators on beacon: store balance (including 0 for exited/slashed)
	// - Validators not on beacon: skip if never deposited, store 0 if previously had balance
	stored := 0
	skipped := 0
	notOnBeacon := 0
	insertErrors := 0

	for _, v := range validators {
		pubkeyHex := fmt.Sprintf("0x%x", v.ValidatorPubkey)
		newBalance, onBeacon := balanceMap[pubkeyHex]

		// Check previous balance first (need hasPrev for onBeacon logic)
		key := fmt.Sprintf("%x:%x", v.ClusterID, v.ValidatorPubkey)
		prevBalance, hasPrev := prevBalances[key]

		if !onBeacon {
			notOnBeacon++
			// Not on beacon and no previous record → never deposited, skip (implicit 0)
			if !hasPrev {
				skipped++
				continue
			}
			// Had previous balance but now gone from beacon → treat as 0
			newBalance = 0
		}

		// Skip if balance unchanged
		if hasPrev && prevBalance == newBalance {
			skipped++
			continue
		}

		// Store balance (including 0 for exited/withdrawn validators)
		balance := &ethsync.ValidatorBalance{
			ClusterID:        v.ClusterID,
			ValidatorPubkey:  v.ValidatorPubkey,
			Epoch:            targetEpoch,
			EffectiveBalance: newBalance,
		}

		if err := o.storage.InsertValidatorBalance(ctx, balance); err != nil {
			log.Printf("Warning: failed to insert balance for validator %s: %v", pubkeyHex, err)
			insertErrors++
		} else {
			stored++
		}
	}

	// Log concise summary
	log.Printf("Balances: %d/%d from beacon, %d changed, %d not deposited",
		len(balanceMap), len(validators), stored, notOnBeacon)

	if insertErrors > 0 {
		log.Printf("Warning: %d balance insert errors", insertErrors)
	}

	return nil
}
