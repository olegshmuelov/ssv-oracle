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

// Oracle coordinates the cluster balance tracking and Merkle root commitments.
type Oracle struct {
	storage            ethsync.Storage
	contractClient     *contract.Client
	lastCommittedRound uint64                 // Tracks last committed round to prevent duplicates
	timingConfig       *contract.OracleConfig // Cached timing config from contract
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

// Run starts the oracle loop, waking at each epoch boundary to check for commit opportunities.
func (o *Oracle) Run(ctx context.Context, syncer *ethsync.EventSyncer, beaconClient *ethsync.BeaconClient) error {
	log.Println("Oracle starting...")

	spec, err := beaconClient.GetSpec(ctx)
	if err != nil {
		return fmt.Errorf("failed to get beacon spec: %w", err)
	}
	log.Printf("Beacon spec: genesis=%s, slotsPerEpoch=%d, slotDuration=%v",
		spec.GenesisTime.Format(time.RFC3339), spec.SlotsPerEpoch, spec.SlotDuration)

	// Fetch and cache timing config from contract
	if err := o.loadTimingConfig(ctx); err != nil {
		return fmt.Errorf("failed to get oracle config: %w", err)
	}

	log.Printf("Oracle config: startEpoch=%d, epochInterval=%d epochs",
		o.timingConfig.StartEpoch, o.timingConfig.EpochInterval)

	// Run initial cycle check (important after initial sync or restart)
	if err := o.cycle(ctx, syncer, beaconClient); err != nil {
		log.Printf("Initial cycle error: %v", err)
	}

	for {
		now := time.Now()
		currentSlot := uint64(now.Sub(spec.GenesisTime) / spec.SlotDuration)
		currentEpoch := currentSlot / spec.SlotsPerEpoch
		nextEpoch := currentEpoch + 1

		nextEpochSlot := nextEpoch * spec.SlotsPerEpoch
		nextEpochTime := spec.GenesisTime.Add(time.Duration(nextEpochSlot) * spec.SlotDuration)
		waitDuration := time.Until(nextEpochTime)

		log.Printf("Waiting for epoch %d (in %v)", nextEpoch, waitDuration.Round(time.Second))

		select {
		case <-ctx.Done():
			log.Println("Oracle stopping...")
			return ctx.Err()
		case <-time.After(waitDuration):
		}

		if err := o.cycle(ctx, syncer, beaconClient); err != nil {
			log.Printf("Cycle error: %v", err)
		}
	}
}

// cycle executes one oracle cycle:
// 1. Syncs events to finalized epoch
// 2. Calculates current round and target epoch using spec formulas
// 3. Checks if target epoch is finalized and not yet committed
// 4. Fetches effective balances for target epoch
// 5. Builds merkle root from cluster balances
// 6. Commits to contract and updates tracking
func (o *Oracle) cycle(ctx context.Context, syncer *ethsync.EventSyncer, beaconClient *ethsync.BeaconClient) error {
	if err := syncer.SyncIncremental(ctx); err != nil {
		return fmt.Errorf("failed to sync events: %w", err)
	}

	config := o.timingConfig

	finalizedEpoch, err := beaconClient.GetFinalizedEpoch(ctx)
	if err != nil {
		return fmt.Errorf("failed to get finalized epoch: %w", err)
	}

	if finalizedEpoch < config.StartEpoch {
		log.Printf("Waiting for start epoch (finalized=%d, start=%d)", finalizedEpoch, config.StartEpoch)
		return nil
	}

	epochsSinceStart := finalizedEpoch - config.StartEpoch
	// Ceiling division: determines which round we're in based on epochs elapsed
	currentRound := (epochsSinceStart + config.EpochInterval - 1) / config.EpochInterval

	targetEpoch := config.StartEpoch + (currentRound * config.EpochInterval)

	if targetEpoch > finalizedEpoch {
		log.Printf("Waiting for target epoch %d to be finalized (round %d, finalized %d)",
			targetEpoch, currentRound, finalizedEpoch)
		return nil
	}

	// Check if already committed (prevents duplicate attempts when finalization is stuck)
	if currentRound <= o.lastCommittedRound {
		log.Printf("Round %d already committed (last=%d), skipping", currentRound, o.lastCommittedRound)
		return nil
	}

	log.Printf("--- Cycle: round %d, target epoch %d (finalized %d) ---",
		currentRound, targetEpoch, finalizedEpoch)

	targetBlock, err := beaconClient.GetLastBlockOfEpoch(ctx, targetEpoch)
	if err != nil {
		return fmt.Errorf("failed to get last block of epoch %d: %w", targetEpoch, err)
	}

	// Sync events up to last block of target epoch for complete cluster membership
	if err := syncer.SyncToBlock(ctx, targetBlock); err != nil {
		return fmt.Errorf("failed to sync to block %d: %w", targetBlock, err)
	}

	spec, err := beaconClient.GetSpec(ctx)
	if err != nil {
		return fmt.Errorf("failed to get beacon spec: %w", err)
	}

	if err := o.fetchAndStoreBalances(ctx, beaconClient, targetEpoch, spec.SlotsPerEpoch); err != nil {
		return fmt.Errorf("failed to fetch balances: %w", err)
	}

	clusterBalances, err := o.storage.GetClusterBalances(ctx, targetEpoch, spec.SlotsPerEpoch)
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

	txHash, err := o.contractClient.CommitRoot(ctx, currentRound, merkleRoot, targetBlock, targetEpoch)
	if err != nil {
		return fmt.Errorf("failed to commit: %w", err)
	}

	receipt, err := o.contractClient.WaitForReceipt(ctx, txHash)
	if err != nil {
		return fmt.Errorf("failed waiting for receipt: %w", err)
	}

	if receipt.Status == 1 {
		log.Printf("✓ Committed (tx: %s)", txHash)
		o.lastCommittedRound = currentRound
	} else {
		return fmt.Errorf("transaction reverted")
	}

	return nil
}

// loadTimingConfig fetches timing config from contract and caches it.
func (o *Oracle) loadTimingConfig(ctx context.Context) error {
	config, err := o.contractClient.GetTimingConfig(ctx)
	if err != nil {
		return err
	}

	if config.EpochInterval == 0 {
		return fmt.Errorf("invalid epoch interval: 0")
	}

	o.timingConfig = config
	return nil
}

// fetchAndStoreBalances fetches effective balances from beacon chain for all active validators
// at the target epoch. Only stores balances that changed since the previous epoch.
func (o *Oracle) fetchAndStoreBalances(ctx context.Context, beaconClient *ethsync.BeaconClient, targetEpoch uint64, slotsPerEpoch uint64) error {
	validators, err := o.storage.GetActiveValidatorsWithClusters(ctx, targetEpoch, slotsPerEpoch)
	if err != nil {
		return fmt.Errorf("failed to get active validators: %w", err)
	}

	if len(validators) == 0 {
		log.Printf("Balances: no active validators")
		return nil
	}

	// Deduplicate validator pubkeys for beacon query
	pubkeySet := make(map[string]struct{})
	var pubkeys [][]byte
	for _, v := range validators {
		pubkeyHex := fmt.Sprintf("0x%x", v.ValidatorPubkey)
		if _, exists := pubkeySet[pubkeyHex]; !exists {
			pubkeySet[pubkeyHex] = struct{}{}
			pubkeys = append(pubkeys, v.ValidatorPubkey)
		}
	}

	balanceMap, err := beaconClient.GetValidatorBalances(ctx, targetEpoch, pubkeys)
	if err != nil {
		return fmt.Errorf("failed to fetch validator balances: %w", err)
	}

	prevBalances, err := o.storage.GetLatestValidatorBalances(ctx, validators, targetEpoch)
	if err != nil {
		return fmt.Errorf("failed to get previous balances: %w", err)
	}

	// Process balances and store only changed values
	stored := 0
	skipped := 0
	notOnBeacon := 0
	insertErrors := 0

	for _, v := range validators {
		pubkeyHex := fmt.Sprintf("0x%x", v.ValidatorPubkey)
		newBalance, onBeacon := balanceMap[pubkeyHex]

		key := fmt.Sprintf("%x:%x", v.ClusterID, v.ValidatorPubkey)
		prevBalance, hasPrev := prevBalances[key]

		if !onBeacon {
			notOnBeacon++
			if !hasPrev {
				// Validator registered to SSV but never deposited to beacon - skip (implicit 0)
				skipped++
				continue
			}
			// Previously had balance but now gone from beacon (exited/withdrawn) - record as 0
			newBalance = 0
		}

		if hasPrev && prevBalance == newBalance {
			skipped++
			continue
		}

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

	log.Printf("Balances: %d/%d from beacon, %d changed, %d not deposited",
		len(balanceMap), len(validators), stored, notOnBeacon)

	if insertErrors > 0 {
		log.Printf("Warning: %d balance insert errors", insertErrors)
	}

	return nil
}
