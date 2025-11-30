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

// Run starts the oracle loop, processing rounds continuously.
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

	log.Printf("Oracle config: startEpoch=%d, epochInterval=%d",
		o.timingConfig.StartEpoch, o.timingConfig.EpochInterval)

	// On startup, calculate which rounds are already fully finalized and skip them.
	// This avoids duplicate commits if oracle restarts after committing.
	// A round N (target = startEpoch + N*interval) is fully finalized when checkpoint.Epoch > target.
	checkpoint, err := beaconClient.GetFinalizedCheckpoint(ctx)
	if err != nil {
		return fmt.Errorf("failed to get finalized checkpoint: %w", err)
	}

	if checkpoint.Epoch > o.timingConfig.StartEpoch {
		// Calculate highest round that is fully finalized (checkpoint.Epoch > targetEpoch)
		// Round N target = startEpoch + N * interval
		// Fully finalized when: checkpoint.Epoch > startEpoch + N * interval
		// i.e., N < (checkpoint.Epoch - startEpoch) / interval
		// So max finalized round = floor((checkpoint.Epoch - startEpoch - 1) / interval)
		maxFinalizedRound := (checkpoint.Epoch - o.timingConfig.StartEpoch - 1) / o.timingConfig.EpochInterval
		o.lastCommittedRound = maxFinalizedRound
		nextRound := maxFinalizedRound + 1
		nextTargetEpoch := o.timingConfig.StartEpoch + (nextRound * o.timingConfig.EpochInterval)
		if maxFinalizedRound > 0 {
			log.Printf("Startup: assuming rounds 1-%d already committed, next is round %d (target epoch %d)",
				maxFinalizedRound, nextRound, nextTargetEpoch)
		} else {
			log.Printf("Startup: no rounds finalized yet, will start from round %d (target epoch %d)",
				nextRound, nextTargetEpoch)
		}
	}

	// Main loop: process rounds continuously
	for {
		if err := o.processRound(ctx, syncer, beaconClient, spec); err != nil {
			if ctx.Err() != nil {
				log.Println("Oracle stopping...")
				return ctx.Err()
			}
			log.Printf("Round error: %v", err)
			// Brief pause before retrying on error
			time.Sleep(10 * time.Second)
		}
	}
}

// processRound handles one oracle round:
// 1. Waits for target epoch to be finalized (polling every slot)
// 2. Syncs events to finalized block
// 3. Fetches effective balances for target epoch
// 4. Builds merkle root from cluster balances
// 5. Commits to contract and updates tracking
func (o *Oracle) processRound(ctx context.Context, syncer *ethsync.EventSyncer, beaconClient *ethsync.BeaconClient, spec *ethsync.Spec) error {
	config := o.timingConfig

	nextRound := o.lastCommittedRound + 1
	targetEpoch := config.StartEpoch + (nextRound * config.EpochInterval)

	log.Printf("--- Round %d: target epoch %d ---", nextRound, targetEpoch)

	// Step 1: Wait for target epoch to be finalized
	checkpoint, err := o.waitForFinalization(ctx, beaconClient, spec, targetEpoch)
	if err != nil {
		return err
	}

	currentSlot := uint64(time.Now().Sub(spec.GenesisTime) / spec.SlotDuration)
	currentEpoch := currentSlot / spec.SlotsPerEpoch

	log.Printf("Epoch %d finalized (current=%d, checkpoint=%d, block=%d)",
		targetEpoch, currentEpoch, checkpoint.Epoch, checkpoint.BlockNum)

	// Step 2: Sync events to finalized block
	if err := syncer.SyncToBlock(ctx, checkpoint.BlockNum); err != nil {
		return fmt.Errorf("failed to sync to block %d: %w", checkpoint.BlockNum, err)
	}

	// Step 3: Fetch and store validator balances
	// Query at first slot of checkpoint.Epoch (the epoch boundary after targetEpoch)
	balanceSlot := checkpoint.Epoch * spec.SlotsPerEpoch
	if err := o.fetchAndStoreBalances(ctx, beaconClient, targetEpoch, balanceSlot); err != nil {
		return fmt.Errorf("failed to fetch balances: %w", err)
	}

	// Step 4: Build merkle tree
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

	// Step 5: Commit to contract
	txHash, err := o.contractClient.CommitRoot(ctx, nextRound, merkleRoot, checkpoint.BlockNum, targetEpoch)
	if err != nil {
		return fmt.Errorf("failed to commit: %w", err)
	}

	receipt, err := o.contractClient.WaitForReceipt(ctx, txHash)
	if err != nil {
		return fmt.Errorf("failed waiting for receipt: %w", err)
	}

	if receipt.Status == 1 {
		log.Printf("Committed round %d (tx: %s)", nextRound, txHash)
		o.lastCommittedRound = nextRound
	} else {
		return fmt.Errorf("transaction reverted")
	}

	return nil
}

// waitForFinalization waits until targetEpoch is fully finalized.
// Polls at slot boundaries, with coarse waiting when target is far ahead.
func (o *Oracle) waitForFinalization(ctx context.Context, beaconClient *ethsync.BeaconClient, spec *ethsync.Spec, targetEpoch uint64) (*ethsync.FinalizedCheckpoint, error) {
	var lastLoggedCheckpoint uint64
	var lastLoggedSlot uint64

	for {
		now := time.Now()
		currentSlot := uint64(now.Sub(spec.GenesisTime) / spec.SlotDuration)
		currentEpoch := currentSlot / spec.SlotsPerEpoch
		slotInEpoch := (currentSlot % spec.SlotsPerEpoch) + 1

		checkpoint, err := beaconClient.GetFinalizedCheckpoint(ctx)
		if err != nil {
			log.Printf("Warning: failed to get checkpoint: %v, retrying...", err)
			time.Sleep(spec.SlotDuration)
			continue
		}

		// Finalized when checkpoint.Epoch > targetEpoch
		if targetEpoch < checkpoint.Epoch {
			log.Printf("Slot %d (epoch %d, %d/%d) - finalization detected!",
				currentSlot, currentEpoch, slotInEpoch, spec.SlotsPerEpoch)
			return checkpoint, nil
		}

		// Wait based on distance to target
		epochsAhead := int64(targetEpoch) - int64(checkpoint.Epoch)
		if epochsAhead > 1 {
			// Far from target: coarse wait
			if checkpoint.Epoch != lastLoggedCheckpoint {
				log.Printf("Slot %d (epoch %d, %d/%d) - waiting for epoch %d (checkpoint: %d, need > %d)",
					currentSlot, currentEpoch, slotInEpoch, spec.SlotsPerEpoch,
					targetEpoch, checkpoint.Epoch, targetEpoch)
				lastLoggedCheckpoint = checkpoint.Epoch
				lastLoggedSlot = currentSlot
			}
			waitEpochs := epochsAhead - 1
			waitTime := time.Duration(uint64(waitEpochs)*spec.SlotsPerEpoch) * spec.SlotDuration
			log.Printf("Target is %d epochs ahead, waiting %v", epochsAhead, waitTime.Round(time.Second))

			select {
			case <-ctx.Done():
				return nil, ctx.Err()
			case <-time.After(waitTime):
			}
		} else {
			// Close to target: poll at slot boundaries
			if checkpoint.Epoch != lastLoggedCheckpoint {
				log.Printf("Slot %d (epoch %d, %d/%d) - waiting for epoch %d (checkpoint: %d, need > %d)",
					currentSlot, currentEpoch, slotInEpoch, spec.SlotsPerEpoch,
					targetEpoch, checkpoint.Epoch, targetEpoch)
				lastLoggedCheckpoint = checkpoint.Epoch
				lastLoggedSlot = currentSlot
			} else if currentSlot != lastLoggedSlot {
				log.Printf("Slot %d (epoch %d, %d/%d)", currentSlot, currentEpoch, slotInEpoch, spec.SlotsPerEpoch)
				lastLoggedSlot = currentSlot
			}

			nextSlotTime := spec.GenesisTime.Add(time.Duration(currentSlot+1) * spec.SlotDuration)
			select {
			case <-ctx.Done():
				return nil, ctx.Err()
			case <-time.After(time.Until(nextSlotTime)):
			}
		}
	}
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
// balanceSlot is the beacon slot to query for effective balances.
func (o *Oracle) fetchAndStoreBalances(ctx context.Context, beaconClient *ethsync.BeaconClient, targetEpoch uint64, balanceSlot uint64) error {
	spec, err := beaconClient.GetSpec(ctx)
	if err != nil {
		return fmt.Errorf("failed to get beacon spec: %w", err)
	}

	validators, err := o.storage.GetActiveValidatorsWithClusters(ctx, targetEpoch, spec.SlotsPerEpoch)
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

	balanceMap, err := beaconClient.GetValidatorBalances(ctx, balanceSlot, pubkeys)
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
