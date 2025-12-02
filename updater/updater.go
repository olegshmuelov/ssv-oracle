package updater

import (
	"bytes"
	"context"
	"fmt"
	"log"
	"time"

	"ssv-oracle/contract"
	"ssv-oracle/merkle"
	"ssv-oracle/pkg/ethsync"

	"github.com/ethereum/go-ethereum/common"
)

// Updater listens for RootCommitted events and submits merkle proofs to update cluster balances.
type Updater struct {
	storage        *ethsync.PostgresStorage
	contractClient *contract.Client
	timingConfig   *contract.OracleConfig // For calculating targetEpoch in real mode
	slotsPerEpoch  uint64
	mockMode       bool
	dbConnString   string // For LISTEN/NOTIFY in mock mode
}

// Config holds updater configuration.
type Config struct {
	Storage        *ethsync.PostgresStorage
	ContractClient *contract.Client
	TimingConfig   *contract.OracleConfig
	SlotsPerEpoch  uint64
	MockMode       bool
	DBConnString   string // Required for mock mode (LISTEN/NOTIFY)
}

// New creates a new Updater.
func New(cfg *Config) *Updater {
	return &Updater{
		storage:        cfg.Storage,
		contractClient: cfg.ContractClient,
		timingConfig:   cfg.TimingConfig,
		slotsPerEpoch:  cfg.SlotsPerEpoch,
		mockMode:       cfg.MockMode,
		dbConnString:   cfg.DBConnString,
	}
}

// Run starts the updater.
// In mock mode: listens for PostgreSQL NOTIFY on new commits.
// In real mode: subscribes to RootCommitted events.
func (u *Updater) Run(ctx context.Context) error {
	log.Println("Updater starting...")

	if u.mockMode {
		return u.runMockMode(ctx)
	}
	return u.runRealMode(ctx)
}

// runMockMode listens for new commits via PostgreSQL LISTEN/NOTIFY.
func (u *Updater) runMockMode(ctx context.Context) error {
	log.Println("Updater running in mock mode (LISTEN/NOTIFY)")

	// Start listening for new commits
	roundChan, err := u.storage.ListenForCommits(ctx, u.dbConnString)
	if err != nil {
		return fmt.Errorf("failed to start listener: %w", err)
	}

	log.Println("Listening for new oracle commits...")

	for {
		select {
		case <-ctx.Done():
			log.Println("Updater stopping...")
			return ctx.Err()

		case roundID, ok := <-roundChan:
			if !ok {
				log.Println("Listener channel closed, exiting...")
				return fmt.Errorf("listener closed")
			}

			log.Printf("Received notification for round %d", roundID)

			// Get the commit details
			commit, err := u.storage.GetCommitByRound(ctx, roundID)
			if err != nil {
				log.Printf("Error getting commit for round %d: %v", roundID, err)
				continue
			}
			if commit == nil {
				log.Printf("Warning: commit for round %d not found", roundID)
				continue
			}

			if err := u.processCommit(ctx, commit.RoundID, commit.TargetEpoch, commit.MerkleRoot); err != nil {
				log.Printf("Error processing commit round %d: %v", roundID, err)
			}
		}
	}
}

// runRealMode subscribes to RootCommitted events.
func (u *Updater) runRealMode(ctx context.Context) error {
	log.Println("Updater running in real mode (event subscription)")

	for {
		events, errChan, err := u.contractClient.SubscribeRootCommitted(ctx, 0)
		if err != nil {
			log.Printf("Failed to subscribe to events: %v, retrying in 10s...", err)
			time.Sleep(10 * time.Second)
			continue
		}

		log.Println("Subscribed to RootCommitted events")

		// Process events until error or context done
	innerLoop:
		for {
			select {
			case <-ctx.Done():
				log.Println("Updater stopping...")
				return ctx.Err()

			case err := <-errChan:
				log.Printf("Subscription error: %v, reconnecting...", err)
				break innerLoop

			case event, ok := <-events:
				if !ok {
					log.Println("Event channel closed, reconnecting...")
					break innerLoop
				}

				// Calculate targetEpoch from round
				targetEpoch := u.timingConfig.StartEpoch + (event.Round * u.timingConfig.EpochInterval)

				log.Printf("Received RootCommitted: round=%d, targetEpoch=%d, merkleRoot=0x%x, blockNum=%d",
					event.Round, targetEpoch, event.MerkleRoot[:8], event.BlockNum)

				if err := u.processCommit(ctx, event.Round, targetEpoch, event.MerkleRoot[:]); err != nil {
					log.Printf("Error processing commit round %d: %v", event.Round, err)
				}
			}
		}

		// Brief pause before reconnecting
		log.Println("Pausing 5s before reconnecting...")
		time.Sleep(5 * time.Second)
	}
}

// processCommit rebuilds the merkle tree, validates root, and submits proofs.
func (u *Updater) processCommit(ctx context.Context, round, targetEpoch uint64, committedRoot []byte) error {
	log.Printf("Processing round %d (targetEpoch=%d, committedRoot=0x%x)",
		round, targetEpoch, committedRoot[:8])

	// 1. Query cluster balances from DB for targetEpoch
	clusterBalances, err := u.storage.GetClusterBalances(ctx, targetEpoch, u.slotsPerEpoch)
	if err != nil {
		return fmt.Errorf("failed to get cluster balances: %w", err)
	}

	log.Printf("Round %d: found %d clusters with balances", round, len(clusterBalances))

	if len(clusterBalances) == 0 {
		log.Printf("Round %d: no clusters to update", round)
		return nil
	}

	// 2. Build merkle tree with proofs
	clusterMap := make(map[[32]byte]uint64)
	for _, bal := range clusterBalances {
		var clusterID [32]byte
		copy(clusterID[:], bal.ClusterID)
		clusterMap[clusterID] = bal.TotalEffectiveBalance
		log.Printf("  Cluster %x: totalBalance=%d Gwei (%d validators)",
			bal.ClusterID[:8], bal.TotalEffectiveBalance, bal.ValidatorCount)
	}

	tree := merkle.BuildMerkleTreeWithProofs(clusterMap)
	log.Printf("Round %d: built merkle tree with root 0x%x", round, tree.Root[:8])

	// 3. Validate: computed root == committedRoot
	if !bytes.Equal(tree.Root[:], committedRoot) {
		return fmt.Errorf("root mismatch: computed=0x%x, committed=0x%x",
			tree.Root[:8], committedRoot[:8])
	}

	log.Printf("Round %d: root validated ✓, processing %d clusters", round, len(clusterBalances))

	// 4. For each cluster: get state, generate proof, call UpdateClusterBalance
	updated := 0
	skipped := 0
	errors := 0

	for _, leaf := range tree.Leaves {
		// Get cluster state from DB
		clusterState, err := u.storage.GetClusterState(ctx, leaf.ClusterID[:])
		if err != nil {
			log.Printf("Warning: failed to get cluster state for %x: %v", leaf.ClusterID[:8], err)
			errors++
			continue
		}
		if clusterState == nil {
			log.Printf("Warning: cluster %x not found in state", leaf.ClusterID[:8])
			skipped++
			continue
		}

		// Generate merkle proof
		proof, err := tree.GetProof(leaf.ClusterID)
		if err != nil {
			log.Printf("Warning: failed to get proof for cluster %x: %v", leaf.ClusterID[:8], err)
			errors++
			continue
		}

		// Convert to contract types
		owner := common.BytesToAddress(clusterState.OwnerAddress)
		cluster := contract.Cluster{
			ValidatorCount:  clusterState.ValidatorCount,
			NetworkFeeIndex: clusterState.NetworkFeeIndex,
			Index:           clusterState.Index,
			Active:          clusterState.IsActive,
			Balance:         clusterState.Balance,
		}

		// Call UpdateClusterBalance
		txHash, err := u.contractClient.UpdateClusterBalance(
			ctx,
			owner,
			clusterState.OperatorIDs,
			cluster,
			leaf.EffectiveBalance,
			proof,
		)
		if err != nil {
			log.Printf("Warning: failed to update cluster %x: %v", leaf.ClusterID[:8], err)
			errors++
			continue
		}

		if u.mockMode {
			log.Printf("  Cluster %x: effectiveBalance=%d Gwei, proof=%d siblings (mock tx: %s)",
				leaf.ClusterID[:8], leaf.EffectiveBalance, len(proof), txHash.Hex()[:16])
		} else {
			log.Printf("  Cluster %x: effectiveBalance=%d Gwei, tx=%s",
				leaf.ClusterID[:8], leaf.EffectiveBalance, txHash.Hex())
		}

		updated++
	}

	log.Printf("Round %d complete: %d updated, %d skipped, %d errors",
		round, updated, skipped, errors)

	return nil
}
