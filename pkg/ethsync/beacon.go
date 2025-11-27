package ethsync

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"strings"
	"sync"
	"time"

	"golang.org/x/sync/errgroup"
)

// BeaconClient queries the Ethereum beacon chain API.
type BeaconClient struct {
	url        string
	httpClient *http.Client
	maxRetries int
	retryDelay time.Duration
	spec       *Spec // Cached spec (populated on first GetSpec call)
}

// BeaconClientConfig holds configuration for the beacon client.
type BeaconClientConfig struct {
	URL        string
	Timeout    time.Duration
	MaxRetries int
	RetryDelay time.Duration
}

// NewBeaconClient creates a new beacon client.
func NewBeaconClient(cfg BeaconClientConfig) *BeaconClient {
	if cfg.Timeout == 0 {
		cfg.Timeout = 30 * time.Second
	}
	if cfg.MaxRetries == 0 {
		cfg.MaxRetries = 3
	}
	if cfg.RetryDelay == 0 {
		cfg.RetryDelay = 5 * time.Second
	}

	return &BeaconClient{
		url: cfg.URL,
		httpClient: &http.Client{
			Timeout: cfg.Timeout,
		},
		maxRetries: cfg.MaxRetries,
		retryDelay: cfg.RetryDelay,
	}
}

// FinalityCheckpoints represents the beacon chain finality checkpoints.
type FinalityCheckpoints struct {
	Data struct {
		PreviousJustified struct {
			Epoch string `json:"epoch"`
			Root  string `json:"root"`
		} `json:"previous_justified"`
		CurrentJustified struct {
			Epoch string `json:"epoch"`
			Root  string `json:"root"`
		} `json:"current_justified"`
		Finalized struct {
			Epoch string `json:"epoch"`
			Root  string `json:"root"`
		} `json:"finalized"`
	} `json:"data"`
}

// GetSpec fetches beacon chain spec parameters and returns a Spec struct.
// The spec is cached after the first call.
func (c *BeaconClient) GetSpec(ctx context.Context) (*Spec, error) {
	if c.spec != nil {
		return c.spec, nil
	}

	genesisTime, err := c.GetGenesisTime(ctx)
	if err != nil {
		return nil, fmt.Errorf("failed to get genesis time: %w", err)
	}

	url := fmt.Sprintf("%s/eth/v1/config/spec", c.url)

	// Use interface{} because beacon spec contains mixed types (strings and arrays)
	var response struct {
		Data map[string]interface{} `json:"data"`
	}

	if err := c.doRequest(ctx, url, &response); err != nil {
		return nil, fmt.Errorf("failed to get spec: %w", err)
	}

	var slotsPerEpoch uint64
	if val, ok := response.Data["SLOTS_PER_EPOCH"]; ok {
		if strVal, ok := val.(string); ok {
			if _, err := fmt.Sscanf(strVal, "%d", &slotsPerEpoch); err != nil {
				return nil, fmt.Errorf("failed to parse SLOTS_PER_EPOCH: %w", err)
			}
		} else {
			return nil, fmt.Errorf("SLOTS_PER_EPOCH is not a string")
		}
	} else {
		return nil, fmt.Errorf("SLOTS_PER_EPOCH not found in spec")
	}

	var secondsPerSlot uint64
	if val, ok := response.Data["SECONDS_PER_SLOT"]; ok {
		if strVal, ok := val.(string); ok {
			if _, err := fmt.Sscanf(strVal, "%d", &secondsPerSlot); err != nil {
				return nil, fmt.Errorf("failed to parse SECONDS_PER_SLOT: %w", err)
			}
		} else {
			return nil, fmt.Errorf("SECONDS_PER_SLOT is not a string")
		}
	} else {
		return nil, fmt.Errorf("SECONDS_PER_SLOT not found in spec")
	}

	c.spec = &Spec{
		GenesisTime:   genesisTime,
		SlotsPerEpoch: slotsPerEpoch,
		SlotDuration:  time.Duration(secondsPerSlot) * time.Second,
	}

	return c.spec, nil
}

// GetGenesisTime returns the beacon chain genesis time.
func (c *BeaconClient) GetGenesisTime(ctx context.Context) (time.Time, error) {
	url := fmt.Sprintf("%s/eth/v1/beacon/genesis", c.url)

	var response struct {
		Data struct {
			GenesisTime string `json:"genesis_time"`
		} `json:"data"`
	}

	err := c.doRequest(ctx, url, &response)
	if err != nil {
		return time.Time{}, fmt.Errorf("failed to get genesis: %w", err)
	}

	var genesisTimestamp int64
	if _, err := fmt.Sscanf(response.Data.GenesisTime, "%d", &genesisTimestamp); err != nil {
		return time.Time{}, fmt.Errorf("failed to parse genesis time: %w", err)
	}

	return time.Unix(genesisTimestamp, 0).UTC(), nil
}

// GetFinalizedEpoch returns the latest finalized epoch.
// Uses head state to get the most up-to-date finalization info.
func (c *BeaconClient) GetFinalizedEpoch(ctx context.Context) (uint64, error) {
	url := fmt.Sprintf("%s/eth/v1/beacon/states/head/finality_checkpoints", c.url)

	var checkpoints FinalityCheckpoints
	err := c.doRequest(ctx, url, &checkpoints)
	if err != nil {
		return 0, fmt.Errorf("failed to get finality checkpoints: %w", err)
	}

	var epoch uint64
	if _, err := fmt.Sscanf(checkpoints.Data.Finalized.Epoch, "%d", &epoch); err != nil {
		return 0, fmt.Errorf("failed to parse finalized epoch: %w", err)
	}

	return epoch, nil
}

const (
	// validatorBatchSize is the max number of validators per beacon API request.
	validatorBatchSize = 1000
	// maxParallelRequests limits concurrent beacon API requests.
	maxParallelRequests = 5
)

// GetValidatorBalances fetches effective balances for validators at a specific epoch.
// pubkeys is a list of validator public keys (48 bytes each).
// Returns a map of pubkey (hex with 0x prefix) -> effective balance in Gwei.
//
// Requests are batched (1000 per request) with limited parallelism (5 concurrent).
//
// TODO: Currently uses "finalized" state instead of specific epoch because most beacon
// nodes prune historical states. Switch to epoch-specific query when using archive node.
func (c *BeaconClient) GetValidatorBalances(ctx context.Context, epoch uint64, pubkeys [][]byte) (map[string]uint64, error) {
	_ = epoch // TODO: use epoch when we have archive node

	if len(pubkeys) == 0 {
		return make(map[string]uint64), nil
	}

	// Split into batches
	var batches [][][]byte
	for i := 0; i < len(pubkeys); i += validatorBatchSize {
		end := i + validatorBatchSize
		if end > len(pubkeys) {
			end = len(pubkeys)
		}
		batches = append(batches, pubkeys[i:end])
	}

	// Fetch batches with limited parallelism
	g, ctx := errgroup.WithContext(ctx)
	g.SetLimit(maxParallelRequests)

	var mu sync.Mutex
	merged := make(map[string]uint64, len(pubkeys))

	for _, batch := range batches {
		g.Go(func() error {
			balances, err := c.fetchValidatorBatch(ctx, batch)
			if err != nil {
				return err
			}
			mu.Lock()
			for k, v := range balances {
				merged[k] = v
			}
			mu.Unlock()
			return nil
		})
	}

	if err := g.Wait(); err != nil {
		return nil, err
	}

	return merged, nil
}

// fetchValidatorBatch fetches effective balances for a single batch of validators.
func (c *BeaconClient) fetchValidatorBatch(ctx context.Context, pubkeys [][]byte) (map[string]uint64, error) {
	// TODO: Use specific slot when we have archive node:
	// slot := epoch * 32
	// stateID := fmt.Sprintf("%d", slot)
	// For now, use "finalized" since most beacon nodes prune historical states
	url := fmt.Sprintf("%s/eth/v1/beacon/states/finalized/validators", c.url)

	// Use POST request with validator IDs to fetch only the validators we need
	ids := make([]string, len(pubkeys))
	for i, pubkey := range pubkeys {
		ids[i] = fmt.Sprintf("0x%x", pubkey)
	}

	requestBody := struct {
		IDs []string `json:"ids"`
	}{
		IDs: ids,
	}

	var response struct {
		Data []struct {
			Index     string `json:"index"`
			Balance   string `json:"balance"`
			Status    string `json:"status"`
			Validator struct {
				Pubkey                     string `json:"pubkey"`
				EffectiveBalance           string `json:"effective_balance"`
				WithdrawalCredentials      string `json:"withdrawal_credentials"`
				Slashed                    bool   `json:"slashed"`
				ActivationEligibilityEpoch string `json:"activation_eligibility_epoch"`
				ActivationEpoch            string `json:"activation_epoch"`
				ExitEpoch                  string `json:"exit_epoch"`
				WithdrawableEpoch          string `json:"withdrawable_epoch"`
			} `json:"validator"`
		} `json:"data"`
	}

	err := c.doPostRequest(ctx, url, requestBody, &response)
	if err != nil {
		return nil, fmt.Errorf("failed to get validators: %w", err)
	}

	// Extract effective balances
	// Use lowercase pubkey as key for consistent lookups
	result := make(map[string]uint64)
	for _, validator := range response.Data {
		var effectiveBalance uint64
		if _, err := fmt.Sscanf(validator.Validator.EffectiveBalance, "%d", &effectiveBalance); err != nil {
			return nil, fmt.Errorf("failed to parse effective balance for %s: %w", validator.Validator.Pubkey, err)
		}

		// Normalize pubkey to lowercase for consistent map lookups
		pubkeyLower := strings.ToLower(validator.Validator.Pubkey)
		result[pubkeyLower] = effectiveBalance
	}

	return result, nil
}

// GetValidatorBalance fetches effective balance for a single validator.
func (c *BeaconClient) GetValidatorBalance(ctx context.Context, epoch uint64, pubkey []byte) (uint64, error) {
	balances, err := c.GetValidatorBalances(ctx, epoch, [][]byte{pubkey})
	if err != nil {
		return 0, err
	}

	pubkeyHex := fmt.Sprintf("0x%x", pubkey)
	balance, ok := balances[pubkeyHex]
	if !ok {
		return 0, fmt.Errorf("validator %s not found", pubkeyHex)
	}

	return balance, nil
}

// ErrSlotMissed indicates a slot had no block (missed slot).
var ErrSlotMissed = fmt.Errorf("slot missed")

// GetLastBlockOfEpoch returns the execution block number of the last block in the given epoch.
// It tries the last slot of the epoch first, then walks backwards if slots are missed.
func (c *BeaconClient) GetLastBlockOfEpoch(ctx context.Context, epoch uint64) (uint64, error) {
	spec, err := c.GetSpec(ctx)
	if err != nil {
		return 0, fmt.Errorf("failed to get spec: %w", err)
	}

	startSlot := epoch * spec.SlotsPerEpoch
	endSlot := startSlot + spec.SlotsPerEpoch - 1

	for slot := endSlot; slot >= startSlot; slot-- {
		blockNum, err := c.getExecutionBlockAtSlot(ctx, slot)
		if err == ErrSlotMissed {
			continue
		}
		if err != nil {
			return 0, fmt.Errorf("failed to get block at slot %d: %w", slot, err)
		}
		return blockNum, nil
	}

	return 0, fmt.Errorf("no blocks found in epoch %d (all %d slots missed)", epoch, spec.SlotsPerEpoch)
}

// getExecutionBlockAtSlot returns the execution block number for a given beacon slot.
// Returns ErrSlotMissed if the slot has no block.
func (c *BeaconClient) getExecutionBlockAtSlot(ctx context.Context, slot uint64) (uint64, error) {
	url := fmt.Sprintf("%s/eth/v2/beacon/blocks/%d", c.url, slot)

	req, err := http.NewRequestWithContext(ctx, "GET", url, nil)
	if err != nil {
		return 0, fmt.Errorf("failed to create request: %w", err)
	}
	req.Header.Set("Accept", "application/json")

	resp, err := c.httpClient.Do(req)
	if err != nil {
		return 0, err
	}
	defer resp.Body.Close()

	// 404 means slot was missed (no block)
	if resp.StatusCode == http.StatusNotFound {
		return 0, ErrSlotMissed
	}

	if resp.StatusCode != http.StatusOK {
		body, _ := io.ReadAll(resp.Body)
		return 0, fmt.Errorf("unexpected status %d: %s", resp.StatusCode, string(body))
	}

	// Parse response to get execution block number
	var response struct {
		Data struct {
			Message struct {
				Body struct {
					ExecutionPayload struct {
						BlockNumber string `json:"block_number"`
					} `json:"execution_payload"`
				} `json:"body"`
			} `json:"message"`
		} `json:"data"`
	}

	if err := json.NewDecoder(resp.Body).Decode(&response); err != nil {
		return 0, fmt.Errorf("failed to decode response: %w", err)
	}

	var blockNum uint64
	if _, err := fmt.Sscanf(response.Data.Message.Body.ExecutionPayload.BlockNumber, "%d", &blockNum); err != nil {
		return 0, fmt.Errorf("failed to parse block number: %w", err)
	}

	return blockNum, nil
}

// doRequest performs an HTTP GET request with retries.
func (c *BeaconClient) doRequest(ctx context.Context, url string, result interface{}) error {
	var lastErr error

	for attempt := 0; attempt < c.maxRetries; attempt++ {
		req, err := http.NewRequestWithContext(ctx, "GET", url, nil)
		if err != nil {
			return fmt.Errorf("failed to create request: %w", err)
		}

		req.Header.Set("Accept", "application/json")

		resp, err := c.httpClient.Do(req)
		if err != nil {
			lastErr = err
			if attempt < c.maxRetries-1 {
				time.Sleep(c.retryDelay)
				continue
			}
			break
		}

		defer resp.Body.Close()

		if resp.StatusCode != http.StatusOK {
			body, _ := io.ReadAll(resp.Body)
			lastErr = fmt.Errorf("unexpected status %d: %s", resp.StatusCode, string(body))
			if attempt < c.maxRetries-1 {
				time.Sleep(c.retryDelay)
				continue
			}
			break
		}

		if err := json.NewDecoder(resp.Body).Decode(result); err != nil {
			return fmt.Errorf("failed to decode response: %w", err)
		}

		return nil
	}

	return fmt.Errorf("request failed after %d attempts: %w", c.maxRetries, lastErr)
}

// doPostRequest performs an HTTP POST request with JSON body and retries.
func (c *BeaconClient) doPostRequest(ctx context.Context, url string, body interface{}, result interface{}) error {
	var lastErr error

	for attempt := 0; attempt < c.maxRetries; attempt++ {
		bodyBytes, err := json.Marshal(body)
		if err != nil {
			return fmt.Errorf("failed to marshal request body: %w", err)
		}

		req, err := http.NewRequestWithContext(ctx, "POST", url, bytes.NewReader(bodyBytes))
		if err != nil {
			return fmt.Errorf("failed to create request: %w", err)
		}

		req.Header.Set("Accept", "application/json")
		req.Header.Set("Content-Type", "application/json")

		resp, err := c.httpClient.Do(req)
		if err != nil {
			lastErr = err
			if attempt < c.maxRetries-1 {
				time.Sleep(c.retryDelay)
				continue
			}
			break
		}

		defer resp.Body.Close()

		if resp.StatusCode != http.StatusOK {
			respBody, _ := io.ReadAll(resp.Body)
			lastErr = fmt.Errorf("unexpected status %d: %s", resp.StatusCode, string(respBody))
			if attempt < c.maxRetries-1 {
				time.Sleep(c.retryDelay)
				continue
			}
			break
		}

		if err := json.NewDecoder(resp.Body).Decode(result); err != nil {
			return fmt.Errorf("failed to decode response: %w", err)
		}

		return nil
	}

	return fmt.Errorf("POST request failed after %d attempts: %w", c.maxRetries, lastErr)
}
