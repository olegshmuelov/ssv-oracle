package contract

import (
	"context"
	_ "embed"
	"fmt"
	"math/big"
	"strings"

	"github.com/ethereum/go-ethereum"
	"github.com/ethereum/go-ethereum/accounts/abi"
	"github.com/ethereum/go-ethereum/accounts/abi/bind"
	"github.com/ethereum/go-ethereum/common"
	"github.com/ethereum/go-ethereum/core/types"
	"github.com/ethereum/go-ethereum/crypto"
	"github.com/ethereum/go-ethereum/ethclient"
)

//go:embed Oracle.abi
var oracleABI string

// OracleConfig represents the timing configuration from the oracle contract.
type OracleConfig struct {
	StartEpoch    uint64
	EpochInterval uint64
}

// Cluster represents the SSV Cluster struct as used in the contract.
type Cluster struct {
	ValidatorCount  uint32
	NetworkFeeIndex uint64
	Index           uint64
	Active          bool
	Balance         *big.Int
}

// Client is an Ethereum client for interacting with the Oracle contract.
type Client struct {
	ethClient       *ethclient.Client
	contractAddress common.Address
	contractABI     abi.ABI
	privateKey      []byte
	chainID         *big.Int
	mockMode        bool // PoC: mock mode until contract is ready
	mockConfig      *OracleConfig
	storage         MockStorage // For storing mock state
}

// MockStorage interface for mock mode persistence.
type MockStorage interface {
	GetMockLatestCommittedRound(ctx context.Context) (uint64, error)
	SetMockLatestCommittedRound(ctx context.Context, round uint64) error
	InsertOracleCommit(ctx context.Context, roundID, targetEpoch uint64, merkleRoot []byte, referenceBlock uint64, txHash []byte) error
}

// NewClient creates a new Ethereum client.
// Chain ID is auto-detected from the RPC endpoint.
func NewClient(rpcURL string, contractAddress string, privateKeyHex string) (*Client, error) {
	// Connect to Ethereum node
	ethClient, err := ethclient.Dial(rpcURL)
	if err != nil {
		return nil, fmt.Errorf("failed to connect to Ethereum node: %w", err)
	}

	// Auto-detect chain ID
	chainID, err := ethClient.ChainID(context.Background())
	if err != nil {
		return nil, fmt.Errorf("failed to get chain ID: %w", err)
	}

	// Parse contract address
	contractAddr := common.HexToAddress(contractAddress)

	// Parse ABI
	contractABI, err := abi.JSON(strings.NewReader(oracleABI))
	if err != nil {
		return nil, fmt.Errorf("failed to parse contract ABI: %w", err)
	}

	// Parse private key
	privateKey, err := crypto.HexToECDSA(privateKeyHex)
	if err != nil {
		return nil, fmt.Errorf("failed to parse private key: %w", err)
	}

	return &Client{
		ethClient:       ethClient,
		contractAddress: contractAddr,
		contractABI:     contractABI,
		privateKey:      crypto.FromECDSA(privateKey),
		chainID:         chainID,
		mockMode:        false,
	}, nil
}

// NewMockClient creates a mock Ethereum client for PoC testing (no real contract needed).
func NewMockClient(startEpoch, epochInterval uint64, storage MockStorage) *Client {
	return &Client{
		mockMode: true,
		mockConfig: &OracleConfig{
			StartEpoch:    startEpoch,
			EpochInterval: epochInterval,
		},
		storage: storage,
	}
}

// GetTimingConfig reads the timing configuration from the oracle contract.
func (c *Client) GetTimingConfig(ctx context.Context) (*OracleConfig, error) {
	// Mock mode: return static config
	if c.mockMode {
		return c.mockConfig, nil
	}

	// Real mode: call contract
	data, err := c.contractABI.Pack("getOracleTimingConfig")
	if err != nil {
		return nil, fmt.Errorf("failed to pack function call: %w", err)
	}

	msg := ethereum.CallMsg{
		To:   &c.contractAddress,
		Data: data,
	}

	result, err := c.ethClient.CallContract(ctx, msg, nil)
	if err != nil {
		return nil, fmt.Errorf("failed to call contract: %w", err)
	}

	var out struct {
		StartEpoch    uint64
		EpochInterval uint64
	}

	err = c.contractABI.UnpackIntoInterface(&out, "getOracleTimingConfig", result)
	if err != nil {
		return nil, fmt.Errorf("failed to unpack result: %w", err)
	}

	return &OracleConfig{
		StartEpoch:    out.StartEpoch,
		EpochInterval: out.EpochInterval,
	}, nil
}

// GetLatestCommittedRound returns the latest round that has been committed to the oracle contract.
// Returns 0 if no rounds have been committed yet.
func (c *Client) GetLatestCommittedRound(ctx context.Context) (uint64, error) {
	// Mock mode: read from storage
	if c.mockMode {
		return c.storage.GetMockLatestCommittedRound(ctx)
	}

	// Real mode: call contract
	data, err := c.contractABI.Pack("latestCommittedRound")
	if err != nil {
		return 0, fmt.Errorf("failed to pack function call: %w", err)
	}

	msg := ethereum.CallMsg{
		To:   &c.contractAddress,
		Data: data,
	}

	result, err := c.ethClient.CallContract(ctx, msg, nil)
	if err != nil {
		return 0, fmt.Errorf("failed to call contract: %w", err)
	}

	var round uint64
	err = c.contractABI.UnpackIntoInterface(&round, "latestCommittedRound", result)
	if err != nil {
		return 0, fmt.Errorf("failed to unpack result: %w", err)
	}

	return round, nil
}

// CommitRoot submits a Merkle root commitment to the oracle contract.
func (c *Client) CommitRoot(ctx context.Context, roundID uint64, merkleRoot [32]byte, blockNum uint64, targetEpoch uint64) (common.Hash, error) {
	// Mock mode: update storage without sending real transaction
	if c.mockMode {
		// Generate fake transaction hash
		txHash := common.BytesToHash([]byte(fmt.Sprintf("mock-tx-%d", roundID)))

		// Record the commit
		if err := c.storage.InsertOracleCommit(ctx, roundID, targetEpoch, merkleRoot[:], blockNum, txHash.Bytes()); err != nil {
			return common.Hash{}, fmt.Errorf("failed to insert oracle commit: %w", err)
		}

		// Update mock storage
		if err := c.storage.SetMockLatestCommittedRound(ctx, roundID); err != nil {
			return common.Hash{}, fmt.Errorf("failed to update mock committed round: %w", err)
		}

		return txHash, nil
	}

	// Real mode: send actual transaction
	privateKey, err := crypto.ToECDSA(c.privateKey)
	if err != nil {
		return common.Hash{}, fmt.Errorf("failed to convert private key: %w", err)
	}

	from := crypto.PubkeyToAddress(privateKey.PublicKey)
	nonce, err := c.ethClient.PendingNonceAt(ctx, from)
	if err != nil {
		return common.Hash{}, fmt.Errorf("failed to get nonce: %w", err)
	}

	// Get gas price
	gasPrice, err := c.ethClient.SuggestGasPrice(ctx)
	if err != nil {
		return common.Hash{}, fmt.Errorf("failed to get gas price: %w", err)
	}

	// Encode function call
	data, err := c.contractABI.Pack("commitRoot", roundID, merkleRoot, blockNum)
	if err != nil {
		return common.Hash{}, fmt.Errorf("failed to pack function call: %w", err)
	}

	// Estimate gas
	gasLimit, err := c.ethClient.EstimateGas(ctx, ethereum.CallMsg{
		From: from,
		To:   &c.contractAddress,
		Data: data,
	})
	if err != nil {
		// Use default gas limit if estimation fails
		gasLimit = 200000
	}

	// Create transaction
	tx := types.NewTransaction(
		nonce,
		c.contractAddress,
		big.NewInt(0), // value
		gasLimit,
		gasPrice,
		data,
	)

	// Sign transaction
	signedTx, err := types.SignTx(tx, types.NewEIP155Signer(c.chainID), privateKey)
	if err != nil {
		return common.Hash{}, fmt.Errorf("failed to sign transaction: %w", err)
	}

	// Send transaction
	err = c.ethClient.SendTransaction(ctx, signedTx)
	if err != nil {
		return common.Hash{}, fmt.Errorf("failed to send transaction: %w", err)
	}

	return signedTx.Hash(), nil
}

// WaitForReceipt waits for a transaction receipt and returns the status.
func (c *Client) WaitForReceipt(ctx context.Context, txHash common.Hash) (*types.Receipt, error) {
	// Mock mode: return fake successful receipt
	if c.mockMode {
		return &types.Receipt{
			Status:      1,                // Success
			BlockNumber: big.NewInt(1000), // Fake block number
		}, nil
	}

	// Real mode: wait for actual receipt
	receipt, err := bind.WaitMined(ctx, c.ethClient, &types.Transaction{})
	if err != nil {
		return nil, fmt.Errorf("failed to wait for transaction: %w", err)
	}

	// Get the actual receipt
	receipt, err = c.ethClient.TransactionReceipt(ctx, txHash)
	if err != nil {
		return nil, fmt.Errorf("failed to get receipt: %w", err)
	}

	return receipt, nil
}

// UpdateClusterBalance calls the contract to update a cluster's effective balance.
// In mock mode, logs the call instead of sending a transaction.
func (c *Client) UpdateClusterBalance(
	ctx context.Context,
	owner common.Address,
	operatorIds []uint64,
	cluster Cluster,
	effectiveBalance uint64,
	proof [][32]byte,
) (common.Hash, error) {
	// Mock mode: log the call instead of sending real transaction
	if c.mockMode {
		// Hash the owner to get a unique mock tx hash per cluster
		txHash := crypto.Keccak256Hash([]byte("mock-update"), owner.Bytes())
		return txHash, nil
	}

	// Real mode: send actual transaction
	privateKey, err := crypto.ToECDSA(c.privateKey)
	if err != nil {
		return common.Hash{}, fmt.Errorf("failed to convert private key: %w", err)
	}

	from := crypto.PubkeyToAddress(privateKey.PublicKey)
	nonce, err := c.ethClient.PendingNonceAt(ctx, from)
	if err != nil {
		return common.Hash{}, fmt.Errorf("failed to get nonce: %w", err)
	}

	// Get EIP-1559 gas parameters
	gasTipCap, err := c.ethClient.SuggestGasTipCap(ctx)
	if err != nil {
		return common.Hash{}, fmt.Errorf("failed to get gas tip cap: %w", err)
	}

	header, err := c.ethClient.HeaderByNumber(ctx, nil)
	if err != nil {
		return common.Hash{}, fmt.Errorf("failed to get latest header: %w", err)
	}

	// GasFeeCap = 2 * baseFee + gasTipCap (standard formula)
	gasFeeCap := new(big.Int).Add(
		new(big.Int).Mul(header.BaseFee, big.NewInt(2)),
		gasTipCap,
	)

	// Encode function call
	data, err := c.contractABI.Pack("updateClusterBalance", owner, operatorIds, cluster, effectiveBalance, proof)
	if err != nil {
		return common.Hash{}, fmt.Errorf("failed to pack function call: %w", err)
	}

	// Estimate gas
	gasLimit, err := c.ethClient.EstimateGas(ctx, ethereum.CallMsg{
		From: from,
		To:   &c.contractAddress,
		Data: data,
	})
	if err != nil {
		// Use default gas limit if estimation fails
		gasLimit = 300000
	}

	// Create EIP-1559 transaction
	tx := types.NewTx(&types.DynamicFeeTx{
		ChainID:   c.chainID,
		Nonce:     nonce,
		GasTipCap: gasTipCap,
		GasFeeCap: gasFeeCap,
		Gas:       gasLimit,
		To:        &c.contractAddress,
		Value:     big.NewInt(0),
		Data:      data,
	})

	// Sign transaction
	signedTx, err := types.SignTx(tx, types.LatestSignerForChainID(c.chainID), privateKey)
	if err != nil {
		return common.Hash{}, fmt.Errorf("failed to sign transaction: %w", err)
	}

	// Send transaction
	err = c.ethClient.SendTransaction(ctx, signedTx)
	if err != nil {
		return common.Hash{}, fmt.Errorf("failed to send transaction: %w", err)
	}

	return signedTx.Hash(), nil
}

// Close closes the Ethereum client connection.
func (c *Client) Close() {
	if c.ethClient != nil {
		c.ethClient.Close()
	}
}
