# SSV Oracle Client

Oracle client that publishes Merkle roots of SSV cluster effective balances to an onchain oracle contract.

## Quick Start

### Prerequisites

- Go 1.24+
- Docker (for PostgreSQL)
- Ethereum node (execution layer + beacon node)

### Setup

```bash
cp .env.example .env
cp config.yaml.example config.yaml
# Edit config.yaml with your RPC endpoints
source .env
make fresh
```

This will build the binary, start PostgreSQL, and sync from genesis.

## Commands

```bash
make build       # Build binary
make test        # Run tests
make fresh       # Fresh start (clear DB)
make start       # Resume from last state
make db-shell    # PostgreSQL shell
make db-logs     # Database logs
make docker-down # Stop everything
```

## Configuration

```yaml
eth_rpc: "http://localhost:8545"        # Execution layer RPC
beacon_rpc: "http://localhost:5052"     # Beacon node RPC
ssv_contract: "0x..."                   # SSV contract address
mock_mode: true                         # PoC - no real contract calls
```

Chain ID is auto-detected from RPC.

## Project Structure

```
ssv-oracle/
├── cmd/oracle/      # CLI (cobra)
├── contract/        # Ethereum client & oracle contract
├── merkle/          # Merkle tree (Bitcoin/OpenZeppelin standard)
├── oracle/          # Oracle commit loop
└── pkg/ethsync/     # Event syncing & storage (PostgreSQL)
```

## Oracle Cycle

1. Sync SSV events incrementally
2. Get timing config (startEpoch, epochInterval)
3. Calculate target epoch and round ID
4. Check finalization via beacon API
5. Fetch effective balances
6. Build Merkle tree
7. Commit root to contract

## Merkle Tree

- Leaf: `keccak256(abi.encode(clusterId, effectiveBalance))`
- Sort leaves by clusterId (bytes comparison)
- Duplicate last node if odd count (Bitcoin standard)
- Sort siblings before hashing (OpenZeppelin standard)
- Empty tree: `keccak256("")`

## Cluster ID

```
keccak256(abi.encodePacked(owner, uint256(op1), uint256(op2), ...))
```

Operator IDs are sorted ascending before hashing.

## Check Database

```bash
make db-shell

# Then run:
SELECT * FROM sync_progress;
SELECT COUNT(*) FROM contract_events;
SELECT COUNT(*) FROM cluster_state WHERE is_active = true;
\q
```

## Troubleshooting

**"failed to connect to Ethereum node"**
- Check your execution layer is running and RPC is accessible

**"failed to get finalized epoch"**
- Check beacon node is synced: `curl <beacon_rpc>/eth/v1/node/syncing`
- Wait for `"is_syncing": false`

**Initial sync is slow**
- Normal on first run (depends on network history)
- Subsequent runs are fast (incremental)

## Documentation

- [CLAUDE.md](./CLAUDE.md) - Architecture details
