import csv
import datetime
import logging
import os
from typing import Optional

from requests.adapters import HTTPAdapter
from tqdm import tqdm
from web3 import Web3

from eth_abi import abi
from eth_defi.abi import get_contract
from eth_defi.event_reader.conversion import (
    convert_uint256_bytes_to_address,
    convert_int256_bytes_to_int,
    convert_uint256_string_to_address,
    decode_data,
)
from eth_defi.event_reader.logresult import LogContext
from eth_defi.event_reader.reader import LogResult, read_events_concurrent
from eth_defi.event_reader.web3factory import TunedWeb3Factory
from eth_defi.event_reader.web3worker import create_thread_pool_executor
from eth_defi.token import TokenDetails, fetch_erc20_details

#: List of output columns to swaps.csv

PoolBalanceChanged_FIELD_NAMES = [
    "event_type",
    "block_number",
    "timestamp",
    "tx_hash",
    "log_index",
    "pair_contract_address",
    "poolId",
    "tokens",
    "deltas",
    "protocolFeeAmounts"
]

Swap_FIELD_NAMES = [
    "event_type",
    "block_number",
    "timestamp",
    "tx_hash",
    "log_index",
    "pair_contract_address",
    "poolId",
    "tokenIn",
    "tokenOut",
    "amountIn",
    "amountOut"
]

PoolBalanceManaged_FIELD_NAMES = [
    "event_type",
    "block_number",
    "timestamp",
    "tx_hash",
    "log_index",
    "pair_contract_address",
    "poolId",
    "token",
    "cashDelta",
    "managedDelta",
]

class TokenCache(LogContext):
    """Manage cache of token data when doing PairCreated look-up.

    Do not do extra requests for already known tokens.
    """

    def __init__(self):
        self.cache = {}

    def get_token_info(self, web3: Web3, address: str) -> TokenDetails:
        if address not in self.cache:
            self.cache[address] = fetch_erc20_details(web3, address, raise_on_error=False)
        return self.cache[address]

def save_state(state_fname, last_block):
    """Saves the last block we have read."""
    with open(state_fname, "wt") as f:
        print(f"{last_block}", file=f)

def restore_state(state_fname, default_block: int) -> int:
    """Restore the last block we have processes."""
    if os.path.exists(state_fname):
        with open(state_fname, "rt") as f:
            last_block_text = f.read()
            return int(last_block_text)

    return default_block

def decode_PoolBalanceChanged(log: LogResult) -> dict:
    """
        event PoolBalanceChanged(
        bytes32 indexed poolId,
        address indexed liquidityProvider,
        IERC20[] tokens,
        int256[] deltas,
        uint256[] protocolFeeAmounts
    """

    # Raw example event
    # {'address': '0xb4e16d0168e52d35cacd2c6185b44281ec28c9dc', 'blockHash': '0x4ba33a650f9e3d8430f94b61a382e60490ec7a06c2f4441ecf225858ec748b78', 'blockNumber': '0x98b7f6', 'data': '0x00000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000046ec814a2e900000000000000000000000000000000000000000000000000000000000003e80000000000000000000000000000000000000000000000000000000000000000', 'logIndex': '0x4', 'removed': False, 'topics': ['0xd78ad95fa46c994b6551d0da85fc275fe613ce37657fb8d5e3d130840159d822', '0x000000000000000000000000f164fc0ec4e93095b804a4795bbe1e041497b92a', '0x0000000000000000000000008688a84fcfd84d8f78020d0fc0b35987cc58911f'], 'transactionHash': '0x932cb88306450d481a0e43365a3ed832625b68f036e9887684ef6da594891366', 'transactionIndex': '0x1', 'context': <__main__.TokenCache object at 0x104ab7e20>, 'event': <class 'web3._utils.datatypes.Swap'>, 'timestamp': 1588712972}

    block_time = datetime.datetime.utcfromtimestamp(log["timestamp"])

    pair_contract_address = log["address"]

    event_signature, poolId, liquidityProvider = log["topics"]
    data_entries = abi.decode(['address[]', 'int256[]', 'uint256[]'], bytes.fromhex(log["data"][2:]))

    data = {
        "event_type": "PoolBalanceChanged",
        "block_number": int(log["blockNumber"], 16),
        "timestamp": block_time.isoformat(),
        "tx_hash": log["transactionHash"],
        "log_index": int(log["logIndex"], 16),
        "pair_contract_address": pair_contract_address,
        "poolId": poolId,
        "tokens": data_entries[0],
        "deltas": data_entries[1],
        "protocolFeeAmounts": data_entries[2],
    }
    return data


def decode_Swap(log: LogResult) -> dict:
    """
        event Swap(
        bytes32 indexed poolId,
        IERC20 indexed tokenIn,
        IERC20 indexed tokenOut,
        uint256 amountIn,
        uint256 amountOut
    """

    # Raw example event
    # {'address': '0xb4e16d0168e52d35cacd2c6185b44281ec28c9dc', 'blockHash': '0x4ba33a650f9e3d8430f94b61a382e60490ec7a06c2f4441ecf225858ec748b78', 'blockNumber': '0x98b7f6', 'data': '0x00000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000046ec814a2e900000000000000000000000000000000000000000000000000000000000003e80000000000000000000000000000000000000000000000000000000000000000', 'logIndex': '0x4', 'removed': False, 'topics': ['0xd78ad95fa46c994b6551d0da85fc275fe613ce37657fb8d5e3d130840159d822', '0x000000000000000000000000f164fc0ec4e93095b804a4795bbe1e041497b92a', '0x0000000000000000000000008688a84fcfd84d8f78020d0fc0b35987cc58911f'], 'transactionHash': '0x932cb88306450d481a0e43365a3ed832625b68f036e9887684ef6da594891366', 'transactionIndex': '0x1', 'context': <__main__.TokenCache object at 0x104ab7e20>, 'event': <class 'web3._utils.datatypes.Swap'>, 'timestamp': 1588712972}

    block_time = datetime.datetime.utcfromtimestamp(log["timestamp"])

    pair_contract_address = log["address"]

    # Chop data blob to byte32 entries
    event_signature, poolId, tokenIn, tokenOut = log["topics"]
    data_entries = abi.decode(['uint256', 'uint256'], bytes.fromhex(log["data"][2:]))

    data = {
        "event_type": "Swap",
        "block_number": int(log["blockNumber"], 16),
        "timestamp": block_time.isoformat(),
        "tx_hash": log["transactionHash"],
        "log_index": int(log["logIndex"], 16),
        "pair_contract_address": pair_contract_address,
        "poolId": poolId,
        "tokenIn": tokenIn,
        "tokenOut": tokenOut,
        "amountIn": data_entries[0],
        "amountOut": data_entries[1]
    }
    return data

def decode_PoolBalanceManaged(log: LogResult) -> dict:
    """
        event PoolBalanceManaged(
        bytes32 indexed poolId,
        address indexed assetManager,
        IERC20 indexed token,
        int256 cashDelta,
        int256 managedDelta
    """

    # Raw example event
    # {'address': '0xb4e16d0168e52d35cacd2c6185b44281ec28c9dc', 'blockHash': '0x4ba33a650f9e3d8430f94b61a382e60490ec7a06c2f4441ecf225858ec748b78', 'blockNumber': '0x98b7f6', 'data': '0x00000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000046ec814a2e900000000000000000000000000000000000000000000000000000000000003e80000000000000000000000000000000000000000000000000000000000000000', 'logIndex': '0x4', 'removed': False, 'topics': ['0xd78ad95fa46c994b6551d0da85fc275fe613ce37657fb8d5e3d130840159d822', '0x000000000000000000000000f164fc0ec4e93095b804a4795bbe1e041497b92a', '0x0000000000000000000000008688a84fcfd84d8f78020d0fc0b35987cc58911f'], 'transactionHash': '0x932cb88306450d481a0e43365a3ed832625b68f036e9887684ef6da594891366', 'transactionIndex': '0x1', 'context': <__main__.TokenCache object at 0x104ab7e20>, 'event': <class 'web3._utils.datatypes.Swap'>, 'timestamp': 1588712972}

    block_time = datetime.datetime.utcfromtimestamp(log["timestamp"])

    pair_contract_address = log["address"]

    # Chop data blob to byte32 entries
    event_signature, poolId, assetManager, token = log["topics"]
    data_entries = abi.decode(['int256', 'int256'], bytes.fromhex(log["data"][2:]))

    data = {
        "event_type": "TokenExchangeUnderlying",
        "block_number": int(log["blockNumber"], 16),
        "timestamp": block_time.isoformat(),
        "tx_hash": log["transactionHash"],
        "log_index": int(log["logIndex"], 16),
        "pair_contract_address": pair_contract_address,
        "poolId": poolId,
        "token": token,
        "cashDelta": data_entries[0],
        "managedDelta": data_entries[1],
    }
    return data

def main():
    logging.basicConfig(level="INFO", handlers=[logging.StreamHandler()])

    # Mute noise
    logging.getLogger("web3.providers.HTTPProvider").setLevel(logging.WARNING)
    logging.getLogger("web3.RequestManager").setLevel(logging.WARNING)
    logging.getLogger("urllib3.connectionpool").setLevel(logging.WARNING)
    logging.getLogger("futureproof.executors").setLevel(logging.WARNING)
    logging.getLogger("urllib3.connectionpool").setLevel(logging.ERROR)  # WARNING:urllib3.connectionpool:Connection pool is full, discarding connection: eth-mainnet.alchemyapi.io. Connection pool size: 10

    json_rpc_url = ""
    token_cache = TokenCache()
    threads = 16
    http_adapter = HTTPAdapter(pool_connections=threads, pool_maxsize=threads)
    web3_factory = TunedWeb3Factory(json_rpc_url, http_adapter)
    web3 = web3_factory(token_cache)
    executor = create_thread_pool_executor(web3_factory, token_cache, max_workers=threads)

    # Get contracts
    abi = [{"inputs":[{"internalType":"contract IAuthorizer","name":"authorizer","type":"address"},{"internalType":"contract IWETH","name":"weth","type":"address"},{"internalType":"uint256","name":"pauseWindowDuration","type":"uint256"},{"internalType":"uint256","name":"bufferPeriodDuration","type":"uint256"}],"stateMutability":"nonpayable","type":"constructor"},{"anonymous":False,"inputs":[{"indexed":True,"internalType":"contract IAuthorizer","name":"newAuthorizer","type":"address"}],"name":"AuthorizerChanged","type":"event"},{"anonymous":False,"inputs":[{"indexed":True,"internalType":"contract IERC20","name":"token","type":"address"},{"indexed":True,"internalType":"address","name":"sender","type":"address"},{"indexed":False,"internalType":"address","name":"recipient","type":"address"},{"indexed":False,"internalType":"uint256","name":"amount","type":"uint256"}],"name":"ExternalBalanceTransfer","type":"event"},{"anonymous":False,"inputs":[{"indexed":True,"internalType":"contract IFlashLoanRecipient","name":"recipient","type":"address"},{"indexed":True,"internalType":"contract IERC20","name":"token","type":"address"},{"indexed":False,"internalType":"uint256","name":"amount","type":"uint256"},{"indexed":False,"internalType":"uint256","name":"feeAmount","type":"uint256"}],"name":"FlashLoan","type":"event"},{"anonymous":False,"inputs":[{"indexed":True,"internalType":"address","name":"user","type":"address"},{"indexed":True,"internalType":"contract IERC20","name":"token","type":"address"},{"indexed":False,"internalType":"int256","name":"delta","type":"int256"}],"name":"InternalBalanceChanged","type":"event"},{"anonymous":False,"inputs":[{"indexed":False,"internalType":"bool","name":"paused","type":"bool"}],"name":"PausedStateChanged","type":"event"},{"anonymous":False,"inputs":[{"indexed":True,"internalType":"bytes32","name":"poolId","type":"bytes32"},{"indexed":True,"internalType":"address","name":"liquidityProvider","type":"address"},{"indexed":False,"internalType":"contract IERC20[]","name":"tokens","type":"address[]"},{"indexed":False,"internalType":"int256[]","name":"deltas","type":"int256[]"},{"indexed":False,"internalType":"uint256[]","name":"protocolFeeAmounts","type":"uint256[]"}],"name":"PoolBalanceChanged","type":"event"},{"anonymous":False,"inputs":[{"indexed":True,"internalType":"bytes32","name":"poolId","type":"bytes32"},{"indexed":True,"internalType":"address","name":"assetManager","type":"address"},{"indexed":True,"internalType":"contract IERC20","name":"token","type":"address"},{"indexed":False,"internalType":"int256","name":"cashDelta","type":"int256"},{"indexed":False,"internalType":"int256","name":"managedDelta","type":"int256"}],"name":"PoolBalanceManaged","type":"event"},{"anonymous":False,"inputs":[{"indexed":True,"internalType":"bytes32","name":"poolId","type":"bytes32"},{"indexed":True,"internalType":"address","name":"poolAddress","type":"address"},{"indexed":False,"internalType":"enum IVault.PoolSpecialization","name":"specialization","type":"uint8"}],"name":"PoolRegistered","type":"event"},{"anonymous":False,"inputs":[{"indexed":True,"internalType":"address","name":"relayer","type":"address"},{"indexed":True,"internalType":"address","name":"sender","type":"address"},{"indexed":False,"internalType":"bool","name":"approved","type":"bool"}],"name":"RelayerApprovalChanged","type":"event"},{"anonymous":False,"inputs":[{"indexed":True,"internalType":"bytes32","name":"poolId","type":"bytes32"},{"indexed":True,"internalType":"contract IERC20","name":"tokenIn","type":"address"},{"indexed":True,"internalType":"contract IERC20","name":"tokenOut","type":"address"},{"indexed":False,"internalType":"uint256","name":"amountIn","type":"uint256"},{"indexed":False,"internalType":"uint256","name":"amountOut","type":"uint256"}],"name":"Swap","type":"event"},{"anonymous":False,"inputs":[{"indexed":True,"internalType":"bytes32","name":"poolId","type":"bytes32"},{"indexed":False,"internalType":"contract IERC20[]","name":"tokens","type":"address[]"}],"name":"TokensDeregistered","type":"event"},{"anonymous":False,"inputs":[{"indexed":True,"internalType":"bytes32","name":"poolId","type":"bytes32"},{"indexed":False,"internalType":"contract IERC20[]","name":"tokens","type":"address[]"},{"indexed":False,"internalType":"address[]","name":"assetManagers","type":"address[]"}],"name":"TokensRegistered","type":"event"},{"inputs":[],"name":"WETH","outputs":[{"internalType":"contract IWETH","name":"","type":"address"}],"stateMutability":"view","type":"function"},{"inputs":[{"internalType":"enum IVault.SwapKind","name":"kind","type":"uint8"},{"components":[{"internalType":"bytes32","name":"poolId","type":"bytes32"},{"internalType":"uint256","name":"assetInIndex","type":"uint256"},{"internalType":"uint256","name":"assetOutIndex","type":"uint256"},{"internalType":"uint256","name":"amount","type":"uint256"},{"internalType":"bytes","name":"userData","type":"bytes"}],"internalType":"struct IVault.BatchSwapStep[]","name":"swaps","type":"tuple[]"},{"internalType":"contract IAsset[]","name":"assets","type":"address[]"},{"components":[{"internalType":"address","name":"sender","type":"address"},{"internalType":"bool","name":"fromInternalBalance","type":"bool"},{"internalType":"address payable","name":"recipient","type":"address"},{"internalType":"bool","name":"toInternalBalance","type":"bool"}],"internalType":"struct IVault.FundManagement","name":"funds","type":"tuple"},{"internalType":"int256[]","name":"limits","type":"int256[]"},{"internalType":"uint256","name":"deadline","type":"uint256"}],"name":"batchSwap","outputs":[{"internalType":"int256[]","name":"assetDeltas","type":"int256[]"}],"stateMutability":"payable","type":"function"},{"inputs":[{"internalType":"bytes32","name":"poolId","type":"bytes32"},{"internalType":"contract IERC20[]","name":"tokens","type":"address[]"}],"name":"deregisterTokens","outputs":[],"stateMutability":"nonpayable","type":"function"},{"inputs":[{"internalType":"bytes32","name":"poolId","type":"bytes32"},{"internalType":"address","name":"sender","type":"address"},{"internalType":"address payable","name":"recipient","type":"address"},{"components":[{"internalType":"contract IAsset[]","name":"assets","type":"address[]"},{"internalType":"uint256[]","name":"minAmountsOut","type":"uint256[]"},{"internalType":"bytes","name":"userData","type":"bytes"},{"internalType":"bool","name":"toInternalBalance","type":"bool"}],"internalType":"struct IVault.ExitPoolRequest","name":"request","type":"tuple"}],"name":"exitPool","outputs":[],"stateMutability":"nonpayable","type":"function"},{"inputs":[{"internalType":"contract IFlashLoanRecipient","name":"recipient","type":"address"},{"internalType":"contract IERC20[]","name":"tokens","type":"address[]"},{"internalType":"uint256[]","name":"amounts","type":"uint256[]"},{"internalType":"bytes","name":"userData","type":"bytes"}],"name":"flashLoan","outputs":[],"stateMutability":"nonpayable","type":"function"},{"inputs":[{"internalType":"bytes4","name":"selector","type":"bytes4"}],"name":"getActionId","outputs":[{"internalType":"bytes32","name":"","type":"bytes32"}],"stateMutability":"view","type":"function"},{"inputs":[],"name":"getAuthorizer","outputs":[{"internalType":"contract IAuthorizer","name":"","type":"address"}],"stateMutability":"view","type":"function"},{"inputs":[],"name":"getDomainSeparator","outputs":[{"internalType":"bytes32","name":"","type":"bytes32"}],"stateMutability":"view","type":"function"},{"inputs":[{"internalType":"address","name":"user","type":"address"},{"internalType":"contract IERC20[]","name":"tokens","type":"address[]"}],"name":"getInternalBalance","outputs":[{"internalType":"uint256[]","name":"balances","type":"uint256[]"}],"stateMutability":"view","type":"function"},{"inputs":[{"internalType":"address","name":"user","type":"address"}],"name":"getNextNonce","outputs":[{"internalType":"uint256","name":"","type":"uint256"}],"stateMutability":"view","type":"function"},{"inputs":[],"name":"getPausedState","outputs":[{"internalType":"bool","name":"paused","type":"bool"},{"internalType":"uint256","name":"pauseWindowEndTime","type":"uint256"},{"internalType":"uint256","name":"bufferPeriodEndTime","type":"uint256"}],"stateMutability":"view","type":"function"},{"inputs":[{"internalType":"bytes32","name":"poolId","type":"bytes32"}],"name":"getPool","outputs":[{"internalType":"address","name":"","type":"address"},{"internalType":"enum IVault.PoolSpecialization","name":"","type":"uint8"}],"stateMutability":"view","type":"function"},{"inputs":[{"internalType":"bytes32","name":"poolId","type":"bytes32"},{"internalType":"contract IERC20","name":"token","type":"address"}],"name":"getPoolTokenInfo","outputs":[{"internalType":"uint256","name":"cash","type":"uint256"},{"internalType":"uint256","name":"managed","type":"uint256"},{"internalType":"uint256","name":"lastChangeBlock","type":"uint256"},{"internalType":"address","name":"assetManager","type":"address"}],"stateMutability":"view","type":"function"},{"inputs":[{"internalType":"bytes32","name":"poolId","type":"bytes32"}],"name":"getPoolTokens","outputs":[{"internalType":"contract IERC20[]","name":"tokens","type":"address[]"},{"internalType":"uint256[]","name":"balances","type":"uint256[]"},{"internalType":"uint256","name":"lastChangeBlock","type":"uint256"}],"stateMutability":"view","type":"function"},{"inputs":[],"name":"getProtocolFeesCollector","outputs":[{"internalType":"contract ProtocolFeesCollector","name":"","type":"address"}],"stateMutability":"view","type":"function"},{"inputs":[{"internalType":"address","name":"user","type":"address"},{"internalType":"address","name":"relayer","type":"address"}],"name":"hasApprovedRelayer","outputs":[{"internalType":"bool","name":"","type":"bool"}],"stateMutability":"view","type":"function"},{"inputs":[{"internalType":"bytes32","name":"poolId","type":"bytes32"},{"internalType":"address","name":"sender","type":"address"},{"internalType":"address","name":"recipient","type":"address"},{"components":[{"internalType":"contract IAsset[]","name":"assets","type":"address[]"},{"internalType":"uint256[]","name":"maxAmountsIn","type":"uint256[]"},{"internalType":"bytes","name":"userData","type":"bytes"},{"internalType":"bool","name":"fromInternalBalance","type":"bool"}],"internalType":"struct IVault.JoinPoolRequest","name":"request","type":"tuple"}],"name":"joinPool","outputs":[],"stateMutability":"payable","type":"function"},{"inputs":[{"components":[{"internalType":"enum IVault.PoolBalanceOpKind","name":"kind","type":"uint8"},{"internalType":"bytes32","name":"poolId","type":"bytes32"},{"internalType":"contract IERC20","name":"token","type":"address"},{"internalType":"uint256","name":"amount","type":"uint256"}],"internalType":"struct IVault.PoolBalanceOp[]","name":"ops","type":"tuple[]"}],"name":"managePoolBalance","outputs":[],"stateMutability":"nonpayable","type":"function"},{"inputs":[{"components":[{"internalType":"enum IVault.UserBalanceOpKind","name":"kind","type":"uint8"},{"internalType":"contract IAsset","name":"asset","type":"address"},{"internalType":"uint256","name":"amount","type":"uint256"},{"internalType":"address","name":"sender","type":"address"},{"internalType":"address payable","name":"recipient","type":"address"}],"internalType":"struct IVault.UserBalanceOp[]","name":"ops","type":"tuple[]"}],"name":"manageUserBalance","outputs":[],"stateMutability":"payable","type":"function"},{"inputs":[{"internalType":"enum IVault.SwapKind","name":"kind","type":"uint8"},{"components":[{"internalType":"bytes32","name":"poolId","type":"bytes32"},{"internalType":"uint256","name":"assetInIndex","type":"uint256"},{"internalType":"uint256","name":"assetOutIndex","type":"uint256"},{"internalType":"uint256","name":"amount","type":"uint256"},{"internalType":"bytes","name":"userData","type":"bytes"}],"internalType":"struct IVault.BatchSwapStep[]","name":"swaps","type":"tuple[]"},{"internalType":"contract IAsset[]","name":"assets","type":"address[]"},{"components":[{"internalType":"address","name":"sender","type":"address"},{"internalType":"bool","name":"fromInternalBalance","type":"bool"},{"internalType":"address payable","name":"recipient","type":"address"},{"internalType":"bool","name":"toInternalBalance","type":"bool"}],"internalType":"struct IVault.FundManagement","name":"funds","type":"tuple"}],"name":"queryBatchSwap","outputs":[{"internalType":"int256[]","name":"","type":"int256[]"}],"stateMutability":"nonpayable","type":"function"},{"inputs":[{"internalType":"enum IVault.PoolSpecialization","name":"specialization","type":"uint8"}],"name":"registerPool","outputs":[{"internalType":"bytes32","name":"","type":"bytes32"}],"stateMutability":"nonpayable","type":"function"},{"inputs":[{"internalType":"bytes32","name":"poolId","type":"bytes32"},{"internalType":"contract IERC20[]","name":"tokens","type":"address[]"},{"internalType":"address[]","name":"assetManagers","type":"address[]"}],"name":"registerTokens","outputs":[],"stateMutability":"nonpayable","type":"function"},{"inputs":[{"internalType":"contract IAuthorizer","name":"newAuthorizer","type":"address"}],"name":"setAuthorizer","outputs":[],"stateMutability":"nonpayable","type":"function"},{"inputs":[{"internalType":"bool","name":"paused","type":"bool"}],"name":"setPaused","outputs":[],"stateMutability":"nonpayable","type":"function"},{"inputs":[{"internalType":"address","name":"sender","type":"address"},{"internalType":"address","name":"relayer","type":"address"},{"internalType":"bool","name":"approved","type":"bool"}],"name":"setRelayerApproval","outputs":[],"stateMutability":"nonpayable","type":"function"},{"inputs":[{"components":[{"internalType":"bytes32","name":"poolId","type":"bytes32"},{"internalType":"enum IVault.SwapKind","name":"kind","type":"uint8"},{"internalType":"contract IAsset","name":"assetIn","type":"address"},{"internalType":"contract IAsset","name":"assetOut","type":"address"},{"internalType":"uint256","name":"amount","type":"uint256"},{"internalType":"bytes","name":"userData","type":"bytes"}],"internalType":"struct IVault.SingleSwap","name":"singleSwap","type":"tuple"},{"components":[{"internalType":"address","name":"sender","type":"address"},{"internalType":"bool","name":"fromInternalBalance","type":"bool"},{"internalType":"address payable","name":"recipient","type":"address"},{"internalType":"bool","name":"toInternalBalance","type":"bool"}],"internalType":"struct IVault.FundManagement","name":"funds","type":"tuple"},{"internalType":"uint256","name":"limit","type":"uint256"},{"internalType":"uint256","name":"deadline","type":"uint256"}],"name":"swap","outputs":[{"internalType":"uint256","name":"amountCalculated","type":"uint256"}],"stateMutability":"payable","type":"function"},{"stateMutability":"payable","type":"receive"}]
    Vault = web3.eth.contract(abi=abi)
    events = [Vault.events.PoolBalanceChanged, Vault.events.Swap, Vault.events.PoolBalanceManaged]  # https://etherscan.io/txs?ea=0x5c69bee701ef814a2b6a3edd4b1652cb9cc5aa6f&topic0=0x0d3648bd0f6ba80134a33ba9275ac585d9d315f0ad8355cddefde31afa28d0e9

    PoolBalanceChanged_fname = "balancer-poolbalancechanged.csv"
    Swap_fname = "balancer-swap.csv"
    PoolBalanceManaged_fname = "balancer-poolbalancemanaged.csv"
    state_fname = "balancer-last-block-state.txt"

    start_block = restore_state(state_fname, 15_537_393)
    end_block = 18_083_778

    max_blocks = end_block - start_block

    PoolBalanceChanged_event_buffer = []
    Swap_event_buffer = []
    PoolBalanceManaged_event_buffer = []

    print(f"Starting to read block range {start_block:,} - {end_block:,}")

    with open(PoolBalanceChanged_fname, "a") as PoolBalanceChanged_out, \
            open(Swap_fname, "a") as Swap_out, \
            open(PoolBalanceManaged_fname, "a") as PoolBalanceManaged_out:
        PoolBalanceChanged_writer = csv.DictWriter(PoolBalanceChanged_out, fieldnames=PoolBalanceChanged_FIELD_NAMES)
        Swap_writer = csv.DictWriter(Swap_out, fieldnames=Swap_FIELD_NAMES)
        PoolBalanceManaged_writer = csv.DictWriter(PoolBalanceManaged_out, fieldnames=PoolBalanceManaged_FIELD_NAMES)

        with tqdm(total=max_blocks) as progress_bar:
            #  1. Update the progress bar
            #  2. save any events in the buffer in to a file in one go
            def update_progress(current_block, start_block, end_block, chunk_size: int, total_events: int, last_timestamp: Optional[int], context: TokenCache):
                nonlocal PoolBalanceChanged_event_buffer
                nonlocal Swap_event_buffer
                nonlocal PoolBalanceManaged_event_buffer

                if last_timestamp:
                    # Display progress with the date information
                    d = datetime.datetime.utcfromtimestamp(last_timestamp)
                    formatted_time = d.strftime("%d-%m-%Y")
                    progress_bar.set_description(f"Block: {current_block:,}, events: {total_events:}, time:{formatted_time}")
                else:
                    progress_bar.set_description(f"Block: {current_block:,}, events: {total_events:,}")

                progress_bar.update(chunk_size)

                # Output scanned events
                for entry in PoolBalanceChanged_event_buffer:
                    PoolBalanceChanged_writer.writerow(entry)

                for entry in Swap_event_buffer:
                    Swap_writer.writerow(entry)

                for entry in PoolBalanceManaged_event_buffer:
                    PoolBalanceManaged_writer.writerow(entry)

                save_state(state_fname, current_block -1)

                # Reset buffer
                PoolBalanceChanged_event_buffer = []
                Swap_event_buffer = []
                PoolBalanceManaged_event_buffer = []

            # Read specified events in block range
            for log_result in read_events_concurrent(
                executor,
                start_block,
                end_block,
                events,
                update_progress,
                chunk_size=100,
                context=token_cache,
            ):
                # We are getting two kinds of log entries, pairs and swaps.
                # Choose between where to store.
                try:
                    if log_result["event"].event_name == "PoolBalanceChanged":
                        PoolBalanceChanged_event_buffer.append(decode_PoolBalanceChanged(log_result))
                    elif log_result["event"].event_name == "Swap":
                        Swap_event_buffer.append(decode_Swap(log_result))
                    elif log_result["event"].event_name == "PoolBalanceManaged":
                        PoolBalanceManaged_event_buffer.append(decode_PoolBalanceManaged(log_result))
                except Exception as e:
                    raise RuntimeError(f"Could not decode {log_result}") from e

    print("Wrote pairs, swaps")


if __name__ == "__main__":
    main()