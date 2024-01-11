from eth_defi.uniswap_v3.events import fetch_events_to_csv
from eth_defi.event_reader.json_state import JSONFileScanState
from eth_defi.uniswap_v3.constants import UNISWAP_V3_FACTORY_CREATED_AT_BLOCK
import os

from web3 import Web3, HTTPProvider

# Get your node JSON-RPC URL
json_rpc_url = ""
web3 = Web3(HTTPProvider(json_rpc_url))

start_block = UNISWAP_V3_FACTORY_CREATED_AT_BLOCK
end_block = 15_537_392

# Stores the last block number of event data we store
state = JSONFileScanState("uniswap-v3-price-scan.json")

fetch_events_to_csv(json_rpc_url, state, start_block=start_block, end_block=end_block, output_folder=os.path.dirname(os.path.abspath(__file__)))