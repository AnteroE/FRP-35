import pandas as pd

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


# 1. Read the CSV
swaps = pd.read_csv('balancer-swap.csv', names=Swap_FIELD_NAMES)
poolbalancechanged = pd.read_csv('balancer-poolbalancechanged.csv', names=PoolBalanceChanged_FIELD_NAMES)
poolbalancemanaged = pd.read_csv('balancer-poolbalancemanaged.csv', names=PoolBalanceManaged_FIELD_NAMES)

all_addresses = pd.concat([
    swaps['poolId'],
    poolbalancechanged['poolId'],
    poolbalancemanaged['poolId']
], ignore_index=True)

# Counting the occurrences of each unique 'pair_contract_address'
address_counts = all_addresses.value_counts()

frequent_address_counts = address_counts[address_counts >= 1000]

print("Addresses along with their counts that appear at least 1000 times:")
print(frequent_address_counts)

# Convert the index to a Series
frequent_addresses_series = pd.Series(frequent_address_counts.index, name='poolId')

min_block_swaps = swaps.groupby('poolId')['block_number'].min()
min_block_poolbalancechanged = poolbalancechanged.groupby('poolId')['block_number'].min()
min_block_poolbalancemanaged = poolbalancemanaged.groupby('poolId')['block_number'].min()

# Combine all these Series into a single DataFrame
min_blocks_df = pd.concat([
    min_block_swaps,
    min_block_poolbalancechanged,
    min_block_poolbalancemanaged
], axis=1)

# The column names will be the same ('block_number') so let's give them unique names
min_blocks_df.columns = ['swaps', 'poolbalancechanged', 'poolbalancemanaged']

# Find the overall minimum block_number for each pair_contract_address
min_blocks_df['min_block_number'] = min_blocks_df.min(axis=1)
min_blocks_df = min_blocks_df['min_block_number'].astype(int)

# Filter to include only the frequent addresses
min_blocks_frequent = min_blocks_df.loc[min_blocks_df.index.isin(frequent_addresses_series)]

frequent_addresses_df = frequent_addresses_series.reset_index(drop=True).to_frame()

merged_df = pd.merge(frequent_addresses_df, min_blocks_frequent.reset_index(), how='left', left_on='poolId', right_on='poolId')

merged_df.to_csv('balancer-frequent-addresses.csv', index=False)


