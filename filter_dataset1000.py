import pandas as pd

# 1. Read the CSV
df = pd.read_csv('uni-v2-syncs.csv', names=[
    "block_number", "timestamp", "tx_hash", "log_index",
    "pair_contract_address", "reserve0", "reserve1",
])

filtered_df = df.groupby('pair_contract_address').filter(lambda x: len(x) >= 1000)

# Save to a new CSV
filtered_df.to_csv('filtered-syncs.csv', index=False)