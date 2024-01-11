import pandas as pd

# 1. Read the CSV
df = pd.read_csv('uniswap-v3-swap.csv')

filtered_df = df.groupby('pool_contract_address').filter(lambda x: len(x) >= 1000)

# Save to a new CSV
filtered_df.to_csv('uniswap-v3-filtered-swap.csv', index=False)