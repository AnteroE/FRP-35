import pandas as pd

swap_df = pd.read_csv("uniswap-v3-swap.csv")

print(f"We have total {len(swap_df):,} Uniswap swap events in the loaded dataset")
column_names = ", ".join([n for n in swap_df.columns])
print("Swap data columns are:", column_names)

from eth_defi.uniswap_v3.pool import fetch_pool_details

pool_address = "0x8ad599c3A0ff1De082011EFDDc58f1908eb6e6D8"

df = swap_df.loc[swap_df["pool_contract_address"] == pool_address.lower()]
df