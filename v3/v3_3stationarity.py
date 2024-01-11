import pandas as pd
from statsmodels.tsa.stattools import adfuller

# 1. Read the CSV
df = pd.read_csv('uniswap-v3-filtered-swap.csv')

# Specify the columns that should be integers
int_columns = ["block_number", "log_index", "amount0", "amount1", "sqrt_price_x96", "liquidity", "tick"]

df["exchange_rate"] = 1.0001 ** df["tick"]

# Function to safely convert to int64, and set to NaN if conversion fails
def safe_convert(x):
    try:
        return int(x)
    except OverflowError:
        return pd.NA

# Apply the safe conversion function to the specified columns
for col in int_columns:
    df[col] = df[col].apply(safe_convert)

print(df['pool_contract_address'].nunique())
#addresses that have a lot of data and need to be analyzed on their own for memory reasons
dropped_addresses = ['0x88e6a0c2ddd26feeb64f039a2c41296fcb3f5640', '0x11b815efb8f581194ae79006d24e0d814b7697f6']

# 3. Analyze Stationarity
results = {}
i = 0
for address, group in df.groupby('pool_contract_address'):
    if address not in dropped_addresses:
        group = group.dropna(subset=['exchange_rate'])
        if group['exchange_rate'].nunique() > 1:
            is_stationary = adfuller(group['exchange_rate'])[1] < 0.05  # if p-value < 0.05, reject null hypothesis and the series is stationary
            results[address] = is_stationary
        else:
            print("The series is constant. Skipping the adfuller test.")
    if i % 100 == 0:
        print(i)
    i += 1

print(results)

# Filter rows with stationary pair_contract_address
stationary_addresses = [address for address, is_stationary in results.items() if is_stationary]
stationary_df = df[df['pool_contract_address'].isin(stationary_addresses)]

# Save to a new CSV
stationary_df.to_csv('uniswap-v3-stationary-swaps2.csv', index=False)
