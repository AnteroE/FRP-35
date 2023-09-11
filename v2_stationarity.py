import pandas as pd
from statsmodels.tsa.stattools import adfuller

# 1. Read the CSV
df = pd.read_csv('filtered-syncs.csv')

# Specify the columns that should be integers
int_columns = ["block_number", "log_index", "reserve0", "reserve1"]

# Function to safely convert to int64, and set to NaN if conversion fails
def safe_convert(x):
    try:
        return int(x)
    except OverflowError:
        return pd.NA

# Apply the safe conversion function to the specified columns
for col in int_columns:
    df[col] = df[col].apply(safe_convert)

df['reserve1'] = df['reserve1'].replace(0, pd.NA)

print(1)

df['exchange_rate'] = df['reserve0'] / df['reserve1']

print(df['pair_contract_address'].nunique())

# 3. Analyze Stationarity
results = {}
i = 0
for address, group in df.groupby('pair_contract_address'):
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
stationary_df = df[df['pair_contract_address'].isin(stationary_addresses)]

# Save to a new CSV
stationary_df.to_csv('stationary-syncs.csv', index=False)
