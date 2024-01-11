import pandas as pd
from statsmodels.tsa.stattools import adfuller

df = pd.read_csv("balancer-weigh-cleaned.csv")

df[['balance1', 'balance2', 'weight1', 'weight2']] = df[['balance1', 'balance2', 'weight1', 'weight2']].apply(pd.to_numeric, errors='coerce')

# Calculate exchange_rate and create new column
# Adding small constants in the denominator to avoid division by zero
epsilon = 1e-10
df['exchange_rate'] = (df['balance1'] / (df['weight1'] + epsilon)) / (df['balance2'] / (df['weight2'] + epsilon))

# 3. Analyze Stationarity
results = {}
i = 0
for (address, token1, token2), group in df.groupby(['poolId', 'token1', 'token2']):
    group = group.dropna(subset=['exchange_rate'])
    if group['exchange_rate'].nunique() > 1:
        is_stationary = adfuller(group['exchange_rate'])[1] < 0.05  # if p-value < 0.05, reject null hypothesis and the series is stationary
        results[address, token1, token2] = is_stationary
    else:
        print("The series is constant. Skipping the adfuller test.")
    if i % 100 == 0:
        print(i)
    i += 1

print(results)

# Filter rows with stationary pair_contract_address
stationary_addresses = [group for group, is_stationary in results.items() if is_stationary]
stationary_df = df[df.set_index(['poolId', 'token1', 'token2']).index.isin(stationary_addresses)]


# Now df has the new 'exchange_rate' column
# You can save this DataFrame to a new CSV file if needed
stationary_df.to_csv('balancer-weigh-stationary.csv', index=False)