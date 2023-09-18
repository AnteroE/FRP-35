import pandas as pd

# Load CSVs into pandas DataFrames
df_params = pd.read_csv('mr-parameters.csv')
df_rates = pd.read_csv('mr-syncs.csv')

# Merge the DataFrames on pair_contract_address
df = df_rates.merge(df_params, on='pair_contract_address')

# Redefine bounds
df['upper_bound'] = df[['mu', 'sigma']].apply(lambda x: max(x[0] + 2*x[1], 1.003**2 * x[0]), axis=1)
df['lower_bound'] = df[['mu', 'sigma']].apply(lambda x: min(x[0] - 2*x[1], (2 - 1.003**2) * x[0]), axis=1)
df['upper_extreme'] = df['mu'] + 4*df['sigma']
df['lower_extreme'] = df['mu'] - 4*df['sigma']

# Determine which threshold the exchange rate reaches first after a bound violation
results = []
for name, group in df.groupby('pair_contract_address'):
    above_upper_bound = False
    below_lower_bound = False
    for idx, row in group.iterrows():
        if above_upper_bound:
            if row['exchange_rate'] <= row['mu']:
                results.append((name, 'above_upper_to_mu', row['timestamp'], row['exchange_rate']))
                above_upper_bound = False
            elif row['exchange_rate'] > row['upper_extreme']:
                results.append((name, 'above_upper_to_upper_extreme', row['timestamp'], row['exchange_rate']))
                above_upper_bound = False

        if below_lower_bound:
            if row['exchange_rate'] >= row['mu']:
                results.append((name, 'below_lower_to_mu', row['timestamp'], row['exchange_rate']))
                below_lower_bound = False
            elif row['exchange_rate'] < row['lower_extreme']:
                results.append((name, 'below_lower_to_lower_extreme', row['timestamp'], row['exchange_rate']))
                below_lower_bound = False

        if not above_upper_bound and row['exchange_rate'] > row['upper_bound']:
            above_upper_bound = True

        if not below_lower_bound and row['exchange_rate'] < row['lower_bound']:
            below_lower_bound = True

# Convert results to DataFrame and print
result_df = pd.DataFrame(results, columns=['pair_contract_address', 'event', 'timestamp', 'exchange_rate'])
print(result_df)
