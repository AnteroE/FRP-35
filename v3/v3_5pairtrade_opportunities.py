import pandas as pd

# Load CSVs into pandas DataFrames
df_rates = pd.read_csv('uni-v3-mr-swaps2.csv')
df_rates = df_rates[['block_number', 'tick', 'exchange_rate', 'pool_contract_address', 'unique_id']]

df_params = pd.read_csv('uni-v3-mr-parameters2.csv')
df_params = df_params[['pool_contract_address', 'mu', 'sigma']]

df_fees = pd.read_csv('uniswap-v3-poolcreated.csv')
df_fees = df_fees[['pool_contract_address', 'fee']]

df_fees['pool_contract_address'] = df_fees['pool_contract_address'].str.lower()

df_params = df_params.merge(df_fees, on='pool_contract_address', how='left')


# Redefine bounds
df_params['upper_bound'] = df_params[['mu', 'sigma', 'fee']].apply(lambda x: max(x[0] + 2*x[1], x[0] + x[2]/500_000), axis=1)
df_params['lower_bound'] = df_params[['mu', 'sigma', 'fee']].apply(lambda x: min(x[0] - 2*x[1], x[0] - x[2]/500_000), axis=1)
df_params['upper_extreme'] = df_params['mu'] + 4*df_params['sigma']
df_params['lower_extreme'] = df_params['mu'] - 4*df_params['sigma']


print(df_params.head())

# Determine which threshold the exchange rate reaches first after a bound violation
results = []
for name, group in df_rates.groupby('pool_contract_address'):
    matching_row = df_params[df_params['pool_contract_address'] == name]
    upper_bound = matching_row['upper_bound'].values[0]
    lower_bound = matching_row['lower_bound'].values[0]
    upper_extreme = matching_row['upper_extreme'].values[0]
    lower_extreme = matching_row['lower_extreme'].values[0]
    mu = matching_row['mu'].values[0]

    above_upper_bound = False
    below_lower_bound = False
    upper_entry = False
    lower_entry = False
    for idx, row in group.iterrows():
        if above_upper_bound:
            if upper_entry:
                if row['exchange_rate'] < upper_bound or row['exchange_rate'] > upper_extreme:
                    blocks = blocks + row['block_number'] - entry_start
                    upper_entry = False
            elif row['exchange_rate'] > upper_bound and row['exchange_rate'] < upper_extreme:
                upper_entry = True
                entry_start = row['block_number']
            if row['exchange_rate'] <= mu:
                results.append((name, 'above_upper_to_mu', row['block_number'], row['exchange_rate'], blocks))
                above_upper_bound = False
            elif row['exchange_rate'] > upper_extreme:
                results.append(
                    (name, 'above_upper_to_upper_extreme', row['block_number'], row['exchange_rate'], blocks))
                above_upper_bound = False

        if below_lower_bound:
            if lower_entry:
                if row['exchange_rate'] > lower_bound or row['exchange_rate'] < lower_extreme:
                    blocks = blocks + row['block_number'] - entry_start
                    lower_entry = False
            elif row['exchange_rate'] < lower_bound and row['exchange_rate'] > lower_extreme:
                lower_entry = True
                entry_start = row['block_number']
            if row['exchange_rate'] >= mu:
                results.append((name, 'below_lower_to_mu', row['block_number'], row['exchange_rate'], blocks))
                below_lower_bound = False
            elif row['exchange_rate'] < lower_extreme:
                results.append(
                    (name, 'below_lower_to_lower_extreme', row['block_number'], row['exchange_rate'], blocks))
                below_lower_bound = False

        if not above_upper_bound and row['exchange_rate'] > upper_bound:
            if row['exchange_rate'] < upper_extreme:
                results.append((name, 'above_upper_entry', row['block_number'], row['exchange_rate'], blocks))
                upper_entry = True
                entry_start = row['block_number']
            above_upper_bound = True
            blocks = 0

        if not below_lower_bound and row['exchange_rate'] < lower_bound:
            if row['exchange_rate'] > lower_extreme:
                results.append((name, 'below_lower_entry', row['block_number'], row['exchange_rate'], blocks))
                lower_entry = True
                entry_start = row['block_number']
            below_lower_bound = True
            blocks = 0


# Convert results to DataFrame and print
result_df = pd.DataFrame(results, columns=['unique_id', 'event', 'block_number', 'exchange_rate', 'entry_length'])
print(result_df)
result_df.to_csv('uni-v3-pairtrade.csv', index=False)
