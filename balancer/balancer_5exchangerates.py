import pandas as pd
from itertools import combinations
import ast
from web3 import Web3

df_startinglinear = pd.read_csv('balancer-startinglinear.csv')
df_startingstable = pd.read_csv('balancer-startingstable.csv')
df_startingweigh = pd.read_csv('balancer-startingweigh.csv')

unique_values_df1 = set(df_startinglinear['poolId'].unique())
unique_values_df2 = set(df_startingstable['poolId'].unique())
unique_values_df3 = set(df_startingweigh['poolId'].unique())

# Combining unique values from all DataFrames
all_unique_values = unique_values_df1.union(unique_values_df2, unique_values_df3)

# Converting the set of unique values to a list
poolIds = list(all_unique_values)


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

df_swaps = pd.read_csv('balancer-swap.csv', names=Swap_FIELD_NAMES)
df_poolbalancechanged = pd.read_csv('balancer-poolbalancechanged.csv', names=PoolBalanceChanged_FIELD_NAMES)
df_poolbalancemanaged = pd.read_csv('balancer-poolbalancemanaged.csv', names=PoolBalanceManaged_FIELD_NAMES)

df_swaps = df_swaps.drop_duplicates(subset=['tx_hash','log_index'])
df_poolbalancechanged = df_poolbalancechanged.drop_duplicates(subset=['tx_hash','log_index'])
df_poolbalancemanaged = df_poolbalancemanaged.drop_duplicates(subset=['tx_hash','log_index'])

df_swaps = df_swaps[df_swaps['poolId'].isin(poolIds)]
df_poolbalancechanged = df_poolbalancechanged[df_poolbalancechanged['poolId'].isin(poolIds)]
df_poolbalancemanaged = df_poolbalancemanaged[df_poolbalancemanaged['poolId'].isin(poolIds)]

# Initialize list to hold data for the new DataFrame
df_startinglinear2 = []
df_startingstable2 = []
df_startingweigh2 = []
# Iterate over rows in the original DataFrame

df_startinglinear['tokens'] = df_startinglinear['tokens'].apply(ast.literal_eval)
df_startinglinear['balances'] = df_startinglinear['balances'].apply(ast.literal_eval)
for idx, row in df_startinglinear.iterrows():
    poolId = row['poolId']
    min_block_number = row['min_block_number']
    first_key = row['first_key']
    fees = row['fees']
    lowers = row['lowers']
    uppers = row['uppers']

    # Iterate over combinations of token and balance pairs
    for (token1, balance1), (token2, balance2) in combinations(zip(row['tokens'], row['balances']), 2):
        # Create a new row for each combination of token pairs and balance pairs
        new_row = {
            'poolId': poolId,
            'min_block_number': min_block_number,
            'first_key': first_key,
            'token1': token1,
            'token2': token2,
            'balance1': balance1,
            'balance2': balance2,
            'fees': fees,
            'lowers': lowers,
            'uppers': uppers
        }

        # Append new row to new_data list
        df_startinglinear2.append(new_row)

df_startingstable['tokens'] = df_startingstable['tokens'].apply(ast.literal_eval)
df_startingstable['balances'] = df_startingstable['balances'].apply(ast.literal_eval)
for idx, row in df_startingstable.iterrows():
    poolId = row['poolId']
    min_block_number = row['min_block_number']
    first_key = row['first_key']
    fees = row['fees']
    amplifiers = row['amplifiers']

    # Iterate over combinations of token and balance pairs
    for (token1, balance1), (token2, balance2) in combinations(zip(row['tokens'], row['balances']), 2):
        # Create a new row for each combination of token pairs and balance pairs
        new_row = {
            'poolId': poolId,
            'min_block_number': min_block_number,
            'first_key': first_key,
            'token1': token1,
            'token2': token2,
            'balance1': balance1,
            'balance2': balance2,
            'fees': fees,
            'amplifiers': amplifiers
        }

        # Append new row to new_data list
        df_startingstable2.append(new_row)

df_startingweigh['tokens'] = df_startingweigh['tokens'].apply(ast.literal_eval)
df_startingweigh['balances'] = df_startingweigh['balances'].apply(ast.literal_eval)
df_startingweigh['weights'] = df_startingweigh['weights'].apply(ast.literal_eval)
for idx, row in df_startingweigh.iterrows():
    poolId = row['poolId']
    min_block_number = row['min_block_number']
    first_key = row['first_key']
    fees = row['fees']

    # Iterate over combinations of token, balance, and weight triples
    for (token1, balance1, weight1), (token2, balance2, weight2) in combinations(
            zip(row['tokens'], row['balances'], row['weights']), 2):
        # Create a new row for each combination of token pairs, balance pairs, and weight pairs
        new_row = {
            'poolId': poolId,
            'min_block_number': min_block_number,
            'first_key': first_key,
            'token1': token1,
            'token2': token2,
            'balance1': balance1,
            'balance2': balance2,
            'weight1': weight1,
            'weight2': weight2,
            'fees': fees,
        }

        # Append new row to new_data list
        df_startingweigh2.append(new_row)

# Create new DataFrame from the list of dictionaries
df_startinglinear2 = pd.DataFrame(df_startinglinear2)
df_startingstable2 = pd.DataFrame(df_startingstable2)
df_startingweigh2 = pd.DataFrame(df_startingweigh2)

pd.set_option('display.max_columns', None)  # Set the maximum number of columns displayed to unlimited
pd.set_option('display.width', None)  # Set the display width to unlimited

df_linearprices = df_startinglinear2[['poolId', 'min_block_number', 'token1', 'token2', 'balance1', 'balance2', 'fees', 'lowers', 'uppers']]
df_stableprices = df_startingstable2[['poolId', 'min_block_number', 'token1', 'token2', 'balance1', 'balance2', 'fees', 'amplifiers']]
df_weighprices = df_startingweigh2[['poolId', 'min_block_number', 'token1', 'token2', 'balance1', 'balance2', 'weight1', 'weight2', 'fees']]

df_linearprices.rename(columns={'min_block_number': 'block_number'}, inplace=True)
df_stableprices.rename(columns={'min_block_number': 'block_number'}, inplace=True)
df_weighprices.rename(columns={'min_block_number': 'block_number'}, inplace=True)

event_cols = ["event_type", "block_number", "tx_hash", "log_index", "poolId"]
df_events = pd.concat([df_swaps[event_cols], df_poolbalancechanged[event_cols], df_poolbalancemanaged[event_cols]], ignore_index=True)

df_events = df_events.sort_values(by=['block_number', 'log_index'], ascending=[True, True]).reset_index(drop=True)


print(len(df_events))
n = 0
for idx, row in df_events.iterrows():
    # Get event_type from current row
    event_type = row['event_type']
    if event_type == 'Swap':
        parameters = df_swaps[
            (df_swaps['block_number'] == row['block_number']) &
            (df_swaps['tx_hash'] == row['tx_hash']) &
            (df_swaps['log_index'] == row['log_index'])
            ]
        tokenIn = parameters['tokenIn'].iloc[0]
        try:
            tokenIn = Web3.toChecksumAddress("0x" + tokenIn.lower().lstrip('0x').lstrip('0'))
        except ValueError:
            tokenIn = Web3.toChecksumAddress("0x0" + tokenIn.lower().lstrip('0x').lstrip('0'))
        tokenOut = parameters['tokenOut'].iloc[0]
        try:
            tokenOut = Web3.toChecksumAddress("0x" + tokenOut.lower().lstrip('0x').lstrip('0'))
        except ValueError:
            tokenOut = Web3.toChecksumAddress("0x0" + tokenOut.lower().lstrip('0x').lstrip('0'))
        df_type = "linear"
        filtered_df = df_linearprices[
            (df_linearprices['poolId'] == row['poolId']) &
            (
                    ((df_linearprices['token1'] == tokenIn) & (df_linearprices['token2'] == tokenOut)) |
                    ((df_linearprices['token1'] == tokenOut) & (df_linearprices['token2'] == tokenIn))
            )
            ]

        if filtered_df.empty:
            filtered_df = df_stableprices[
                (df_stableprices['poolId'] == row['poolId']) &
                (
                        ((df_stableprices['token1'] == tokenIn) & (df_stableprices['token2'] == tokenOut)) |
                        ((df_stableprices['token1'] == tokenOut) & (df_stableprices['token2'] == tokenIn))
                )
                ]
            df_type = "stable"
        if filtered_df.empty:
            filtered_df = df_weighprices[
                (df_weighprices['poolId'] == row['poolId']) &
                (
                        ((df_weighprices['token1'] == tokenIn) & (df_weighprices['token2'] == tokenOut)) |
                        ((df_weighprices['token1'] == tokenOut) & (df_weighprices['token2'] == tokenIn))
                )
                ]
            df_type = "weigh"
        if filtered_df.empty == False:
            last_row = filtered_df.iloc[-1]
            if last_row['token1'] == tokenIn:
                last_row['balance1'] += int(parameters["amountIn"])
                last_row['balance2'] -= int(parameters["amountOut"])
                last_row['block_number'] = parameters['block_number']
            elif last_row['token2'] == tokenIn:
                last_row['balance2'] += int(parameters["amountIn"])
                last_row['balance1'] -= int(parameters["amountOut"])
                last_row['block_number'] = parameters['block_number']

            if df_type == "linear":
                df_linearprices = df_linearprices.append(last_row, ignore_index=True)
            elif df_type == "stable":
                df_stableprices = df_stableprices.append(last_row, ignore_index=True)
            elif df_type == "weigh":
                df_weighprices = df_weighprices.append(last_row, ignore_index=True)

    elif event_type == 'PoolBalanceManaged':
        parameters = df_poolbalancemanaged[
            (df_poolbalancemanaged['block_number'] == row['block_number']) &
            (df_poolbalancemanaged['tx_hash'] == row['tx_hash']) &
            (df_poolbalancemanaged['log_index'] == row['log_index'])
            ]

        token = parameters['token'].iloc[0]
        try:
            token = Web3.toChecksumAddress("0x" + token.lower().lstrip('0x').lstrip('0'))
        except ValueError:
            token = Web3.toChecksumAddress("0x0" + token.lower().lstrip('0x').lstrip('0'))
        df_type = "linear"
        filtered_df = df_linearprices[
            (df_linearprices['poolId'] == row['poolId']) &
            (
                    ((df_linearprices['token1'] == token) |
                    (df_linearprices['token2'] == token))
            )
            ]

        if filtered_df.empty:
            filtered_df = df_stableprices[
                (df_stableprices['poolId'] == row['poolId']) &
                (
                    ((df_stableprices['token1'] == token) |
                     (df_stableprices['token2'] == token))
                )
                ]
            df_type = "stable"
        if filtered_df.empty:
            filtered_df = df_weighprices[
                (df_weighprices['poolId'] == row['poolId']) &
                (
                    ((df_weighprices['token1'] == token) |
                     (df_weighprices['token2'] == token))
                )
                ]
            df_type = "weigh"
        if filtered_df.empty == False:
            last_row = filtered_df.groupby(['token1', 'token2']).last().reset_index()
            last_row.loc[last_row['token1'] == token, 'balance1'] += int(parameters['managedDelta'])
            last_row.loc[last_row['token2'] == token, 'balance2'] += int(parameters['managedDelta'])
            last_row['block_number'] = parameters['block_number']

            if df_type == "linear":
                df_linearprices = df_linearprices.append(last_row, ignore_index=True)
            elif df_type == "stable":
                df_stableprices = df_stableprices.append(last_row, ignore_index=True)
            elif df_type == "weigh":
                df_weighprices = df_weighprices.append(last_row, ignore_index=True)

    elif event_type == 'PoolBalanceChanged':
        parameters = df_poolbalancechanged[
            (df_poolbalancechanged['block_number'] == row['block_number']) &
            (df_poolbalancechanged['tx_hash'] == row['tx_hash']) &
            (df_poolbalancechanged['log_index'] == row['log_index'])
            ]

        parameters['tokens'] = parameters['tokens'].apply(ast.literal_eval)
        parameters['deltas'] = parameters['deltas'].apply(ast.literal_eval)

        result = []

        # Iterate through DataFrame rows and process tokens and deltas
        for idx, row in parameters.iterrows():
            token_pairs = row['tokens']
            delta_pairs = row['deltas']

            # Generate combinations of every two tokens and their deltas
            for (token1, delta1), (token2, delta2) in combinations(zip(token_pairs, delta_pairs), 2):
                result.append({
                    'token1': Web3.toChecksumAddress(token1),
                    'delta1': delta1,
                    'token2': Web3.toChecksumAddress(token2),
                    'delta2': delta2,
                    'block_number': row['block_number'],
                    'poolId': row['poolId']
                })

        # Create DataFrame from the result
        result_df = pd.DataFrame(result)

        filtered_df = pd.merge(df_linearprices, result_df, on=['poolId', 'token1', 'token2'], how='inner')
        df_type = "linear"

        if filtered_df.empty:
            filtered_df = pd.merge(df_stableprices, result_df, on=['poolId', 'token1', 'token2'], how='inner')
            df_type = "stable"

        if filtered_df.empty:
            filtered_df = pd.merge(df_weighprices, result_df, on=['poolId', 'token1', 'token2'], how='inner')
            df_type = "weigh"

        last_row = filtered_df.groupby(['token1', 'token2']).last().reset_index()
        if last_row.empty == False:
            last_row.loc[last_row['token1'] == result_df['token1'], 'balance1'] += result_df['delta1']
            last_row.loc[last_row['token2'] == result_df['token2'], 'balance2'] += result_df['delta2']
            last_row['block_number'] = result_df['block_number']
            if df_type == "linear":
                last_row = last_row[['token1', 'token2', 'poolId', 'balance1', 'balance2', 'fees', 'lowers', 'uppers', 'block_number']]
                df_linearprices = df_linearprices.append(last_row, ignore_index=True)
            elif df_type == "stable":
                last_row = last_row[['token1', 'token2', 'poolId', 'balance1', 'balance2', 'fees', 'amplifiers', 'block_number']]
                df_stableprices = df_stableprices.append(last_row, ignore_index=True)
            elif df_type == "weigh":
                last_row = last_row[['token1', 'token2', 'poolId', 'balance1', 'balance2', 'fees', 'weight1', 'weight2', 'block_number']]
                df_weighprices = df_weighprices.append(last_row, ignore_index=True)

    n += 1
    if n % 1000 == 0:
        print(n)

df_linearprices.to_csv('balancer-priceslinear.csv', index=False)
df_stableprices.to_csv('balancer-pricesstable.csv', index=False)
df_weighprices.to_csv('balancer-pricesweigh.csv', index=False)