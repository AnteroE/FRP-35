import pandas as pd
from web3 import Web3
import requests
import json

w3 = Web3(Web3.HTTPProvider(""))

df = pd.read_csv('balancer-frequent-addresses.csv')
df['poolId'] = df['poolId'].str.slice(stop=42)

poolIds = df['poolId'].tolist()

first_keys = []
for i in poolIds:
    url = f"https://api.etherscan.io/api?module=contract&action=getsourcecode&address={i}&apikey="

    response = requests.get(url)
    json_data = response.json()

    # Check if status is "1" (success)
    try:
        # Extract the source code from the JSON data
        source_code = json_data['result'][0]['SourceCode']

        # Extract and clean the Solidity source code
        corrected_source_code_string = source_code[1:-1]  # Remove the enclosing double curly braces
        solidity_code = json.loads(corrected_source_code_string)['sources']

        first_key = list(solidity_code.keys())[0]

        # Print or use the Solidity source code
        print(first_key)
        first_keys.append(first_key)
    except:
        first_keys.append("error")

df['first_key'] = first_keys

df.to_csv('balancer-type.csv', index=False)