# Installation Instructions: https://web3py.readthedocs.io/en/latest/quickstart.html#installation
import csv
from web3 import Web3

# Initialize a Web3.py instance
w3 = Web3(Web3.HTTPProvider(''))

def get_fee_history(w3, block_count, block_number, reward_percentiles=None):
    fee_history = w3.eth.fee_history(block_count, block_number, reward_percentiles)
    return fee_history

def save_to_csv(filename, data):
    with open(filename, mode='a', newline='') as file:
        writer = csv.writer(file)
      #  writer.writerow(['Block Number', 'BaseFeePerGas', 'Reward', 'GasFee'])
        for entry in data:
            writer.writerow(entry)

def process_fee_history(fee_history, start_block, end_block):
    result = []

    for i, block in enumerate(range(fee_history['oldestBlock'], end_block)):
        print(block)
        if start_block <= block <= end_block:
            if i < len(fee_history['baseFeePerGas']):  # Exclude the last value in baseFeePerGas
                base_fee = fee_history['baseFeePerGas'][i]
                reward = fee_history['reward'][i][0]
                gas_fee = base_fee + reward
                result.append([block, base_fee, reward, gas_fee])

    return result

block_count = 1024
reward_percentiles = [50]
filename = 'fee_history_data.csv'
start_block = 15537394
end_block = 18083778

current_block = start_block + 1023

while current_block < end_block + 1023:
    print(current_block)
    fee_history = get_fee_history(w3, block_count, current_block, reward_percentiles)
    # Process fee_history and save to CSV
    data_to_save = process_fee_history(fee_history, current_block -1023, current_block)
    save_to_csv(filename, data_to_save)
    current_block += (block_count-1)

#univ2 152809
#univ3 184523
#balancer 196625



