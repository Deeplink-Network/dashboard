'''
This module contains the main function for the dashboard, calling all the other relevant modules.
'''

# local imports
from pool_collector_v2 import get_latest_pool_data, collect_curve_pools, reformat_balancer_v1_pools, reformat_balancer_v2_pools
# third party imports
import logging
from constants import UNISWAP_V2, UNISWAP_V3, SUSHISWAP_V2, CURVE, BALANCER_V1, BALANCER_V2, DODO, PANCAKESWAP_V3
from heapq import merge
import json
import time
import string
import pandas as pd
import numpy as np
import os
import glob
import random


MAX_ORDERS = 20

DEX_LIST = (
    #UNISWAP_V2,
    UNISWAP_V3,
    SUSHISWAP_V2,
    CURVE,
    BALANCER_V1,
    BALANCER_V2,
    DODO,
    PANCAKESWAP_V3
)

DEX_METRIC_MAP = {
    UNISWAP_V2: 'reserveUSD',
    UNISWAP_V3: 'totalValueLockedUSD',
    SUSHISWAP_V2: 'liquidityUSD',
    CURVE: 'reserveUSD',
    BALANCER_V1: 'liquidity',
    BALANCER_V2: 'totalLiquidity',
    DODO: 'volumeUSD',
    PANCAKESWAP_V3: 'totalValueLockedUSD'
}

BLACKLISTED_TOKENS = [
    '0xd233d1f6fd11640081abb8db125f722b5dc729dc'  # Dollar Protocol
]

pool_dict = {}

if os.path.exists('test_results/pool_dict.json'):
    with open('test_results/pool_dict.json', 'r') as f:
        pool_dict = json.load(f)

pools = {
    exch: {
        'metric': DEX_METRIC_MAP[exch],
        'pools': [None] * 10_000
    } for exch in DEX_LIST
}

DEX_LIQUIDITY_METRIC_MAP = {
    UNISWAP_V2: 'reserveUSD',
    UNISWAP_V3: 'totalValueLockedUSD',
    SUSHISWAP_V2: 'liquidityUSD',
    CURVE: 'reserveUSD',
    BALANCER_V1: 'reserveUSD',
    BALANCER_V2: 'reserveUSD',
    DODO: 'reserveUSD',
    PANCAKESWAP_V3: 'totalValueLockedUSD'
}

MIN_LIQUIDITY_THRESHOLD = 500_000
MAX_LIQUIDITY_THRESHOLD = 2_000_000_000

# Initialize empty dictionary for storing token liquidity
token_liquidity = {}

async def refresh_pools(protocol: str):
    global pools
    global pool_dict
    
    if protocol == CURVE:
        new_curve_pools = await collect_curve_pools()
        for pool in new_curve_pools:
            token0_id = pool['token0']['id']
            token1_id = pool['token1']['id']
            key = f"{pool['id']}_{token0_id}_{token1_id}"
            pool_dict[key] = pool
            pools[protocol]['pools'].append(pool)
        return
    # get the latest pool data
    new_pools = []
    metric_to_use = pools[protocol]['metric']
    last_pool_metric = None
    for i in range(0, 1):
        for skip in (0, 1000, 2000, 3000, 4000, 5000):
            logging.info(
                f'getting pools {i*6000 + skip} to {i*6000 + skip + 1000}...')
            new_pools = await get_latest_pool_data(protocol=protocol, skip=skip, max_metric=last_pool_metric)
            if new_pools:
                if protocol == BALANCER_V1:
                    new_balancer_pools = reformat_balancer_v1_pools(new_pools)
                    for pool in new_balancer_pools:
                        token0_id = pool['token0']['id']
                        token1_id = pool['token1']['id']
                        key = f"{pool['id']}_{token0_id}_{token1_id}"
                        pool_dict[key] = pool
                        pools[protocol]['pools'].append(pool) 
                elif protocol == BALANCER_V2:
                    new_balancer_pools = reformat_balancer_v2_pools(new_pools)
                    for pool in new_balancer_pools:
                        token0_id = pool['token0']['id']
                        token1_id = pool['token1']['id']
                        key = f"{pool['id']}_{token0_id}_{token1_id}"
                        pool_dict[key] = pool
                        pools[protocol]['pools'].append(pool)
                else:
                    for pool in new_pools:
                        token0_id = pool['token0']['id']
                        token1_id = pool['token1']['id']
                        key = f"{pool['id']}_{token0_id}_{token1_id}"
                        pool_dict[key] = pool
                        pools[protocol]['pools'].append(pool)  

                last_pool = new_pools[-1]
                last_pool_metric = float(last_pool[metric_to_use])
                

def refresh_matrix():
    # print("Refreshing matrix...")
    global pool_dict
    # Calculate total liquidity for each token
    for pool in pool_dict.values():
        token0_id = pool['token0']['id']
        token1_id = pool['token1']['id']
        
        # Ignore tokens with punctuation in their symbol, band-aid fix for synthetic and irreputable tokens
        if any(char in string.punctuation for char in pool['token0']['symbol']):
            continue
        if any(char in string.punctuation for char in pool['token1']['symbol']):
            continue
        # Ignore Balancer_V1 and Balancer_V2 pools, another band-aid fix as their USD price is not accurate
        if pool['protocol'] == BALANCER_V1 or pool['protocol'] == BALANCER_V2 or pool['protocol'] == DODO:
            continue

        liquidity = float(pool[DEX_LIQUIDITY_METRIC_MAP[pool['protocol']]])
        reserve0 = float(pool['reserve0'])
        reserve1 = float(pool['reserve1'])
        total_reserve = reserve0 + reserve1

        # Calculate and add liquidity to respective tokens
        if token0_id in token_liquidity:
            try:
                token_liquidity[token0_id]['liquidity'] += liquidity * (reserve0 / total_reserve)
            except ZeroDivisionError:
                token_liquidity[token0_id]['liquidity'] += 0
        else:
            try:
                token_liquidity[token0_id] = {'symbol': pool['token0']['symbol'], 'liquidity': liquidity * (reserve0 / total_reserve)}
            except ZeroDivisionError:
                token_liquidity[token0_id] = {'symbol': pool['token0']['symbol'], 'liquidity': 0}
        if token1_id in token_liquidity:
            try:
                token_liquidity[token1_id]['liquidity'] += liquidity * (reserve1 / total_reserve)
            except ZeroDivisionError:
                token_liquidity[token1_id]['liquidity'] += 0
        else:
            try:
                token_liquidity[token1_id] = {'symbol': pool['token1']['symbol'], 'liquidity': liquidity * (reserve1 / total_reserve)}
            except ZeroDivisionError:
                token_liquidity[token1_id] = {'symbol': pool['token1']['symbol'], 'liquidity': 0}

    # sort token_liquidity by liquidity
    sorted_token_liquidity = dict(sorted(token_liquidity.items(), key=lambda item: item[1]['liquidity'], reverse=True))

    # drop any tokens with liquidity less than LIQUIDITY_THRESHOLD
    trimmed_sorted_token_liquidity = {}

    # Iterate over sorted_token_liquidity dictionary
    for k, v in sorted_token_liquidity.items():
        # If the liquidity is greater than the threshold, add it to the new dictionary
        if v['liquidity'] > MIN_LIQUIDITY_THRESHOLD and v['liquidity'] < MAX_LIQUIDITY_THRESHOLD:
            trimmed_sorted_token_liquidity[k] = v

    # Initialize a new dictionary for the trimmed pool_dict
    trimmed_pool_dict = {}

    # Iterate over the original pool_dict
    for pool_id, pool in pool_dict.items():
        # Get the IDs of the tokens in the current pool
        token0_id = pool['token0']['id']
        token1_id = pool['token1']['id']

        # If either of the tokens is in trimmed_sorted_token_liquidity, add the pool to trimmed_pool_dict
        if token0_id in trimmed_sorted_token_liquidity or token1_id in trimmed_sorted_token_liquidity:
            trimmed_pool_dict[pool_id] = pool

    trimmed_sorted_pools = sorted(trimmed_pool_dict.values(), key=lambda pool: float(pool[DEX_LIQUIDITY_METRIC_MAP[pool['protocol']]]), reverse=True)

    # Drop extreme pools, another band-aid fix
    # trimmed_sorted_pool_dict = {k: v for k, v in trimmed_sorted_pools.items() if float(v[DEX_LIQUIDITY_METRIC_MAP[v['protocol']]]) > MIN_LIQUIDITY_THRESHOLD and float(v[DEX_LIQUIDITY_METRIC_MAP[v['protocol']]]) < MAX_LIQUIDITY_THRESHOLD}

    trimmed_sorted_pools = list(filter(lambda pool: float(pool[DEX_LIQUIDITY_METRIC_MAP[pool['protocol']]]) > MIN_LIQUIDITY_THRESHOLD and float(pool[DEX_LIQUIDITY_METRIC_MAP[pool['protocol']]]) < MAX_LIQUIDITY_THRESHOLD, trimmed_sorted_pools))

    # Create a dictionary to store prices and identify tokens
    price_dict = {} # Dictionary for storing prices
    exchange_dict = {} # Dictionary for storing exchanges
    movement_dict_5m = {}  # Dictionary for storing price movement percentages for 5 minutes
    movement_dict_1h = {}  # Dictionary for storing price movement percentages for 1 hour
    movement_dict_24h = {}  # Dictionary for storing price movement percentages for 24 hours
    token_set = set() # Used to avoid duplicates
    TOKEN_SYMBOL_MAP = {} # Dictionary for storing token ids:symbols
    TOKEN_NAME_MAP = {} # Dictionary for storing token ids:names

    # Populate the dictionaries
    for pool in trimmed_sorted_pools:
        token0_id = pool['token0']['id']
        token1_id = pool['token1']['id']
        token0_symbol = pool['token0']['symbol']
        token1_symbol = pool['token1']['symbol']
        TOKEN_SYMBOL_MAP[token0_id] = token0_symbol
        TOKEN_SYMBOL_MAP[token1_id] = token1_symbol
        TOKEN_NAME_MAP[token0_id] = pool['token0']['name']
        TOKEN_NAME_MAP[token1_id] = pool['token1']['name']
        token_set.add(token0_id)
        token_set.add(token1_id)
        try:
            price0 = float(pool['token0'].get('priceUSD', 0))
            price1 = float(pool['token1'].get('priceUSD', 0))
            timestamp = time.time()  # Current time in seconds since the Epoch
            if price0 != 0 and price1 != 0:
                pair1 = (token0_id, token1_id)
                pair2 = (token1_id, token0_id)
                if pair1 not in price_dict:
                    price_dict[pair1] = []
                if pair2 not in price_dict:
                    price_dict[pair2] = []
                price_dict[pair1].append((timestamp, price1))
                price_dict[pair2].append((timestamp, price0))
                if pair1 not in exchange_dict:
                    exchange_dict[pair1] = []
                if pair2 not in exchange_dict:
                    exchange_dict[pair2] = []
                exchange_dict[pair1].append(pool['protocol'])
                exchange_dict[pair2].append(pool['protocol'])
        except Exception as e:
            print(e)

    movement_df_5m = pd.DataFrame(0, index=token_set, columns=token_set)  # DataFrame for storing price movement percentages
    movement_df_1h = pd.DataFrame(0, index=token_set, columns=token_set)  # DataFrame for storing 1-hour price movement percentages
    movement_df_24h = pd.DataFrame(0, index=token_set, columns=token_set)  # DataFrame for storing 24-hour price movement percentages

    # Compute the price movement percentage
    for pair, prices in price_dict.items():
        current_price = prices[-1][1]
        past_price_5m = None
        past_price_1h = None
        past_price_24h = None

        for timestamp, price in reversed(prices):
            if past_price_5m is None and timestamp <= time.time() - 60*5:  # Find the price 5 minutes ago
                past_price_5m = price
            if past_price_1h is None and timestamp <= time.time() - 60*60:  # Find the price 1 hour ago
                past_price_1h = price
            if past_price_24h is None and timestamp <= time.time() - 24*60*60:  # Find the price 24 hours ago
                past_price_24h = price
            if past_price_5m is not None and past_price_1h is not None and past_price_24h is not None:
                break

        movement_dict_5m[pair] = ((current_price - (past_price_5m or current_price)) / (past_price_5m or current_price)) * 100
        movement_dict_1h[pair] = ((current_price - (past_price_1h or current_price)) / (past_price_1h or current_price)) * 100
        movement_dict_24h[pair] = ((current_price - (past_price_24h or current_price)) / (past_price_24h or current_price)) * 100


    # Create an empty DataFrame for storing liquidity data
    liquidity_df = pd.DataFrame(0, index=token_set, columns=token_set) # DataFrame for storing liquidity
    # Create an empty DataFrame for storing volume data
    volume_df_24h = pd.DataFrame(0, index=token_set, columns=token_set)
    volume_df_1h = pd.DataFrame(0, index=token_set, columns=token_set)
    # Dict for storing which pools pairs can be found in
    pool_dict_for_pairs = {}

    # Populate the DataFrames
    for pool in trimmed_sorted_pools:
        token0_id = pool['token0']['id']
        token1_id = pool['token1']['id']

        if token0_id in TOKEN_SYMBOL_MAP and token1_id in TOKEN_SYMBOL_MAP:
            try:
                # Compute liquidity
                liquidity = float(pool[DEX_LIQUIDITY_METRIC_MAP[pool['protocol']]])
                liquidity_df.loc[token0_id, token1_id] += liquidity
                liquidity_df.loc[token1_id, token0_id] += liquidity

                # Compute volume
                volume_24h = float(pool['volume_24h'])
                volume_1h = float(pool['volume_1h'])
                volume_df_24h.loc[token0_id, token1_id] += volume_24h
                volume_df_24h.loc[token1_id, token0_id] += volume_24h
                volume_df_1h.loc[token0_id, token1_id] += volume_1h
                volume_df_1h.loc[token1_id, token0_id] += volume_1h
                
                # Compute price ratio
                price0 = float(pool['token0'].get('priceUSD', 0))
                price1 = float(pool['token1'].get('priceUSD', 0))

                if price0 != 0 and price1 != 0:
                    pair1 = (token0_id, token1_id)
                    pair2 = (token1_id, token0_id)
	
                    if pair1 not in pool_dict_for_pairs:
                        pool_dict_for_pairs[pair1] = []
                    if pair2 not in pool_dict_for_pairs:
                        pool_dict_for_pairs[pair2] = []
                    pool_dict_for_pairs[pair1].append(pool['id'])
                    pool_dict_for_pairs[pair2].append(pool['id'])

                    movement_df_5m.loc[pair1] = movement_dict_5m.get(pair1, 0)
                    movement_df_5m.loc[pair2] = movement_dict_5m.get(pair2, 0)
                    movement_df_1h.loc[pair1] = movement_dict_1h.get(pair1, 0)
                    movement_df_1h.loc[pair2] = movement_dict_1h.get(pair2, 0)
                    movement_df_24h.loc[pair1] = movement_dict_24h.get(pair1, 0)
                    movement_df_24h.loc[pair2] = movement_dict_24h.get(pair2, 0)

            except Exception as e:
                # print(f"Error with token pair ({TOKEN_SYMBOL_MAP[token0_id]}, {TOKEN_SYMBOL_MAP[token1_id]}): {e}")
                continue

    # Convert tuple keys to string
    str_price_dict = {str(k): v for k, v in price_dict.items()}

    # Save price_dict to data
    with open('data/price_dict.json', 'w') as f:
        json.dump(str_price_dict, f)

    # Create a DataFrame for average prices
    average_price_df = pd.DataFrame(np.nan, index=token_set, columns=token_set)
    movement_df_5m.replace(0, np.nan, inplace=True)  # Replace 0s with NaN for calculating percentage movements
    movement_df_1h.replace(0, np.nan, inplace=True)  # Replace 0s with NaN for calculating percentage movements
    movement_df_24h.replace(0, np.nan, inplace=True)  # Replace 0s with NaN for calculating percentage movements

    # Compute IQR for outlier detection for each pair
    trimmed_average_price_dict = {}
    limit = 1.5

    for k, v in price_dict.items():
        if v: # if there are prices
            Q1 = np.quantile(v, 0.25)
            Q3 = np.quantile(v, 0.75)
            IQR = Q3 - Q1

            # Define bounds for outliers
            lower_bound = Q1 - limit * IQR
            upper_bound = Q3 + limit * IQR

            # Trim outliers
            trimmed_prices = [price[1] for price in v if lower_bound <= price[1] <= upper_bound]
            # check if there are any trimmed prices using a.any()
            if trimmed_prices: # avoid division by zero
                trimmed_average_price_dict[k] = sum(trimmed_prices) / len(trimmed_prices)

    # Convert tuple keys to string
    str_trimmed_average_price_dict = {str(k): v for k, v in trimmed_average_price_dict.items()}

    # Populate the average_price_df using the trimmed_average_price_dict
    for (token0, token1), average_price in trimmed_average_price_dict.items():
        average_price_df.loc[token0, token1] = average_price

    # Save the trimmed average price dictionary to a file with a timestamp
    unix_timestamp_str = str(int(time.time()))
    trimmed_average_price_dict_path = f'data/trimmed_average_price_dict_{unix_timestamp_str}.json'
    with open(trimmed_average_price_dict_path, 'w') as f:
        json.dump(str_trimmed_average_price_dict, f)

    # Find the file that was created closest to 5 minutes ago
    filepaths = glob.glob('data/trimmed_average_price_dict_*.json')
    closest_filepath = None
    closest_diff = None

    for filepath in filepaths:
        file_timestamp_str = filepath.split('_')[-1].split('.')[0]
        file_timestamp = int(file_timestamp_str)
        diff = abs(time.time() - file_timestamp - 60*5)

        # Delete any files older than 25 hours
        if diff > 60*60*25:
            os.remove(filepath)

        if closest_diff is None or diff < closest_diff:
            closest_diff = diff
            closest_filepath = filepath

    if closest_filepath:
        with open(closest_filepath, 'r') as f:
            past_price_dict = json.load(f)
    else:
        past_price_dict = {}

    # Compute the price movement percentage using the current prices and the prices from 5 minutes ago
    for (token0, token1), current_price in trimmed_average_price_dict.items():
        past_price = past_price_dict.get(str((token0, token1)), current_price)  # default to current_price to avoid division by zero
        price_movement = ((current_price - past_price) / past_price) * 100
        movement_df_5m.loc[token0, token1] = price_movement

    # Compute total liquidity for each token
    all_ids = list(set(liquidity_df.columns).union(set(liquidity_df.index)))
    row_sums = liquidity_df.sum(axis=0).reindex(all_ids, fill_value=0)
    col_sums = liquidity_df.sum(axis=1).reindex(all_ids, fill_value=0)
    liquidity_totals = row_sums + col_sums

    # Sort by total liquidity
    sorted_ids = liquidity_totals.sort_values(ascending=False).index

    # Reindex DataFrames according to sorted liquidity
    liquidity_df = liquidity_df.reindex(index=sorted_ids, columns=sorted_ids)
    average_price_df = average_price_df.reindex(index=sorted_ids, columns=sorted_ids)
    movement_df_5m = movement_df_5m.reindex(index=sorted_ids, columns=sorted_ids)
    movement_df_1h = movement_df_1h.reindex(index=sorted_ids, columns=sorted_ids)
    movement_df_24h = movement_df_24h.reindex(index=sorted_ids, columns=sorted_ids)

    # Remove the diagonal values as they don't represent valid token pairs
    np.fill_diagonal(liquidity_df.values, np.nan)
    np.fill_diagonal(average_price_df.values, np.nan)
    np.fill_diagonal(movement_df_5m.values, np.nan)
    np.fill_diagonal(movement_df_1h.values, np.nan)
    np.fill_diagonal(movement_df_24h.values, np.nan)

    # replace all nans with None
    liquidity_df = liquidity_df.where(pd.notnull(liquidity_df), None)
    average_price_df = average_price_df.where(pd.notnull(average_price_df), None)
    movement_df_5m = movement_df_5m.where(pd.notnull(movement_df_5m), None)
    movement_df_1h = movement_df_1h.where(pd.notnull(movement_df_1h), None)
    movement_df_24h = movement_df_24h.where(pd.notnull(movement_df_24h), None)

    # Create a combined DataFrame
    combined_df = pd.DataFrame(index=liquidity_df.index, columns=liquidity_df.columns)

    for row_id in combined_df.index:
        for col_id in combined_df.columns:
            liquidity = liquidity_df.loc[row_id, col_id] if pd.notnull(liquidity_df.loc[row_id, col_id]) else 0
            avg_price = average_price_df.loc[row_id, col_id] if pd.notnull(average_price_df.loc[row_id, col_id]) else 0
            price_movement_5m = movement_df_5m.loc[row_id, col_id] if pd.notnull(movement_df_5m.loc[row_id, col_id]) else 0
            price_movement_1h = movement_df_1h.loc[row_id, col_id] if pd.notnull(movement_df_1h.loc[row_id, col_id]) else 0
            price_movement_24h = movement_df_24h.loc[row_id, col_id] if pd.notnull(movement_df_24h.loc[row_id, col_id]) else 0
            volume_24h = volume_df_24h.loc[row_id, col_id]
            volume_1h = volume_df_1h.loc[row_id, col_id]
            safety_score = random.randrange(0, 5)
            pair = {}
            pair[row_id] = {
                'id': row_id,  
                'symbol': TOKEN_SYMBOL_MAP[row_id],
                'name': TOKEN_NAME_MAP[row_id],
            }
            pair[col_id] = {
                'id': col_id,  
                'symbol': TOKEN_SYMBOL_MAP[col_id],
                'name': TOKEN_NAME_MAP[col_id],
            }

            combined_df.at[row_id, col_id] = {
                'pair': pair,
                'liquidity': liquidity,
                'average_price': avg_price,
                'price_movement_5m': price_movement_5m,
                'price_movement_1h': price_movement_1h,
                'price_movement_24h': price_movement_24h,
                'volume_1h': volume_1h,
                'volume_24h': volume_24h,
                'safety_score': safety_score,
                'exchanges': exchange_dict.get((row_id, col_id), []),
                'pools': list(set(pool_dict_for_pairs.get((row_id, col_id), [])))
            }
            # check if the cell is a diagonal
            if row_id == col_id:
                combined_df.at[row_id, col_id]['diagonal'] = True
                # set all values to 0
                combined_df.at[row_id, col_id]['liquidity'] = 0
                combined_df.at[row_id, col_id]['average_price'] = 0
                combined_df.at[row_id, col_id]['price_movement_5m'] = 0
                combined_df.at[row_id, col_id]['price_movement_1h'] = 0
                combined_df.at[row_id, col_id]['price_movement_24h'] = 0
                combined_df.at[row_id, col_id]['volume_24h'] = 0
                combined_df.at[row_id, col_id]['safety_score'] = 0
                combined_df.at[row_id, col_id]['exchanges'] = []
                combined_df.at[row_id, col_id]['pools'] = []
            else:
                combined_df.at[row_id, col_id]['diagonal'] = False
            # if the pool is not found in any pools or exchanges then any nonzero data is invalid
            if combined_df.at[row_id, col_id]['pools'] == [] or combined_df.at[row_id, col_id]['exchanges'] == []:
                combined_df.at[row_id, col_id]['liquidity'] = 0
                combined_df.at[row_id, col_id]['average_price'] = 0
                combined_df.at[row_id, col_id]['price_movement_5m'] = 0
                combined_df.at[row_id, col_id]['price_movement_1h'] = 0
                combined_df.at[row_id, col_id]['price_movement_24h'] = 0
                combined_df.at[row_id, col_id]['volume_24h'] = 0
                combined_df.at[row_id, col_id]['safety_score'] = 0
    # combined_df.to_csv('data/combined_df.csv')
    combined_df.to_json('data/combined_df_liquidity.json', orient='split')

    combined_df['mean_avg_price'] = combined_df.applymap(lambda x: x['average_price']).mean(axis=1)
    combined_df.sort_values(by=['mean_avg_price'], ascending=False, inplace=True)
    del combined_df['mean_avg_price']
    combined_df.to_json('data/combined_df_average_price.json', orient='split')

    combined_df['max_volume_24h'] = combined_df.applymap(lambda x: x['volume_24h']).max(axis=1)
    combined_df.sort_values(by=['max_volume_24h'], ascending=False, inplace=True)
    del combined_df['max_volume_24h']
    combined_df.to_json('data/combined_df_volume_24h.json', orient='split')



def get_matrix_segment(df, x, y, i, j):
    segment = df.iloc[x:y, i:j]
    segment_dict = segment.to_dict(orient="split")
    return segment_dict


def filter_matrix_by_asset(df, asset_id):
    # Filter rows where index matches asset_id or any column has asset_id
    filtered_df = df[df.apply(lambda row: (row.name == asset_id) or (asset_id in row.values), axis=1)]
    
    # Also include the column with the asset_id
    if asset_id in df.columns:
        filtered_df = pd.concat([filtered_df, df[[asset_id]]], axis=1)

    # Drop duplicates and handle dictionaries
    filtered_df_str = filtered_df.applymap(lambda d: json.dumps(d, sort_keys=True) if isinstance(d, dict) else d)
    filtered_df_str = filtered_df_str.drop_duplicates()
    filtered_df = filtered_df_str.applymap(lambda s: json.loads(s) if isinstance(s, str) else s)
    
    # Drop rows with all NaN values
    filtered_df = filtered_df.dropna()
    
    # Convert to dict
    segment_dict = filtered_df.to_dict(orient='split')
    
    segment = {
        'data': segment_dict['data'],
        'index': segment_dict['index'],
        'columns': segment_dict['columns']
    }

    return segment
