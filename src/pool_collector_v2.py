'''
This file handles the querying of liquidity pool data from various DEXs.
'''

# locla utility imports
from constants import UNISWAP_V2, UNISWAP_V3, SUSHISWAP_V2, CURVE, BALANCER_V1, BALANCER_V2, DODO, PANCAKESWAP_V3

# standard library imports
import asyncio
import aiohttp
import json
from itertools import combinations
import logging
import requests
import os
import dotenv
import datetime

if os.path.exists(".env"):
    dotenv.load_dotenv(".env")
    
THE_GRAPH_KEY = os.getenv("THE_GRAPH_KEY")

# Collect the list of bad_tokens
with open(r'data/bad_tokens.json') as f:
    BAD_TOKENS = json.load(f)
    BAD_TOKEN_SYMS = [token['symbol'] for token in BAD_TOKENS['tokens']]

UNISWAPV2_ENDPOINT = "https://api.thegraph.com/subgraphs/name/uniswap/uniswap-v2"
UNISWAPV3_ENDPOINT = "https://api.thegraph.com/subgraphs/name/uniswap/uniswap-v3"
SUSHISWAPV2_ENDPOINT = "https://api.thegraph.com/subgraphs/name/sushi-v2/sushiswap-ethereum"
CURVE_ENDPOINTS = [
    "https://api.curve.fi/api/getPools/ethereum/main",
    "https://api.curve.fi/api/getPools/ethereum/crypto",
]
CURVE_SUBGRAPH_ENDPOINT = f"https://gateway.thegraph.com/api/{THE_GRAPH_KEY}/subgraphs/id/4yx4rR6Kf8WH4RJPGhLSHojUxJzRWgEZb51iTran1sEG"
BALANCER_V1_ENDPOINT = "https://api.thegraph.com/subgraphs/name/balancer-labs/balancer"
BALANCER_V2_ENDPOINT = "https://api.thegraph.com/subgraphs/name/balancer-labs/balancer-v2"
DODO_ENDPOINT = "https://api.thegraph.com/subgraphs/name/dodoex/dodoex-v2"
PANCAKESWAP_V3_ENDPOINT = "https://api.thegraph.com/subgraphs/name/pancakeswap/exchange-v3-eth"

def uniswap_v2_query(X: int, skip: int, max_metric: float, is_hourly: bool):
    query_type = "pairHourDatas" if is_hourly else "pairDayDatas"
    if query_type == "pairHourDatas":
        timestamp_term = "hourStartUnix"
        volume_term = "hourlyVolumeUSD"
    else:
        timestamp_term = "date"
        volume_term = "volumeUSD"
    
    if not max_metric:
        return f"""
        {{
          {query_type}(orderBy: {timestamp_term}, orderDirection: desc, first: 1) {{
            {timestamp_term}
            {volume_term}
          }}
          pairs(first: {X}, orderBy: reserveUSD, orderDirection: desc, skip: {skip}) {{
            id
            reserveUSD
            reserve0
            reserve1
            token0Price
            token1Price
            token0 {{
              id
              symbol
              name
              decimals
            }}
            token1 {{
              id
              symbol
              name
              decimals
            }}
          }}
        }}
        """
    else:
        return f"""
        {{
          {query_type}(orderBy: {timestamp_term}, orderDirection: desc, first: 1) {{
            {timestamp_term}
            {volume_term}
          }}
          pairs(first: {X}, orderBy: reserveUSD, orderDirection: desc, skip: {skip}, where: {{ reserveUSD_lt: {max_metric} }}) {{
            id
            reserveUSD
            reserve0
            reserve1
            token0Price
            token1Price
            token0 {{
              id
              symbol
              name
              decimals
            }}
            token1 {{
              id
              symbol
              name
              decimals
            }}
          }}
        }}
        """
        
def sushiswap_v2_query(X: int, skip: int, max_metric: float, is_hourly: bool):
    query_type = "pairHourSnapshots" if is_hourly else "pairDaySnapshots"
    
    if not max_metric:
        return f"""
        {{
          {query_type}(orderBy: date, orderDirection: desc, first: 1) {{
            date
            volumeUSD
          }}
          pairs(first: {X}, orderBy: liquidityUSD, orderDirection: desc, skip: {skip}) {{
            id
            liquidityUSD
            reserve0
            reserve1
            token0Price
            token1Price
            token0 {{
              id
              symbol
              name
              decimals
            }}
            token1 {{
              id
              symbol
              name
              decimals
            }}
          }}
        }}
        """
    else:
        return f"""
        {{
          {query_type}(orderBy: date, orderDirection: desc, first: 1) {{
            volumeUSD
            date
          }}
          pairs(first: {X}, orderBy: liquidityUSD, orderDirection: desc, skip: {skip}, where: {{ liquidityUSD_lt: {max_metric} }}) {{
            id
            liquidityUSD
            reserve0
            reserve1
            token0Price
            token1Price
            token0 {{
              id
              symbol
              name
              decimals
            }}
            token1 {{
              id
              symbol
              name
              decimals
            }}
          }}
        }}
        """

def uniswap_v3_query(X: int, skip: int, max_metric: float, is_hourly: bool):
    if not max_metric:
        return f"""
        {{
          pools(first: {X}, skip: {skip}, orderDirection: desc, orderBy: totalValueLockedUSD) {{
            token0 {{
              id
              symbol
              name
              decimals
            }}
            token1 {{
              symbol
              name
              id
              decimals
            }}
            id
            totalValueLockedToken0
            totalValueLockedToken1
            totalValueLockedUSD
            volumeUSD
            token0Price
            token1Price
            liquidity
            sqrtPrice
            poolDayData(first: 1, orderBy: date, orderDirection: desc) {{
              date
              volumeUSD
            }}
            poolHourData(first: 1, orderBy: periodStartUnix, orderDirection: desc) {{
              periodStartUnix
              volumeUSD
            }}
          }}
        }}
        """
    else:
        return f"""
        {{
          pools(first: {X}, skip: {skip}, orderDirection: desc, orderBy: totalValueLockedUSD, where: {{ totalValueLockedUSD_lt: {max_metric} }}) {{
            token0 {{
              id
              symbol
              name
              decimals
            }}
            token1 {{
              symbol
              name
              id
              decimals
            }}
            id
            totalValueLockedToken0
            totalValueLockedToken1
            totalValueLockedUSD
            volumeUSD
            token0Price
            token1Price
            liquidity
            sqrtPrice
            poolDayData(first: 1, orderBy: date, orderDirection: desc) {{
              date
              volumeUSD
            }}
            poolHourData(first: 1, orderBy: periodStartUnix, orderDirection: desc) {{
              periodStartUnix
              volumeUSD
            }}
          }}
        }}
        """

def pancakeswap_v3_query(X: int, skip: int, max_metric: float, is_hourly: bool):
    if not max_metric:
        return f"""
        {{
        pools(first: {X}, skip: {skip}, orderDirection: desc, orderBy: totalValueLockedUSD) {{
            token0 {{
            id
            symbol
            name
            decimals
            }}
            token1 {{
            symbol
            name
            id
            decimals
            }}
            id
            totalValueLockedToken0
            totalValueLockedToken1
            totalValueLockedUSD
            liquidity
            token0Price
            token1Price
            feeTier
            sqrtPrice
            volumeUSD
            poolHourData(first: 1, orderBy: periodStartUnix, orderDirection: desc) {{
                volumeUSD
            }}
            poolDayData(first: 1, orderBy: date, orderDirection: desc) {{
                volumeUSD

        }}
        }}
        }}
        """
    else:
        return f"""
        {{
        pools(first: {X}, skip: {skip}, orderDirection: desc, orderBy: totalValueLockedUSD, where: {{totalValueLockedUSD_lt: {max_metric}}}) {{
            token0 {{
            id
            symbol
            name
            decimals
            }}
            token1 {{
            symbol
            id
            decimals
            }}
            id
            totalValueLockedToken0
            totalValueLockedToken1
            totalValueLockedUSD
            liquidity
            token0Price
            token1Price
            feeTier
            sqrtPrice
            volumeUSD
            poolHourData(first: 1, orderBy: periodStartUnix, orderDirection: desc) {{
                volumeUSD
            }}
            poolDayData(first: 1, orderBy: date, orderDirection: desc) {{
                volumeUSD
        }}
        }}
        }}
        """

async def collect_dodo_data():
    res = []
    
    async with aiohttp.ClientSession() as session:

            # Fetch the first 1000 pools
            pairs_query = """
            {
              pairs(first: 1000, orderBy: volumeUSD, orderDirection: desc) {
                baseReserve
                baseToken {
                  totalSupply
                  decimals
                  id
                  name
                  usdPrice
                  symbol
                }
                i
                id
                feeUSD
                feeBase
                feeQuote
                k
                lastTradePrice
                quoteReserve
                quoteToken {
                  totalSupply
                  decimals
                  id
                  name
                  usdPrice
                  symbol
                }
                volumeUSD
                type
              }
            }
            """
            async with session.post(DODO_ENDPOINT, json={'query': pairs_query}) as response:
                obj = await response.json()
                pairs_data = obj.get('data', {}).get('pairs', [])
                
            # Fetch the remaining pools using pagination and id_gte
            id_gte = pairs_data[-1]['id'] if pairs_data else ""
            remaining_pairs_query = f"""
            {{
              pairs(first: 1000, orderBy: volumeUSD, orderDirection: desc, where: {{ id_gte: "{id_gte}" }}) {{
                baseReserve
                baseToken {{
                  totalSupply
                  decimals
                  id
                  name
                  usdPrice
                  symbol
                }}
                i
                id
                feeUSD
                feeBase
                feeQuote
                k
                lastTradePrice
                quoteReserve
                quoteToken {{
                  totalSupply
                  decimals
                  id
                  name
                  usdPrice
                  symbol
                }}
                volumeUSD
                type
              }}
            }}
            """
            async with session.post(DODO_ENDPOINT, json={'query': remaining_pairs_query}) as response:
                obj = await response.json()
                remaining_pairs_data = obj.get('data', {}).get('pairs', [])
            
            # Fetch pairHourDatas
            hour_gte = int((datetime.datetime.now() - datetime.timedelta(hours=24)).timestamp())
            pair_hour_datas_query = f"""
            {{
              pairHourDatas(orderBy: pair__volumeUSD, orderDirection: desc, where: {{ hour_gte: {hour_gte} }}, first: 1000) {{
                hour
                volumeUSD
                pairAddress
              }}
            }}
            """
            async with session.post(DODO_ENDPOINT, json={'query': pair_hour_datas_query}) as response:
                obj = await response.json()
                pair_hour_datas = obj.get('data', {}).get('pairHourDatas', [])
            
            # Fetch pairDayDatas
            date_gte = int((datetime.datetime.now() - datetime.timedelta(days=7)).timestamp())
            pair_day_datas_query = f"""
            {{
              pairDayDatas(orderBy: pair__volumeUSD, orderDirection: desc, where: {{ date_gte: {date_gte} }}, first: 1581) {{
                date
                volumeUSD
                pairAddress
              }}
            }}
            """
            async with session.post(DODO_ENDPOINT, json={'query': pair_day_datas_query}) as response:
                obj = await response.json()
                pair_day_datas = obj.get('data', {}).get('pairDayDatas', [])
            
            # Combine the data from all queries
            pool_data = pairs_data + remaining_pairs_data
            volume_data = {pair['pairAddress']: pair['volumeUSD'] for pair in pair_hour_datas + pair_day_datas}
            
            # Process the pool data and map volumes
            for pool in pool_data:
                    
                pair_address = pool['id']
                volume = volume_data.get(pair_address, "0")
                
                if pool['baseToken']['usdPrice'] is None or pool['quoteToken']['usdPrice'] is None:
                    continue
                
                decimals0 = int(pool['baseToken']['decimals'])
                decimals1 = int(pool['quoteToken']['decimals'])
                balance0 = float(pool['baseReserve'])
                balance1 = float(pool['quoteReserve'])
                
                new_pair = {}
                new_pair['id'] = pair_address.lower()
                new_pair['reserve0'] = balance0 
                new_pair['reserve1'] = balance1 
                new_pair['volume_24h'] = float(volume) if volume else 0
                new_pair['volume_1h'] = None  # DODO does not provide hourly volume data
                new_pair['token0'] = {
                    'id': pool['baseToken']['id'].lower(),
                    'symbol': pool['baseToken']['symbol'],
                    'name': pool['baseToken']['name'],
                    'decimals': decimals0,
                    'priceUSD': pool['baseToken']['usdPrice']
                }
                new_pair['token1'] = {
                    'id': pool['quoteToken']['id'].lower(),
                    'symbol': pool['quoteToken']['symbol'],
                    'name': pool['quoteToken']['name'],
                    'decimals': decimals1,
                    'priceUSD': pool['quoteToken']['usdPrice']
                }
                new_pair['token0Price'] = float(pool['quoteToken']['usdPrice']) / float(pool['baseToken']['usdPrice']) if float(pool['baseToken']['usdPrice']) != 0 else 0
                new_pair['token1Price'] = float(pool['baseToken']['usdPrice']) / float(pool['quoteToken']['usdPrice']) if float(pool['quoteToken']['usdPrice']) != 0 else 0
                new_pair['reserveUSD'] = float(new_pair['reserve0']) * float(new_pair['token0']['priceUSD']) + float(new_pair['reserve1']) * float(new_pair['token1']['priceUSD'])
                new_pair['protocol'] = DODO
                new_pair['dangerous'] = new_pair['token0']['symbol'] in BAD_TOKEN_SYMS or new_pair['token1']['symbol'] in BAD_TOKEN_SYMS
                new_pair['type'] = pool['type']
                new_pair['volumeUSD'] = pool['volumeUSD']
                
                res.append(new_pair)
            
            # remove any pairs in res where type = VIRTUAL
            res = [pair for pair in res if pair['type'] != 'VIRTUAL']

    return res

async def fetch_and_calculate_curve_volumes(retries=10, backoff_factor=1):
    # Define the query
    query = """
    {
      pools(first: 1000, orderBy: id, orderDirection: desc) {
        id
        coins {
          balance
        }
        dailyVolumes(first: 1, orderBy: timestamp, orderDirection: desc) {
          volume
        }
        hourlyVolumes(first: 1, orderBy: timestamp, orderDirection: desc) {
          volume
        }
      }
    }
    """

    # Request the data
    for attempt in range(retries):
        try:
            async with aiohttp.ClientSession() as session:
                async with session.post(CURVE_SUBGRAPH_ENDPOINT, json={'query': query}) as response:
                    data = await response.json()
                    print(f'enpoint: {CURVE_SUBGRAPH_ENDPOINT}')
                    print(data)
                    print()
                    break
        except Exception as e:
            wait_time = backoff_factor * (2 ** attempt)
            # print(f"Error occurred: {e}, retrying in {wait_time} seconds...")
            await asyncio.sleep(wait_time)
    else:
        # print("Failed to fetch data from subgraph after multiple attempts")
        return {}

    # Map pool id to its volumes
    volume_data = {}
    for pool in data.get('data', {}).get('pools', []):
        total_balance = sum(float(coin['balance']) for coin in pool['coins'])
        
        daily_volume = float(pool['dailyVolumes'][0]['volume']) if pool['dailyVolumes'] else None
        hourly_volume = float(pool['hourlyVolumes'][0]['volume']) if pool['hourlyVolumes'] else None

        volume_data[pool['id']] = (daily_volume, hourly_volume, total_balance)

    return volume_data

async def collect_curve_pools():
    # print('collecting data from curve...')
    res = []
    volume_data = await fetch_and_calculate_curve_volumes()
    
    async with aiohttp.ClientSession() as session:
        for endpoint in CURVE_ENDPOINTS:
            # print(f"Processing endpoint: {endpoint}")
            async with session.get(endpoint) as response:
                obj = await response.json()
                data = obj['data']['poolData']
                
                for pool in data:
                    # print(f"Processing pool: {pool['address']}")
                    try:
                        if pool['address'].lower() not in volume_data:
                            # print(f"Missing volume data for pool: {pool['address']}")
                            continue
                        # else:
                            # print(f"Found volume data for pool: {pool['address']}")

                        daily_volume, hourly_volume, total_balance = volume_data[pool['address'].lower()]
                        pairs = combinations(pool['coins'], 2)
                        
                        for pair in pairs:
                            # Check if either usdPrice is None and skip this pair if true
                            if pair[0]['usdPrice'] is None or pair[1]['usdPrice'] is None:
                                continue
                                
                            decimals0 = int(pair[0]['decimals'])
                            decimals1 = int(pair[1]['decimals'])
                            balance0 = int(pair[0]['poolBalance'])
                            balance1 = int(pair[1]['poolBalance'])

                            pair_balance = balance0 + balance1
                            pair_proportion = pair_balance / total_balance if total_balance != 0 else 0

                            new_pair = {}
                            new_pair['id'] = pool['address'].lower()
                            new_pair['reserve0'] = balance0 / 10**decimals0
                            new_pair['reserve1'] = balance1 / 10**decimals1
                            new_pair['volume_24h'] = daily_volume * pair_proportion if daily_volume != None else None
                            new_pair['volume_1h'] = hourly_volume * pair_proportion if hourly_volume != None else None

                            new_pair['token0'] = {
                                'id': pair[0]['address'].lower(),
                                'symbol': pair[0].get('symbol'),
                                'name': pair[0].get('name'), 
                                'decimals': decimals0,
                                'priceUSD': pair[0].get('usdPrice')
                            }
                            new_pair['token1'] = {
                                'id': pair[1]['address'].lower(),
                                'symbol': pair[1].get('symbol'),
                                'name': pair[1].get('name'),
                                'decimals': decimals1,
                                'priceUSD': pair[1].get('usdPrice')
                            }


                            new_pair['token0Price'] = pair[1]['usdPrice'] / pair[0]['usdPrice'] if pair[0]['usdPrice'] != 0 else 0
                            new_pair['token1Price'] = pair[0]['usdPrice'] / pair[1]['usdPrice'] if pair[1]['usdPrice'] != 0 else 0
                            new_pair['reserveUSD'] = new_pair['reserve0'] * pair[0]['usdPrice'] + new_pair['reserve1'] * pair[1]['usdPrice']
                            new_pair['protocol'] = CURVE
                            new_pair['dangerous'] = new_pair['token0']['symbol'] in BAD_TOKEN_SYMS or new_pair['token1']['symbol'] in BAD_TOKEN_SYMS

                            res.append(new_pair)
                    except Exception as e:
                        print(f'Error occurred: {e}')
                        break
    return res

def balancer_v1_query(X: int, skip: int, max_metric: float, is_hourly: bool):
    timestamp_24h_ago = int((datetime.datetime.now() - datetime.timedelta(hours=24)).timestamp())
    
    if not max_metric:
        return f"""
        {{
          pools(first: {X}, orderBy: liquidity, orderDirection: desc, skip: {skip}) {{
            id
            liquidity
            swapFee
            tokensList
            tokens(orderBy: address) {{
              address
              balance
              symbol
              name
              denormWeight
            }}
            swaps(first: 1000, orderBy: timestamp, orderDirection: desc, where: {{ timestamp_gte: {timestamp_24h_ago} }}) {{
              timestamp
              value
              tokenIn
              tokenOut
              tokenAmountIn
              tokenAmountOut
            }}
          }}
        }}
        """
    else:
        return f"""
        {{
          pools(first: {X}, orderBy: liquidity, orderDirection: desc, skip: {skip}, where: {{ liquidity_lt: {max_metric} }}) {{
            id
            liquidity
            swapFee
            tokensList
            tokens(orderBy: address) {{
              address
              balance
              symbol
              name
              denormWeight
            }}
            swaps(first: 1000, orderBy: timestamp, orderDirection: desc, where: {{ timestamp_gte: {timestamp_24h_ago} }}) {{
              timestamp
              value
              tokenIn
              tokenOut
              tokenAmountIn
              tokenAmountOut
            }}
          }}
        }}
        """


def fetch_balancer_v1_token_prices(token_ids, missing_ids=None):
    # GraphQL endpoint
    url = BALANCER_V1_ENDPOINT

    # GraphQL query for token prices
    query = """
    query ($tokenIds: [String!]) {
      tokenPrices(where: {id_in: $tokenIds}) {
        id
        price
      }
    }
    """

    def chunks(lst, n):
        """Yield successive n-sized chunks from lst."""
        for i in range(0, len(lst), n):
            yield lst[i:i + n]

    token_prices = {}
    chunk_size = 50
    for token_chunk in chunks(token_ids, chunk_size):
        # Fetch token prices
        response = requests.post(url, json={'query': query, 'variables': {'tokenIds': token_chunk}})
        prices = json.loads(response.text)["data"]["tokenPrices"]
        token_prices.update({price["id"]: float(price["price"]) for price in prices})

    # Check if there are any missing token IDs and attempt to fetch their prices
    if missing_ids:
        missing_prices = fetch_balancer_v1_token_prices(missing_ids)
        token_prices.update(missing_prices)

    # If any token_ids are still missing, set their price to 0
    for token_id in token_ids:
        if token_id not in token_prices:
            token_prices[token_id] = 0.0

    return token_prices

def reformat_balancer_v1_pools(pool_list, swaps):
    balancer_pool_tokens = set()
    for pool in pool_list:
        for token in pool['tokens']:
            balancer_pool_tokens.add(token['address'])
    
    token_prices = fetch_balancer_v1_token_prices(list(balancer_pool_tokens))

    all_reformatted_pools = []
    for pool in pool_list:
        reformatted_pools = reformat_balancer_v1_pool(pool, token_prices, swaps)
        all_reformatted_pools.extend(reformatted_pools)
    return all_reformatted_pools


def reformat_balancer_v1_pool(pool, token_prices, swaps):
    token_combinations = list(combinations(pool['tokens'], 2))
    reformatted_pools = []

    for combination in token_combinations:
        token0 = combination[0]
        token1 = combination[1]

        new_pair = {
            'id': pool['id'],
            'liquidity': pool['liquidity'],
            'swapFee': pool['swapFee'],
            'reserve0': token0['balance'],
            'reserve1': token1['balance'],
            'token0': {
                'id': token0['address'],
                'symbol': token0['symbol'],
                'name': token0['name'],
                'denormWeight': token0['denormWeight'],
            },
            'token1': {
                'id': token1['address'],
                'symbol': token1['symbol'],
                'name': token1['name'],
                'denormWeight': token1['denormWeight']
            },
            'protocol': 'Balancer_V1',
        }

        try:
            # Calculate 24-hour volume
            timestamp_24h_ago = int((datetime.datetime.now() - datetime.timedelta(hours=24)).timestamp())
            volume_24h = sum(
                float(swap['value']) for swap in swaps
                if swap['timestamp'] >= timestamp_24h_ago
                and swap['tokenIn'] in (token0['address'], token1['address'])
                and swap['tokenOut'] in (token0['address'], token1['address'])
            )

            new_pair['volume_24h'] = volume_24h

            # Calculate 1-hour volume
            timestamp_1h_ago = int((datetime.datetime.now() - datetime.timedelta(hours=1)).timestamp())
            volume_1h = sum(
                float(swap['value']) for swap in swaps
                if swap['timestamp'] >= timestamp_1h_ago
                and swap['tokenIn'] in (token0['address'], token1['address'])
                and swap['tokenOut'] in (token0['address'], token1['address'])
            )

            new_pair['volume_1h'] = volume_1h

            # Derive USD prices from token_prices
            token0_price = token_prices.get(token0['address'], 0.0)
            token1_price = token_prices.get(token1['address'], 0.0)
            new_pair['token0']['priceUSD'] = token0_price
            new_pair['token1']['priceUSD'] = token1_price

        except KeyError as e:
            new_pair['volume_24h'] = None
            new_pair['volume_1h'] = None
            new_pair['token0']['priceUSD'] = None
            new_pair['token1']['priceUSD'] = None

        reformatted_pools.append(new_pair)

    return reformatted_pools

def balancer_v2_query(X: int, skip: int, max_metric: float = None, is_hourly: bool = False):
    max_metric_filter = f', where: {{ pool__totalLiquidity_lt: {max_metric} }}' if max_metric else ''

    return f"""
    {{
      poolSnapshots(first: {X}, orderBy: pool__totalLiquidity, orderDirection: desc, skip: {skip}{max_metric_filter}) {{
        swapVolume
        timestamp
        pool {{
          address
          swapFee
          tokensList
          totalLiquidity
          tokens {{
            address
            balance
            symbol
            name
            weight
            token {{
              totalBalanceUSD
              latestUSDPrice
              latestUSDPriceTimestamp
            }}
          }}
          swaps(orderBy: timestamp, orderDirection: desc, where: {{ timestamp_gte: {int(datetime.datetime.now().timestamp()) - 24 * 60 * 60} }}) {{
            valueUSD
            tokenIn
            tokenOut
          }}
        }}
      }}
    }}
    """

def reformat_balancer_v2_pools(pool_list):
    ''' Reformats a list of Balancer V2 pools into the uniswap/sushiswap format. '''

    def calculate_price_usd(token_amount_in, token_amount_out, value_usd):
        try:
            price_usd = float(value_usd) / (float(token_amount_in) or float(token_amount_out))
        except ZeroDivisionError:
            price_usd = None
        return price_usd

    def calculate_reserve_usd(total_liquidity, reserve0, reserve1, price_usd0, price_usd1):
        try:
            proportion0 = float(reserve0) / (float(reserve0) + float(reserve1))
            proportion1 = float(reserve1) / (float(reserve0) + float(reserve1))
            reserve_usd = float(total_liquidity) * (float(price_usd0) * proportion0 + float(price_usd1) * proportion1)
        except ZeroDivisionError:
            reserve_usd = 0.0
        return reserve_usd

    all_reformatted_pools = []
    for snapshot in pool_list:
        pool = snapshot['pool']
        reformatted_pools = []
        token_prices = {}

        # Extract token prices
        for token in pool['tokens']:
            token_prices[token['address']] = token['token']['latestUSDPrice']

        token_combinations = list(combinations(pool['tokens'], 2))
        for combination in token_combinations:
            token0 = combination[0]
            token1 = combination[1]
            new_pair = {
                'id': pool['address'],
                'totalLiquidity': pool['totalLiquidity'],
                'swapFee': pool['swapFee'],
                'reserve0': token0['balance'],
                'reserve1': token1['balance'],
                'token0': {
                    'id': token0['address'],
                    'symbol': token0['symbol'],
                    'name': token0['name'],
                    'weight': token0['weight'],
                    'totalBalanceUSD': token0['token']['totalBalanceUSD'],
                    'priceUSD': token0['token']['latestUSDPrice'],
                },
                'token1': {
                    'id': token1['address'],
                    'symbol': token1['symbol'],
                    'name': token1['name'],
                    'weight': token1['weight'],
                    'totalBalanceUSD': token1['token']['totalBalanceUSD'],
                    'priceUSD': token1['token']['latestUSDPrice'],
                },
                'protocol': 'Balancer_V2',
            }

            new_pair['dangerous'] = new_pair['token0']['symbol'] in BAD_TOKEN_SYMS or new_pair['token1']['symbol'] in BAD_TOKEN_SYMS

            # Calculate priceUSD if it's None
            if new_pair['token0']['priceUSD'] is None:
                new_pair['token0']['priceUSD'] = calculate_price_usd(token0['tokenAmountIn'], token0['tokenAmountOut'], swap['valueUSD'])
            if new_pair['token1']['priceUSD'] is None:
                new_pair['token1']['priceUSD'] = calculate_price_usd(token1['tokenAmountIn'], token1['tokenAmountOut'], swap['valueUSD'])

            # Calculate reserveUSD
            new_pair['reserveUSD'] = calculate_reserve_usd(new_pair['totalLiquidity'], new_pair['reserve0'], new_pair['reserve1'], new_pair['token0']['priceUSD'], new_pair['token1']['priceUSD'])

            # Calculate 24-hour volume
            volume_24h = sum(
                float(swap['valueUSD']) for swap in pool['swaps']
                if swap['tokenIn'] in (token0['address'], token1['address'])
                and swap['tokenOut'] in (token0['address'], token1['address'])
            )

            # Calculate 1-hour volume
            timestamp_1h_ago = int((datetime.datetime.now() - datetime.timedelta(hours=1)).timestamp())
            volume_1h = sum(
                float(swap['valueUSD']) for swap in pool['swaps']
                if swap['timestamp'] >= timestamp_1h_ago
                and swap['tokenIn'] in (token0['address'], token1['address'])
                and swap['tokenOut'] in (token0['address'], token1['address'])
            )

            new_pair['volume_24h'] = volume_24h
            new_pair['volume_1h'] = volume_1h

            reformatted_pools.append(new_pair)

        all_reformatted_pools.extend(reformatted_pools)

    return all_reformatted_pools

async def get_latest_pool_data(protocol: str, X: int = 1000, skip: int = 0, max_metric: float = None, is_hourly: bool = False) -> dict:
    if protocol == UNISWAP_V2:
        endpoint = UNISWAPV2_ENDPOINT
        print('collecting data from Uniswap V2...')
        data_field = 'pairs'
        query_func = uniswap_v2_query
    elif protocol == SUSHISWAP_V2:
        endpoint = SUSHISWAPV2_ENDPOINT
        print('collecting data from Sushiswap V2...')
        data_field = 'pairs'
        query_func = sushiswap_v2_query
    elif protocol == UNISWAP_V3:
        endpoint = UNISWAPV3_ENDPOINT
        print('collecting data from Uniswap V3...')
        if is_hourly:
            data_field = 'pairHourDatas'
        else:
            data_field = 'pairDayDatas'
        query_func = uniswap_v3_query
    elif protocol == BALANCER_V1:
        endpoint = BALANCER_V1_ENDPOINT
        print('collecting data from Balancer V1...')
        data_field = 'pools'
        query_func = balancer_v1_query
    elif protocol == BALANCER_V2:
        endpoint = BALANCER_V2_ENDPOINT
        print('collecting data from Balancer V2...')
        data_field = 'pools'
        query_func = balancer_v2_query
    elif protocol == DODO:
        print('collecting data from DODO...')
        pools = await collect_dodo_data()
        return pools
    elif protocol == PANCAKESWAP_V3:
        endpoint = PANCAKESWAP_V3_ENDPOINT
        print('collecting data from Pancakeswap V3...')
        query_func = pancakeswap_v3_query

    while True:
        try:
            query = query_func(X, skip, max_metric, is_hourly)

            async with aiohttp.ClientSession() as session:
                async with session.post(endpoint, json={'query': query}) as response:
                    obj = await response.json()
                    pools = obj['data'][data_field]

                    if protocol == UNISWAP_V2:
                        # Process Uniswap V2 pool data
                        for pool in pools:
                            pool['protocol'] = protocol
                            # Additional processing specific to Uniswap V2
                            try:
                                pool['token0']['priceUSD'] = float(pool['reserveUSD']) / float(pool['reserve0'])
                                pool['token1']['priceUSD'] = float(pool['reserveUSD']) / float(pool['reserve1'])
                            except:
                                pool['token0']['priceUSD'] = 0
                                pool['token1']['priceUSD'] = 0
                            if is_hourly:
                                pool['volume_1h'] = obj['data']['pairHourDatas'][0]['hourlyVolumeUSD']
                            else:
                                pool['volume_24h'] = obj['data']['pairDayDatas'][0]['volumeUSD']

                    elif protocol == SUSHISWAP_V2:
                        # Process Sushiswap V2 pool data
                        for pool in pools:
                            pool['protocol'] = protocol
                            # Additional processing specific to Sushiswap V2
                            try:
                                pool['token0']['priceUSD'] = float(pool['liquidityUSD']) / float(pool['reserve0'])
                                pool['token1']['priceUSD'] = float(pool['liquidityUSD']) / float(pool['reserve1'])
                            except:
                                pool['token0']['priceUSD'] = 0
                                pool['token1']['priceUSD'] = 0

                    elif protocol == UNISWAP_V3:
                        # Process Uniswap V3 pool data
                        if is_hourly:
                            for pool in pools:
                                # Process the hourly pool data
                                pool['protocol'] = protocol
                                # Additional processing specific to Uniswap V3 hourly data
                                if pool['sqrtPrice'] == '0':
                                    pool['reserve0'] = 0
                                    pool['reserve1'] = 0
                                    continue
                                sqrtPrice = float(pool['sqrtPrice']) / (2 ** 96)
                                liquidity = int(pool['liquidity'])
                                reserve0raw = liquidity / sqrtPrice
                                reserve1raw = liquidity * sqrtPrice
                                reserve0 = reserve0raw / (10 ** int(pool['token0']['decimals']))
                                reserve1 = reserve1raw / (10 ** int(pool['token1']['decimals']))
                                pool['reserve0'] = reserve0
                                pool['reserve1'] = reserve1
                                try:
                                    pool['token0']['priceUSD'] = float(pool['totalValueLockedUSD']) / float(pool['reserve0'])
                                    pool['token1']['priceUSD'] = float(pool['totalValueLockedUSD']) / float(pool['reserve1'])
                                except Exception as e:
                                    # Handle exception (if any)
                                    pool['token0']['priceUSD'] = 0
                                    pool['token1']['priceUSD'] = 0
                        else:
                            for pool in pools:
                                # Process the daily pool data
                                pool['protocol'] = protocol
                                # Additional processing specific to Uniswap V3 daily data
                                if pool['sqrtPrice'] == '0':
                                    pool['reserve0'] = 0
                                    pool['reserve1'] = 0
                                    continue
                                sqrtPrice = float(pool['sqrtPrice']) / (2 ** 96)
                                liquidity = int(pool['liquidity'])
                                reserve0raw = liquidity / sqrtPrice
                                reserve1raw = liquidity * sqrtPrice
                                reserve0 = reserve0raw / (10 ** int(pool['token0']['decimals']))
                                reserve1 = reserve1raw / (10 ** int(pool['token1']['decimals']))
                                pool['reserve0'] = reserve0
                                pool['reserve1'] = reserve1
                                try:
                                    pool['token0']['priceUSD'] = float(pool['totalValueLockedUSD']) / float(pool['reserve0'])
                                    pool['token1']['priceUSD'] = float(pool['totalValueLockedUSD']) / float(pool['reserve1'])
                                except Exception as e:
                                    # Handle exception (if any)
                                    pool['token0']['priceUSD'] = 0
                                    pool['token1']['priceUSD'] = 0

                    elif protocol == BALANCER_V1:
                        # Process Balancer V1 pool data
                        for pool in pools:
                            pool['protocol'] = protocol
                            # Additional processing specific to Balancer V1

                    elif protocol == BALANCER_V2:
                        # Process Balancer V2 pool data
                        for pool in pools:
                            pool['protocol'] = protocol
                            # Additional processing specific to Balancer V2

                    return pools
        except KeyError as e:
            logging.error("Key error while fetching pools, retrying...")
            print(e)
            print(protocol)
            print(query)
            continue
        except asyncio.exceptions.TimeoutError as e:
            logging.error("Timeout error while fetching pools, retrying...")
            continue
        except Exception as e:
            logging.error("Error while fetching pools, retrying...")
            logging.error(e)
            continue
