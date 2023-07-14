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
# import dotenv
import datetime

# !!! REMOVE IS_HOURLY AND IS_DAILY FLAGS

'''if os.path.exists(".env"):
    dotenv.load_dotenv(".env")'''
    
THE_GRAPH_KEY = os.getenv("THE_GRAPH_KEY")

# Define the protocol fetch flags
protocol_fetch_flags = {
    UNISWAP_V2: {'hourly': True, 'daily': False},
    SUSHISWAP_V2: {'hourly': True, 'daily': False},
    UNISWAP_V3: {'hourly': False, 'daily': True},
    BALANCER_V1: {'hourly': False, 'daily': True},
    BALANCER_V2: {'hourly': False, 'daily': True},
    DODO: {'hourly': True, 'daily': True},
    PANCAKESWAP_V3: {'hourly': False, 'daily': True},
}

# Collect the list of bad_tokens
with open(r'data/bad_tokens.json') as f:
    BAD_TOKENS = json.load(f)
    BAD_TOKEN_SYMS = [token['symbol'] for token in BAD_TOKENS['tokens']]

UNISWAPV2_ENDPOINT = "https://api.thegraph.com/subgraphs/name/ianlapham/uniswap-v2-dev"
UNISWAPV3_ENDPOINT = "https://api.thegraph.com/subgraphs/name/uniswap/uniswap-v3"
SUSHISWAPV2_ENDPOINT = "https://api.thegraph.com/subgraphs/name/sushi-v2/sushiswap-ethereum"
CURVE_ENDPOINTS = [
    "https://api.curve.fi/api/getPools/ethereum/main",
    "https://api.curve.fi/api/getPools/ethereum/crypto",
]
CURVE_SUBGRAPH_ENDPOINT = "https://api.thegraph.com/subgraphs/name/messari/curve-finance-ethereum"
BALANCER_V1_ENDPOINT = "https://api.thegraph.com/subgraphs/name/balancer-labs/balancer"
BALANCER_V2_ENDPOINT = "https://api.thegraph.com/subgraphs/name/balancer-labs/balancer-v2"
DODO_ENDPOINT = "https://api.thegraph.com/subgraphs/name/dodoex/dodoex-v2"
PANCAKESWAP_V3_ENDPOINT = "https://api.thegraph.com/subgraphs/name/pancakeswap/exchange-v3-eth"

def uniswap_v2_query(X: int = 1000, skip: int = 0, max_metric: float = None, is_hourly: bool = True):
    # If no max_metric is defined, we only specify the count, ordering and skip
    if not max_metric:
        query = f"""
        {{
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
    # If max_metric is defined, we also add a where clause to filter the data
    else:
        query = f"""
        {{
        pairs(first: {X}, orderBy: reserveUSD, orderDirection: desc, skip: {skip}, where: {{reserveUSD_lt: {max_metric}}}) {{
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

    return query

        
def sushiswap_v2_query(X: int, skip: int, max_metric: float, is_hourly: bool):
    if not max_metric:
        return f"""
        {{
          pairs(first: {X}, orderBy: liquidityUSD, orderDirection: desc, skip: {skip}) {{
            hourSnapshots(first: 1, orderBy: date, orderDirection: desc) {{
              volumeUSD
              date
            }}
            daySnapshots(first: 1, orderBy: date, orderDirection: desc) {{
              volumeUSD
              date
            }}
            liquidityUSD
            volumeUSD
            id
            reserve0
            reserve1
            swapFee
            token0Price
            token1Price
            token0 {{
              decimals
              id
              name
              symbol
            }}
            token1 {{
              decimals
              id
              name
              symbol
            }}
          }}
        }}
        """
    else:
        return f"""
        {{
          pairs(first: {X}, orderBy: liquidityUSD, orderDirection: desc, skip: {skip}, where: {{ liquidityUSD_lt: {max_metric} }}) {{
            hourSnapshots(first: 1, orderBy: date, orderDirection: desc) {{
              volumeUSD
              date
            }}
            daySnapshots(first: 1, orderBy: date, orderDirection: desc) {{
              volumeUSD
              date
            }}
            liquidityUSD
            volumeUSD
            id
            reserve0
            reserve1
            swapFee
            token0Price
            token1Price
            token0 {{
              decimals
              id
              name
              symbol
            }}
            token1 {{
              decimals
              id
              name
              symbol
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
            poolDayData {{
              date
              volumeUSD
            }}
            poolHourData {{
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
            poolDayData {{
              date
              volumeUSD
            }}
            poolHourData {{
              periodStartUnix
              volumeUSD
            }}
          }}
        }}
        """


def pancakeswap_v3_query(X: int, skip: int, max_metric: float, is_hourly: bool):
    timestamp_24h_ago = int((datetime.datetime.now() - datetime.timedelta(hours=24)).timestamp())
    timestamp_1h_ago = int((datetime.datetime.now() - datetime.timedelta(hours=1)).timestamp())

    if not max_metric:
        return f"""
        {{
        pools(first: {X}, skip: {skip}, orderDirection: desc, orderBy: totalValueLockedUSD) {{
            token0 {{
            id
            symbol
            name
            decimals
            derivedUSD
            }}
            token1 {{
            symbol
            name
            id
            decimals
            derivedUSD
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
            poolHourData(first: 1, orderBy: periodStartUnix, orderDirection: desc, where: {{periodStartUnix_gte: {timestamp_1h_ago}}}) {{
                periodStartUnix
                volumeUSD
            }}
            poolDayData(first: 1, orderBy: date, orderDirection: desc, where: {{date_gte: {timestamp_24h_ago}}}) {{
                date
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
            derivedUSD
            }}
            token1 {{
            symbol
            id
            decimals
            derivedUSD
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
            poolHourData(first: 1, orderBy: periodStartUnix, orderDirection: desc, where: {{periodStartUnix_gte: {timestamp_1h_ago}}}) {{
                periodStartUnix
                volumeUSD
            }}
            poolDayData(first: 1, orderBy: date, orderDirection: desc, where: {{date_gte: {timestamp_24h_ago}}}) {{
                date
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
        date_gte = int((datetime.datetime.now() - datetime.timedelta(days=1)).timestamp())
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

        # Map volume data to pool objects
        for pool in pool_data:
            pair_address = pool['id']
            volume_1h = None
            volume_24h = float(volume_data.get(pair_address, 0))

            if pair_address in volume_data:
                # Find the corresponding volume data in pairHourDatas
                hour_volume_data = next((data for data in pair_hour_datas if data['pairAddress'] == pair_address), None)
                if hour_volume_data:
                    volume_1h = float(hour_volume_data['volumeUSD'])

            pool['volume_1h'] = volume_1h
            pool['volume_24h'] = volume_24h

            # Additional processing specific to DODO
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
            new_pair['volume_24h'] = float(volume_24h)
            new_pair['volume_1h'] = float(volume_1h) if volume_1h is not None else None
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
            new_pair['token0Price'] = (
                float(pool['quoteToken']['usdPrice']) / float(pool['baseToken']['usdPrice'])
                if float(pool['baseToken']['usdPrice']) != 0
                else 0
            )
            new_pair['token1Price'] = (
                float(pool['baseToken']['usdPrice']) / float(pool['quoteToken']['usdPrice'])
                if float(pool['quoteToken']['usdPrice']) != 0
                else 0
            )
            new_pair['reserveUSD'] = (
                float(new_pair['reserve0']) * float(new_pair['token0']['priceUSD'])
                + float(new_pair['reserve1']) * float(new_pair['token1']['priceUSD'])
            )
            new_pair['protocol'] = DODO
            new_pair['dangerous'] = (
                new_pair['token0']['symbol'] in BAD_TOKEN_SYMS
                or new_pair['token1']['symbol'] in BAD_TOKEN_SYMS
            )
            new_pair['type'] = pool['type']
            new_pair['volumeUSD'] = pool['volumeUSD']

            res.append(new_pair)

        # Remove any pairs in res where type = VIRTUAL
        res = [pair for pair in res if pair['type'] != 'VIRTUAL']

    return res


async def fetch_and_calculate_curve_volumes(retries=10, backoff_factor=1):
    # Define the new query
    query = """
    {
      liquidityPools(first: 1000, orderBy: id) {
        id
        dailySnapshots(first: 1, where: {timestamp_gte: "1688636630"}) {
          dailyVolumeUSD
          timestamp
        }
        hourlySnapshots(first: 1, where: {timestamp_gte: "1688719430"}) {
          hourlyVolumeUSD
          timestamp
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
    for pool in data.get('data', {}).get('liquidityPools', []):
        
        daily_volume = float(pool['dailySnapshots'][0]['dailyVolumeUSD']) if pool['dailySnapshots'] else None
        hourly_volume = float(pool['hourlySnapshots'][0]['hourlyVolumeUSD']) if pool['hourlySnapshots'] else None

        volume_data[pool['id']] = (daily_volume, hourly_volume)

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

                        daily_volume, hourly_volume = volume_data[pool['address'].lower()]

                        # check if either are null, if so, set to 0
                        if daily_volume is None:
                            daily_volume = 0
                        if hourly_volume is None:
                            hourly_volume = 0

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

                            new_pair = {}
                            new_pair['id'] = pool['address'].lower()
                            new_pair['reserve0'] = balance0 / 10**decimals0
                            new_pair['reserve1'] = balance1 / 10**decimals1
                            new_pair['volume_24h'] = daily_volume
                            new_pair['volume_1h'] = hourly_volume

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
            swaps(orderBy: timestamp, orderDirection: desc, where: {{ timestamp_gte: {timestamp_24h_ago} }}) {{
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
            swaps(orderBy: timestamp, orderDirection: desc, where: {{ timestamp_gte: {timestamp_24h_ago} }}) {{
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


def reformat_balancer_v1_pools(pool_list):
    balancer_pool_tokens = set()
    for pool in pool_list:
        for token in pool['tokens']:
            balancer_pool_tokens.add(token['address'])
    
    token_prices = fetch_balancer_v1_token_prices(list(balancer_pool_tokens))

    all_reformatted_pools = []
    for pool in pool_list:
        reformatted_pools = reformat_balancer_v1_pool(pool, token_prices)
        all_reformatted_pools.extend(reformatted_pools)
    return all_reformatted_pools


def reformat_balancer_v1_pool(pool, token_prices):
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
            new_pair['reserveUSD'] = float(new_pair['reserve0']) * token_prices[new_pair['token0']['id']] + float(new_pair['reserve1']) * token_prices[new_pair['token1']['id']]
            new_pair['token0']['priceUSD'] = token_prices[new_pair['token0']['id']]
            new_pair['token1']['priceUSD'] = token_prices[new_pair['token1']['id']]
        except KeyError as e:
            missing_id = str(e)
            missing_prices = fetch_balancer_v1_token_prices([], [missing_id])
            token_prices.update(missing_prices)
            new_pair['reserveUSD'] = float(new_pair['reserve0']) * token_prices[new_pair['token0']['id']] + float(new_pair['reserve1']) * token_prices[new_pair['token1']['id']]
        # check if priceUSD keys exist in both tokens and if not, set priceUSD to 0
        for token in [new_pair['token0'], new_pair['token1']]:
            if 'priceUSD' not in token:
                token['priceUSD'] = 0.0

        # Calculate 24-hour volume
        volume_24h = sum(
            float(swap['value']) 
            for swap in pool['swaps']
            if swap['tokenIn'] in (token0['address'], token1['address'])
            and swap['tokenOut'] in (token0['address'], token1['address'])
        )

        # Calculate 1-hour volume
        timestamp_1h_ago = int((datetime.datetime.now() - datetime.timedelta(hours=1)).timestamp())
        volume_1h = sum(
            float(swap['value']) 
            for swap in pool['swaps']
            # add a few minutes to the timestamp to account for the delay between collection and processing
            if swap['timestamp'] >= timestamp_1h_ago - 60 * 5
            and swap['tokenIn'] in (token0['address'], token1['address'])
            and swap['tokenOut'] in (token0['address'], token1['address'])
        )

        new_pair['volume_24h'] = volume_24h
        new_pair['volume_1h'] = volume_1h

        new_pair['dangerous'] = (
            new_pair['token0']['symbol'] in BAD_TOKEN_SYMS or
            new_pair['token1']['symbol'] in BAD_TOKEN_SYMS or
            new_pair['reserve0'] == 0 or
            new_pair['reserve1'] == 0
        )

        reformatted_pools.append(new_pair)

    return reformatted_pools


def balancer_v2_query(X: int, skip: int, max_metric: float = None, is_hourly: bool = False):
    max_metric_filter = f', where: {{totalLiquidity_lt: {max_metric}}}' if max_metric else ''

    return f"""
    {{
      pools(first: {X}, orderBy: totalLiquidity, orderDirection: desc, skip: {skip}{max_metric_filter}) {{
        address
        swapFee
        tokensList
        totalLiquidity
        poolType
        swaps(orderBy: timestamp, orderDirection: desc, where: {{ timestamp_gte: {int(datetime.datetime.now().timestamp()) - 24 * 60 * 60} }}) {{
            valueUSD
            tokenIn
            tokenOut
            timestamp
          }}
        tokens {{
          address
          balance
          symbol
          name
          weight
          token {{
            totalBalanceUSD
            latestUSDPrice
          }}
        }}
      }}
    }}
    """


def reformat_balancer_v2_pools(pool_list):
    ''' Reformats a list of Balancer V2 pools into the uniswap/sushiswap format. '''

    def reformat_balancer_v2_pool(pool):
        token_combinations = list(combinations(pool['tokens'], 2))
        reformatted_pools = []

        for combination in token_combinations:
            token0 = combination[0]
            token1 = combination[1]
            print('balances:')
            print(token0['balance'])
            print(token1['balance'])
            print('prices:')
            print(token0['token']['latestUSDPrice'])
            print(token1['token']['latestUSDPrice'])
            print()
            token0_balance = float(token0['balance'])
            token1_balance = float(token1['balance'])
            token0_price = float(token0['token']['latestUSDPrice']) if token0['token']['latestUSDPrice'] is not None else 0.0
            token1_price = float(token1['token']['latestUSDPrice']) if token1['token']['latestUSDPrice'] is not None else 0.0
            new_pair = {
                'id': pool['address'],
                'swapFee': pool['swapFee'],
                'reserve0': token0['balance'],
                'reserve1': token1['balance'],
                'token0': {
                    'id': token0['address'],
                    'symbol': token0['symbol'],
                    'name': token0['name'],
                    'weight': token0['weight'],
                    'balanceUSD': float(token0['balance']) * float(token0['token']['latestUSDPrice']) if token0['token']['latestUSDPrice'] is not None else 0.0, 
                    'priceUSD': token0['token']['latestUSDPrice'] if token0['token']['latestUSDPrice'] is not None else 0.0,
                },
                'token1': {
                    'id': token1['address'],
                    'symbol': token1['symbol'],
                    'name': token1['name'],
                    'weight': token1['weight'],
                    'balanceUSD': float(token1['balance']) * float(token1['token']['latestUSDPrice']) if token1['token']['latestUSDPrice'] is not None else 0.0,
                    'priceUSD': token1['token']['latestUSDPrice'] if token1['token']['latestUSDPrice'] is not None else 0.0,
                },
                'totalLiquidity': float(token0['balance']) * float(token0['token']['latestUSDPrice']) + float(token1['balance']) * float(token1['token']['latestUSDPrice']) if token0['token']['latestUSDPrice'] is not None and token1['token']['latestUSDPrice'] is not None else 0.0,
                'protocol': 'Balancer_V2',
            }
            reformatted_pools.append(new_pair)
            new_pair['dangerous'] = new_pair['token0']['symbol'] in BAD_TOKEN_SYMS or new_pair['token1']['symbol'] in BAD_TOKEN_SYMS
            # check if priceUSD is None (or 0 which can be resultant of it previously being None) and calculate it if it is
            if new_pair['token0']['priceUSD'] is None or new_pair['token0']['priceUSD'] == 0:
                try:
                    new_pair['token0']['priceUSD'] = float(new_pair['token0']['balanceUSD']) / float(new_pair['reserve0']) # new method
                except ZeroDivisionError:
                    new_pair['token0']['priceUSD'] = 0
            if new_pair['token1']['priceUSD'] is None or new_pair['token1']['priceUSD'] == 0:
                try:
                    new_pair['token1']['priceUSD'] = float(new_pair['token1']['balanceUSD']) / float(new_pair['reserve1']) # new method
                except ZeroDivisionError:
                    new_pair['token1']['priceUSD'] = 0
            # calculate reserveUSD
            try:
                reserveUSD = float(new_pair['totalLiquidity']) * float(new_pair['token0']['priceUSD']) * (float(new_pair['reserve0']) / (float(new_pair['reserve0']) + float(new_pair['reserve1']))) + float(new_pair['totalLiquidity']) * float(new_pair['token1']['priceUSD']) * (float(new_pair['reserve1']) / (float(new_pair['reserve0']) + float(new_pair['reserve1'])))
                new_pair['reserveUSD'] = reserveUSD
            except ZeroDivisionError:
                new_pair['reserveUSD'] = 0

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
                # add a few minutes to the timestamp to account for the delay between collection and processing
                if swap['timestamp'] >= timestamp_1h_ago - 60 * 5
                and swap['tokenIn'] in (token0['address'], token1['address'])
                and swap['tokenOut'] in (token0['address'], token1['address'])
            )

            new_pair['volume_24h'] = volume_24h
            new_pair['volume_1h'] = volume_1h

            # create a 'swaps' key in new_pair that contains all swaps involving the two tokens in the pair
            new_pair['swaps'] = [swap for swap in pool['swaps'] if swap['tokenIn'] in (token0['address'], token1['address']) and swap['tokenOut'] in (token0['address'], token1['address'])]

        return reformatted_pools

    all_reformatted_pools = []
    for pool in pool_list:
        reformatted_pools = reformat_balancer_v2_pool(pool)
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
        data_field = 'pools'
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
        data_field = 'pools'
        query_func = pancakeswap_v3_query

    while True:
        try:
            query = query_func(X, skip, max_metric, is_hourly)
            # print(query)

            async with aiohttp.ClientSession() as session:
                async with session.post(endpoint, json={'query': query}) as response:
                    obj = await response.json()
                    pools = obj['data'][data_field]

                    timestamp_24h_ago = int((datetime.datetime.now() - datetime.timedelta(hours=24)).timestamp())
                    timestamp_1h_ago = int((datetime.datetime.now() - datetime.timedelta(hours=1)).timestamp())

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

                            # Placeholder for volume_1h and volume_24h while subgraph is being updated
                            pool['volume_1h'] = 0
                            pool['volume_24h'] = 0

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
                          
                          # Set volume_1h and volume_24h
                          if 'hourSnapshots' in pool:
                              pool['volume_1h'] = float(pool['hourSnapshots'][0]['volumeUSD'])
                          else:
                              pool['volume_1h'] = 0
                          
                          if 'daySnapshots' in pool:
                              pool['volume_24h'] = float(pool['daySnapshots'][0]['volumeUSD'])
                          else:
                              pool['volume_24h'] = 0
                              
                    elif protocol == PANCAKESWAP_V3:
                      for pool in pools:
                            pool['protocol'] = protocol
                            pool['reserve0'] = pool.pop('totalValueLockedToken0')
                            pool['reserve1'] = pool.pop('totalValueLockedToken1')
                            
                            # rename derivedUSD in token0 and token1 to priceUSD
                            pool['token0']['priceUSD'] = pool['token0'].pop('derivedUSD')
                            pool['token1']['priceUSD'] = pool['token1'].pop('derivedUSD')
                                
                            if pool['poolHourData'] != []:
                                pool['volume_1h'] = float(pool['poolHourData'][0]['volumeUSD'])
                            else:
                                pool['volume_1h'] = 0
                                
                            if pool['poolDayData'] != []:
                                pool['volume_24h'] = float(pool['poolDayData'][0]['volumeUSD'])
                                
                            else:
                                pool['volume_24h'] = 0
                                

                    elif protocol == UNISWAP_V3:
                        for pool in pools:
                            pool['protocol'] = protocol
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
                                # Calculate the total value in terms of token0
                                total_value_token0 = float(pool['totalValueLockedToken0']) + float(pool['token1Price']) * float(pool['totalValueLockedToken1'])
                                # Calculate the total value in terms of token1
                                total_value_token1 = float(pool['totalValueLockedToken1']) + 1/float(pool['token0Price']) * float(pool['totalValueLockedToken0'])
                                # Calculate the price of each token
                                pool['token0']['priceUSD'] = float(pool['totalValueLockedUSD']) / total_value_token0
                                pool['token1']['priceUSD'] = float(pool['totalValueLockedUSD']) / total_value_token1
                            except Exception as e:
                                print(e)
                                print(pool)
                                pool['token0']['priceUSD'] = 0
                                pool['token1']['priceUSD'] = 0
                            
                            # Set volume values, they will be overwritten later if available
                            pool['volume_1h'] = 0
                            pool['volume_24h'] = 0

                            # Retrieve volumeUSD from poolDayData within the last 24 hours
                            volume_24h = 0
                            for pool_day_data in pool['poolDayData']:
                                if int(pool_day_data['date']) >= timestamp_24h_ago:
                                    volume_24h = float(pool_day_data['volumeUSD'])
                                    break

                            # Retrieve volumeUSD from poolHourData within the last 1 hour
                            # Hourly data provided by the subgraph is almost always 0
                            volume_1h = 0
                            for pool_hour_data in pool['poolHourData']:
                                if int(pool_hour_data['periodStartUnix']) >= timestamp_1h_ago:
                                    volume_1h = float(pool_hour_data['volumeUSD'])
                                    break

                            # Calculate volume_24h using poolDayData
                            volume_24h = sum(float(day_data['volumeUSD']) for day_data in pool['poolDayData'] if int(day_data['date']) >= timestamp_24h_ago)
                            pool['volume_24h'] = volume_24h

                            # Calculate volume_1h using poolHourData
                            volume_1h = sum(float(hour_data['volumeUSD']) for hour_data in pool['poolHourData'] if int(hour_data['periodStartUnix']) >= timestamp_1h_ago)
                            pool['volume_1h'] = volume_1h


                            # store the latest poolHourData unix timestamp, timestamps are not sorted
                            pool['latestHourDataUnix'] = max(int(hour_data['periodStartUnix']) for hour_data in pool['poolHourData'])

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
                            
                    for pool in pools:
                      # Set volume_24h and volume_1h to 0 if they are were somehow missed
                      # !!! CHANGE BACK TO 0s BEFORE PUSHING
                      # Check if the keys exist first
                      if 'volume_24h' not in pool:
                        pool['volume_24h'] = 0
                      if 'volume_1h' not in pool:
                        pool['volume_1h'] = 0
                      # Set volume_24h and volume_1h to 0 if they are None
                      if pool['volume_24h'] is None:
                        pool['volume_24h'] = 0
                      if pool['volume_1h'] is None:
                        pool['volume_1h'] = 0
                        
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