import aiohttp
import datetime
from constants import DODO
import json
import asyncio

with open(r'data/bad_tokens.json') as f:
    BAD_TOKENS = json.load(f)
    BAD_TOKEN_SYMS = [token['symbol'] for token in BAD_TOKENS['tokens']]


DODO_ENDPOINT = "https://api.thegraph.com/subgraphs/name/dodoex/dodoex-v2"

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
                
                res.append(new_pair)
            
            # remove any pairs in res where type = VIRTUAL
            res = [pair for pair in res if pair['type'] != 'VIRTUAL']

    return res

# test the function
def main():
        loop = asyncio.get_event_loop()
        res = loop.run_until_complete(collect_dodo_data())
        # save the data
        with open('test_results/dodo.json', 'w') as f:
                json.dump(res, f, indent=4)
                
if __name__ == '__main__':
    main()