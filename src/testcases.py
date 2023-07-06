'''
This file is for testing functions.
'''

# local imports
from dashboard import pools, pool_dict
from constants import *
from pool_collector_v2 import collect_curve_pools, get_latest_pool_data, reformat_balancer_v1_pools, reformat_balancer_v2_pools
# third party imports
import json
import asyncio
import logging
import datetime

DEX_LIST = (
    UNISWAP_V2,
    #UNISWAP_V3,
    #SUSHISWAP_V2,
    #CURVE,
    BALANCER_V1,
    BALANCER_V2,
    #DODO,
    #PANCAKESWAP_V3,
)

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
                print(last_pool)
                last_pool_metric = float(last_pool[metric_to_use])

async def main():
    # Get the latest pool data 
    print("Testing collection, getting latest pool data...")

    for dex in DEX_LIST:
        try:
            await refresh_pools(dex)
            # Save the pool data
            with open(f'test_results/pool_dict_{dex}.json', 'w') as f:
                json.dump(pool_dict, f)

        except KeyboardInterrupt:
            break        

if __name__ == "__main__":
    asyncio.run(main())
