'''
This file is for testing functions.
'''

# local imports
from dashboard import pools, pool_dict, DEX_LIST, DEX_METRIC_MAP
from constants import *
from pool_collector_v2 import collect_curve_pools, get_latest_pool_data, reformat_balancer_v1_pools, reformat_balancer_v2_pools
# third party imports
import json
import asyncio
import logging

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

async def refresh_pools(protocol: str):
    global pools
    global pool_dict
    new_pools_hourly = []  # Define new_pools_hourly outside the block
    
    if protocol == CURVE:
        new_curve_pools = await collect_curve_pools()
        for pool in new_curve_pools:
            token0_id = pool['token0']['id']
            token1_id = pool['token1']['id']
            key = f"{pool['id']}_{token0_id}_{token1_id}"
            pool_dict[key] = pool
            pools[protocol]['pools'].append(pool)
        return
    
    # Define the metric to use based on the protocol
    metric_to_use = pools[protocol]['metric']
    
    # Get the fetch flags for the protocol
    fetch_flags = protocol_fetch_flags.get(protocol, {})
    hourly_fetch = fetch_flags.get('hourly', False)
    daily_fetch = fetch_flags.get('daily', False)
    
    # Get the latest pool data (hourly)
    if hourly_fetch:
        last_pool_metric_hourly = None
        for skip in (0, 1000, 2000, 3000, 4000, 5000):
            logging.info(f'Getting hourly pools {skip} to {skip + 1000}...')
            new_pools_hourly = await get_latest_pool_data(protocol=protocol, skip=skip, max_metric=last_pool_metric_hourly, is_hourly=True)
            if new_pools_hourly:
                if protocol == BALANCER_V1:
                    new_balancer_pools = reformat_balancer_v1_pools(new_pools_hourly)
                    for pool in new_balancer_pools:
                        token0_id = pool['token0']['id']
                        token1_id = pool['token1']['id']
                        key = f"{pool['id']}_{token0_id}_{token1_id}"
                        pool_dict[key] = pool
                        pools[protocol]['pools'].append(pool)
                elif protocol == BALANCER_V2:
                    new_balancer_pools = reformat_balancer_v2_pools(new_pools_hourly)
                    for pool in new_balancer_pools:
                        token0_id = pool['token0']['id']
                        token1_id = pool['token1']['id']
                        key = f"{pool['id']}_{token0_id}_{token1_id}"
                        pool_dict[key] = pool
                        pools[protocol]['pools'].append(pool)
                else:
                    for pool in new_pools_hourly:
                        token0_id = pool['token0']['id']
                        token1_id = pool['token1']['id']
                        key = f"{pool['id']}_{token0_id}_{token1_id}"
                        
                        print(pool)
                        
                        # Check if the pool already exists in the pool_dict
                        if key in pool_dict:
                            existing_pool = pool_dict[key]
                            # existing_pool['volume_1h'] += pool['volumeUSD']
                        else:
                            pool_dict[key] = pool
                            pools[protocol]['pools'].append(pool)
                            '''pool['volume_1h'] = pool['volumeUSD']
                            pool['volume_24h'] = 0'''
                    
                last_pool_hourly = new_pools_hourly[-1]
                print()
                print('last_pool_hourly')
                print(last_pool_hourly)
                print()
                last_pool_metric_hourly = float(last_pool_hourly[metric_to_use])
    
    # Get the latest pool data (daily)
    if daily_fetch:
        new_pools_daily = []
        last_pool_metric_daily = None
        for skip in (0, 1000, 2000, 3000, 4000, 5000):
            logging.info(f'Getting daily pools {skip} to {skip + 1000}...')
            new_pools_daily = await get_latest_pool_data(protocol=protocol, skip=skip, max_metric=last_pool_metric_daily, is_hourly=False)
            if new_pools_daily:
                if protocol == BALANCER_V1:
                    new_balancer_pools = reformat_balancer_v1_pools(new_pools_daily, new_pools_hourly)
                    for pool in new_balancer_pools:
                        token0_id = pool['token0']['id']
                        token1_id = pool['token1']['id']
                        key = f"{pool['id']}_{token0_id}_{token1_id}"
                        pool_dict[key] = pool
                        pools[protocol]['pools'].append(pool)
                elif protocol == BALANCER_V2:
                    new_balancer_pools = reformat_balancer_v2_pools(new_pools_daily)
                    for pool in new_balancer_pools:
                        token0_id = pool['token0']['id']
                        token1_id = pool['token1']['id']
                        key = f"{pool['id']}_{token0_id}_{token1_id}"
                        pool_dict[key] = pool
                        pools[protocol]['pools'].append(pool)
                else:
                    for pool in new_pools_daily:
                        token0_id = pool['token0']['id']
                        token1_id = pool['token1']['id']
                        key = f"{pool['id']}_{token0_id}_{token1_id}"
                        
                        '''# Check if the pool already exists in the pool_dict
                        if key in pool_dict:
                            existing_pool = pool_dict[key]
                            existing_pool['volume_24h'] += pool['volumeUSD']
                        else:
                            pool_dict[key] = pool
                            pools[protocol]['pools'].append(pool)
                            pool['volume_1h'] = 0
                            pool['volume_24h'] = pool['volumeUSD']'''
                    
                last_pool_daily = new_pools_daily[-1]
                last_pool_metric_daily = float(last_pool_daily[metric_to_use])

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

    '''# get the latest pool data 
    print("testing collection, getting latest pool data...")

    refresh_tasks = [refresh_pools(dex) for dex in DEX_LIST]
    try:
        await asyncio.gather(*refresh_tasks)
    except KeyboardInterrupt:
        for task in asyncio.all_tasks():
            if task is not asyncio.current_task():
                task.cancel()
        await asyncio.gather(*asyncio.all_tasks(), return_exceptions=True)

    # save the pool data
    with open('test_results\\pool_dict.json', 'w') as f:
        print("saving pool data...")
        json.dump(pool_dict, f)
    with open('test_results\\pools.json', 'w') as f:
        print("saving pools data...")
        json.dump(pools, f)'''
        

if __name__ == "__main__":
    asyncio.run(main())
