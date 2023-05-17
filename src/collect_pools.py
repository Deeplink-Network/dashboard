'''
This file is for testing functions without having to run the entire dashboard.
'''

# local imports
from pool_collector import *
from dashboard import *
# third party imports
from constants import *
from heapq import merge
import json
import asyncio
import time


async def main():
    # get the latest pool data 
    print("testing collection, getting latest pool data...")

    # start a timer
    start = time.time()

    refresh_tasks = [refresh_pools(dex) for dex in DEX_LIST]
    try:
        await asyncio.gather(*refresh_tasks)
    except KeyboardInterrupt:
        for task in asyncio.all_tasks():
            if task is not asyncio.current_task():
                task.cancel()
        await asyncio.gather(*asyncio.all_tasks(), return_exceptions=True)

    # save the pool data
    with open('data/pool_dict.json', 'w') as f:
        json.dump(pool_dict, f)

    # end the timer
    end = time.time()
    print(f"finished in {end - start} seconds")


if __name__ == '__main__':
    asyncio.run(main())
