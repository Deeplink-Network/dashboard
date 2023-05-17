from dashboard import refresh_pools, refresh_matrix, get_matrix_segment, DEX_LIST
from threading import Thread
from flask import Flask, request, jsonify
from flask_cors import CORS
import asyncio
import pandas as pd
import time

app = Flask(__name__)
CORS(app)

loop = asyncio.get_event_loop()

async def refresh_all_pools():
    # start a timer
    start = time.time()
    refresh_tasks = [refresh_pools(dex) for dex in DEX_LIST]
    try:
        await asyncio.gather(*refresh_tasks)
        # finish the timer and print the time it took to refresh all pools
        end = time.time()
        print(f"finished in {end - start} seconds")
        refresh_matrix()  # Refresh matrix after all pools have been refreshed
        await asyncio.sleep(30)
    except KeyboardInterrupt:
        for task in asyncio.all_tasks():
            if task is not asyncio.current_task():
                task.cancel()
        await asyncio.gather(*asyncio.all_tasks())

def pool_thread_task():
    asyncio.run(refresh_all_pools())

@app.route('/health', methods=['GET'])
async def health():
    return jsonify({'status': 'ok'})      

@app.route('/matrix', methods=['GET'])
async def matrix():
    x = int(request.args.get('x')) # starting row index
    y = int(request.args.get('y')) # ending row index
    i = int(request.args.get('i')) # starting column index
    j = int(request.args.get('j')) # ending column index

    # check if the matrix file exists
    try:
        # open the combined_df.json file
        with open('data/combined_df.json', 'r') as f:
            df = pd.read_json(f, orient='split')
    except:
        # if the matrix file does not exist, return something to let the frontend know that the matrix is not ready
        return jsonify({'status': 'matrix not ready'})
        
    # get the matrix segment
    segment = get_matrix_segment(df, x, y, i, j)

    # return the matrix segment
    return jsonify(segment)

def init():
    threads = [
        Thread(target=pool_thread_task)
    ]

    for thread in threads:
        thread.start()

def main():
    init()
    return app

if __name__ == '__main__':
    loop.run_until_complete(main())
    