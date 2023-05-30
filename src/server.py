from dashboard import refresh_pools, refresh_matrix, get_matrix_segment, filter_matrix_by_asset, DEX_LIST
from threading import Thread
from flask import Flask, request, jsonify
from flask_cors import CORS
import asyncio
import pandas as pd
import time
from flask_swagger_ui import get_swaggerui_blueprint

app = Flask(__name__)
CORS(app)

SWAGGER_URL = '/'  # URL for exposing Swagger UI (without trailing '/')
API_URL = '/static/swagger.yaml'  # Our Swagger schema file

# Call factory function to create our blueprint
swaggerui_blueprint = get_swaggerui_blueprint(
    SWAGGER_URL,  
    API_URL,
    config={  # Swagger UI config overrides
        'app_name': "Your API Name"
    }
)

app.register_blueprint(swaggerui_blueprint)

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
        return make_response(jsonify({'status': 'matrix not ready'}), 503)
        
    # get the matrix segment
    segment = get_matrix_segment(df, x, y, i, j)

    # return the matrix segment
    return jsonify(segment)


@app.route('/matrix_filter', methods=['GET'])
async def matrix_filter():
    asset_id = request.args.get('asset_id')  # Asset ID to filter by

    # Check if the matrix file exists
    try:
        # Open the combined_df.json file
        with open('data/combined_df.json', 'r') as f:
            df = pd.read_json(f, orient='split')
    except FileNotFoundError:
        # If the matrix file does not exist, return a response indicating that the matrix is not ready
        return make_response(jsonify({'status': 'matrix not ready'}), 503)

    # Filter the matrix by asset ID
    filtered_data = filter_matrix_by_asset(df, asset_id)

    # Return the filtered matrix segment
    return jsonify(filtered_data)


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
    