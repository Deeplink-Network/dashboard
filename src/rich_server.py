from dashboard import refresh_pools, refresh_matrix, get_matrix_segment, filter_matrix_by_asset, DEX_LIST
from threading import Thread
from flask import Flask, request, jsonify, redirect
from flask_cors import CORS
import asyncio
import pandas as pd
from rich.console import Console
from rich.logging import RichHandler
from rich.progress import Progress
from logging import basicConfig, INFO
from flask_swagger_ui import get_swaggerui_blueprint
from whitenoise import WhiteNoise

# Create a console for rich output
console = Console()

# Configure logging for rich
basicConfig(
    level=INFO,
    format="%(message)s",
    datefmt="[%X]",
    handlers=[RichHandler(console=console)]
)

app = Flask(__name__)
CORS(app)
app.wsgi_app = WhiteNoise(app.wsgi_app, root='static/')  # the 'static' directory for swagger.yaml

SWAGGER_URL = '/api/docs'  # URL for exposing Swagger UI (without trailing '/')
API_URL = '/static/swagger.yaml'  # Our Swagger schema file

# Call factory function to create our blueprint
swaggerui_blueprint = get_swaggerui_blueprint(
    SWAGGER_URL,  
    API_URL,
    config={  # Swagger UI config overrides
        'app_name': "app"
    }
)

app.register_blueprint(swaggerui_blueprint)

loop = asyncio.get_event_loop()

async def refresh_data():
    while True:
        with Progress(console=console, transient=True) as progress:
            task = progress.add_task("[cyan]Refreshing pools...", total=len(DEX_LIST))
            refresh_tasks = [refresh_pools(dex) for dex in DEX_LIST]
            results = await asyncio.gather(*refresh_tasks)
            for _ in results:
                progress.advance(task)
            console.log("All pools refreshed.")
            console.log("Refreshing matrix...")
            refresh_matrix()
            console.log("Matrix refreshed.")
        await asyncio.sleep(30)

def pool_thread_task():
    asyncio.run(refresh_data())
    
@app.route('/', methods=['GET'])
def index():
    return redirect('/api/docs')

@app.route('/health', methods=['GET'])
async def health():
    console.log(f"Health check from {request.remote_addr}")
    return jsonify({'status': 'ok'})      

@app.route('/matrix', methods=['GET'])
async def matrix():
    console.log(f"Matrix data requested by {request.remote_addr}")
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
        return jsonify({'status': 'matrix not ready'})

    # Filter the matrix by asset ID
    filtered_data = filter_matrix_by_asset(df, asset_id)

    if not filtered_data:
        # If the filtered data is empty, it means the asset ID was not found in the matrix
        return jsonify({'status': 'asset ID not found'})

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
