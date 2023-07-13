from dashboard import refresh_pools, refresh_matrix, get_matrix_segment, filter_matrix_by_asset, DEX_LIST
from threading import Thread
from flask import Flask, request, jsonify, redirect
from flask_cors import CORS
import asyncio
import pandas as pd
from rich.console import Console
from rich.logging import RichHandler
from rich.progress import Progress
from rich.table import Table
from logging import basicConfig, INFO
from flask_swagger_ui import get_swaggerui_blueprint
from whitenoise import WhiteNoise
import os
import psutil

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
        'app_name': "Dashboard"
    }
)

app.register_blueprint(swaggerui_blueprint)

loop = asyncio.get_event_loop()

def print_system_usage():
    # Get the current process
    p = psutil.Process()

    # Get system usage data
    mem_info = psutil.virtual_memory()
    cpu_times = psutil.cpu_times()
    process_mem_info = p.memory_info()
    process_cpu_times = p.cpu_times()

    # Create a table
    table = Table(title="System Resource Usage")

    # Add columns to the table
    table.add_column("Resource", justify="right", style="cyan", no_wrap=True)
    table.add_column("Usage", style="magenta")

    # Convert bytes to gigabytes for total/available memory and to megabytes for process memory
    bytes_in_gb = 1024 ** 3
    bytes_in_mb = 1024 ** 2

    # Add rows to the table
    table.add_row("Total Memory", f"{mem_info.total / bytes_in_gb:.2f} GB ({mem_info.percent}%)")
    table.add_row("Available Memory", f"{mem_info.available / bytes_in_gb:.2f} GB")
    table.add_row("CPU Times", f"user: {cpu_times.user}s, system: {cpu_times.system}s, idle: {cpu_times.idle}s")
    table.add_row("Process Resident Memory", f"{process_mem_info.rss / bytes_in_mb:.2f} MB")
    table.add_row("Process Virtual Memory", f"{process_mem_info.vms / bytes_in_mb:.2f} MB")
    table.add_row("Process CPU Times", f"user: {process_cpu_times.user}s, system: {process_cpu_times.system}s")

    # Return the table
    return table

def load_matrix_file(sort_by='liquidity'):
    if sort_by == 'liquidity':
        with open('data/combined_df_liquidity.json', 'r') as f:
            return pd.read_json(f, orient='split')
    elif sort_by == 'volume':
        with open('data/combined_df_volume_24h.json', 'r') as f:
            return pd.read_json(f, orient='split')
    elif sort_by == 'price':
        with open('data/combined_df_average_price.json', 'r') as f:
            return pd.read_json(f, orient='split')
    elif sort_by == 'popular':
        with open('data/combined_df_popular.json', 'r') as f:
            return pd.read_json(f, orient='split')
    else:
        return None

async def refresh_data():
    while True:
        with Progress(console=console, transient=True) as progress:
            if not os.path.exists('test_results/pool_dict.json'):
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
    table = print_system_usage()  # Get the system usage
    console.print(table)  # Print the system usage
    return jsonify({'status': 'ok'})  

@app.route('/matrix', methods=['GET'])
async def matrix():
    console.log(f"Matrix data requested by {request.remote_addr}")
    x = int(request.args.get('x')) # starting row index
    y = int(request.args.get('y')) # ending row index
    i = int(request.args.get('i')) # starting column index
    j = int(request.args.get('j')) # ending column index
    sort_by = request.args.get('sort_by', default='liquidity') # filter by asset ID

    # check if the matrix file exists
    try:
        # open the combined_df.json file
        df = load_matrix_file(sort_by=sort_by)
        if df is None:
            return jsonify({'status': 'invalid sort parameter'})
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
    sort_by = request.args.get('sort_by', default='liquidity')

    # Check if the matrix file exists
    try:
        # Open the combined_df.json file
        df = load_matrix_file(sort_by=sort_by)
        if df is None:
            return jsonify({'status': 'invalid sort parameter'})

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
