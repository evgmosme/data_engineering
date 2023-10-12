import os
import sys
import shutil
import subprocess
import time
import multiprocessing as mp
from cassandra.cluster import Cluster
from tqdm import tqdm
from datetime import datetime
from threading import Thread

# Constants for database, paths, and retries
KEYSPACE = 'financial_data'
TABLE = 'test_data_bars_1m_adjsplit'
CSV_PATH = '/home/jj/anaconda3/envs/stocks/Database/1M/data'
CSV_PATH_TEMP = '/home/jj/anaconda3/envs/stocks/Database/1M/temp'
CSV_PATH_LOG = '/home/jj/anaconda3/envs/stocks/Database/1M/log'
MAX_RETRIES = 5
RETRY_PAUSE = 60  # Duration in seconds to pause between retries
SCYLLA_NODE_IP = '192.168.3.41' # Node IP address
SCYLLA_NODE_PORT = '9042' # Node port

def divide_list_into_chunks(lst, m):
    # Split a list into approximately equal-sized chunks
    n = len(lst)
    chunk_size = n // m
    for i in range(0, m - 1):
        yield lst[i * chunk_size : (i + 1) * chunk_size]
    yield lst[(m - 1) * chunk_size:]

def list_files(path):
    # Lists all '.txt' files in the provided directory path
    all_files = os.listdir(path)
    files = list(filter(lambda f: f.endswith('.txt'), all_files))
    return files
    
def clear_temp_folder():
    """Purges the contents of the designated temporary directory."""
    
    for filename in os.listdir(CSV_PATH_TEMP):
        file_path = os.path.join(CSV_PATH_TEMP, filename)
        
        try:
            # Identify and remove files or symbolic links
            if os.path.isfile(file_path) or os.path.islink(file_path):
                os.unlink(file_path)
            # Recognize and delete directories along with their inner contents
            elif os.path.isdir(file_path):
                shutil.rmtree(file_path)
        except Exception as e:
            # Capture and report any encountered deletion issues
            print(f'Failed to delete {file_path}. Reason: {e}')

def log_error(symbol, error_message):
    """
    Appends error messages related to specific financial symbols to a designated log file.

    Parameters:
        symbol (str): The identifier of the financial instrument or stock.
        error_message (str): A concise description of the encountered issue.
    """
    
    log_file = os.path.join(CSV_PATH_LOG, "error_log.txt")
    
    with open(log_file, "a") as file:
        file.write(f"Symbol: {symbol} - Error: {error_message}\n")

def execute_with_retry(command, symbol):
    """
    Tries executing a given command and retries in case of specific exceptions.
    
    Parameters:
        command (list): The command to be executed as a list of strings.
        symbol (str): The financial symbol for which the command is being executed.
        
    Returns:
        subprocess.CompletedProcess: The result of the executed command.
    """
    
    # Initialize the retry count
    retries = 0
    
    # Keep trying until reaching the maximum allowed retries
    while retries < MAX_RETRIES:
        try:
            # Run the command and capture its output
            result = subprocess.run(command, capture_output=True, text=True)
            
            # If the error stream does not indicate a "WriteTimeout", return the result
            if "WriteTimeout" not in result.stderr:
                return result
            else:
                # Otherwise, raise a custom exception to trigger a retry
                raise Exception("WriteTimeout encountered")
        except Exception as e:
            # If any exception occurs, increment the retry count
            retries += 1
            
            # Log the error for the specific symbol and the exception encountered
            log_error(symbol, str(e))
            
            # Pause the execution for a specified duration before the next retry
            time.sleep(RETRY_PAUSE)
    
    # If the function reaches here, it means max retries were attempted and all failed
    print(f"Max retries reached for symbol {symbol}. Moving on to the next file...")

def create_keyspace_and_table():
    """
    Establishes a connection to Scylla DB, then initializes a keyspace and a 
    corresponding table if they aren't present.

    This function serves as the initial setup phase in the data processing 
    pipeline, ensuring the database is ready for incoming data. The table is 
    designed to store financial data like stock prices, and its schema 
    optimizes query performance for time-based lookups and data compactness.
    """
    # Connect to the Scylla cluster
    cluster = Cluster([SCYLLA_NODE_IP], port=SCYLLA_NODE_PORT)
    session = cluster.connect()
    
    # Create the keyspace if it doesn't exist
    session.execute(
        f"CREATE KEYSPACE IF NOT EXISTS {KEYSPACE} WITH replication = "
        f"{{'class': 'SimpleStrategy', 'replication_factor' : 1}};"
    )
    session.set_keyspace(KEYSPACE)
    
    # Define and create the table if it's not already there
    session.execute(f"""
        CREATE TABLE IF NOT EXISTS {KEYSPACE}.{TABLE} (
            symbol text,
            bucket text,
            timestamp text,
            open float,
            high float,
            low float,
            close float,
            volume float,
            PRIMARY KEY ((symbol, bucket), timestamp)
        ) WITH compaction = {{
            'class': 'TimeWindowCompactionStrategy',
            'compaction_window_unit': 'DAYS',
            'compaction_window_size': 30
        }};
    """)
    
    # Disconnect from the cluster and end the session
    session.shutdown()
    cluster.shutdown()

def get_bucket_from_timestamp(timestamp):
    # Extracts year-month information from a timestamp
    dt_obj = datetime.strptime(timestamp, '%Y-%m-%d %H:%M:%S')
    return f"{dt_obj.year}-{dt_obj.month:02}"

def preprocess_and_load(symbol):
    """
    Preprocesses and then loads data for a specific financial symbol 
    into the database.
    
    Parameters:
        symbol (str): The financial symbol to be processed.
        
    Returns:
        int: The number of lines (data entries) processed for the given symbol.
    """
    # Construct the paths for the input and temporary files
    input_file = os.path.join(CSV_PATH, f"{symbol}_full_1min_adjsplit.txt")
    temp_file = os.path.join(CSV_PATH_TEMP, f"{symbol}_temp.txt")
    
    line_count = 0
    
    with open(input_file, 'r') as source, open(temp_file, 'w') as target:
        for line in source:
            line_count += 1
            data = line.strip().split(',')
            timestamp = data[0]
            bucket = get_bucket_from_timestamp(timestamp)
            target.write(f"{symbol},{bucket},{timestamp},{','.join(data[1:])}\n")
    
    copy_cmd = ['./bin/cqlsh', '-e', f"COPY {KEYSPACE}.{TABLE} "
                f"(symbol, bucket, timestamp, open, high, low, close, volume) "
                f"FROM '{temp_file}' WITH DELIMITER=',' AND HEADER=FALSE"]
    result = execute_with_retry(copy_cmd, symbol)
    
    os.remove(temp_file)
    return line_count


def process_chunk(chunk):
    """
    Process a subset (chunk) of financial symbol data files.
    The function will preprocess and load data for each symbol in the chunk.
    """
    total_rows = 0
    for symbol in chunk:
        # Preprocess and load data for the current symbol and count the number of rows.
        total_rows += preprocess_and_load(symbol)
        # Indicate that processing for one symbol has finished.
        progress_queue.put(1)
    return total_rows

def monitor_progress():
    """
    Monitors and updates the progress of the data loading process.
    It counts processed symbols and updates the progress bar.
    """
    processed = 0
    while processed < len(files_list):
        # Wait until a symbol has been processed.
        _ = progress_queue.get()
        # Update the progress bar.
        pbar.update(1)
        processed += 1

if __name__ == "__main__":
    # Main script execution starts.
    
    # Clear any existing temporary files.
    clear_temp_folder()
    # Create the required keyspace and table in ScyllaDB.
    create_keyspace_and_table()
    # List all the financial data files.
    files = list_files(CSV_PATH)
    # Extract symbol names from file names.
    files_list = [element.replace('_full_1min_adjsplit.txt', '') for element in files]
    # Sort the file list for consistent processing order.
    files_list.sort()
    # Initialize a multiprocessing queue to track progress.
    progress_queue = mp.Queue()
    processes = 5
    # (Optional) Limit to the first 10 files for processing.
    files_list = files_list[0:20]
    # Divide the file list into chunks for parallel processing.
    chunks = divide_list_into_chunks(files_list, processes)
    # Initialize a progress bar using tqdm.
    pbar = tqdm(total=len(files_list))
    # Start a separate thread to monitor processing progress.
    t = Thread(target=monitor_progress)
    t.start()
    with mp.Pool(processes=processes) as pool:
        results = []
        start_time = time.time()
        # Process each chunk in parallel using multiprocessing.
        for chunk in chunks:
            results.append(pool.apply_async(process_chunk, (chunk,)))
        # Wait for all processes to finish and get their results.
        results = [res.get() for res in results]
        end_time = time.time()
        # Calculate total processing time.
        total_time = end_time - start_time
        # Aggregate the number of rows processed across all chunks.
        total_rows_aggregated = sum(results)
        print(f"\nTotal rows inserted across processes: {total_rows_aggregated:,.0f} rows")
        print(f"Average insertion rate: {total_rows_aggregated / total_time:,.0f} rows/sec")
    # Ensure the progress monitoring thread completes.
    t.join()