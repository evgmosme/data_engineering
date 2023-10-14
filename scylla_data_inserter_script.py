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
CSV_PATH_TEMP = '/home/jj/anaconda3/envs/stocks//Database/1M/temp'
CSV_PATH_LOG = '/home/jj/anaconda3/envs/stocks//Database/1M/log'
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
            # Identify/remove files or symbolic links
            if os.path.isfile(file_path) or os.path.islink(file_path):
                os.unlink(file_path)
            # Recognize/delete directories with their contents
            elif os.path.isdir(file_path):
                shutil.rmtree(file_path)
        except Exception as e:
            # Report any deletion issues
            print(f'Failed to delete {file_path}. Reason: {e}')

def log_error(symbol, error_message):
    """
    Logs error messages for specific financial symbols to a log file.

    Parameters:
        symbol (str): The financial instrument identifier.
        error_message (str): Description of the issue.
    """
    
    log_file = os.path.join(CSV_PATH_LOG, "error_log.txt")
    
    with open(log_file, "a") as file:
        file.write(f"Symbol: {symbol} - Error: {error_message}\n")

def execute_with_retry(command, symbol):
    """
    Executes a command and retries on specific exceptions.
    
    Parameters:
        command (list): The command as a list of strings.
        symbol (str): The financial symbol for the command.
        
    Returns:
        subprocess.CompletedProcess: The executed command result.
    """
    
    # Initialize retry count
    retries = 0
    
    # Keep trying until max retries
    while retries < MAX_RETRIES:
        try:
            # Run the command, capture output
            result = subprocess.run(command, capture_output=True, text=True)
            
            # Return result if no "WriteTimeout" in error
            if "WriteTimeout" not in result.stderr:
                return result
            else:
                # Else, trigger a retry
                raise Exception("WriteTimeout encountered")
        except Exception as e:
            # Increment retry count if exception
            retries += 1
            
            # Log the error for the symbol
            log_error(symbol, str(e))
            
            # Pause before next retry
            time.sleep(RETRY_PAUSE)
    
    # If here, max retries attempted and failed
    print(f"Max retries reached for {symbol}. Moving on...")

def clear_temp_folder():
    """Purges the contents of the designated temporary directory."""
    
    for filename in os.listdir(CSV_PATH_TEMP):
        file_path = os.path.join(CSV_PATH_TEMP, filename)
        
        try:
            # Identify/remove files or symbolic links
            if os.path.isfile(file_path) or os.path.islink(file_path):
                os.unlink(file_path)
            # Recognize/delete directories with their contents
            elif os.path.isdir(file_path):
                shutil.rmtree(file_path)
        except Exception as e:
            # Report any deletion issues
            print(f'Failed to delete {file_path}. Reason: {e}')

def log_error(symbol, error_message):
    """
    Logs error messages for specific financial symbols to a log file.

    Parameters:
        symbol (str): The financial instrument identifier.
        error_message (str): Description of the issue.
    """
    
    log_file = os.path.join(CSV_PATH_LOG, "error_log.txt")
    
    with open(log_file, "a") as file:
        file.write(f"Symbol: {symbol} - Error: {error_message}\n")

def execute_with_retry(command, symbol):
    """
    Executes a command and retries on specific exceptions.
    
    Parameters:
        command (list): The command as a list of strings.
        symbol (str): The financial symbol for the command.
        
    Returns:
        subprocess.CompletedProcess: The executed command result.
    """
    
    # Initialize retry count
    retries = 0
    
    # Keep trying until max retries
    while retries < MAX_RETRIES:
        try:
            # Run the command, capture output
            result = subprocess.run(command, capture_output=True, text=True)
            
            # Return result if no "WriteTimeout" in error
            if "WriteTimeout" not in result.stderr:
                return result
            else:
                # Else, trigger a retry
                raise Exception("WriteTimeout encountered")
        except Exception as e:
            # Increment retry count if exception
            retries += 1
            
            # Log the error for the symbol
            log_error(symbol, str(e))
            
            # Pause before next retry
            time.sleep(RETRY_PAUSE)
    
    # If here, max retries attempted and failed
    print(f"Max retries reached for {symbol}. Moving on...")

def create_keyspace_and_table():
    """
    Establishes a connection to Scylla DB and initializes a keyspace and a 
    table if they aren't present. This function serves as the initial setup 
    phase, ensuring the database is ready for incoming data. The table stores 
    financial data like stock prices optimizing query performance.
      
    Steps:
    1. Connect to the Scylla cluster.
    2. Create keyspace if absent. It uses a simple replication strategy with 
       a factor of 1, implying data on one node. This may not be optimal for 
       production systems where redundancy is vital.
    3. Set active keyspace for the session.
    4. Design and initialize the table. Schema has:
       - Composite primary key for efficient time-based queries.
       - 'TimeWindowCompactionStrategy' for time series data in 30-day windows.
    5. Terminate the session and connection after operations.
    """
    
    cluster = Cluster([SCYLLA_NODE_IP], port=SCYLLA_NODE_PORT)
    session = cluster.connect()
    
    session.execute(f"CREATE KEYSPACE IF NOT EXISTS {KEYSPACE} "
                    f"WITH replication = {{'class': 'SimpleStrategy', "
                    f"'replication_factor' : 1}};")
    session.set_keyspace(KEYSPACE)
    
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
    
    # The decision to not use the default_time_to_live property in the table 
    # definition was made to ensure continuous access to historical financial data.
    
    session.shutdown()
    cluster.shutdown()

def get_bucket_from_timestamp(timestamp):
    dt_obj = datetime.strptime(timestamp, '%Y-%m-%d %H:%M:%S')
    return f"{dt_obj.year}-{dt_obj.month:02}"

def preprocess_and_load(symbol):
    """
    Preprocesses and loads data for a symbol into the database.
    
    Parameters:
        symbol (str): The symbol to be processed.
        
    Returns:
        int: The number of entries processed for the symbol.
    """
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
    
    start_time = time.time()
    copy_cmd = ['/home/jj/anaconda3/envs/stocks/bin/cqlsh', '-e',
                f"COPY {KEYSPACE}.{TABLE} FROM '{temp_file}' "
                f"WITH DELIMITER=',' AND HEADER=FALSE"]
    
    result = execute_with_retry(copy_cmd, symbol)
    os.remove(temp_file)
    end_time = time.time()
    time_taken = end_time - start_time
    
    return line_count, time_taken

def process_chunk(chunk):
    """
    Process a subset (chunk) of financial symbol data files.
    Preprocesses and loads data for each symbol in the chunk.
    """
    total_rows = 0
    total_time = 0
    chunk_start_time = time.time()  # Mark the start time for the chunk processing

    for symbol in chunk:
        rows, time_taken = preprocess_and_load(symbol)
        total_rows += rows
        total_time += time_taken
        progress_queue.put(1)

    chunk_end_time = time.time()  # Mark the end time for the chunk processing
    chunk_total_time = chunk_end_time - chunk_start_time  # Calculate total time taken for the chunk

    return total_rows, total_time, round(chunk_total_time, 2)  # Return the total time taken for the chunk

def monitor_progress():
    """
    Monitors the progress of the data loading process.
    Counts processed symbols and updates the progress bar.
    """
    processed = 0
    while processed < len(files_list):
        _ = progress_queue.get()
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
    files_list = files_list[0:100]
    # Divide the file list into chunks for parallel processing.
    chunks = divide_list_into_chunks(files_list, processes)
    # Initialize a progress bar using tqdm.
    pbar = tqdm(total=len(files_list))
    # Start a separate thread to monitor processing progress.
    t = Thread(target=monitor_progress)
    t.start()
    with mp.Pool(processes=processes) as pool:
        global_start_time = time.time()
        results = []
        for chunk in chunks:
            results.append(pool.apply_async(process_chunk, (chunk,)))
        results = [res.get() for res in results]
        global_end_time = time.time()
        
        # Aggregate the number of rows processed and time taken across all chunks
        total_rows_aggregated = sum([r[0] for r in results])
        total_time_aggregated = sum([r[1] for r in results])
        process_times = [r[2] for r in results]  # List of total execution times for each process

        print(f"\nTotal rows inserted across processes: {total_rows_aggregated:,.0f} rows")
        print(f"\nTotal time for all processes: {global_end_time - global_start_time:.2f} seconds")
        print(f"Average insertion rate for all processes combined: {total_rows_aggregated / (global_end_time - global_start_time):,.0f} rows/sec")
        print(f"Average individual insertion rate: {total_rows_aggregated / total_time_aggregated:,.0f} rows/sec")
        print(f"\nIndividual process times: {process_times}")

    t.join()
