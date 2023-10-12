from datetime import datetime, timedelta
import pandas as pd
from cassandra.cluster import Cluster

def fetch_minute_data(symbol, start_time, end_time, KEYSPACE, TABLE, trading_hours_only, rounding):
    """
    Fetch minute-level financial data for a specified symbol and timeframe from a ScyllaDB 
    (or Cassandra) database.
    
    Parameters:
    - symbol (str): The ticker symbol to retrieve data for.
    - start_time (str): The start of the desired timeframe in the format 'YYYY-MM-DD HH:MM:SS'.
    - end_time (str): The end of the desired timeframe in the format 'YYYY-MM-DD HH:MM:SS'.
    - KEYSPACE (str): The ScyllaDB/Cassandra keyspace to connect to.
    - TABLE (str): The table within the keyspace to query from.
    
    Returns:
    - dataframe (pd.DataFrame): A pandas DataFrame containing the queried financial data.
    """
    
    # Helper function to generate monthly bucket strings (YYYY-MM) for data partitioning
    def generate_monthly_buckets(start_time, end_time):
        start_date = datetime.strptime(start_time, '%Y-%m-%d %H:%M:%S')
        end_date = datetime.strptime(end_time, '%Y-%m-%d %H:%M:%S')
        current_date = start_date
        buckets = []
        
        # Loop through each month in the timeframe to generate bucket strings
        while current_date <= end_date:
            buckets.append(f"{current_date.year}-{current_date.month:02}")
            
            # Advance to the next month
            current_date = current_date + timedelta(days=31)
            current_date = datetime(current_date.year, current_date.month, 1)
        return buckets

    # Generate the necessary bucket strings for the desired timeframe
    buckets = generate_monthly_buckets(start_time, end_time)
    
    # List to store dataframes from each bucket
    dfs = []
    
    # Establish a connection to the ScyllaDB/Cassandra cluster and the specific keyspace
    cluster = Cluster([SCYLLA_NODE_IP], port=SCYLLA_NODE_PORT)
    session = cluster.connect(KEYSPACE)
    
    # Prepare the query to fetch data
    query = f"""SELECT timestamp, open, high, low, close, volume 
            FROM {TABLE} WHERE symbol = ? AND bucket = ? AND 
            \"timestamp\" >= ? AND \"timestamp\" <= ?
            """
    prepared = session.prepare(query)
    futures = []

    # For each bucket, asynchronously fetch data for the symbol within the given timeframe
    for bucket in buckets:
        future = session.execute_async(prepared, (symbol, bucket, start_time, end_time))
        futures.append(future)

    # As queries complete, convert results to pandas dataframes
    for future in futures:
        rows = future.result()
        dfs.append(pd.DataFrame(list(rows)))

    # Shutdown the session and cluster connection after fetching data
    session.shutdown()
    cluster.shutdown()
    
    # Combine all the dataframes into one dataframe
    dataframe = pd.concat(dfs, ignore_index=True)
    
    # Set timestamp as the dataframe index and remove the timestamp column
    dataframe.index = dataframe['timestamp'].astype('datetime64[ns]') # type: ignore
    dataframe.drop('timestamp', inplace=True, axis=1)

    if trading_hours_only:
        dataframe = dataframe.between_time('09:30:00', '16:00:00')
    if rounding:
        dataframe = dataframe.round(4)
    return dataframe

"""
# Uncomment to test

KEYSPACE = 'financial_data'
TABLE = 'test_data_bars_1m_adjsplit'
SCYLLA_NODE_IP = '192.168.3.41' # Node IP address
SCYLLA_NODE_PORT = '9042' # Node port
symbol = 'A'
KEYSPACE = 'financial_data'
TABLE = 'test_data_bars_1m_adjsplit'
ohlc_df = fetch_minute_data(symbol, '2005-01-01 00:00:00', '2023-10-05 23:59:00', KEYSPACE, TABLE, trading_hours_only=False, rounding=True)
print(ohlc_df.info())
"""
