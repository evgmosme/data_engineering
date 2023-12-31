# Overview:

This utility script provides a streamlined way to process and load large volumes of financial data into a ScyllaDB database. It primarily targets minute-resolution, adjusted-split financial bars but can be adapted for different data structures. In addition to its core functionalities, it offers the `fetch_minute_data` function to retrieve specific minute-level datasets from the database. The script uses efficient techniques such as multiprocessing, batching, and asynchronous fetching to enhance speed and consistency.

---

## Features:

- **Parallel Processing:** Utilizes Python's multiprocessing library to parallelize data processing, making full use of available CPU cores.
- **Automatic Retries:** Implements a retry mechanism for data insertions, ensuring data consistency even in the face of transient database errors.
- **Progress Monitoring:** Offers a real-time progress bar to keep track of the data loading process.
- **Error Logging:** Captures and logs errors encountered during data processing and loading for easier troubleshooting.
- **Data Preprocessing:** Preprocesses raw data files to extract relevant information and organize it into a more database-friendly format.
- **Custom Date Ranges:** Users can set specific start and end times using `fetch_minute_data`.
- **Trading Hours Filter:** Provides an option to retrieve data only from standard trading hours.
- **Month-based Bucketing System:** Organizes data for efficient retrieval.
- **Asynchronous Data Fetching:** Enables parallel data retrieval across different time intervals.

---

## Setup and Usage:

### Prerequisites:
- Linux
- Python 3.11.4
- ScyllaDB
- Python packages: `os`, `shutil`, `subprocess`, `multiprocessing`, `cassandra-driver`, `tqdm`, `datetime`

### Configuration:
1. Adjust the constants at the beginning of the script, such as `KEYSPACE`, `TABLE`, and `CSV_PATH`, to match your setup.
2. Ensure the ScyllaDB cluster is up and accessible from the script execution environment.

### Execution:
- Simply run the cells sequentially. By default, the script processes a specific number of .txt files located in the directory specified by `CSV_PATH`. Adjust as needed for processing different file counts.

---

## Function Descriptions:

- `divide_list_into_chunks()`: Splits a list into approximately equal-sized chunks for parallel processing.
- `list_files()`: Lists all '.txt' files in the specified directory.
- `create_keyspace_and_table()`: Establishes a connection to ScyllaDB and creates a keyspace and table if not already present.
- `clear_temp_folder()`: Clears any existing temporary files before processing starts.
- `log_error()`: Logs errors encountered during data processing.
- `execute_with_retry()`: Retries data insertion in case of transient database errors.
- `get_bucket_from_timestamp()`: Extracts year-month info from timestamps, useful for bucketing data.
- `preprocess_and_load()`: Processes each financial data file and loads the data into ScyllaDB.
- `process_chunk()`: Handles a chunk of data files, invoking preprocessing and loading for each.
- `monitor_progress()`: A thread-safe function that updates the progress bar as data files are processed.
- `fetch_minute_data`: Retrieves minute-level financial data from the database based on specified criteria.

---

### XFS Filesystem:

ScyllaDB, being a high-performance NoSQL database, has specific requirements when it comes to disk I/O. It's crucial to choose the right filesystem to achieve optimal performance. Here's why XFS is recommended for ScyllaDB:

- **High Throughput:** XFS is designed for high parallelism, which is in line with ScyllaDB's architecture. This ensures the database can manage multiple read/write operations efficiently.

- **Delayed Allocation:** XFS improves disk performance with its delayed allocation feature, helping in reducing fragmentation.

- **Scalability:** XFS supports filesystems up to 8 exabytes, which makes it suitable for ScyllaDB deployments that might need to handle large volumes of data.

- **Journaling Capability:** XFS has a robust journaling mechanism that ensures data integrity even in case of system crashes. This feature is crucial for databases like ScyllaDB, where data integrity is paramount.

- **Optimized for Large Files:** ScyllaDB often deals with large SSTable files. XFS's design is optimized for handling large files, ensuring quick reads and writes.

- **Built-in Utilities:** XFS comes with a set of administrative utilities like `xfs_repair` (for repairing the filesystem) and `xfs_growfs` (for resizing the filesystem), which can be beneficial for database management.

When deploying ScyllaDB, using the XFS filesystem ensures you are aligning with ScyllaDB’s design principles and maximizing the database's performance capabilities.

**Note:** Always ensure you have backups of your data and that you've set up appropriate monitoring and alerting for your ScyllaDB cluster. Regularly check the error log for any issues during data loading.
