import json
import multiprocessing
import os
import logging
import argparse
import psycopg2

def db_handles(count, pg_host, pg_port, pg_user, pg_password, pg_database):
    """
    Create multiple database connections.
    """
    handles = []
    for i in range(count):
        handles.append(
            psycopg2.connect(
                f"host={pg_host} port={pg_port} user={pg_user} password={pg_password} dbname={pg_database}",
            ))
    return handles

def write_to_db(batch, db_handle):
    """
    Write a batch of data to the database.
    """
    db_handle.cursor().executemany("INSERT INTO users (first_name, last_name, email) VALUES (%(first_name)s, %(last_name)s, %(email)s)", batch)
    db_handle.commit()
    pass

def worker_process(queue, worker_id, batch_size, logger, db_handle):
    """
    Worker function to read from the queue and write results in batches.
    """
    logger.info(f"Worker {worker_id} started on PID {os.getpid()}")
    batch = []
    while True:
        try:
            # Get an item from the queue with a timeout
            item = queue.get(timeout=1)
            if item is None:  # Sentinel value to signal termination
                logger.info(f"Worker {worker_id} received termination signal.")
                break
            batch.append(item)
            if len(batch) >= batch_size:
                logger.info(f"Worker {worker_id} (PID {os.getpid()}) processing batch of {len(batch)} items:")
                write_to_db(batch, db_handle)
                batch = []
        except Exception as e:
            # If queue is empty for a while, process any remaining items in the batch
            if batch:
                logger.info(f"Worker {worker_id} (PID {os.getpid()}) processing remaining batch of {len(batch)} items due to queue empty:")
                write_to_db(batch, db_handle)
                batch = []
            # Allow workers to exit if the queue is empty for a prolonged period
            # or if the main process has signaled termination.
            # In a real-world scenario, you might have a more robust termination strategy.
            pass
    # Process any remaining items in the batch before exiting
    if batch:
        logger.info(f"Worker {worker_id} (PID {os.getpid()}) processing final batch of {len(batch)} items:")
        write_to_db(batch, db_handle)
    logger.info(f"Worker {worker_id} finished.")

def main(json_file_path, num_workers, batch_size, pg_host, pg_port, pg_user, pg_password, pg_database):
    """
    Main function to load JSON, populate queue, and manage workers.
    """
    manager = multiprocessing.Manager()
    data_queue = manager.Queue()
    logger = logging.getLogger(__name__)

    # Load JSON data
    try:
        with open(json_file_path, 'r') as f:
            data = json.load(f)
        logger.info(f"Successfully loaded {len(data)} entries from {json_file_path}")
    except FileNotFoundError:
        logger.error(f"Error: JSON file not found at {json_file_path}")
        return
    except json.JSONDecodeError:
        logger.error(f"Error: Could not decode JSON from {json_file_path}. Please check file format.")
        return

    # Start worker processes
    handles = db_handles(num_workers, pg_host, pg_port, pg_user, pg_password, pg_database)
    workers = []
    for i in range(num_workers):
        p = multiprocessing.Process(target=worker_process, args=(data_queue, i + 1, batch_size, logger, handles[i]))
        workers.append(p)
        p.start()

    # Populate the queue
    for entry in data:
        data_queue.put(entry)

    # Add sentinel values to the queue to signal workers to terminate
    for _ in range(num_workers):
        data_queue.put(None)

    # Wait for all worker processes to finish
    for p in workers:
        p.join()

    logger.info("All workers have finished. Main process exiting.")

if __name__ == "__main__":
    
    parser = argparse.ArgumentParser(prog="data_loader", description="Load data from a JSON file and process it in parallel.")
    parser.add_argument("--data-file", type=str, help="Path to the JSON file to load", default="data.json")
    parser.add_argument("--num-workers", type=int, help="Number of worker processes to use", default=10)
    parser.add_argument("--batch-size", type=int, help="Number of items to process in each batch", default=200)
    parser.add_argument("--pg-host", type=str, help="Postgres host", default="localhost")
    parser.add_argument("--pg-port", type=int, help="Postgres port", default=5432)
    parser.add_argument("--pg-user", type=str, help="Postgres user", default="postgres")
    parser.add_argument("--pg-password", type=str, help="Postgres password", default="FastLoad3RR")
    parser.add_argument("--pg-database", type=str, help="Postgres database", default="fastload")

    args = parser.parse_args()

    main(
        args.data_file, args.num_workers, args.batch_size,
        args.pg_host, args.pg_port, args.pg_user, args.pg_password, args.pg_database,
    )
