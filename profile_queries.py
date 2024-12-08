'''
Profiles all the queries and save results as .csv to SAVE_DIR
'''

from queries import QUERY_DICT, QUERY_DICT_SHORT, QUERY_DICT_LONG
import sqlite3
from sqlite3 import Connection
import os
import sys
import psutil
from time import perf_counter
from multiprocessing import Process, Queue
import multiprocessing as mp
import gc
import csv

# Constants
DB_FILE = 'file:data/tpch.db?mode=ro' # read only mode
SAVE_DIR = './save/profile/'
LOG_COLS = ['query','run','elapsed_time','cpu','ram', 'swap'] # TODO add disk, io blocking as well
MAX_RUNS = 1
LOG_FREQ = 0.1 # in seconds 

def parse_args():
    # parse args
    query_dict = QUERY_DICT
    dict_type = 'All'
    if len(sys.argv) == 2:
        arg = sys.argv[1]
        if arg == '-s':
            query_dict = QUERY_DICT_SHORT
            dict_type = 'Short'
        elif arg == '-l':
            query_dict = QUERY_DICT_LONG
            dict_type = 'Long'
    return query_dict, dict_type   

def execute_query(query:str, con:Connection, q: Queue):
    cursor = con.execute(query) # execute query
    result = cursor.fetchall() # fetch all rows but discard result
    q.put(True) # signal log_metrics that query is done
    # free memory for accurate memory usage
    del result
    gc.collect()

def get_stats_row(query_process, elapsed_time, query_num, run_num):
    # 'query,run,elapsed_time,cpu,ram,swap'
    cpu_usage = query_process.cpu_percent()
    # convert to MB
    mem_usage_ram = query_process.memory_info().rss*1e-6
    mem_usage_swap = query_process.memory_info().vms*1e-6 - mem_usage_ram
    return [query_num, run_num, elapsed_time, cpu_usage, mem_usage_ram, mem_usage_swap]

# records resource usage/metrics for a single query
# process_id, csv_writer, current query, iteration number, message queue for multiprocessing
def log_metrics(pid, csv_writer, query_num, run_num, q: Queue):
    # start timer
    start_time = perf_counter()
    import time
    time.sleep
    import time
    elapsed_time = 0
    query_process = psutil.Process(pid)
    while True:
        # get current stats
        row = get_stats_row(query_process, elapsed_time, query_num, run_num)
        csv_writer.writerow(row) # log to csv
        # check pipe
        if not q.empty():
            if q.get() == True:
                break
        time.sleep(LOG_FREQ)
        elapsed_time += LOG_FREQ
        elapsed_time = round(elapsed_time,1)
    # print(f'{query_num}, {run_num}: {perf_counter()-start_time}')
    
def reset_connection(con):
    con.close()
    con = sqlite3.connect(DB_FILE, uri=True)
    return con
 
if __name__ == "__main__":
    query_dict, dict_type = parse_args()
    print(f"Profiling {dict_type} Queries..")
    # create save dir
    if not os.path.exists(SAVE_DIR):
        os.mkdir(SAVE_DIR)
    filename = SAVE_DIR+f'profile_results.csv'  
    # connect to tpc-h db
    con = sqlite3.connect(DB_FILE, uri=True)
    q = Queue()
    # TODO add delay between each run
    # execute each query in order
    try:
        with open(filename, mode='w') as csv_file:
            csv_writer = csv.writer(csv_file)
            csv_writer.writerow(LOG_COLS) # log columns names
            for query_num in query_dict: # need another loop for the number of runs
                for run_num in range(1, MAX_RUNS+1):
                    query = query_dict.get(query_num)
                    p = Process(target=execute_query, args=(query,con,q,))
                    p.start()
                    log_metrics(p.pid, csv_writer, query_num, run_num, q)
                    p.join()
                    p.close()
                    con = reset_connection(con)
            con.close()
    except KeyboardInterrupt:
        print('Exiting..')
        con.interrupt()
        con.close()
        sys.exit()

    # close the connection
    print("Done..")
    