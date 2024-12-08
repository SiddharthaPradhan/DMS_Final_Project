'''
Runs all the queries and save results as .csv to SAVE_DIR
'''

from queries import QUERY_DICT, QUERY_DICT_SHORT, QUERY_DICT_LONG
import sqlite3
import csv
import os
import sys
from time import perf_counter

# Constants
DB_FILE = 'file:data/tpch.db?mode=ro'
SAVE_DIR = './save/results/'


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

if __name__ == "__main__":
    query_dict, dict_type = parse_args()
    print(f"Running {dict_type} Queries..")
    # create save dir
    if not os.path.exists(SAVE_DIR):
        os.mkdir(SAVE_DIR)
    # connect to tpc-h db
    con = sqlite3.connect(DB_FILE, uri=True)
    # get table names
    table_names = con.execute("SELECT name FROM sqlite_master;")
    print(f"Table names: {table_names.fetchall()}")
    # execute each query in order
    try:
        for query_num in query_dict:
            query = query_dict.get(query_num)
            filename = SAVE_DIR+f'query_{query_num}.csv'                                                                                                                                          
            start_time = perf_counter()
            query_cursor = con.execute(query)
            with open(filename, mode='w') as csv_file:
                csv_writer = csv.writer(csv_file)
                col_names = [desc[0] for desc in query_cursor.description]
                csv_writer.writerow(col_names)
                # query_cursor.arraysize = 10000
                query_result = query_cursor.fetchall()
                end_time = perf_counter()
                csv_writer.writerows(query_result)
                print(f'Query #{query_num}, {round(end_time-start_time,2)}')
        con.close()
    except KeyboardInterrupt:
        print('Exiting..')
        con.interrupt()
        con.close()
        sys.exit()

    # close the connection
    print("Done..")
    