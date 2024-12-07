'''
Runs all the queries and save results as .csv to SAVE_DIR
'''

from queries import QUERY_LIST
import sqlite3
import pandas as pd
import os
from tqdm import tqdm

# Constants
DB_FILE = 'file:data/tpch.db?mode=ro'
SAVE_DIR = './save/'


if __name__ == "__main__":
    # create save dir
    if not os.path.exists(SAVE_DIR):
        os.mkdir(SAVE_DIR)
    # connect to tpc-h db
    con = sqlite3.connect(DB_FILE, uri=True)
    # get table names
    table_names = con.execute("SELECT name FROM sqlite_master;")
    print(f"Table names: {table_names.fetchall()}")
    print("Running Queries..")
    # execute each query in order
    for i, query in enumerate(tqdm(QUERY_LIST)):
        result_df = pd.read_sql_query(query, con)
        result_df.to_csv(SAVE_DIR+f'query_{i+1}.csv', index=False)
    # close the connection
    con.close()
    print("Done..")