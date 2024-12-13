'''
Preprocess data collected from profile_queries.py
'''
import pandas as pd
LOAD_LOCATION = './save/profile/profile_results.csv' # this is the correct one
LOAD_LOCATION = './save/profile/profile_results_full.csv'
SAVE_LOCATION = './save/processed.csv'
if __name__ == "__main__":
    print("Starting preprocessing..")
    raw_df = pd.read_csv(LOAD_LOCATION)
    raw_df = raw_df.groupby(['query', 'run']).agg(
        elapsed_time = ('elapsed_time', 'max'),
        cpu_avg = ('cpu', 'mean'),
        cpu_max = ('cpu', 'max'),
        ram_avg = ('ram', 'mean'),
        swap_avg = ('swap', 'mean'),
        # disk_read = ('disk_read', 'max'), # cumulative
        # disk_write = ('disk_write', 'max'), # cumulative
    ).reset_index()
    data_df = raw_df.groupby('query').agg('mean').reset_index()
    data_df = data_df.drop('run', axis=1)
    data_df.to_csv(SAVE_LOCATION, index=False)
    print(data_df)
    