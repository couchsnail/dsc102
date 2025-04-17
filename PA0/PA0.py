import json
import ctypes
import dask 
from dask.distributed import Client
import numpy as np
import pandas as pd
import dask.dataframe as dd
import dask.array as da
import dask.bag as db

def trim_memory() -> int:
    libc = ctypes.CDLL("libc.so.6")
    return libc.malloc_trim(0)

def PA0(path_to_user_reviews_csv):
    client = Client()
    # Helps fix any memory leaks.
    client.run(trim_memory)
    client = client.restart()
    
    
    #submit = <YOUR_USERS_DATAFRAME>.describe().compute().round(2)    
    with open('results_PA0.json', 'w') as outfile: 
        json.dump(json.loads(submit.to_json()), outfile)

# drop reviewername, reviewertext, summary
def create_subset(csv_name):
    ddf = dd.read_csv(csv_name)
    #print(ddf.head(10))
    #ddf = dd.from_pandas(df, partitions = 10)
    #print(ddf.get_partition(0).head(10))
    ddf = ddf.repartition(npartitions = 5000)
    ddf.get_partition(0).to_csv('subset.csv', single_file = True)
    #print(ddf._meta)
    #print(ddf.get_partition(0))

create_subset('user_reviews.csv')