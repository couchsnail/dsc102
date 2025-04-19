import json
import ctypes
import dask 
from dask.distributed import Client
import numpy as np
import pandas as pd
import dask.dataframe as dd
import dask.array as da
import dask.bag as db
import datetime

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

# create_subset('user_reviews.csv')
def preprocess(csv_name):
    ddf = dd.read_csv(csv_name)
    prod_rated = ddf.groupby('reviewerID').asin.count()
    number_products_rated = prod_rated.compute()
    
    prod_rated = ddf[['reviewerID', 'asin']].groupby(by='reviewerID').count()
    number_products_rated = prod_rated.compute()
    reviewerID = number_products_rated.index

    # avg_rating = ddf[['reviewerID', 'overall']].groupby(by='reviewerID').mean()['overall']
    # ddf['year'] = ddf['reviewTime'].apply(lambda x: int(x[-4:]))
    # reviewing_since = ddf[['reviewerID', 'year']].groupby(by='revewerID').min()['year']
    # ddf['helpful_votes'] = ddf['helpful'].apply(lambda x: x[0])
    # ddf['total_votes'] = ddf['helpful'].apply(lambda x: x[1])
    # helpful_votes = ddf[['reviewerID', 'helpful_votes']].groupby(by='reviewerID').sum()['helpful_votes']
    # total_votes = ddf[['reviewerID', 'total_votes']].groupby(by='reviewerID').sum()['total_votes']
    
    result = pd.DataFrame({'reviewerID': reviewerID, 'number_products_rated': number_products_rated['asin']})
    # result = pd.DataFrame({'reviewerID': ddf['reivewerID'], 'number_products_rated': prod_rated, 'avg_ratings': avg_rating, 'reviewing_since': reviewing_since, 'helpful_votes': helpful_votes, 'total_votes': total_votes})

    dask_result = dd.from_pandas(result)
    dask_result.get_partition(0).to_csv('test.csv', single_file = True)

    return dask_result

preprocess('subset.csv')

    
    